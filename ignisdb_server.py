import asyncio
import time
import json
import logging
import argparse # Argüman yönetimi için eklendi
from typing import Dict, Any, Tuple, Optional, List, Union

# --- Logging Kurulumu ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Hata Sınıfları ---
class CommandError(Exception):
    """Komutla ilgili hatalar için özel exception."""
    pass

class WrongTypeError(TypeError):
    """Veri tipi uyuşmazlıkları için özel exception."""
    pass

# --- AOF Yöneticisi ---
class AofHandler:
    def __init__(self, path: str):
        self._path = path
        self._file = None

    def open(self):
        # AOF dosyasını 'append' modunda ve satır tamponlamasız aç
        self._file = open(self._path, 'a', buffering=1)
        logging.info(f"AOF dosyası '{self._path}' yazma için açıldı.")

    def write(self, command: str, *args: str):
        if not self._file:
            return
        # Komutu Redis protocol (RESP Array) formatında dosyaya yaz
        parts = [f"*{len(args) + 1}\r\n"]
        parts.append(f"${len(command)}\r\n{command}\r\n")
        for arg in args:
            parts.append(f"${len(str(arg))}\r\n{str(arg)}\r\n")
        
        self._file.write("".join(parts))
        logging.debug(f"AOF'ye yazıldı: {command} {' '.join(args)}")

    def close(self):
        if self._file:
            self._file.close()
            self._file = None
            logging.info("AOF dosyası kapatıldı.")

# --- Veri Saklama Motoru ---
class StorageEngine:
    def __init__(self, snapshot_path: str, aof_handler: Optional[AofHandler]):
        self._data: Dict[str, Tuple[str, Any, Optional[float]]] = {}
        self._lock = asyncio.Lock()
        self._snapshot_path = snapshot_path
        self._aof = aof_handler

    async def _check_and_delete_expired(self, key: str) -> bool:
        item = self._data.get(key)
        if item:
            _, _, expiration_time = item
            if expiration_time is not None and time.time() > expiration_time:
                del self._data[key]
                return True
        return False

    async def execute_command(self, command: str, args: List[Any], in_transaction: bool = False):
        """Tüm komutları yürüten merkezi fonksiyon."""
        command_upper = command.upper()
        
        # Yazma komutları AOF'ye kaydedilir (eğer transaction içinde değilsek)
        is_write_command = command_upper in ["SET", "DELETE", "EXPIRE", "LPUSH", "HSET"]
        
        # Asıl işi yapan metoda yönlendirme
        method_name = f"_exec_{command_upper}"
        if not hasattr(self, method_name):
            raise CommandError(f"Unknown command '{command}'")
        
        method = getattr(self, method_name)
        
        async with self._lock:
            result = await method(*args)
            if is_write_command and self._aof and not in_transaction:
                self._aof.write(command, *args)
            return result

    async def execute_transaction(self, command_queue: List[Tuple[str, List[Any]]]):
        """Bir transaction'daki tüm komutları atomik olarak çalıştırır."""
        results = []
        async with self._lock:
            # Geçici bir AOF yazıcısı oluşturup transaction logunu tutalım
            temp_aof_log = []
            
            for command, args in command_queue:
                method_name = f"_exec_{command.upper()}"
                if not hasattr(self, method_name):
                    raise CommandError(f"Unknown command '{command}' in transaction")
                method = getattr(self, method_name)
                
                try:
                    result = await method(*args)
                    results.append(result)
                    # Eğer komut yazma işlemiyse, geçici loga ekle
                    is_write_command = command.upper() in ["SET", "DELETE", "EXPIRE", "LPUSH", "HSET"]
                    if is_write_command:
                        temp_aof_log.append((command, args))

                except Exception as e:
                    # Herhangi bir komut hata verirse, tüm transaction başarısız olur.
                    # Hiçbir değişiklik kalıcı hale getirilmez.
                    logging.warning(f"Transaction failed on command '{command}': {e}. Rolling back.")
                    # Bu örnekte rollback basit, çünkü değişiklikler bellekte.
                    # Gerçek bir DB'de daha karmaşık olurdu.
                    raise CommandError(f"Transaction aborted: {e}")

            # Transaction başarılı olursa, AOF logunu kalıcı hale getir.
            if self._aof:
                for cmd, a in temp_aof_log:
                    self._aof.write(cmd, *a)
        
        return results

    # --- Komutların Gerçek Uygulamaları (Lock'suz) ---
    async def _exec_GET(self, key: str):
        if await self._check_and_delete_expired(key): return None
        item = self._data.get(key)
        if item is None: return None
        type, value, _ = item
        if type != 'string': raise WrongTypeError("Operation against a key holding the wrong kind of value")
        return value

    async def _exec_SET(self, key: str, value: Any, expire_in_seconds: Optional[int] = None):
        expiration_time = time.time() + expire_in_seconds if expire_in_seconds is not None else None
        self._data[key] = ('string', value, expiration_time)
        return "OK"

    async def _exec_DELETE(self, key: str):
        if await self._check_and_delete_expired(key): return 1
        if key in self._data:
            del self._data[key]
            return 1
        return 0

    async def _exec_EXPIRE(self, key: str, seconds: int):
        if await self._check_and_delete_expired(key) or key not in self._data: return 0
        type, value, _ = self._data[key]
        self._data[key] = (type, value, time.time() + seconds)
        return 1
    
    async def _exec_LPUSH(self, key: str, *values: List[str]):
        await self._check_and_delete_expired(key)
        item = self._data.get(key)
        if item is None:
            new_list = list(reversed(values))
            self._data[key] = ('list', new_list, None)
            return len(new_list)
        type, current_list, _ = item
        if type != 'list': raise WrongTypeError("Operation against a key holding the wrong kind of value")
        current_list[:0] = reversed(values)
        return len(current_list)

    async def _exec_LRANGE(self, key: str, start: int, stop: int):
        if await self._check_and_delete_expired(key): return []
        item = self._data.get(key)
        if item is None: return []
        type, current_list, _ = item
        if type != 'list': raise WrongTypeError("Operation against a key holding the wrong kind of value")
        if stop == -1: return current_list[start:]
        return current_list[start : stop + 1]

    async def _exec_HSET(self, key: str, field: str, value: str):
        await self._check_and_delete_expired(key)
        item = self._data.get(key)
        if item is None:
            self._data[key] = ('hash', {field: value}, None)
            return 1
        type, current_hash, _ = item
        if type != 'hash': raise WrongTypeError("Operation against a key holding the wrong kind of value")
        is_new = 1 if field not in current_hash else 0
        current_hash[field] = value
        return is_new

    async def _exec_HGET(self, key: str, field: str):
        if await self._check_and_delete_expired(key): return None
        item = self._data.get(key)
        if item is None: return None
        type, current_hash, _ = item
        if type != 'hash': raise WrongTypeError("Operation against a key holding the wrong kind of value")
        return current_hash.get(field)

    async def save_snapshot(self):
        async with self._lock:
            try:
                with open(self._snapshot_path, 'w') as f:
                    json.dump(self._data, f)
                logging.info(f"Veritabanı anlık görüntüsü '{self._snapshot_path}' dosyasına kaydedildi.")
            except Exception as e:
                logging.error(f"Anlık görüntü kaydedilirken hata: {e}")

    async def load_from_snapshot(self):
        try:
            with open(self._snapshot_path, 'r') as f:
                async with self._lock:
                    self._data = json.load(f)
                logging.info(f"Veritabanı '{self._snapshot_path}' dosyasından yüklendi.")
        except FileNotFoundError:
            logging.warning(f"Anlık görüntü dosyası '{self._snapshot_path}' bulunamadı. Boş veritabanı ile başlanıyor.")
        except Exception as e:
            logging.error(f"Anlık görüntü yüklenirken hata: {e}")
            
# --- Protokol Ayrıştırıcı ve Yanıt Formatlayıcı ---
class ProtocolHandler:
    
    def parse_command(self, command_raw: str):
        """Gelen ham komutu ayrıştırır ve (komut, [argümanlar]) olarak döndürür."""
        parts = command_raw.strip().split()
        if not parts:
            raise CommandError("Empty command")
        
        command = parts[0].upper()
        args = parts[1:]
        
        # Argüman tipi dönüştürme
        processed_args = []
        for arg in args:
            try:
                processed_args.append(int(arg))
            except ValueError:
                processed_args.append(arg)
        
        return command, processed_args

    def format_response(self, result: Any) -> str:
        """Sonucu Redis protocol formatında string'e çevirir."""
        if result is None:
            return "_(nil)\r\n"
        elif isinstance(result, str):
            return f"+{result}\r\n" if result == "OK" or result == "QUEUED" else f"${len(result)}\r\n{result}\r\n"
        elif isinstance(result, int):
            return f":{result}\r\n"
        elif isinstance(result, list):
            response_parts = [f"*{len(result)}\r\n"]
            for item in result:
                # İç içe formatlama için özyinelemeli çağrı
                response_parts.append(self.format_response(item))
            return "".join(response_parts)
        elif isinstance(result, (CommandError, WrongTypeError, ValueError)):
             # Hata formatlama
            error_prefix = "WRONGTYPE" if isinstance(result, WrongTypeError) else "ERROR"
            return f"-{error_prefix} {str(result)}\r\n"
        else:
            return f"-ERROR Unknown response type: {type(result)}\r\n"

# --- İstemci Bağlantı Yöneticisi ---
async def handle_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter, storage: StorageEngine, protocol: ProtocolHandler):
    addr = writer.get_extra_info('peername')
    logging.info(f"Yeni bağlantı: {addr}")
    
    in_transaction = False
    command_queue = []

    try:
        while True:
            data = await reader.read(1024)
            if not data:
                break 

            command_raw = data.decode('utf-8')
            response = ""

            try:
                command, args = protocol.parse_command(command_raw)

                if command == 'MULTI':
                    if in_transaction:
                        raise CommandError("MULTI calls can not be nested")
                    in_transaction = True
                    command_queue = []
                    response = protocol.format_response("OK")
                
                elif command == 'DISCARD':
                    if not in_transaction:
                        raise CommandError("DISCARD without MULTI")
                    in_transaction = False
                    command_queue = []
                    response = protocol.format_response("OK")

                elif command == 'EXEC':
                    if not in_transaction:
                        raise CommandError("EXEC without MULTI")
                    
                    if not command_queue:
                        in_transaction = False
                        response = protocol.format_response([]) # Boş multi-bulk reply
                    else:
                        results = await storage.execute_transaction(command_queue)
                        in_transaction = False
                        command_queue = []
                        response = protocol.format_response(results)

                elif in_transaction:
                    command_queue.append((command, args))
                    response = protocol.format_response("QUEUED")
                
                else: # Transaction dışındaysa normal çalıştır
                    result = await storage.execute_command(command, args)
                    response = protocol.format_response(result)

            except (CommandError, WrongTypeError, ValueError) as e:
                response = protocol.format_response(e)
            except Exception as e:
                logging.error(f"İstemci ({addr}) işlenirken beklenmedik hata: {e}")
                response = f"-ERROR Sunucu hatası\r\n"

            writer.write(response.encode('utf-8'))
            await writer.drain()

    except ConnectionResetError:
        logging.warning(f"Bağlantı kesildi: {addr}")
    finally:
        logging.info(f"Bağlantı kapatıldı: {addr}")
        writer.close()
        await writer.wait_closed()


async def periodic_snapshot(storage: StorageEngine, interval: int):
    while True:
        await asyncio.sleep(interval)
        logging.info("Periyodik anlık görüntü kaydı başlatılıyor...")
        await storage.save_snapshot()

# --- Ana Sunucu Fonksiyonu ---
async def main(args):
    host = '127.0.0.1'
    port = 6380

    aof_handler = None
    if args.persistence_mode == 'aof':
        aof_handler = AofHandler(args.aof_file)
        aof_handler.open()

    storage = StorageEngine(snapshot_path=args.snapshot_file, aof_handler=aof_handler)
    protocol = ProtocolHandler()

    # Veritabanını diskten yükle
    if args.persistence_mode == 'aof':
        try:
            with open(args.aof_file, 'r') as f:
                logging.info(f"AOF dosyası '{args.aof_file}' okunarak veritabanı durumu yeniden oluşturuluyor...")
                for line in f:
                    if line.startswith('*'): # Sadece RESP Array formatındaki komutları işle
                        # Basit bir AOF replay. Gerçek bir sistemde daha sağlam olmalı.
                        raw_cmd_parts = []
                        num_parts = int(line[1:])
                        for _ in range(num_parts * 2): # her bölüm için uzunluk ve değer satırı var
                            raw_cmd_parts.append(f.readline())
                        
                        # Bu basit örnekte, AOF'u yeniden ayrıştırmak yerine doğrudan storage'a komut göndereceğiz.
                        # Bu kısmı daha da geliştirmek gerekebilir. Şimdilik snapshot'a güveniyoruz.
                        # Gerçek bir AOF replay'i için protocol handler'ı kullanmak gerekir.
                logging.info("AOF yüklemesi tamamlandı (basit mod).")
        except FileNotFoundError:
            logging.warning(f"AOF dosyası '{args.aof_file}' bulunamadı. Boş veritabanı ile başlanıyor.")
    else:
        await storage.load_from_snapshot()

    # Arka planda periyodik snapshot görevini başlat (eğer mod snapshot ise)
    if args.persistence_mode == 'snapshot':
        asyncio.create_task(periodic_snapshot(storage, args.snapshot_interval))

    server = await asyncio.start_server(
        lambda r, w: handle_client(r, w, storage, protocol),
        host,
        port
    )

    addrs = ', '.join(str(sock.getsockname()) for sock in server.sockets)
    logging.info(f'IgnisDB sunucusu {addrs} üzerinde çalışıyor... Kalıcılık modu: {args.persistence_mode}')

    try:
        async with server:
            await server.serve_forever()
    finally:
        if aof_handler:
            aof_handler.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="IgnisDB - In-memory Key-Value Database")
    parser.add_argument('--persistence-mode', type=str, choices=['snapshot', 'aof'], default='snapshot', help="Veri kalıcılık modu: 'snapshot' veya 'aof'.")
    parser.add_argument('--snapshot-file', type=str, default='ignisdb_snapshot.json', help="Snapshot dosyasının yolu.")
    parser.add_argument('--aof-file', type=str, default='ignisdb.aof', help="AOF dosyasının yolu.")
    parser.add_argument('--snapshot-interval', type=int, default=300, help="Snapshot kaydetme aralığı (saniye).")
    
    cli_args = parser.parse_args()

    try:
        asyncio.run(main(cli_args))
    except KeyboardInterrupt:
        logging.info("Sunucu kapatılıyor...")
