"""
SQL to IgnisDB Migration (Snapshot-Based)
==========================================
Instead of sending 1.6M commands through the network+AOF, this script:
  1. Parses SQL → writes a compact JSON snapshot directly
  2. IgnisDB loads it via IMPORT (instant)

Optimization (Low RAM):
  - Row keys are offloaded to temporary disk files during parsing.
  - Keys are streamed back into the snapshot at the end.
  - This keeps RAM usage constant even for huge SQL files.

Usage:
  python sql_to_ignis.py lirakazan.sql
  python sql_to_ignis.py lirakazan.sql --output data.json --dry-run
"""

import re
import sys
import asyncio
import argparse
import time
import os
import json
import glob
import traceback


# ─── Streaming SQL Parser ─────────────────────────────────────────

class StreamingSqlParser:
    """Memory-efficient line-by-line SQL parser."""

    def __init__(self, filepath: str):
        self.filepath = filepath
        self.file_size = os.path.getsize(filepath)
        self.tables = {}
        self.total_rows = 0
        self.bytes_read = 0

    def parse(self):
        """Yields (table_name, columns, row_values) tuples."""
        # Use errors='replace' to handle potential encoding issues without crashing
        with open(self.filepath, 'r', encoding='utf-8', errors='replace') as f:
            buffer = ""
            for line in f:
                self.bytes_read += len(line.encode('utf-8', errors='replace'))
                stripped = line.strip()

                if not stripped or stripped.startswith('--') or stripped.startswith('/*'):
                    if stripped.startswith('/*') and '*/' not in stripped:
                        for cline in f:
                            self.bytes_read += len(cline.encode('utf-8', errors='replace'))
                            if '*/' in cline:
                                break
                    continue

                buffer += line
                if not stripped.endswith(';'):
                    continue

                statement = buffer
                buffer = ""

                upper = statement.upper()
                if 'CREATE TABLE' in upper:
                    self._parse_create(statement)
                elif 'INSERT INTO' in upper:
                    yield from self._parse_insert(statement)
                del statement

    def _parse_create(self, stmt: str):
        match = re.search(
            r'CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?[`"\']?(\w+)[`"\']?\s*\((.*?)\)\s*(?:ENGINE|DEFAULT|;)',
            stmt, re.IGNORECASE | re.DOTALL)
        if not match:
            return
        table_name = match.group(1).lower()
        body = match.group(2)
        columns = []
        for part in body.split('\n'):
            part = part.strip().rstrip(',')
            if not part:
                continue
            if re.match(r'^\s*(PRIMARY|UNIQUE|INDEX|KEY|CONSTRAINT|FOREIGN|CHECK)', part, re.IGNORECASE):
                continue
            col_match = re.match(r'[`"\']?(\w+)[`"\']?\s+', part)
            if col_match:
                columns.append(col_match.group(1).lower())
        self.tables[table_name] = columns

    def _parse_insert(self, stmt: str):
        match = re.search(r'INSERT\s+INTO\s+[`"\']?(\w+)[`"\']?', stmt, re.IGNORECASE)
        if not match:
            return
        table_name = match.group(1).lower()
        columns = self.tables.get(table_name, [])
        col_match = re.search(
            r'INSERT\s+INTO\s+[`"\']?\w+[`"\']?\s*\(([^)]+)\)\s*VALUES',
            stmt, re.IGNORECASE)
        if col_match:
            columns = [c.strip().strip('`"\'').lower() for c in col_match.group(1).split(',')]
            self.tables[table_name] = columns
        val_match = re.search(r'VALUES\s*', stmt, re.IGNORECASE)
        if not val_match:
            return
        for row in self._extract_rows(stmt[val_match.end():]):
            self.total_rows += 1
            yield (table_name, columns, row)

    def _extract_rows(self, text: str):
        i = 0
        n = len(text)
        while i < n:
            if text[i] == '(':
                depth = 1
                start = i + 1
                i += 1
                in_str = None
                while i < n and depth > 0:
                    ch = text[i]
                    if in_str:
                        if ch == '\\' and i + 1 < n:
                            i += 2
                            continue
                        if ch == in_str:
                            in_str = None
                    elif ch in ("'", '"'):
                        in_str = ch
                    elif ch == '(':
                        depth += 1
                    elif ch == ')':
                        depth -= 1
                    i += 1
                yield self._parse_values(text[start:i - 1])
            else:
                i += 1

    def _parse_values(self, text: str) -> list:
        values = []
        current = []
        in_str = None
        i = 0
        n = len(text)
        while i < n:
            ch = text[i]
            if in_str:
                if ch == '\\' and i + 1 < n:
                    current.append(ch)
                    current.append(text[i + 1])
                    i += 2
                    continue
                if ch == in_str:
                    if i + 1 < n and text[i + 1] == in_str:
                        current.append(ch)
                        i += 2
                        continue
                    in_str = None
                    current.append(ch)
                else:
                    current.append(ch)
            elif ch in ("'", '"'):
                in_str = ch
                current.append(ch)
            elif ch == ',':
                values.append(self._clean(''.join(current).strip()))
                current = []
            else:
                current.append(ch)
            i += 1
        if current:
            values.append(self._clean(''.join(current).strip()))
        return values

    def _clean(self, val: str) -> str:
        if val.upper() == 'NULL':
            return ''
        if len(val) >= 2 and val[0] in ("'", '"') and val[-1] == val[0]:
            val = val[1:-1]
        val = val.replace("\\'", "'").replace('\\"', '"')
        val = val.replace('\\n', '\n').replace('\\r', '\r').replace('\\\\', '\\')
        return val

    @property
    def progress_pct(self):
        return min(100, self.bytes_read / max(self.file_size, 1) * 100)


# ─── Snapshot Writer ───────────────────────────────────────────────

class SnapshotWriter:
    """Writes IgnisDB-compatible JSON snapshot incrementally."""

    def __init__(self, output_path: str):
        self.path = output_path
        self._file = open(output_path, 'w', encoding='utf-8', buffering=65536)
        self._first = True
        self._file.write('{')
        self.key_count = 0

    def add(self, key: str, value, dtype: str = "string", expire: float = None):
        """Add a key-value pair in IgnisDB snapshot format: [type, value, expire_at]"""
        if not self._first:
            self._file.write(',')
        self._first = False

        exp = expire if expire else None
        # IgnisDB snapshot format: { "key": ["type", value, expire_at] }
        # We manually construct to avoid allocating huge dicts for simple keys
        key_str = json.dumps(key, ensure_ascii=False)
        val_str = json.dumps(value, ensure_ascii=False, separators=(',', ':'))
        exp_str = json.dumps(exp)
        
        self._file.write(f'{key_str}:["{dtype}",{val_str},{exp_str}]')
        self.key_count += 1

    def stream_set(self, key: str, iterator, expire: float = None):
        """Streams a list of strings into a SET value without loading all into RAM."""
        if not self._first:
            self._file.write(',')
        self._first = False

        key_str = json.dumps(key, ensure_ascii=False)
        self._file.write(f'{key_str}:["set",[')

        first_item = True
        for item in iterator:
            if not first_item:
                self._file.write(',')
            first_item = False
            self._file.write(json.dumps(item, ensure_ascii=False))
        
        exp_str = json.dumps(expire if expire else None)
        self._file.write(f'],{exp_str}]')
        self.key_count += 1

    def close(self):
        self._file.write('}')
        self._file.close()


# ─── Migration ─────────────────────────────────────────────────────

def get_memory_mb():
    try:
        import psutil
        return psutil.Process().memory_info().rss / 1024 / 1024
    except ImportError:
        return 0


async def load_snapshot(host: str, port: int, snapshot_path: str):
    """Tells IgnisDB to load the snapshot file."""
    r, w = await asyncio.open_connection(host, port)
    w.write(f"IMPORT {snapshot_path}\\r\\n".encode())
    await w.drain()
    resp = await r.read(4096)
    result = resp.decode(errors='replace').strip()
    w.close()
    await w.wait_closed()
    return result


def cleanup_temp_files(table_stats=None):
    """Cleanup temporary files in a robust way."""
    # 1. Close and remove files from active stats
    if table_stats:
        for t_name in table_stats:
            tmp_fname = f".tmp_keys_{t_name}.txt"
            if os.path.exists(tmp_fname):
                try:
                    os.remove(tmp_fname)
                except OSError:
                    pass
    
    # 2. Sweep any remaining stray temp files matching the pattern
    #    (Useful if table_stats wasn't fully populated or previous runs failed)
    for stray in glob.glob(".tmp_keys_*.txt"):
        try:
            os.remove(stray)
        except OSError:
            pass


def migrate(sql_path: str, output_path: str, dry_run: bool):
    """Parse SQL and write JSON snapshot directly."""
    file_size_mb = os.path.getsize(sql_path) / 1024 / 1024
    print(f"{'=' * 55}")
    print(f"  SQL → IgnisDB Snapshot Migration (Low RAM Mode)")
    print(f"{'=' * 55}")
    print(f"  Input  : {sql_path} ({file_size_mb:.1f} MB)")
    print(f"  Output : {output_path}")
    print(f"  Mode   : {'DRY RUN' if dry_run else 'LIVE'}\\n", flush=True)

    # Initial cleanup to be safe
    cleanup_temp_files()

    parser = StreamingSqlParser(sql_path)
    writer = None if dry_run else SnapshotWriter(output_path)
    
    start = time.time()
    table_stats = {}
    temp_key_files = {} 
    
    try:
        last_report = time.time()

        for table_name, columns, row in parser.parse():
            if table_name not in table_stats:
                table_stats[table_name] = 0
                # Open temp file for this table's keys
                if not dry_run:
                    f = open(f".tmp_keys_{table_name}.txt", "w", encoding="utf-8")
                    temp_key_files[table_name] = f

            # Determine PK column
            pk_idx = 0
            for idx, col in enumerate(columns):
                if col == 'id':
                    pk_idx = idx
                    break

            row_id = row[pk_idx] if pk_idx < len(row) and row[pk_idx] else str(table_stats[table_name] + 1)
            key = f"{table_name}:{row_id}"

            # Build row JSON
            row_dict = {}
            for col_idx, value in enumerate(row):
                field = columns[col_idx] if col_idx < len(columns) else f"col{col_idx}"
                row_dict[field] = value

            # Write actual data row immediately
            if writer:
                # We don't use writer.add here because we want to pass the dict directly
                writer.add(key, row_dict)
                
                # Write key to temp file
                temp_key_files[table_name].write(row_id + "\\n")

            table_stats[table_name] += 1

            # Progress
            now = time.time()
            if now - last_report >= 1.0: # Report more frequently (1s)
                elapsed = now - start
                rate = parser.total_rows / max(elapsed, 0.001)
                mem = get_memory_mb()
                print(
                    f"  [{parser.progress_pct:5.1f}%] "
                    f"{parser.total_rows:>9,} rows | "
                    f"{rate:>7,.0f} r/s | "
                    f"{elapsed:>6.1f}s | "
                    f"RAM: {mem:.0f}MB",
                    end='   \\r', flush=True
                )
                last_report = now

        # Close all temp files for writing
        for f in temp_key_files.values():
            f.close()

        # Write table metadata (table list + key indexes as sets)
        if writer:
            print(f"\\n  Finalizing snapshot (merging keys)...{' '*20}", flush=True)
            
            # _tables set
            table_names = list(table_stats.keys())
            writer.add("_tables", table_names, "set")

            # key indexes per table (stream from file)
            for t_name in table_names:
                tmp_fname = f".tmp_keys_{t_name}.txt"
                if os.path.exists(tmp_fname):
                    def key_generator(fname):
                        with open(fname, "r", encoding="utf-8") as f_in:
                            for line in f_in:
                                yield line.strip()
                    
                    writer.stream_set(f"{t_name}:_keys", key_generator(tmp_fname))

            writer.close()

    except KeyboardInterrupt:
        print("\\n\\n  [!] Migration cancelled by user.", flush=True)
        return None
    except Exception as e:
        print(f"\\n\\n  [!] Error: {e}", flush=True)
        traceback.print_exc()
        return None
    finally:
        # Close open file handles
        for f in temp_key_files.values():
            try:
                f.close()
            except Exception:
                pass
        
        # Cleanup ALL temp files
        cleanup_temp_files(table_stats)

    elapsed = time.time() - start
    rate = parser.total_rows / max(elapsed, 0.001)

    output_size = 0
    if not dry_run and os.path.exists(output_path):
        output_size = os.path.getsize(output_path) / 1024 / 1024

    if writer:
        print(f"\\n\\n{'=' * 55}")
        print(f"  Snapshot {'(DRY RUN) ' if dry_run else ''}Created!")
        print(f"{'=' * 55}")
        print(f"  SQL Size      : {file_size_mb:.1f} MB")
        print(f"  Snapshot Size : {output_size:.1f} MB")
        if file_size_mb > 0:
            ratio = output_size / file_size_mb * 100
            print(f"  Ratio         : {ratio:.0f}% of SQL")
        print(f"  Tables        : {len(table_stats)}")
        print(f"  Total Rows    : {parser.total_rows:,}")
        print(f"  Keys Written  : {writer.key_count if writer else 0:,}")
        print(f"  Time          : {elapsed:.2f}s")
        print(f"  Throughput    : {rate:,.0f} rows/s")
        print(f"{'=' * 55}", flush=True)
        print(f"\\n  Table breakdown:")
        for t, count in sorted(table_stats.items(), key=lambda x: -x[1]):
            print(f"    {t:30s} {count:>8,} rows")

    return output_path


# ─── Entry Point ───────────────────────────────────────────────────

def main():
    p = argparse.ArgumentParser(description="SQL → IgnisDB Snapshot Migration")
    p.add_argument('sql_file', help="Path to .sql dump file")
    p.add_argument('--output', default='ignis_snapshot.json', help="Output snapshot path")
    p.add_argument('--host', default='127.0.0.1')
    p.add_argument('--port', type=int, default=6381)
    p.add_argument('--dry-run', action='store_true', help="Parse only, no output")
    p.add_argument('--no-import', action='store_true', help="Create snapshot but don't auto-import")
    args = p.parse_args()

    # Step 1: Create snapshot
    snapshot = migrate(args.sql_file, args.output, args.dry_run)
    
    if snapshot is None:
        sys.exit(1)

    # Step 2: Auto-import into running IgnisDB
    if not args.dry_run and not args.no_import:
        print(f"\\n  Importing into IgnisDB ({args.host}:{args.port})...", flush=True)
        abs_path = os.path.abspath(snapshot)
        try:
            result = asyncio.run(load_snapshot(args.host, args.port, abs_path))
            print(f"  Server response: {result}")
            print(f"  Done! Data is live.", flush=True)
        except ConnectionRefusedError:
            print(f"  Server not running. Load manually:")
            print(f"    1. Start IgnisDB")
            print(f"    2. IMPORT {abs_path}", flush=True)
        except Exception as e:
            print(f"  Import failed: {e}")
            print(f"  Load manually: IMPORT {abs_path}", flush=True)


if __name__ == "__main__":
    main()
