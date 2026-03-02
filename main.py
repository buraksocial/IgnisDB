import asyncio
import argparse
import logging
from ignisdb.server import IgnisServer

# Configure Logging
handlers = [
    logging.FileHandler("server.log"),
    logging.StreamHandler()
]
logging.getLogger().handlers = []

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=handlers
)

# Set Console to WARNING only to keep it clean
handlers[1].setLevel(logging.WARNING)

def parse_args():
    parser = argparse.ArgumentParser(description="IgnisDB - In-memory Key-Value Database")
    parser.add_argument('--config', default='ignisdb.conf', help="Path to configuration file")
    
    # Arguments that can be overridden by command line
    parser.add_argument('--host', help="Host to bind to")
    parser.add_argument('--port', type=int, help="Port to bind to")
    parser.add_argument('--persistence-mode', choices=['snapshot', 'aof'], help="Persistence mode")
    parser.add_argument('--snapshot-file', help="Snapshot file path")
    parser.add_argument('--aof-file', help="AOF file path")
    parser.add_argument('--snapshot-interval', type=int, help="Snapshot interval in seconds")
    parser.add_argument('--requirepass', help="Password for authentication")
    
    args = parser.parse_args()
    
    # Load config file
    import configparser
    import os
    
    config = configparser.ConfigParser()
    
    # Defaults
    defaults = {
        'host': '127.0.0.1',
        'port': 6381,
        'mode': 'snapshot',
        'snapshot_file': 'ignisdb_snapshot.json',
        'aof_file': 'ignisdb.aof',
        'snapshot_interval': 300,
        'requirepass': None,
        'encryption_key': None
    }
    
    if args.config and os.path.exists(args.config):
        config.read(args.config)
        if 'Network' in config:
            defaults['host'] = config['Network'].get('host', defaults['host'])
            defaults['port'] = config['Network'].getint('port', defaults['port'])
        if 'Persistence' in config:
            defaults['mode'] = config['Persistence'].get('mode', defaults['mode'])
            defaults['snapshot_file'] = config['Persistence'].get('snapshot_file', defaults['snapshot_file'])
            defaults['aof_file'] = config['Persistence'].get('aof_file', defaults['aof_file'])
            defaults['snapshot_interval'] = config['Persistence'].getint('snapshot_interval', defaults['snapshot_interval'])
        if 'Security' in config:
            defaults['requirepass'] = config['Security'].get('requirepass', defaults['requirepass'])
            defaults['encryption_key'] = config['Security'].get('encryption_key', defaults['encryption_key'])
            
    # Command line args override config
    final_host = args.host if args.host else defaults['host']
    final_port = args.port if args.port else defaults['port']
    final_mode = args.persistence_mode if args.persistence_mode else defaults['mode']
    final_snapshot_path = args.snapshot_file if args.snapshot_file else defaults['snapshot_file']
    final_aof_path = args.aof_file if args.aof_file else defaults['aof_file']
    final_interval = args.snapshot_interval if args.snapshot_interval is not None else defaults['snapshot_interval']
    final_password = args.requirepass if args.requirepass else defaults['requirepass']
    
    return argparse.Namespace(
        host=final_host,
        port=final_port,
        persistence_mode=final_mode,
        snapshot_file=final_snapshot_path,
        aof_file=final_aof_path,
        snapshot_interval=final_interval,
        requirepass=final_password,
        encryption_key=defaults['encryption_key']
    )

async def main():
    args = parse_args()
    server = IgnisServer(
        host=args.host,
        port=args.port,
        persistence_mode=args.persistence_mode,
        snapshot_path=args.snapshot_file,
        aof_path=args.aof_file,
        snapshot_interval=args.snapshot_interval,
        password=args.requirepass,
        encryption_key=args.encryption_key
    )
    
    try:
        await server.start()
    except KeyboardInterrupt:
        logging.info("Shutting down...")
        server.shutdown()

if __name__ == "__main__":
    try:
        # Cross-platform Event Loop Optimization
        import sys
        if sys.platform == 'win32':
            try:
                # import winloop
                # asyncio.set_event_loop_policy(winloop.EventLoopPolicy())
                # logging.info("Using winloop event loop.")
                pass
            except ImportError:
                # Fallback to ProactorEventLoop which is default in recent Python versions for Windows
                # but good to be explicit or just let asyncio handle it.
                pass
        else:
            try:
                import uvloop
                uvloop.install()
                logging.info("Using uvloop event loop.")
            except ImportError:
                pass

        asyncio.run(main())
    except KeyboardInterrupt:
        pass
