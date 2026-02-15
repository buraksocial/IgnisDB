"""
Real-Time Collaborative Todo Manager - WebSocket Server
Bridges browser clients to IgnisDB backend via WebSocket
"""
import asyncio
import json
import logging
from typing import Set
from datetime import datetime

# Simple WebSocket server
try:
    import websockets
except ImportError:
    print("Please install websockets: pip install websockets")
    exit(1)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class IgnisDBClient:
    """TCP client to communicate with IgnisDB server"""
    def __init__(self, host='127.0.0.1', port=6380):
        self.host = host
        self.port = port
        self.reader = None
        self.writer = None

    async def connect(self):
        self.reader, self.writer = await asyncio.open_connection(self.host, self.port)
        logging.info(f"Connected to IgnisDB at {self.host}:{self.port}")

    async def send_command(self, command: str, *args) -> str:
        cmd_str = f"{command} {' '.join(map(str, args))}\n"

        logging.debug(f"Sending to IgnisDB: {cmd_str.strip()}")

        self.writer.write(cmd_str.encode('utf-8'))
        await self.writer.drain()

        # Read response (IgnisDB sends responses ending with \r\n)
        data = await self.reader.read(4096)
        response = data.decode('utf-8').strip()

        logging.debug(f"Received from IgnisDB: {response}")
        return response

    async def close(self):
        if self.writer:
            self.writer.close()
            await self.writer.wait_closed()

class TodoServer:
    def __init__(self, ignis_host='127.0.0.1', ignis_port=6380):
        self.ignis_host = ignis_host
        self.ignis_port = ignis_port
        self.clients: Set[websockets.WebSocketServerProtocol] = set()
        self.db_client = None

    async def initialize(self):
        self.db_client = IgnisDBClient(self.ignis_host, self.ignis_port)
        await self.db_client.connect()
        logging.info("Todo server initialized")

    async def broadcast(self, message: dict, exclude=None):
        """Broadcast message to all connected clients except sender"""
        if not self.clients:
            return

        message_str = json.dumps(message)
        disconnected = set()

        for client in self.clients:
            if client == exclude:
                continue
            try:
                await client.send(message_str)
            except websockets.exceptions.ConnectionClosed:
                disconnected.add(client)

        self.clients -= disconnected

    async def load_tasks(self) -> list:
        """Load all tasks from IgnisDB"""
        try:
            response = await self.db_client.send_command("LRANGE", "tasks:list", "0", "-1")

            if response == "*0\r\n" or response == "_(nil)\r\n" or not response or response == "*0":
                logging.info("No tasks found in database")
                return []

            # Parse the RESP array
            task_ids = self._parse_resp_array(response)
            logging.info(f"Found {len(task_ids)} task IDs: {task_ids}")

            tasks = []
            for task_id in task_ids:
                task = await self.load_task(task_id)
                if task:
                    tasks.append(task)

            logging.info(f"Successfully loaded {len(tasks)} tasks")
            return tasks
        except Exception as e:
            logging.error(f"Error loading tasks: {e}")
            return []

    def _parse_resp_array(self, response: str) -> list:
        """Parse RESP array format: *<count>\r\n$<len1>\r\n<val1>\r\n... """
        items = []
        lines = response.split('\r\n')

        if not lines or not lines[0].startswith('*'):
            logging.warning(f"Invalid RESP array format: {response[:50]}")
            return items

        try:
            count = int(lines[0][1:])
            logging.debug(f"Parsing RESP array with {count} items")
        except ValueError:
            logging.error(f"Invalid array count in RESP: {lines[0]}")
            return items

        # Skip the first line (*<count>) and process pairs of ($<len>, <value>)
        i = 1
        while i < len(lines) - 1 and len(items) < count:
            current_line = lines[i]
            if current_line.startswith('$'):
                try:
                    length = int(current_line[1:])

                    # Check for nil response
                    if length == -1:
                        items.append(None)
                        i += 1
                        continue

                    # The next line should be the actual value
                    if i + 1 < len(lines):
                        value = lines[i + 1]
                        # Validate that the length matches (or is close, accounting for encoding)
                        if len(value) == length or abs(len(value) - length) <= 2:
                            items.append(value)
                            logging.debug(f"Parsed item: {value}")
                        else:
                            logging.warning(f"Length mismatch: expected {length}, got {len(value)} for '{value}'")
                            items.append(value)
                    i += 2  # Skip both the $-line and the value line
                except ValueError as e:
                    logging.error(f"Error parsing RESP line '{current_line}': {e}")
                    i += 1
            else:
                i += 1

        logging.debug(f"Parsed {len(items)} items from RESP array")
        return items

    async def load_task(self, task_id: str) -> dict:
        """Load a single task from IgnisDB"""
        try:
            fields = ['title', 'status', 'assignee', 'priority', 'dueDate', 'createdBy', 'createdAt']
            task = {'id': task_id}

            for field in fields:
                response = await self.db_client.send_command("HGET", f"task:{task_id}", field)
                logging.debug(f"HGET task:{task_id} {field}: {repr(response)}")

                # Parse response
                value = self._parse_resp_string(response)
                task[field] = value

                if value:
                    logging.debug(f"Loaded field {field} = {value}")

            if task.get('title'):
                logging.info(f"Successfully loaded task {task_id}: {task.get('title')}")
                return task
            else:
                logging.warning(f"Task {task_id} has no title, skipping")
                return None

        except Exception as e:
            logging.error(f"Error loading task {task_id}: {e}")
            return None

    def _parse_resp_string(self, response: str) -> str:
        """Parse RESP bulk string format: $<len>\r\n<value>\r\n or _(nil)\r\n"""
        if not response:
            return None

        response = response.strip()

        # Check for nil response
        if response == "_(nil)" or response == "_(nil)\r\n":
            return None

        # Parse bulk string format
        if response.startswith('$'):
            lines = response.split('\r\n')
            if len(lines) >= 2:
                length_line = lines[0]
                try:
                    length = int(length_line[1:])

                    # Check for nil ($-1)
                    if length == -1:
                        return None

                    if len(lines) > 1:
                        return lines[1]
                except ValueError:
                    logging.error(f"Invalid bulk string length: {length_line}")
                    return None

        # If it starts with '-', it's an error
        if response.startswith('-'):
            logging.warning(f"Error response from IgnisDB: {response}")
            return None

        # If it starts with '+', it's a simple string
        if response.startswith('+'):
            return response[1:]

        # If it starts with ':', it's an integer
        if response.startswith(':'):
            return response[1:]

        logging.warning(f"Unexpected RESP format: {response[:50]}")
        return response

    async def save_task(self, task: dict):
        """Save a task to IgnisDB"""
        task_id = task['id']

        # Add task ID to the list
        response = await self.db_client.send_command("LPUSH", "tasks:list", task_id)
        logging.debug(f"LPUSH response: {response}")

        # Save task fields as hash
        for key, value in task.items():
            if key != 'id' and value is not None:
                response = await self.db_client.send_command("HSET", f"task:{task_id}", key, str(value))
                logging.debug(f"HSET task:{task_id} {key}:{value} response: {response}")

        # Set expiration if dueDate exists (TTL)
        if 'dueDate' in task and task['dueDate']:
            try:
                due = datetime.fromisoformat(task['dueDate'])
                now = datetime.now()
                ttl = int((due - now).total_seconds())
                if ttl > 0:
                    response = await self.db_client.send_command("EXPIRE", f"task:{task_id}", str(ttl))
                    logging.debug(f"EXPIRE response: {response}")
            except Exception as e:
                logging.warning(f"Could not set expiration for task {task_id}: {e}")

    async def delete_task(self, task_id: str):
        """Delete a task from IgnisDB"""
        response = await self.db_client.send_command("DELETE", f"task:{task_id}")
        logging.debug(f"DELETE response: {response}")

    async def handle_message(self, websocket, message: dict):
        action = message.get('action')

        try:
            if action == 'load_tasks':
                tasks = await self.load_tasks()
                await websocket.send(json.dumps({
                    'type': 'tasks_loaded',
                    'tasks': tasks
                }))

            elif action == 'add_task':
                task = message.get('task')
                await self.save_task(task)

                # Broadcast to other clients
                await self.broadcast({
                    'type': 'task_added',
                    'task': task
                }, exclude=websocket)

                await websocket.send(json.dumps({'type': 'task_added', 'task': task}))

            elif action == 'update_task':
                task_id = message.get('task_id')
                updates = message.get('updates')

                for key, value in updates.items():
                    response = await self.db_client.send_command("HSET", f"task:{task_id}", key, str(value))
                    logging.debug(f"HSET update response: {response}")

                await self.broadcast({
                    'type': 'task_updated',
                    'task_id': task_id,
                    'updates': updates
                }, exclude=websocket)

                await websocket.send(json.dumps({'type': 'task_updated', 'task_id': task_id}))

            elif action == 'delete_task':
                task_id = message.get('task_id')
                await self.delete_task(task_id)

                await self.broadcast({
                    'type': 'task_deleted',
                    'task_id': task_id
                }, exclude=websocket)

                await websocket.send(json.dumps({'type': 'task_deleted', 'task_id': task_id}))

        except Exception as e:
            logging.error(f"Error handling message: {e}", exc_info=True)
            await websocket.send(json.dumps({'type': 'error', 'message': str(e)}))

    async def handle_client(self, websocket):
        self.clients.add(websocket)
        client_id = id(websocket)
        logging.info(f"Client {client_id} connected. Total clients: {len(self.clients)}")

        try:
            async for message in websocket:
                data = json.loads(message)
                await self.handle_message(websocket, data)
        except websockets.exceptions.ConnectionClosed:
            logging.info(f"Client {client_id} disconnected")
        except Exception as e:
            logging.error(f"Error with client {client_id}: {e}", exc_info=True)
        finally:
            if websocket in self.clients:
                self.clients.remove(websocket)
            logging.info(f"Client {client_id} removed. Total clients: {len(self.clients)}")

    async def start(self, host='0.0.0.0', port=8765):
        await self.initialize()

        logging.info(f"Todo WebSocket server running on ws://{host}:{port}")
        logging.info(f"Connected to IgnisDB at {self.ignis_host}:{self.ignis_port}")

        # Start the WebSocket server
        async with websockets.serve(self.handle_client, host, port):
            await asyncio.Future()

async def main():
    server = TodoServer(ignis_host='127.0.0.1', ignis_port=6380)
    await server.start(host='0.0.0.0', port=8765)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Shutting down Todo server...")
