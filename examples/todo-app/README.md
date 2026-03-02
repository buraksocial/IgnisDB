# Collaborative Todo List Example

This is a real-time collaborative Todo List application built to demonstrate **IgnisDB**'s capabilities.

## Architecture

*   **Frontend:** React + Vite (located in `frontend/`)
*   **Middle Tier:** Python WebSocket Server (`todo_server.py`)
*   **Database:** IgnisDB (running on `localhost:6380`)

## How to Run

### 1. Start IgnisDB Server
Open a terminal in the root of the repository and run:
```bash
python main.py --port 6380
```

### 2. Start WebSocket Server
Open a new terminal, navigate to this directory (`examples/todo-app`), and run:
```bash
# Install dependency if needed
pip install websockets

python todo_server.py
```
This server connects to IgnisDB on `6380` and listens for WebSocket connections on `8765`.

### 3. Start Frontend
Open a third terminal, navigate to `examples/todo-app/frontend`:
```bash
cd examples/todo-app/frontend
npm install
npm run dev
```
Open your browser at the URL shown (usually `http://localhost:5173`).

## Usage
*   Enter your name to join.
*   Add, edit, delete tasks.
*   Open multiple browser tabs to see updates sync in real-time!
