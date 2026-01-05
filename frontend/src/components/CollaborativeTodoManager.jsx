
import React, { useState, useEffect, useRef, useCallback } from 'react';
import { CheckCircle2, Circle, Trash2, Plus, Users, Clock, AlertCircle, Wifi, WifiOff, RefreshCw, Database } from 'lucide-react';

class TaskStorage {
  constructor() {
    this.dbName = 'TodoManagerDB';
    this.storeName = 'tasks';
    this.db = null;
    this.initPromise = null;
  }

  async init() {
    // Return existing promise if initialization is in progress
    if (this.initPromise) {
      return this.initPromise;
    }

    // Return existing db if already initialized
    if (this.db) {
      return this.db;
    }

    this.initPromise = new Promise((resolve, reject) => {
      const request = indexedDB.open(this.dbName, 1);

      request.onerror = () => {
        this.initPromise = null;
        reject(request.error);
      };

      request.onsuccess = () => {
        this.db = request.result;
        this.initPromise = null;

        this.db.onclose = () => {
          console.warn('IndexedDB connection closed unexpectedly');
          this.db = null;
        };

        resolve(this.db);
      };

      request.onupgradeneeded = (event) => {
        const db = event.target.result;
        if (!db.objectStoreNames.contains(this.storeName)) {
          const objectStore = db.createObjectStore(this.storeName, { keyPath: 'id' });
          objectStore.createIndex('assignee', 'assignee', { unique: false });
          objectStore.createIndex('createdBy', 'createdBy', { unique: false });
          objectStore.createIndex('status', 'status', { unique: false });
        }
      };
    });

    return this.initPromise;
  }

  async saveTasks(tasks) {
    try {
      await this.init();

      if (!this.db) {
        throw new Error('Database not initialized');
      }

      return new Promise((resolve, reject) => {
        const transaction = this.db.transaction([this.storeName], 'readwrite');
        const objectStore = transaction.objectStore(this.storeName);

        transaction.onerror = () => reject(transaction.error);
        transaction.oncomplete = () => resolve();

        const clearRequest = objectStore.clear();

        clearRequest.onsuccess = () => {
          if (tasks.length === 0) {
            return;
          }

          tasks.forEach(task => {
            objectStore.add(task);
          });
        };

        clearRequest.onerror = () => reject(clearRequest.error);
      });
    } catch (error) {
      console.error('Error in saveTasks:', error);
      throw error;
    }
  }

  async loadTasks() {
    try {
      await this.init();

      if (!this.db) {
        throw new Error('Database not initialized');
      }

      return new Promise((resolve, reject) => {
        const transaction = this.db.transaction([this.storeName], 'readonly');
        const objectStore = transaction.objectStore(this.storeName);
        const request = objectStore.getAll();

        request.onsuccess = () => resolve(request.result || []);
        request.onerror = () => reject(request.error);
      });
    } catch (error) {
      console.error('Error in loadTasks:', error);
      throw error;
    }
  }

  async deleteTask(taskId) {
    try {
      await this.init();

      if (!this.db) {
        throw new Error('Database not initialized');
      }

      return new Promise((resolve, reject) => {
        const transaction = this.db.transaction([this.storeName], 'readwrite');
        const objectStore = transaction.objectStore(this.storeName);
        const request = objectStore.delete(taskId);

        request.onsuccess = () => resolve();
        request.onerror = () => reject(request.error);
      });
    } catch (error) {
      console.error('Error in deleteTask:', error);
      throw error;
    }
  }
}

const CollaborativeTodoManager = () => {
  const [tasks, setTasks] = useState([]);
  const [newTask, setNewTask] = useState('');
  const [assignee, setAssignee] = useState('');
  const [priority, setPriority] = useState('medium');
  const [dueDate, setDueDate] = useState('');
  const [username, setUsername] = useState('');
  const [usernameInput, setUsernameInput] = useState('');
  const [isConnected, setIsConnected] = useState(false);
  const [error, setError] = useState('');
  const [filter, setFilter] = useState('all');
  const [isLoading, setIsLoading] = useState(false);
  const [syncStatus, setSyncStatus] = useState('');
  const ws = useRef(null);
  const usernameRef = useRef('');
  const tasksRef = useRef([]);
  const storageRef = useRef(null);

  // Initialize storage ONCE
  useEffect(() => {
    const initStorage = async () => {
      try {
        console.log('Initializing TaskStorage...');
        storageRef.current = new TaskStorage();
        await storageRef.current.init();
        console.log('IndexedDB initialized successfully');
      } catch (error) {
        console.error('Failed to initialize IndexedDB:', error);
        setError('Storage initialization failed. Some features may not work.');
      }
    };
    initStorage();

    // Cleanup on unmount
    return () => {
      if (storageRef.current && storageRef.current.db) {
        storageRef.current.db.close();
        storageRef.current.db = null;
      }
    };
  }, []);

  // Update refs when state changes
  useEffect(() => {
    usernameRef.current = username;
    tasksRef.current = tasks;
  }, [username, tasks]);

  // Persistent Storage Functions
  const loadTasksFromStorage = useCallback(async () => {
    try {
      setIsLoading(true);
      setSyncStatus('Loading tasks from local storage...');

      if (!storageRef.current) {
        console.warn('Storage not initialized yet');
        setIsLoading(false);
        return [];
      }

      const storedTasks = await storageRef.current.loadTasks();
      console.log('Loaded tasks from IndexedDB:', storedTasks);

      setTasks(storedTasks);
      setSyncStatus('Tasks loaded');
      setTimeout(() => setSyncStatus(''), 2000);
      return storedTasks;
    } catch (error) {
      console.error('Error loading from storage:', error);
      setSyncStatus('');
      // setError('Failed to load tasks from local storage');
      return [];
    } finally {
      setIsLoading(false);
    }
  }, []);

  const saveTasksToStorage = useCallback(async (updatedTasks) => {
    try {
      if (!storageRef.current) {
        console.warn('Storage not initialized yet');
        return;
      }

      await storageRef.current.saveTasks(updatedTasks);
      //console.log('Saved tasks to IndexedDB:', updatedTasks.length);

      setSyncStatus('Saved ✓');
      setTimeout(() => setSyncStatus(''), 1500);
    } catch (error) {
      console.error('Error saving to storage:', error);
      //setError('Failed to save tasks locally');
    }
  }, []);

  // Load username from localStorage 
  useEffect(() => {
    const savedUsername = localStorage.getItem('todo_username');
    if (savedUsername) {
      setUsernameInput(savedUsername);
    }
  }, []);

  // Load tasks when username is set
  useEffect(() => {
    if (username && storageRef.current) {
      loadTasksFromStorage();
    }
  }, [username, loadTasksFromStorage]);

  const sendMessage = useCallback((message) => {
    if (ws.current && ws.current.readyState === WebSocket.OPEN) {
      ws.current.send(JSON.stringify(message));
    } else {
      console.warn('WebSocket not connected, operating in offline mode');
    }
  }, []);

  const loadTasksFromServer = useCallback(() => {
    sendMessage({ action: 'load_tasks' });
  }, [sendMessage]);

  const handleServerMessage = useCallback((data) => {
    switch (data.type) {
      case 'tasks_loaded':
        setTasks(prevTasks => {
          const serverTasks = data.tasks || [];
          const localTaskIds = new Set(prevTasks.map(t => t.id));
          const mergedTasks = [...prevTasks];

          serverTasks.forEach(serverTask => {
            if (!localTaskIds.has(serverTask.id)) {
              mergedTasks.push(serverTask);
            }
          });

          saveTasksToStorage(mergedTasks);
          return mergedTasks;
        });
        break;

      case 'task_added':
        setTasks(prev => {
          const exists = prev.some(t => t.id === data.task.id);
          if (exists) return prev;
          const updated = [data.task, ...prev];
          saveTasksToStorage(updated);
          return updated;
        });
        break;

      case 'task_updated':
        setTasks(prev => {
          const updated = prev.map(t =>
            t.id === data.task_id
              ? { ...t, ...data.updates }
              : t
          );
          saveTasksToStorage(updated);
          return updated;
        });
        break;

      case 'task_deleted':
        setTasks(prev => {
          const updated = prev.filter(t => t.id !== data.task_id);
          saveTasksToStorage(updated);
          return updated;
        });
        break;

      case 'error':
        setError(data.message);
        break;
    }
  }, [saveTasksToStorage]);

  const connectToServer = useCallback(() => {
    const connect = () => {
      try {
        const wsUrl = 'ws://localhost:8765';
        ws.current = new WebSocket(wsUrl);

        ws.current.onopen = () => {
          console.log('Connected to Todo server');
          setIsConnected(true);
          setError('');
          loadTasksFromServer();
        };

        ws.current.onmessage = (event) => {
          const data = JSON.parse(event.data);
          handleServerMessage(data);
        };

        ws.current.onerror = (error) => {
          console.error('WebSocket error:', error);
          setIsConnected(false);
        };

        ws.current.onclose = () => {
          console.log('Disconnected from server');
          setIsConnected(false);

          setTimeout(() => {
            if (usernameRef.current) {
              console.log('Attempting to reconnect...');
              connect();
            }
          }, 5000);
        };
      } catch (err) {
        console.error('Failed to connect to server:', err);
        setIsConnected(false);
      }
    };

    connect();
  }, [loadTasksFromServer, handleServerMessage]);

  const handleLogin = useCallback((e) => {
    if (e) e.preventDefault();
    if (usernameInput.trim()) {
      const trimmedUsername = usernameInput.trim();
      setUsername(trimmedUsername);
      localStorage.setItem('todo_username', trimmedUsername);
      connectToServer();
    }
  }, [usernameInput, connectToServer]);

  const handleLogout = useCallback(() => {
    setUsername('');
    setUsernameInput('');
    setTasks([]);
    localStorage.removeItem('todo_username');
    if (ws.current) {
      ws.current.close();
    }
  }, []);

  const addTask = useCallback(() => {
    if (!newTask.trim() || !username) return;

    const task = {
      id: Date.now().toString(),
      title: newTask,
      status: 'pending',
      assignee: assignee || username,
      priority,
      dueDate,
      createdBy: username,
      createdAt: Date.now().toString()
    };

    const updatedTasks = [task, ...tasksRef.current];
    setTasks(updatedTasks);
    saveTasksToStorage(updatedTasks);

    sendMessage({
      action: 'add_task',
      task
    });

    setNewTask('');
    setAssignee('');
    setDueDate('');
  }, [newTask, assignee, priority, dueDate, username, sendMessage, saveTasksToStorage]);

  const toggleTask = useCallback((id) => {
    const task = tasksRef.current.find(t => t.id === id);
    if (!task) return;

    const newStatus = task.status === 'pending' ? 'completed' : 'pending';

    const updatedTasks = tasksRef.current.map(t =>
      t.id === id ? { ...t, status: newStatus } : t
    );
    setTasks(updatedTasks);
    saveTasksToStorage(updatedTasks);

    sendMessage({
      action: 'update_task',
      task_id: id,
      updates: { status: newStatus }
    });
  }, [sendMessage, saveTasksToStorage]);

  const deleteTask = useCallback((id) => {
    const updatedTasks = tasksRef.current.filter(t => t.id !== id);
    setTasks(updatedTasks);
    saveTasksToStorage(updatedTasks);

    sendMessage({
      action: 'delete_task',
      task_id: id
    });
  }, [sendMessage, saveTasksToStorage]);

  const refreshTasks = useCallback(async () => {
    await loadTasksFromStorage();
    if (isConnected) {
      loadTasksFromServer();
    }
  }, [isConnected, loadTasksFromServer, loadTasksFromStorage]);

  const getPriorityColor = (priority) => {
    const colors = {
      high: 'bg-red-100 text-red-800 border-red-300',
      medium: 'bg-yellow-100 text-yellow-800 border-yellow-300',
      low: 'bg-green-100 text-green-800 border-green-300'
    };
    return colors[priority] || colors.medium;
  };

  const isOverdue = (dueDate) => {
    if (!dueDate) return false;
    return new Date(dueDate) < new Date();
  };

  const filteredTasks = tasks.filter(task => {
    if (filter === 'my') return task.assignee === username;
    if (filter === 'completed') return task.status === 'completed';
    if (filter === 'pending') return task.status === 'pending';
    return true;
  });

  if (!username) {
    return (
      <div className="min-h-screen bg-gradient-to-br from-blue-50 to-indigo-100 flex items-center justify-center p-4">
        <div className="bg-white rounded-xl shadow-lg p-8 max-w-md w-full">
          <div className="text-center mb-6">
            <div className="flex items-center justify-center gap-2 mb-2">
              <h1 className="text-3xl font-bold text-gray-800">Team Tasks</h1>
            </div>
            <p className="text-gray-600">Real-time collaborative todo manager</p>
          </div>

          <div className="space-y-4">
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-2">
                Enter your name
              </label>
              <input
                type="text"
                value={usernameInput}
                onChange={(e) => setUsernameInput(e.target.value)}
                onKeyPress={(e) => e.key === 'Enter' && handleLogin()}
                className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
                placeholder="Your name"
                autoFocus
              />
            </div>

            <button
              onClick={handleLogin}
              className="w-full bg-blue-600 text-white py-2 rounded-lg hover:bg-blue-700 transition-colors font-medium"
            >
              Join Team
            </button>
          </div>

          <div className="mt-6 p-4 bg-blue-50 rounded-lg">
            <p className="text-sm text-gray-700 mb-2">
              <strong>Powered by IgnisDB</strong>
            </p>
            <p className="text-sm text-gray-200">
              Asyncio-based in-memory database with real-time sync
            </p>
          </div>

          {error && (
            <div className="mt-4 p-3 bg-red-50 border border-red-200 rounded-lg">
              <p className="text-sm text-red-600">{error}</p>
            </div>
          )}
        </div>
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-gradient-to-br from-blue-50 to-indigo-100 p-4">
      <div className="max-w-4xl mx-auto">
        <div className="bg-white rounded-xl shadow-lg p-6 mb-6">
          <div className="flex items-center justify-between mb-4">
            <div>
              <div className="flex items-center gap-2">
                <h1 className="text-3xl font-bold text-gray-800">Team Tasks</h1>
              </div>
              <p className="text-gray-600">Welcome, {username}!</p>
            </div>
            <div className="flex items-center gap-2">
              <button
                onClick={refreshTasks}
                className="p-2 text-blue-600 hover:bg-blue-50 rounded-lg transition-colors"
                title="Refresh tasks"
              >
                <RefreshCw size={20} />
              </button>
              {isConnected ? (
                <div className="flex items-center gap-2 px-3 py-1 bg-green-100 text-green-800 rounded-full text-sm">
                  <Wifi size={16} />
                  <span>Online</span>
                </div>
              ) : (
                <div className="flex items-center gap-2 px-3 py-1 bg-orange-100 text-orange-800 rounded-full text-sm">
                  <WifiOff size={16} />
                  <span>Offline</span>
                </div>
              )}
              <button
                onClick={handleLogout}
                className="px-3 py-1 text-sm text-gray-600 hover:text-gray-800 hover:bg-gray-100 rounded-lg transition-colors"
              >
                Logout
              </button>
            </div>
          </div>

          {syncStatus && (
            <div className="mb-4 p-2 bg-blue-50 border border-blue-200 rounded-lg">
              <p className="text-sm text-blue-600">{syncStatus}</p>
            </div>
          )}

          {error && (
            <div className="mb-4 p-3 bg-orange-50 border border-orange-200 rounded-lg flex items-center justify-between">
              <p className="text-sm text-orange-600">{error}</p>
              <button
                onClick={() => setError('')}
                className="text-orange-600 hover:text-orange-800"
              >
                ✕
              </button>
            </div>
          )}

          <div className="space-y-3">
            <input
              type="text"
              value={newTask}
              onChange={(e) => setNewTask(e.target.value)}
              placeholder="What needs to be done?"
              className="w-full px-4 py-3 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
              onKeyPress={(e) => e.key === 'Enter' && addTask()}
            />

            <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-3">
              <input
                type="text"
                value={assignee}
                onChange={(e) => setAssignee(e.target.value)}
                placeholder="Assign to..."
                className="px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
              />

              <select
                value={priority}
                onChange={(e) => setPriority(e.target.value)}
                className="px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
              >
                <option value="low">Low Priority</option>
                <option value="medium">Medium Priority</option>
                <option value="high">High Priority</option>
              </select>

              <input
                type="date"
                value={dueDate}
                onChange={(e) => setDueDate(e.target.value)}
                className="px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
              />

              <button
                onClick={addTask}
                className="bg-blue-600 text-white py-2 rounded-lg hover:bg-blue-700 transition-colors font-medium flex items-center justify-center gap-2"
              >
                <Plus size={20} />
                <span>Add Task</span>
              </button>
            </div>
          </div>
        </div>

        <div className="bg-white rounded-xl shadow-lg p-4 mb-6">
          <div className="flex flex-wrap gap-2">
            {['all', 'my', 'pending', 'completed'].map(f => (
              <button
                key={f}
                onClick={() => setFilter(f)}
                className={`px-4 py-2 rounded-lg font-medium transition-colors ${
                  filter === f
                    ? 'bg-blue-600 text-white'
                    : 'bg-gray-100 text-gray-700 hover:bg-gray-200'
                }`}
              >
                {f.charAt(0).toUpperCase() + f.slice(1)}
                {f === 'my' && ` (${tasks.filter(t => t.assignee === username).length})`}
              </button>
            ))}
          </div>
        </div>

        <div className="space-y-3">
          {isLoading ? (
            <div className="bg-white rounded-xl shadow-lg p-12 text-center">
              <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-600 mx-auto mb-4"></div>
              <p className="text-gray-500 text-lg">Loading tasks...</p>
            </div>
          ) : filteredTasks.length === 0 ? (
            <div className="bg-white rounded-xl shadow-lg p-12 text-center">
              <p className="text-gray-500 text-lg">No tasks found. Add one above!</p>
            </div>
          ) : (
            filteredTasks.map(task => (
              <div
                key={task.id}
                className={`bg-white rounded-xl shadow-lg p-4 transition-all hover:shadow-xl ${
                  task.status === 'completed' ? 'opacity-75' : ''
                }`}
              >
                <div className="flex items-start gap-4">
                  <button
                    onClick={() => toggleTask(task.id)}
                    className="mt-1 flex-shrink-0"
                  >
                    {task.status === 'completed' ? (
                      <CheckCircle2 className="text-green-600" size={24} />
                    ) : (
                      <Circle className="text-gray-400 hover:text-blue-600" size={24} />
                    )}
                  </button>

                  <div className="flex-1 min-w-0">
                    <h3 className={`text-lg font-medium break-words ${
                      task.status === 'completed' ? 'line-through text-gray-500' : 'text-gray-800'
                    }`}>
                      {task.title || 'Untitled Task'}
                    </h3>

                    <div className="flex flex-wrap items-center gap-3 mt-2">
                      <span className={`px-3 py-1 rounded-full text-xs font-medium border ${getPriorityColor(task.priority)}`}>
                        {task.priority || 'medium'}
                      </span>

                      <span className="flex items-center gap-1 text-sm text-gray-600">
                        <Users size={14} />
                        {task.assignee || 'Unassigned'}
                      </span>

                      {task.dueDate && (
                        <span className={`flex items-center gap-1 text-sm ${
                          isOverdue(task.dueDate) ? 'text-red-600 font-medium' : 'text-gray-600'
                        }`}>
                          {isOverdue(task.dueDate) && <AlertCircle size={14} />}
                          <Clock size={14} />
                          {new Date(task.dueDate).toLocaleDateString()}
                        </span>
                      )}

                      {task.createdBy && (
                        <span className="text-xs text-gray-400">
                          by {task.createdBy}
                        </span>
                      )}
                    </div>
                  </div>

                  <button
                    onClick={() => deleteTask(task.id)}
                    className="text-gray-400 hover:text-red-600 transition-colors flex-shrink-0"
                  >
                    <Trash2 size={20} />
                  </button>
                </div>
              </div>
            ))
          )}
        </div>

        {/* Footer */}
        <div className="mt-6 bg-white rounded-xl shadow-lg p-4">
          <div className="text-sm text-gray-600">
            <strong className="block mb-2">IgnisDB Commands Used:</strong>
            <ul className="space-y-1 font-mono text-xs">
              <li>LPUSH tasks:list task_id</li>
              <li>HSET task:id field value</li>
              <li>HGET task:id field</li>
              <li>LRANGE tasks:list 0 -1</li>
              <li>DELETE task:id</li>
              <li>EXPIRE task:id seconds</li>
            </ul>
          </div>
        </div>
      </div>
    </div>
  );
};

export default CollaborativeTodoManager;
