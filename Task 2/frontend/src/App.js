import React, { useState, useEffect } from 'react';
import './index.css';

function App() {
  const [tasks, setTasks] = useState([]);
  const [newTask, setNewTask] = useState('');

  // Fetch tasks
  useEffect(() => {
    fetch('http://localhost:5000/api/tasks')
      .then((res) => res.json())
      .then((data) => setTasks(data));
  }, []);

  // Add task
  const addTask = async (e) => {
    e.preventDefault();
    if (!newTask.trim()) return;
    const res = await fetch('http://localhost:5000/api/tasks', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ title: newTask }),
    });
    const task = await res.json();
    setTasks([...tasks, task]);
    setNewTask('');
  };

  // Delete task
  const deleteTask = async (id) => {
    await fetch(`http://localhost:5000/api/tasks/${id}`, { method: 'DELETE' });
    setTasks(tasks.filter((task) => task._id !== id));
  };

  return (
    <div className="app-bg">
      <div className="task-container">
        <h1 className="app-title">Task Manager - Live Reload Tedkddddd</h1>
        <form onSubmit={addTask} className="task-form">
          <input
            type="text"
            value={newTask}
            onChange={(e) => setNewTask(e.target.value)}
            placeholder="Add a new task"
            className="task-input"
          />
          <button
            type="submit"
            className="add-btn"
          >
            Add Task
          </button>
        </form>
        <ul className="task-list">
          {tasks.map((task) => (
            <li
              key={task._id}
              className="task-item"
            >
              <span>{task.title}</span>
              <button
                onClick={() => deleteTask(task._id)}
                className="delete-btn"
              >
                Delete
              </button>
            </li>
          ))}
        </ul>
      </div>
    </div>
  );
}

export default App;