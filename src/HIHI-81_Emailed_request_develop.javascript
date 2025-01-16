// Backend Service Implementation using Node.js and Express.js

const express = require('express');
const bodyParser = require('body-parser');
const { Pool } = require('pg');
const bcrypt = require('bcrypt');
const axios = require('axios');
const { v4: uuidv4 } = require('uuid');
const app = express();
const port = 5000;

// Database connection setup
const pool = new Pool({
  user: 'your_db_user',
  host: 'localhost',
  database: 'your_db_name',
  password: 'your_db_password',
  port: 5432,
});

// Middleware
app.use(bodyParser.json());

// Error handling middleware
app.use((err, req, res, next) => {
  console.error(err.stack);
  res.status(500).send('Something broke!');
});

// Input validation function
const validateUserData = (data) => {
  const { name, email, phoneNumber } = data;
  if (!name || !email || !phoneNumber) {
    throw new Error('Missing required fields');
  }
  const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
  if (!emailRegex.test(email)) {
    throw new Error('Invalid email format');
  }
  const phoneRegex = /^\+?[1-9]\d{1,14}$/;
  if (!phoneRegex.test(phoneNumber)) {
    throw new Error('Invalid phone number format');
  }
};

// Create user endpoint
app.post('/api/users', async (req, res) => {
  try {
    const userData = req.body;
    validateUserData(userData);

    const hashedPassword = await bcrypt.hash('defaultPassword', 10);
    const userId = uuidv4();

    const query = 'INSERT INTO Users (userId, name, email, phoneNumber, password) VALUES ($1, $2, $3, $4, $5)';
    const values = [userId, userData.name, userData.email, userData.phoneNumber, hashedPassword];

    await pool.query(query, values);

    res.status(201).json({ userId, email: userData.email });
  } catch (error) {
    if (error.message.includes('Invalid')) {
      res.status(400).send(error.message);
    } else {
      res.status(500).send('Internal Server Error');
    }
  }
});

// OAuth 2.0 Authentication
app.post('/api/authenticate', async (req, res) => {
  try {
    const { code } = req.body;
    const response = await axios.post('https://oauth2.example.com/token', {
      code,
      client_id: 'your_client_id',
      client_secret: 'your_client_secret',
      redirect_uri: 'your_redirect_uri',
      grant_type: 'authorization_code',
    });
    res.json(response.data);
  } catch (error) {
    res.status(500).send('Authentication failed');
  }
});

// Notification Service Integration
const sendNotification = async (recipient, message, type) => {
  try {
    const url = type === 'sms' ? 'https://api.twilio.com/send' : 'https://api.sendgrid.com/send';
    await axios.post(url, { recipient, message });
  } catch (error) {
    console.error('Notification failed', error);
  }
};

// Start server
app.listen(port, () => {
  console.log(`Server running at http://localhost:${port}`);
});
