// Import necessary modules
const express = require('express');
const axios = require('axios');
const { Pool } = require('pg');
const { v4: uuidv4 } = require('uuid');
const jwt = require('jsonwebtoken');
const winston = require('winston');

// Initialize Express app
const app = express();
app.use(express.json());

// Configure PostgreSQL connection
const pool = new Pool({
  user: 'your_db_user',
  host: 'localhost',
  database: 'your_db_name',
  password: 'your_db_password',
  port: 5432,
});

// Configure logger
const logger = winston.createLogger({
  level: 'info',
  format: winston.format.json(),
  transports: [
    new winston.transports.Console(),
    new winston.transports.File({ filename: 'app.log' }),
  ],
});

// Middleware for JWT authentication
const authenticateJWT = (req, res, next) => {
  const token = req.header('Authorization');
  if (!token) return res.status(401).send({ error: 'Access denied' });

  try {
    const verified = jwt.verify(token, 'your_jwt_secret');
    req.user = verified;
    next();
  } catch (err) {
    res.status(400).send({ error: 'Invalid token' });
  }
};

// Input validation
const validateInput = (data) => {
  if (!data.userId || !uuidv4().test(data.userId)) {
    throw new Error('Invalid userId format');
  }
  if (!data.action || !['fetchData', 'updateData', 'deleteData', 'createData'].includes(data.action.toLowerCase())) {
    throw new Error('Invalid action');
  }
};

// API endpoint
app.post('/api/data', authenticateJWT, async (req, res) => {
  try {
    validateInput(req.body);
    const { userId, action } = req.body;

    // Database query
    const dbResponse = await pool.query('SELECT * FROM users WHERE id = $1', [userId]);
    if (dbResponse.rows.length === 0) {
      return res.status(404).send({ error: 'User not found' });
    }

    // External API call
    const apiResponse = await axios.get(`https://external.api/data?userId=${userId}`);
    const combinedData = { ...dbResponse.rows[0], externalData: apiResponse.data };

    res.send({ status: 'success', data: combinedData });
  } catch (error) {
    logger.error(error.message);
    res.status(400).send({ status: 'error', message: error.message });
  }
});

// Start server
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  logger.info(`Server running on port ${PORT}`);
});
