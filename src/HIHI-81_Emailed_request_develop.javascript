// server.js
const express = require('express');
const bodyParser = require('body-parser');
const { processRequest } = require('./services/requestService');
const { validateInput } = require('./middleware/validation');
const app = express();

app.use(bodyParser.json());

app.post('/api/request', validateInput, async (req, res) => {
  try {
    const response = await processRequest(req.body);
    res.status(200).json(response);
  } catch (error) {
    console.error('Error processing request:', error);
    res.status(500).json({ status: 'error', message: 'Internal server error' });
  }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});

// middleware/validation.js
const { isUUID, isValidAction } = require('../utils/validators');

function validateInput(req, res, next) {
  const { userId, action } = req.body;
  if (!userId || !isUUID(userId)) {
    return res.status(400).json({ status: 'error', message: 'Invalid userId format' });
  }
  if (!action || !isValidAction(action)) {
    return res.status(400).json({ status: 'error', message: 'Invalid action' });
  }
  next();
}

module.exports = { validateInput };

// services/requestService.js
const axios = require('axios');
const { queryDatabase } = require('../database/db');
const { transformData } = require('../utils/transformers');

async function processRequest(data) {
  const { userId, action } = data;
  const dbData = await queryDatabase(userId, action);
  const transformedData = transformData(dbData);

  if (action === 'fetchData') {
    const externalData = await axios.get(`https://external.api/data?userId=${userId}`);
    return { status: 'success', data: { ...transformedData, external: externalData.data } };
  }

  return { status: 'success', data: transformedData };
}

module.exports = { processRequest };

// database/db.js
const { Pool } = require('pg');
const pool = new Pool();

async function queryDatabase(userId, action) {
  try {
    const result = await pool.query('SELECT * FROM users WHERE id = $1', [userId]);
    return result.rows[0];
  } catch (error) {
    console.error('Database query error:', error);
    throw new Error('Database query failed');
  }
}

module.exports = { queryDatabase };

// utils/validators.js
function isUUID(value) {
  const uuidRegex = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
  return uuidRegex.test(value);
}

function isValidAction(action) {
  const validActions = ['fetchData', 'updateData', 'deleteData', 'createData'];
  return validActions.includes(action.toLowerCase());
}

module.exports = { isUUID, isValidAction };

// utils/transformers.js
function transformData(data) {
  // Example transformation logic
  return {
    id: data.id,
    name: data.name.toUpperCase(),
    email: data.email.toLowerCase(),
  };
}

module.exports = { transformData };
