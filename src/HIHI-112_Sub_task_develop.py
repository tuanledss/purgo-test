from flask import Flask, request, jsonify
import psycopg2
import requests
import logging
import re

app = Flask(__name__)

# Configure logging
logging.basicConfig(level=logging.INFO)

# Database connection setup
def get_db_connection():
    try:
        conn = psycopg2.connect(
            dbname='your_db_name',
            user='your_db_user',
            password='your_db_password',
            host='your_db_host',
            port='your_db_port'
        )
        return conn
    except Exception as e:
        logging.error(f"Database connection error: {e}")
        raise

# Validate email format
def is_valid_email(email):
    return re.match(r"[^@]+@[^@]+\.[^@]+", email) is not None

# Process user data
def process_user_data(user_data):
    if not user_data['userId']:
        return {'status': 400, 'message': 'User ID cannot be empty'}
    if not is_valid_email(user_data['email']):
        return {'status': 400, 'message': 'Invalid email format'}
    if user_data['role'] not in ['admin', 'user', 'guest']:
        return {'status': 400, 'message': 'Unsupported role'}
    
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO users (userId, name, email, role) VALUES (%s, %s, %s, %s)",
            (user_data['userId'], user_data['name'], user_data['email'], user_data['role'])
        )
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        logging.error(f"Error processing user data: {e}")
        return {'status': 500, 'message': 'Internal server error'}
    
    return {'status': 200, 'message': 'User data processed successfully'}

# RESTful API endpoint
@app.route('/api/user', methods=['POST'])
def api_user():
    try:
        user_data = request.get_json()
        response = process_user_data(user_data)
        return jsonify(response), response['status']
    except Exception as e:
        logging.error(f"API error: {e}")
        return jsonify({'status': 500, 'message': 'Internal server error'}), 500

# OAuth 2.0 authentication
def authenticate_user(token):
    try:
        response = requests.get('https://oauth2provider.com/tokeninfo', params={'access_token': token})
        if response.status_code == 200:
            return response.json()
        else:
            return None
    except Exception as e:
        logging.error(f"Authentication error: {e}")
        return None

# Notification service integration
def send_notification(user_id, message):
    try:
        sns_response = requests.post(
            'https://sns.amazonaws.com/send',
            json={'userId': user_id, 'message': message}
        )
        if sns_response.status_code == 200:
            logging.info(f"Notification sent to {user_id}")
        else:
            logging.error(f"Failed to send notification to {user_id}")
    except Exception as e:
        logging.error(f"Notification error: {e}")

if __name__ == '__main__':
    app.run(debug=True)
