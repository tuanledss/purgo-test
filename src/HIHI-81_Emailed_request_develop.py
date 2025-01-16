import logging
import re
import bcrypt
import json
from flask import Flask, request, jsonify
from flask_sqlalchemy import SQLAlchemy
from flask_httpauth import HTTPTokenAuth
from sqlalchemy.exc import SQLAlchemyError
from smtplib import SMTP
from twilio.rest import Client

# Initialize Flask app
app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://user:password@localhost/mydatabase'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

# Initialize database and authentication
db = SQLAlchemy(app)
auth = HTTPTokenAuth(scheme='Bearer')

# Configure logging
logging.basicConfig(level=logging.INFO)

# Database models
class User(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(15), unique=True, nullable=False)
    password_hash = db.Column(db.String(128), nullable=False)
    email = db.Column(db.String(120), unique=True, nullable=False)

class Notification(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    recipient = db.Column(db.String(120), nullable=False)
    message = db.Column(db.String(500), nullable=False)
    type = db.Column(db.String(10), nullable=False)

# Helper functions
def validate_user_data(data):
    if not re.match(r'^[a-zA-Z0-9]{5,15}$', data.get('username', '')):
        return False, "Username must be alphanumeric and between 5 and 15 characters"
    if not re.match(r'^(?=.*[A-Za-z])(?=.*\d)(?=.*[@$!%*#?&])[A-Za-z\d@$!%*#?&]{8,}$', data.get('password', '')):
        return False, "Password must contain at least one letter, one number, and one special character"
    if not re.match(r'^[^@]+@[^@]+\.[^@]+$', data.get('email', '')):
        return False, "Invalid email format"
    return True, ""

def hash_password(password):
    return bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt())

def send_notification(recipient, message, type):
    try:
        if type == 'email':
            with SMTP('smtp.example.com') as smtp:
                smtp.sendmail('from@example.com', recipient, message)
        elif type == 'sms':
            client = Client('ACCOUNT_SID', 'AUTH_TOKEN')
            client.messages.create(to=recipient, from_='+1234567890', body=message)
        return True
    except Exception as e:
        logging.error(f"Failed to send notification: {e}")
        return False

# API endpoints
@app.route('/register', methods=['POST'])
def register():
    data = request.json
    valid, message = validate_user_data(data)
    if not valid:
        return jsonify({'success': False, 'message': message}), 400

    try:
        user = User(username=data['username'], password_hash=hash_password(data['password']), email=data['email'])
        db.session.add(user)
        db.session.commit()
        return jsonify({'success': True, 'message': "User registered successfully"}), 201
    except SQLAlchemyError as e:
        logging.error(f"Database error: {e}")
        return jsonify({'success': False, 'message': "Internal server error"}), 500

@app.route('/login', methods=['POST'])
def login():
    data = request.json
    user = User.query.filter_by(username=data.get('username')).first()
    if user and bcrypt.checkpw(data.get('password').encode('utf-8'), user.password_hash):
        token = 'some_generated_token'  # Token generation logic
        return jsonify({'success': True, 'token': token}), 200
    return jsonify({'success': False, 'message': "Invalid credentials"}), 401

@app.route('/notify', methods=['POST'])
@auth.login_required
def notify():
    data = request.json
    if send_notification(data['recipient'], data['message'], data['type']):
        return jsonify({'success': True, 'message': "Notification sent successfully"}), 200
    return jsonify({'success': False, 'message': "Failed to send notification"}), 500

@auth.verify_token
def verify_token(token):
    # Token verification logic
    return True

if __name__ == '__main__':
    db.create_all()
    app.run(debug=True)
