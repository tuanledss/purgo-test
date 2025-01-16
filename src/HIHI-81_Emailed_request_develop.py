import logging
import re
import bcrypt
import json
import requests
from flask import Flask, request, jsonify
from sqlalchemy import create_engine, Column, String, Integer
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Database setup
DATABASE_URL = "postgresql://user:password@localhost/mydatabase"
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)
Base = declarative_base()

# Database models
class User(Base):
    __tablename__ = 'users'
    id = Column(Integer, primary_key=True)
    username = Column(String, unique=True, nullable=False)
    password = Column(String, nullable=False)
    email = Column(String, unique=True, nullable=False)

Base.metadata.create_all(engine)

# Flask app setup
app = Flask(__name__)

# Helper functions
def validate_user_data(data):
    if not re.match(r'^[a-zA-Z0-9]{5,15}$', data['username']):
        raise ValueError("Invalid username")
    if not re.match(r'^(?=.*[0-9])(?=.*[!@#$%^&*])[a-zA-Z0-9!@#$%^&*]{8,}$', data['password']):
        raise ValueError("Invalid password")
    if not re.match(r'^[^@]+@[^@]+\.[^@]+$', data['email']):
        raise ValueError("Invalid email")

def hash_password(password):
    return bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt())

def authenticate_user(username, password):
    session = Session()
    user = session.query(User).filter_by(username=username).first()
    session.close()
    if user and bcrypt.checkpw(password.encode('utf-8'), user.password.encode('utf-8')):
        return True
    return False

def send_notification(email, message):
    # Placeholder for sending email via SMTP or SMS via Twilio
    logger.info(f"Sending notification to {email}: {message}")

# API endpoints
@app.route('/register', methods=['POST'])
def register():
    try:
        data = request.json
        validate_user_data(data)
        hashed_password = hash_password(data['password'])
        session = Session()
        new_user = User(username=data['username'], password=hashed_password, email=data['email'])
        session.add(new_user)
        session.commit()
        session.close()
        send_notification(data['email'], "Registration successful")
        return jsonify({"success": True, "message": "User registered successfully"}), 201
    except ValueError as e:
        logger.error(f"Validation error: {e}")
        return jsonify({"success": False, "message": str(e)}), 400
    except Exception as e:
        logger.error(f"Error during registration: {e}")
        return jsonify({"success": False, "message": "Internal server error"}), 500

@app.route('/login', methods=['POST'])
def login():
    try:
        data = request.json
        if authenticate_user(data['username'], data['password']):
            # Simulate token generation
            token = "fake-jwt-token"
            return jsonify({"success": True, "token": token}), 200
        else:
            return jsonify({"success": False, "message": "Invalid credentials"}), 401
    except Exception as e:
        logger.error(f"Error during login: {e}")
        return jsonify({"success": False, "message": "Internal server error"}), 500

if __name__ == '__main__':
    app.run(debug=True)
