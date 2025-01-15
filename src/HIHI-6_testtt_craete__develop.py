from flask import Flask, request, jsonify
from flask_sqlalchemy import SQLAlchemy
import re
import logging

# Initialize Flask app and configure database
app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://user:password@localhost/dbname'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)

# Set up logging
logging.basicConfig(level=logging.INFO)

# Define User model
class User(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), nullable=False)
    age = db.Column(db.Integer, nullable=False)
    email = db.Column(db.String(100), nullable=False, unique=True)

# Create database tables
db.create_all()

# Helper function to validate email
def is_valid_email(email):
    return re.match(r"[^@]+@[^@]+\.[^@]+", email)

# API endpoint to create a new user
@app.route('/api/v1/resource', methods=['POST'])
def create_user():
    try:
        data = request.get_json()
        
        # Input validation
        if not data or 'name' not in data or 'age' not in data or 'email' not in data:
            return jsonify({'error': 'Missing required fields'}), 400
        
        name = data['name']
        age = data['age']
        email = data['email']
        
        if not isinstance(name, str) or not isinstance(age, int) or not is_valid_email(email):
            return jsonify({'error': 'Invalid input format'}), 400
        
        # Create new user
        new_user = User(name=name, age=age, email=email)
        db.session.add(new_user)
        db.session.commit()
        
        return jsonify({'success': 'User created successfully'}), 200
    
    except Exception as e:
        logging.error(f"Error creating user: {e}")
        return jsonify({'error': 'Internal server error'}), 500

# Run the app
if __name__ == '__main__':
    app.run(debug=True)
