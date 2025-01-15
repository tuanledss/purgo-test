# Assuming the use of Flask for the Application Logic component and SQLAlchemy for the Data Storage component

from flask import Flask, request, jsonify
from flask_sqlalchemy import SQLAlchemy
import uuid
import logging
from sqlalchemy.exc import SQLAlchemyError

# Initialize Flask app and SQLAlchemy
app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://user:password@localhost/dbname'
db = SQLAlchemy(app)

# Configure logging
logging.basicConfig(level=logging.INFO)

# Database model
class User(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(50), nullable=False)
    email = db.Column(db.String(120), nullable=False)

# Helper function for input validation
def validate_request(data):
    try:
        uuid.UUID(data['userId'])
    except ValueError:
        return False, "Invalid userId"
    
    if data['action'] not in ['create', 'update']:
        return False, "Invalid action"
    
    if not isinstance(data['data']['field2'], int) or data['data']['field2'] < 0:
        return False, "Invalid integer"
    
    return True, ""

# Application Logic component
@app.route('/process', methods=['POST'])
def process_request():
    data = request.get_json()
    
    # Validate input
    is_valid, error_message = validate_request(data)
    if not is_valid:
        logging.error(f"Validation error: {error_message}")
        return jsonify({'message': error_message}), 400
    
    try:
        # Process data based on action
        if data['action'] == 'create':
            new_user = User(name=data['data']['field1'], email=f"{data['data']['field1']}@example.com")
            db.session.add(new_user)
            db.session.commit()
            return jsonify({'message': 'User created successfully'}), 200
        
        elif data['action'] == 'update':
            user = User.query.filter_by(id=data['data']['field2']).first()
            if user:
                user.name = data['data']['field1']
                db.session.commit()
                return jsonify({'message': 'User updated successfully'}), 200
            else:
                return jsonify({'message': 'User not found'}), 404
    
    except SQLAlchemyError as e:
        logging.error(f"Database error: {str(e)}")
        return jsonify({'message': 'Internal server error'}), 500

# Run the app
if __name__ == '__main__':
    app.run(debug=True)

