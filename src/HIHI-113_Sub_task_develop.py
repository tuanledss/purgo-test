import logging
import re
from datetime import datetime
from psycopg2 import connect, sql
from oauthlib.oauth2 import WebApplicationClient
import requests
import boto3
from botocore.exceptions import ClientError
import bcrypt

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Constants
DATABASE_URL = "postgresql://user:password@localhost/dbname"
OAUTH2_PROVIDER_URL = "https://oauth2provider.com"
AWS_REGION = "us-west-2"
SNS_TOPIC_ARN = "arn:aws:sns:us-west-2:123456789012:MyTopic"

# Database connection
def get_db_connection():
    try:
        conn = connect(DATABASE_URL)
        return conn
    except Exception as e:
        logger.error("Database connection failed: %s", e)
        raise

# User Registration Service
class UserRegistrationService:
    def __init__(self):
        self.client = WebApplicationClient(client_id="your_client_id")
        self.sns_client = boto3.client('sns', region_name=AWS_REGION)

    def register_user(self, user_data):
        try:
            # Input validation
            if not self.validate_user_data(user_data):
                return {"success": False, "message": "Invalid input data"}

            # Hash password
            hashed_password = bcrypt.hashpw(user_data['password'].encode('utf-8'), bcrypt.gensalt())

            # Store user in database
            with get_db_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        sql.SQL("INSERT INTO users (username, email, password, created_at) VALUES (%s, %s, %s, %s)"),
                        (user_data['username'], user_data['email'], hashed_password, user_data['created_at'])
                    )
                    conn.commit()

            # Send notification
            self.send_notification(user_data['email'])

            return {"success": True, "message": "User registered successfully"}
        except Exception as e:
            logger.error("User registration failed: %s", e)
            return {"success": False, "message": "Registration failed"}

    def validate_user_data(self, user_data):
        if not user_data.get('username') or not user_data.get('email') or not user_data.get('password'):
            return False
        if not re.match(r"[^@]+@[^@]+\.[^@]+", user_data['email']):
            return False
        if not user_data['created_at'] or datetime.fromisoformat(user_data['created_at']) > datetime.now():
            return False
        return True

    def send_notification(self, email):
        try:
            response = self.sns_client.publish(
                TopicArn=SNS_TOPIC_ARN,
                Message=f"User {email} registered successfully.",
                Subject="User Registration"
            )
            logger.info("Notification sent: %s", response)
        except ClientError as e:
            logger.error("Failed to send notification: %s", e)

# OAuth2 Authentication
def authenticate_user(token):
    try:
        response = requests.get(OAUTH2_PROVIDER_URL, headers={"Authorization": f"Bearer {token}"})
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.error("Authentication failed: %s", e)
        return None
