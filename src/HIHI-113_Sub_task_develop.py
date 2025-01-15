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
AWS_REGION = "us-east-1"
SNS_TOPIC_ARN = "arn:aws:sns:us-east-1:123456789012:MyTopic"

# Database connection
def get_db_connection():
    try:
        conn = connect(DATABASE_URL)
        return conn
    except Exception as e:
        logger.error(f"Database connection error: {e}")
        raise

# User Registration Service
class UserRegistrationService:
    def __init__(self):
        self.db_conn = get_db_connection()

    def register_user(self, user_data):
        try:
            if not self.validate_user_data(user_data):
                return {"success": False, "message": "Invalid user data"}

            hashed_password = bcrypt.hashpw(user_data['password'].encode('utf-8'), bcrypt.gensalt())
            with self.db_conn.cursor() as cursor:
                insert_query = sql.SQL("INSERT INTO users (username, email, password, created_at) VALUES (%s, %s, %s, %s)")
                cursor.execute(insert_query, (user_data['username'], user_data['email'], hashed_password, user_data['created_at']))
                self.db_conn.commit()
            return {"success": True, "message": "User registered successfully"}
        except Exception as e:
            logger.error(f"Error registering user: {e}")
            return {"success": False, "message": "Registration failed"}

    def validate_user_data(self, user_data):
        if not user_data.get('username') or not user_data.get('email') or not user_data.get('password'):
            return False
        if not re.match(r"[^@]+@[^@]+\.[^@]+", user_data['email']):
            return False
        if datetime.fromisoformat(user_data['created_at']) > datetime.now():
            return False
        return True

# Authentication Service
class AuthenticationService:
    def __init__(self):
        self.client = WebApplicationClient(client_id="your_client_id")

    def authenticate_user(self, token):
        try:
            uri, headers, body = self.client.prepare_token_request(OAUTH2_PROVIDER_URL, token=token)
            response = requests.post(uri, headers=headers, data=body)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Authentication error: {e}")
            return None

# Notification Service
class NotificationService:
    def __init__(self):
        self.sns_client = boto3.client('sns', region_name=AWS_REGION)

    def send_notification(self, message, subject):
        try:
            response = self.sns_client.publish(
                TopicArn=SNS_TOPIC_ARN,
                Message=message,
                Subject=subject
            )
            return response
        except ClientError as e:
            logger.error(f"Notification error: {e}")
            return None

