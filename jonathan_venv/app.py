from flask import Flask
import requests
import os 
import time
import paramiko
import json 
import csv
from dotenv import load_dotenv
from datetime import datetime

# Load environment variables from .env file
load_dotenv()

app = Flask(__name__)

# SFTP Variables
HOST = os.getenv('HOST')
PORT = os.getenv('PORT')
PASSWORD = os.getenv('PASSWORD')
SFTP_USERNAME = os.getenv('SFTP_USERNAME')
SFTP_DIR = os.getenv('SFTP_DIR')


# login Variables
LOGIN_USERNAME = os.getenv('LOGIN_USERNAME')
LOGIN_PASSWORD = os.getenv('LOGIN_PASSWORD')
SYSTEM_ID = os.getenv('SYSTEM_ID')

# API endpoints
LOGIN_URL = os.getenv('LOGIN_URL')
UPLOAD_URL = os.getenv('UPLOAD_URL')

# Create date to add to files
now =  datetime.now()
month_day_year = now.strftime("%m%d%y")

#Connect to SFTP server
ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect(hostname=HOST,port=PORT,username=SFTP_USERNAME,password=PASSWORD)

sftp = ssh.open_sftp()

# Get root directory
root_directory = sftp.normalize('.')
print("Root directory:", root_directory)

# Function to create directory if it doesn't exist
def create_directory(directory):
    try:
        sftp.stat(directory)
        print(f"Directory '{directory}' already exists.")
    except FileNotFoundError:
        print(f"Creating directory: {directory}")
        sftp.mkdir(directory)
    except Exception as e:
        print(f"Error creating directory '{directory}': {e}")

UPLOADED_DIR = os.path.join(root_directory, SFTP_DIR,'expected_arrival_uploads').replace('\\', '/')
ERRORS_DIR = os.path.join(root_directory, SFTP_DIR, 'expected_arrival_upload_errors').replace('\\', '/')
BASE_ROOT = os.path.join(root_directory, SFTP_DIR)

# Create "errors" and "uploaded" directories if they don't exist
create_directory(ERRORS_DIR)
create_directory(UPLOADED_DIR)

# List to store uploaded filenames
uploaded_files = []

# Function to authenticate user and obtain bearer token
def authenticate_user():
    credentials = {'userName': LOGIN_USERNAME, 'password': LOGIN_PASSWORD, 'systemId': SYSTEM_ID}
    response = requests.post(LOGIN_URL, json=credentials)
    if response.status_code == 200:
        print("User authenticated successfully")
        print("API Response:", response.json()['Token'])
        return response.json()['Token']
    else:
        print("Failed to authenticate user. Status code:", response.status_code)
        print("Error message:", response.text)
        return None
    
# Function to monitor directory for new files
def monitor_directory():
    files = sftp.listdir(SFTP_DIR)
    print('got the files')
    return files

# Function to upload CSV file via API using bearer token
def upload_csv_file(filename, bearer_token):
    # if filename.endswith('.csv') and filename not in unuploaded_files:
    if filename.endswith('.csv') and filename not in uploaded_files:
        file_path = os.path.join(root_directory, SFTP_DIR, filename)
        print(file_path)
        with sftp.file(file_path, 'r') as file:  # Open file in text mode ('r' for read mode)
            try:
                # Read CSV file and extract required information
                csv_reader = csv.DictReader(file)
                for row in csv_reader:
                    # Construct payload for API request
                    payload = {
                        "ownerID": row.get('Owner ID'),
                        "tradingPartnerID": row.get('Trading Partner ID'),
                        "foreignSystemKey": row.get('Foreign System Key'),
                        "warehouseID": row.get('Warehouse ID'),
                        "anticipatedArrivalDatetime": row.get('Anticipated Arrival Date Time(MM/DD/YYYY)'),
                        "ourPurchaseOrder": row.get('Our PO'),
                        "billOfLadingNumber": row.get('Bill of Lading')
                    }
                    # Convert payload to JSON string
                    payload_json = json.dumps(payload)
                    # Send API request
                    headers = {'Authorization': 'Bearer ' + bearer_token, 'Content-Type': 'application/json'}
                    print(payload_json)
                    response = requests.post(UPLOAD_URL, data=payload_json, headers=headers)
                    if response.status_code == 200:
                        print("CSV file uploaded successfully")
                        print("API Response:", response.text)  # Print the response content
                        uploaded_files.append(filename)  # Add filename to uploaded list
                        move_to_uploaded_dir(UPLOADED_DIR, filename)
                        delete_file(file_path)  # Delete file from root directory
                    else:
                        print("Failed to upload CSV file. Status code:", response.status_code)
                        move_to_errors_dir(ERRORS_DIR,filename)
                        delete_file(file_path)  # Delete file from root directory
            except Exception as e:
                print("An error occurred while processing the file: ", str(e))
                print('errors dir: ', ERRORS_DIR)
                move_to_errors_dir(ERRORS_DIR, filename)
                delete_file(file_path)  # Delete file from root directory
    else:
        print("Skipping already uploaded or non-CSV file:", filename)

# Function to move file to uploaded directory
def move_to_uploaded_dir(file_path, filename):
    uploads_dir_path = os.path.join(root_directory,file_path, filename)
    sftp.rename(os.path.join(BASE_ROOT, filename), uploads_dir_path)
    print('finished moving to uploads directory')
    print("Moved file to 'uploaded' directory:", filename)

# Function to move file to errors directory
def move_to_errors_dir(file_path, filename):
    errors_dir_path = os.path.join(root_directory,file_path, filename)
    sftp.rename(os.path.join(BASE_ROOT, filename), errors_dir_path)
    print('finished moving to errors directory')
    print("Moved file to 'errors' directory:", filename)

# Function to delete file from root directory
def delete_file(file_path):
    print("Before Deleting file from root directory:", file_path)
    print("Deleted file from root directory:", file_path)

# Main function
@app.route('/')
def main():
    bearer_token = authenticate_user()
    if bearer_token:
        while True:
            files = monitor_directory()
            print(files)
            for filename in files:
                if filename not in ['.', '..']:  # ignore parent directories
                    upload_csv_file(filename, bearer_token)
                    # Optionally, move or delete the file after upload
                    # sftp.remove(os.path.join(SFTP_DIR, filename))
            time.sleep(60)  # Check every 60 seconds

if __name__ == '__main__':
    app.run(debug=True)

# TODO: Change upload code to 