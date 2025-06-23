# /watchman/app/__init__.py

import os
from flask import Flask
from dotenv import load_dotenv

def create_app():
    load_dotenv() 

    app = Flask(__name__)

    upload_base_folder = os.path.join(os.getcwd(), 'data', 'raw')
    os.makedirs(upload_base_folder, exist_ok=True)
    app.config['UPLOAD_FOLDER'] = upload_base_folder

    app.config['POSTGRES_CONFIG'] = {
        'host': os.getenv('DB_HOST', 'postgres'), 
        'port': int(os.getenv('DB_PORT', 5432)), 
        'database': os.getenv('AIRFLOW_METADATA_DB_NAME', 'airflow'),
        'user': os.getenv('POSTGRES_USER', 'postgres'), 
        'password': os.getenv('POSTGRES_PASSWORD', 'airflow') 
    }

    from app.routes.file_upload import file_upload_bp
    app.register_blueprint(file_upload_bp, url_prefix='/api')

    return app