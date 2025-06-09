# /watchman/app/__init__.py

import os
from flask import Flask

def create_app():
    app = Flask(__name__)

    upload_base_folder = os.path.join(os.getcwd(), 'data', 'raw')
    os.makedirs(upload_base_folder, exist_ok=True)
    app.config['UPLOAD_FOLDER'] = upload_base_folder

    from app.routes.file_upload import file_upload_bp
    app.register_blueprint(file_upload_bp)

    return app