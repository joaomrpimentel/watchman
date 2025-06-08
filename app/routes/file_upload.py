# /watchman/app/routes/file_upload.py

from flask import Blueprint, request, jsonify, current_app
from app.services.document_saver import NotaFiscalSaver

file_upload_bp = Blueprint('file_upload', __name__)

@file_upload_bp.route('/nota-fiscal', methods=['POST'])
def handle_nota_fiscal_upload():
    current_saver_strategy = NotaFiscalSaver(current_app.config['UPLOAD_FOLDER'])

    try:
        file = request.files.get('nota_fiscal')
        save_info = current_saver_strategy.save(file)

        return jsonify({
            "message": "Nota fiscal salva com sucesso!",
            "filename": save_info["filename"],
            "filepath": save_info["filepath"]
        }), 200

    except ValueError as e:
        return jsonify({"message": str(e)}), 400
    except IOError as e:
        return jsonify({"message": str(e)}), 500
    except Exception as e:
        return jsonify({"message": f"Ocorreu um erro inesperado: {e}"}), 500