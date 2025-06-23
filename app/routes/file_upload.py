# /watchman/app/routes/file_upload.py

from flask import Blueprint, request, jsonify, current_app, abort
from app.services.document_saver import NotaFiscalSaver
from app.services.nota_fiscal_service import NotaFiscalService

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

@file_upload_bp.route('/nota-fiscal/<string:identifier>', methods=['GET'])
def get_nota_fiscal_data(identifier):
    # Instancia o serviço de nota fiscal. current_app é acessível dentro do contexto da requisição.
    service = NotaFiscalService(current_app.config['POSTGRES_CONFIG'])
    try:
        nfe_data = service.get_nota_fiscal_by_identifier(identifier)
        if not nfe_data:
            abort(404, description="Nota Fiscal não encontrada.")
        
        return jsonify(nfe_data), 200
    except RuntimeError as e: # Erros de conexão ou configuração do DB
        return jsonify({"message": str(e)}), 500
    except Exception as e:
        current_app.logger.exception(f"Ocorreu um erro inesperado ao buscar NF-e: {e}")
        return jsonify({"message": f"Ocorreu um erro inesperado: {e}"}), 500

@file_upload_bp.route('/notas-fiscais', methods=['GET'])
def list_notas_fiscais():
    service = NotaFiscalService(current_app.config['POSTGRES_CONFIG'])
    try:
        nfe_summaries = service.list_notas_fiscais_summary()
        return jsonify(nfe_summaries), 200
    except RuntimeError as e: # Erros de conexão ou configuração do DB
        return jsonify({"message": str(e)}), 500
    except Exception as e:
        current_app.logger.exception(f"Ocorreu um erro inesperado ao listar NF-es: {e}")
        return jsonify({"message": f"Ocorreu um erro inesperado: {e}"}), 500