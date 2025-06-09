# /watchman/app/services/document_saver.py

import os
import abc

class DocumentSaver(abc.ABC):
    def __init__(self, upload_folder):
        self.upload_folder = upload_folder

    @abc.abstractmethod
    def save(self, file_storage):
        pass

class NotaFiscalSaver(DocumentSaver):
    def save(self, file_storage):
        if not file_storage or file_storage.filename == '':
            raise ValueError("Arquivo inválido ou nome de arquivo vazio.")

        filename_to_save = file_storage.filename
        filepath = os.path.join(self.upload_folder, filename_to_save)
        
        try:
            file_storage.save(filepath)
            return {"filename": filename_to_save, "filepath": filepath}
        except IOError as e:
            raise IOError(f"Erro ao salvar a nota fiscal: {e}")
        except Exception as e:
            raise Exception(f"Erro inesperado ao processar a nota fiscal: {e}")