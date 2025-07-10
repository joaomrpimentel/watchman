import os
import shutil
import xml.etree.ElementTree as ET
from datetime import datetime
from typing import Dict, List, Optional, Any

from nfe_builder import Builder

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.postgres_hook import PostgresHook


class NFeParserOperator(BaseOperator):
    """
    Operador para extrair dados de arquivos XML de NFe, salvar no PostgreSQL
    e mover arquivos processados para outra pasta, utilizando um esquema otimizado.

    :param source_folder: Caminho da pasta de origem dos arquivos XML
    :param destination_folder: Caminho da pasta de destino para os arquivos processados
    :param postgres_conn_id: ID da conexão do PostgreSQL no Airflow
    """

    @apply_defaults
    def __init__(
        self,
        source_folder: str,
        destination_folder: str,
        postgres_conn_id: str,
        table_name: str = 'nfe',
        *args, **kwargs
    ) -> None:
        super().__init__(*args, **kwargs)
        self.source_folder = source_folder
        self.destination_folder = destination_folder
        self.postgres_conn_id = postgres_conn_id
        self.table_name = table_name
        self.ns = {'nfe': 'http://www.portalfiscal.inf.br/nfe'}

    def execute(self, context: Dict) -> None:
        """
        Executa o operador.

        :param context: Contexto de execução da tarefa
        :return: None
        """
        self.log.info(f"Verificando arquivos na pasta: {self.source_folder}")

        os.makedirs(self.source_folder, exist_ok=True)
        os.makedirs(self.destination_folder, exist_ok=True)

        xml_files = [f for f in os.listdir(self.source_folder)
                     if f.lower().endswith('.xml')]

        if not xml_files:
            self.log.info("Nenhum arquivo XML encontrado na pasta de origem.")
            return

        self.log.info(f"Encontrados {len(xml_files)} arquivos XML para processar.")

        pg_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)

        for xml_file in xml_files:
            xml_path = os.path.join(self.source_folder, xml_file)
            self.log.info(f"Processando arquivo: {xml_path}")

            nfe_id = None
            xml_content = None
            try:
                with open(xml_path, 'r', encoding='utf-8') as f:
                    xml_content = f.read()

                extracted_data = self.extrair_dados_nfe(xml_content, xml_file)

                if not extracted_data:
                    self.log.error(f"Não foi possível extrair dados do arquivo: {xml_path}. O arquivo não será movido.")
                    continue

                nfe_id = self.save_to_postgres(pg_hook, extracted_data)

                if nfe_id is None:
                    self.log.warning(f"A inserção no banco de dados falhou para o arquivo {xml_file} (provavelmente duplicado). O arquivo não será movido.")
                    continue

                dest_path = os.path.join(self.destination_folder, xml_file)
                shutil.move(xml_path, dest_path)

                self.log.info(f"Arquivo processado e movido para: {dest_path}")
                self.save_processing_status(pg_hook, nfe_id, xml_file, 'SUCESSO', 'NFe processada com sucesso.', xml_content)

            except Exception as e:
                self.log.error(f"Erro crítico ao processar o arquivo {xml_path}: {str(e)}", exc_info=True)
                if nfe_id and pg_hook:
                    self.save_processing_status(pg_hook, nfe_id, xml_file, 'ERRO_CRITICO', str(e), xml_content)

    def parse_date_to_timestamp(self, date_str: str) -> Optional[datetime]:
        if not date_str:
            return None
        try:
            # Remove o timezone para usar o fromisoformat que é mais flexível
            if '+' in date_str or (date_str.count('-') > 2 and not date_str.endswith('Z')):
                 date_str = date_str.rsplit('-', 1)[0]
            
            return datetime.fromisoformat(date_str)
        except Exception as e:
            self.log.error(f"Erro ao converter data '{date_str}': {str(e)}")
            return None

    def extrair_dados_nfe(self, xml_content: str, xml_file_name: str) -> Optional[Dict[str, Any]]:
        try:
            builder = Builder()

            root = ET.fromstring(xml_content)
            extracted_data = builder.build_ProductNfe(root)

            return extracted_data

        except Exception as e:
            self.log.error(f"Erro detalhado ao extrair dados do XML: {str(e)}", exc_info=True)
            return None

    def _safe_float(self, value: Optional[str]) -> Optional[float]:
        try:
            return float(value) if value is not None else None
        except (ValueError, TypeError):
            return None

    def _safe_int(self, value: Optional[str]) -> Optional[int]:
        try:
            return int(float(value)) if value is not None else None
        except (ValueError, TypeError):
            return None

    def _insert_or_get_pessoa_id(self, pg_hook: PostgresHook, pessoa_data: Dict[str, Any]) -> Optional[int]:
        cnpj = pessoa_data.get('cnpj')
        cpf = pessoa_data.get('cpf')

        if not cnpj and not cpf:
            self.log.warning(f"Pessoa '{pessoa_data.get('nome')}' sem CNPJ/CPF. Inserção não é permitida pelo schema. Pulando.")
            return None

        # Checa se a pessoa já existe
        if cnpj:
            result = pg_hook.get_first("SELECT id FROM nfe.pessoa WHERE cnpj = %s", (cnpj,))
            if result: return result[0]
        elif cpf:
            result = pg_hook.get_first("SELECT id FROM nfe.pessoa WHERE cpf = %s", (cpf,))
            if result: return result[0]

        # Insere nova pessoa
        valid_data = {k: v for k, v in pessoa_data.items() if v is not None}
        if not valid_data.get('nome'):
            self.log.warning(f"Tentativa de inserir pessoa sem nome. Pulando.")
            return None

        if 'cnpj' in valid_data: valid_data.setdefault('cpf', None); valid_data.setdefault('id_estrangeiro', None)
        elif 'cpf' in valid_data: valid_data.setdefault('cnpj', None); valid_data.setdefault('id_estrangeiro', None)

        cols = ', '.join(valid_data.keys())
        placeholders = ', '.join(['%s'] * len(valid_data))
        sql = f"INSERT INTO nfe.pessoa ({cols}) VALUES ({placeholders}) RETURNING id;"
        try:
            result = pg_hook.run(sql, parameters=list(valid_data.values()), autocommit=True, handler=lambda c: c.fetchone())
            return result[0] if result else None
        except Exception as e:
            self.log.error(f"Erro ao inserir pessoa {valid_data.get('nome')}: {e}")
            return None

    def _insert_endereco(self, pg_hook: PostgresHook, endereco_data: Dict[str, Any], pessoa_id: int):
        endereco_data['pessoa_id'] = pessoa_id
        valid_data = {k: v for k, v in endereco_data.items() if v is not None and k != 'nfe_id'}
        
        required_fields = ['logradouro', 'numero', 'bairro', 'codigo_municipio', 'municipio', 'uf', 'cep']
        if not all(k in valid_data and valid_data[k] is not None for k in required_fields):
            self.log.warning(f"Endereço incompleto para pessoa_id {pessoa_id}. Dados: {valid_data}. Pulando.")
            return

        cols = ', '.join(valid_data.keys())
        placeholders = ', '.join(['%s'] * len(valid_data))
        sql = f"INSERT INTO nfe.endereco ({cols}) VALUES ({placeholders});"
        try:
            pg_hook.run(sql, parameters=list(valid_data.values()), autocommit=True)
        except Exception as e:
            self.log.error(f"Erro ao inserir endereço para pessoa_id {pessoa_id}: {e}")

    def save_to_postgres(self, pg_hook: PostgresHook, data: Dict[str, Any]) -> Optional[int]:
        nfe_id = None
        chave_acesso = data.get('nfe', {}).get('chave_acesso', 'CHAVE_DESCONHECIDA')
        conn = pg_hook.get_conn()
        cur = conn.cursor()

        try:
            cur.execute("BEGIN;")

            emitente_id = self._insert_or_get_pessoa_id(pg_hook, data['emitente'])
            destinatario_id = self._insert_or_get_pessoa_id(pg_hook, data['destinatario'])
            transportadora_id = None
            if data.get('transportadora') and (data['transportadora'].get('cnpj') or data['transportadora'].get('cpf')):
                 transportadora_id = self._insert_or_get_pessoa_id(pg_hook, data.get('transportadora', {}))

            if not emitente_id or not destinatario_id:
                raise Exception("Emitente ou Destinatário não puderam ser inseridos/recuperados.")

            nfe_data = {k: v for k, v in data['nfe'].items() if v is not None}
            nfe_cols = ', '.join(nfe_data.keys())
            nfe_placeholders = ', '.join(['%s'] * len(nfe_data))
            nfe_sql = f"INSERT INTO nfe.nfe ({nfe_cols}) VALUES ({nfe_placeholders}) ON CONFLICT (chave_acesso) DO NOTHING RETURNING id;"
            
            cur.execute(nfe_sql, list(nfe_data.values()))
            result = cur.fetchone()
            
            if not result:
                self.log.warning(f"NFe com chave {chave_acesso} já existe. Pulando.")
                conn.rollback()
                return None
            nfe_id = result[0]

            self._insert_endereco(pg_hook, data['emitente_endereco'], emitente_id)
            self._insert_endereco(pg_hook, data['destinatario_endereco'], destinatario_id)
            if transportadora_id and data.get('transportadora_endereco'):
                self._insert_endereco(pg_hook, data['transportadora_endereco'], transportadora_id)

            cur.execute("INSERT INTO nfe.nfe_pessoa (nfe_id, pessoa_id, tipo_relacao) VALUES (%s, %s, 'EMITENTE'), (%s, %s, 'DESTINATARIO');",
                        (nfe_id, emitente_id, nfe_id, destinatario_id))

            for item in data.get('itens', []):
                item_data = {k: v for k, v in item.items() if k != 'impostos' and v is not None}
                item_data['nfe_id'] = nfe_id
                item_cols = ', '.join(item_data.keys())
                item_placeholders = ', '.join(['%s'] * len(item_data))
                item_sql = f"INSERT INTO nfe.item_nfe ({item_cols}) VALUES ({item_placeholders}) RETURNING id;"
                cur.execute(item_sql, list(item_data.values()))
                item_result = cur.fetchone()
                item_nfe_id = item_result[0] if item_result else None

                if item_nfe_id:
                    for imposto in item.get('impostos', []):
                        imp_data = {k: v for k, v in imposto.items() if v is not None}
                        imp_data['item_nfe_id'] = item_nfe_id
                        imp_cols = ', '.join(imp_data.keys())
                        imp_placeholders = ', '.join(['%s'] * len(imp_data))
                        imp_sql = f"INSERT INTO nfe.imposto ({imp_cols}) VALUES ({imp_placeholders});"
                        cur.execute(imp_sql, list(imp_data.values()))

            totais_data = {k: v for k, v in data.get('totais', {}).items() if v is not None}
            if totais_data:
                totais_data['nfe_id'] = nfe_id
                tot_cols = ', '.join(totais_data.keys())
                tot_placeholders = ', '.join(['%s'] * len(totais_data))
                tot_sql = f"INSERT INTO nfe.totais_nfe ({tot_cols}) VALUES ({tot_placeholders});"
                cur.execute(tot_sql, list(totais_data.values()))

            transporte_data = data.get('transporte')
            if transporte_data:
                transporte_data['nfe_id'] = nfe_id
                if transportadora_id:
                    transporte_data['transportadora_id'] = transportadora_id
                
                transp_data = {k:v for k,v in transporte_data.items() if v is not None}
                transp_cols = ', '.join(transp_data.keys())
                transp_placeholders = ', '.join(['%s'] * len(transp_data))
                transp_sql = f"INSERT INTO nfe.transporte ({transp_cols}) VALUES ({transp_placeholders}) RETURNING id;"
                cur.execute(transp_sql, list(transp_data.values()))
                transp_result = cur.fetchone()
                transporte_id = transp_result[0] if transp_result else None

                if transporte_id:
                    for vol in data.get('volumes', []):
                        vol['transporte_id'] = transporte_id
                        vol_cols = ', '.join(vol.keys())
                        vol_placeholders = ', '.join(['%s'] * len(vol))
                        vol_sql = f"INSERT INTO nfe.transporte_item ({vol_cols}) VALUES ({vol_placeholders});"
                        cur.execute(vol_sql, list(vol.values()))

            info_data = data.get('informacoes_adicionais')
            if info_data and any(info_data.values()):
                info_data['nfe_id'] = nfe_id
                info_cols = ', '.join(info_data.keys())
                info_placeholders = ', '.join(['%s'] * len(info_data))
                info_sql = f"INSERT INTO nfe.informacoes_adicionais ({info_cols}) VALUES ({info_placeholders});"
                cur.execute(info_sql, list(info_data.values()))

            conn.commit()
            return nfe_id

        except Exception as e:
            conn.rollback()
            self.log.error(f"Erro ao salvar dados no PostgreSQL para a chave {chave_acesso}: {str(e)}", exc_info=True)
            if nfe_id:
                self.save_processing_status(pg_hook, nfe_id, chave_acesso, 'FALHA_DB', str(e), data.get('xml_original'))
            return None
        finally:
            cur.close()
            conn.close()


    def save_processing_status(self, pg_hook: PostgresHook, nfe_id: int, chave_acesso_or_filename: str, status: str, message: str, xml_content: Optional[str]):
        try:
            insert_sql = """
                INSERT INTO nfe.processamento_nfe (nfe_id, data_processamento, status, mensagem, xml_original)
                VALUES (%s, %s, %s, %s, %s);
            """
            params = (nfe_id, datetime.now(), status, message, xml_content)
            pg_hook.run(insert_sql, parameters=params, autocommit=True)
            self.log.info(f"Status de processamento '{status}' salvo para NFe ID {nfe_id}")
        except Exception as e:
            self.log.error(f"CRÍTICO: Erro ao salvar status de processamento para NFe ID {nfe_id}: {str(e)}")