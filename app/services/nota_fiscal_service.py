import psycopg2
import psycopg2.extras
from datetime import date, datetime
import logging
from decimal import Decimal 

service_logger = logging.getLogger(__name__)
service_logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
service_logger.addHandler(handler)

class NotaFiscalService:
    def __init__(self, db_config):
        self.db_config = db_config

    def _get_db_connection(self):
        """Função interna para obter a conexão com o banco de dados."""
        if not self.db_config:
            service_logger.error("Configuração do PostgreSQL não encontrada.")
            raise RuntimeError("Configuração do PostgreSQL não encontrada.")
        try:
            conn = psycopg2.connect(
                host=self.db_config['host'],
                port=self.db_config['port'],
                database=self.db_config['database'],
                user=self.db_config['user'],
                password=self.db_config['password']
            )
            service_logger.info("Conexão com o banco de dados estabelecida com sucesso.")
            return conn
        except psycopg2.Error as e:
            service_logger.error(f"Erro ao conectar ao banco de dados: {e}")
            raise RuntimeError(f"Erro ao conectar ao banco de dados: {e}")

    def _json_serial(self, obj):
        """Serializador auxiliar para converter tipos não JSON."""
        if isinstance(obj, (bytes, bytearray)):
            return obj.decode('utf-8')
        if isinstance(obj, (int, float, str, bool, type(None))):
            return obj 
        if isinstance(obj, Decimal):
            return float(obj)       
        if isinstance(obj, (list, tuple)):
            return [self._json_serial(item) for item in obj]
        if isinstance(obj, dict):
            return {k: self._json_serial(v) for k, v in obj.items()}
        if isinstance(obj, (date, datetime)):
            return obj.isoformat()
        raise TypeError(f"Object of type {obj.__class__.__name__} is not JSON serializable")

    def get_nota_fiscal_by_identifier(self, identifier: str) -> dict | None:
        """
        Busca os dados completos de uma Nota Fiscal por ID ou chave de acesso.
        Retorna um dicionário com os dados da NF-e ou None se não encontrada.
        """
        conn = None
        try:
            conn = self._get_db_connection()
            cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

            service_logger.debug(f"Tentando buscar NF-e com identificador: {identifier}")

            if identifier.isdigit():
                query_nfe = """
                SELECT
                    n.id, n.chave_acesso, n.versao, n.codigo_uf, n.codigo_nf, n.natureza_operacao,
                    n.indicador_pagamento, n.modelo, n.serie, n.numero, n.data_emissao,
                    n.data_saida_entrada, n.tipo_nf, n.codigo_municipio_fato_gerador, n.tipo_impressao,
                    n.tipo_emissao, n.digito_verificador, n.ambiente, n.finalidade_nf,
                    n.processo_emissao, n.versao_processo
                FROM nfe.nfe n
                WHERE n.id = %s;
                """
                cur.execute(query_nfe, (int(identifier),))
            else:
                query_nfe = """
                SELECT
                    n.id, n.chave_acesso, n.versao, n.codigo_uf, n.codigo_nf, n.natureza_operacao,
                    n.indicador_pagamento, n.modelo, n.serie, n.numero, n.data_emissao,
                    n.data_saida_entrada, n.tipo_nf, n.codigo_municipio_fato_gerador, n.tipo_impressao,
                    n.tipo_emissao, n.digito_verificador, n.ambiente, n.finalidade_nf,
                    n.processo_emissao, n.versao_processo
                FROM nfe.nfe n
                WHERE n.chave_acesso = %s;
                """
                service_logger.debug(f"Executando query para chave de acesso: {identifier}")
                cur.execute(query_nfe, (identifier,))

            nfe_row = cur.fetchone()
            if not nfe_row:
                service_logger.debug(f"NF-e com identificador {identifier} não encontrada no banco de dados.")
                return None

            nfe_id = nfe_row['id']
            nfe_data = dict(nfe_row)
            service_logger.debug(f"NF-e encontrada: ID={nfe_id}, Dados Iniciais: {nfe_data}")

            # Recupera IDs de pessoas da nfe_pessoa
            cur.execute("""
                SELECT pessoa_id, tipo_relacao FROM nfe.nfe_pessoa
                WHERE nfe_id = %s;
            """, (nfe_id,))
            pessoa_rel_rows = cur.fetchall()
            service_logger.debug(f"Relações de pessoa para NF-e {nfe_id}: {pessoa_rel_rows}")
            
            emitente_id = None
            destinatario_id = None
            for row in pessoa_rel_rows:
                if row['tipo_relacao'] == 'EMITENTE':
                    emitente_id = row['pessoa_id']
                elif row['tipo_relacao'] == 'DESTINATARIO':
                    destinatario_id = row['pessoa_id']

            if emitente_id:
                cur.execute("SELECT * FROM nfe.pessoa WHERE id = %s;", (emitente_id,))
                emitente_row = cur.fetchone()
                if emitente_row:
                    nfe_data['emitente'] = dict(emitente_row)
                    service_logger.debug(f"Emitente encontrado: {nfe_data['emitente']['nome']}")
                    cur.execute("SELECT * FROM nfe.endereco WHERE pessoa_id = %s AND tipo_endereco = 'PRINCIPAL';", (emitente_id,))
                    emitente_endereco_row = cur.fetchone()
                    nfe_data['emitente_endereco'] = dict(emitente_endereco_row) if emitente_endereco_row else None
                    service_logger.debug(f"Endereço do Emitente: {nfe_data['emitente_endereco']}")


            if destinatario_id:
                cur.execute("SELECT * FROM nfe.pessoa WHERE id = %s;", (destinatario_id,))
                destinatario_row = cur.fetchone()
                if destinatario_row:
                    nfe_data['destinatario'] = dict(destinatario_row)
                    service_logger.debug(f"Destinatário encontrado: {nfe_data['destinatario']['nome']}")
                    cur.execute("SELECT * FROM nfe.endereco WHERE pessoa_id = %s AND tipo_endereco = 'PRINCIPAL';", (destinatario_id,))
                    destinatario_endereco_row = cur.fetchone()
                    nfe_data['destinatario_endereco'] = dict(destinatario_endereco_row) if destinatario_endereco_row else None
                    service_logger.debug(f"Endereço do Destinatário: {nfe_data['destinatario_endereco']}")

            # Itens da NF-e
            cur.execute("SELECT * FROM nfe.item_nfe WHERE nfe_id = %s ORDER BY numero_item;", (nfe_id,))
            itens_rows = cur.fetchall()
            itens_list = []
            for item_row in itens_rows:
                item_dict = dict(item_row)
                cur.execute("SELECT * FROM nfe.imposto WHERE item_nfe_id = %s;", (item_dict['id'],))
                impostos_rows = cur.fetchall()
                item_dict['impostos'] = [dict(imp) for imp in impostos_rows]
                itens_list.append(item_dict)
            nfe_data['itens'] = itens_list
            service_logger.debug(f"Itens da NF-e ({len(itens_list)}): {itens_list}")

            # Totais da NF-e
            cur.execute("SELECT * FROM nfe.totais_nfe WHERE nfe_id = %s;", (nfe_id,))
            totais_row = cur.fetchone()
            nfe_data['totais'] = dict(totais_row) if totais_row else None
            service_logger.debug(f"Totais da NF-e: {nfe_data['totais']}")

            # Transporte
            cur.execute("SELECT * FROM nfe.transporte WHERE nfe_id = %s;", (nfe_id,))
            transporte_row = cur.fetchone()
            if transporte_row:
                transporte_dict = dict(transporte_row)
                
                # Transportadora (Pessoa e Endereço)
                if transporte_dict.get('transportadora_id'):
                    cur.execute("SELECT * FROM nfe.pessoa WHERE id = %s;", (transporte_dict['transportadora_id'],))
                    transportadora_row = cur.fetchone()
                    if transportadora_row:
                        transporte_dict['transportadora'] = dict(transportadora_row)
                        cur.execute("SELECT * FROM nfe.endereco WHERE pessoa_id = %s AND tipo_endereco = 'PRINCIPAL';", (transporte_dict['transportadora_id'],))
                        transportadora_endereco_row = cur.fetchone()
                        transporte_dict['transportadora_endereco'] = dict(transportadora_endereco_row) if transportadora_endereco_row else None

                # Volumes, Veículos, Lacres (tudo em transporte_item agora)
                # Volumes
                cur.execute("SELECT * FROM nfe.transporte_item WHERE transporte_id = %s AND tipo_item = 'VOLUME';", (transporte_dict['id'],))
                volumes_rows = cur.fetchall()
                volumes_list = []
                for vol_row in volumes_rows:
                    vol_dict = dict(vol_row)
                    # Lacres do Volume
                    cur.execute("SELECT * FROM nfe.transporte_item WHERE item_pai_id = %s AND tipo_item = 'LACRE';", (vol_dict['id'],))
                    lacres_rows = cur.fetchall()
                    vol_dict['lacres'] = [dict(lac) for lac in lacres_rows]
                    volumes_list.append(vol_dict)
                transporte_dict['volumes'] = volumes_list

                # Veículos
                cur.execute("SELECT * FROM nfe.transporte_item WHERE transporte_id = %s AND tipo_item = 'VEICULO';", (transporte_dict['id'],))
                veiculos_rows = cur.fetchall()
                transporte_dict['veiculos'] = [dict(veic) for veic in veiculos_rows]
                
                nfe_data['transporte'] = transporte_dict
            else:
                nfe_data['transporte'] = None
            service_logger.debug(f"Dados de Transporte: {nfe_data['transporte']}")

            # Informações Adicionais
            cur.execute("SELECT * FROM nfe.informacoes_adicionais WHERE nfe_id = %s;", (nfe_id,))
            inf_adic_row = cur.fetchone()
            nfe_data['informacoes_adicionais'] = dict(inf_adic_row) if inf_adic_row else None
            service_logger.debug(f"Informações Adicionais: {nfe_data['informacoes_adicionais']}")


            final_data = self._json_serial(nfe_data)
            service_logger.debug(f"Dados finais serializados: {final_data}")
            return final_data

        except psycopg2.Error as e:
            service_logger.error(f"Erro no banco de dados ao buscar NF-e: {e}")
            raise RuntimeError(f"Erro no banco de dados ao buscar NF-e: {e}")
        finally:
            if conn:
                conn.close()

    def list_notas_fiscais_summary(self) -> list:
        """
        Lista um resumo das Notas Fiscais no banco de dados.
        """
        conn = None
        try:
            conn = self._get_db_connection()
            cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

            service_logger.debug("Tentando listar NF-es resumidamente.")
            query_list = """
            SELECT
                n.id, n.chave_acesso, n.numero, n.serie, n.data_emissao, n.natureza_operacao,
                np_e.pessoa_id AS emitente_id, np_d.pessoa_id AS destinatario_id
            FROM nfe.nfe n
            LEFT JOIN nfe.nfe_pessoa np_e ON n.id = np_e.nfe_id AND np_e.tipo_relacao = 'EMITENTE'
            LEFT JOIN nfe.nfe_pessoa np_d ON n.id = np_d.nfe_id AND np_d.tipo_relacao = 'DESTINATARIO'
            ORDER BY n.data_emissao DESC, n.numero DESC;
            """
            cur.execute(query_list)
            nfe_summaries = cur.fetchall()
            service_logger.debug(f"Sumário de NF-es encontradas: {len(nfe_summaries)}")

            results = []
            for row in nfe_summaries:
                summary_dict = dict(row)
                
                # Buscar nome do emitente
                if summary_dict.get('emitente_id'):
                    cur.execute("SELECT nome FROM nfe.pessoa WHERE id = %s;", (summary_dict['emitente_id'],))
                    emitente_name_row = cur.fetchone()
                    summary_dict['emitente_nome'] = emitente_name_row['nome'] if emitente_name_row else None

                # Buscar nome do destinatário
                if summary_dict.get('destinatario_id'):
                    cur.execute("SELECT nome FROM nfe.pessoa WHERE id = %s;", (summary_dict['destinatario_id'],))
                    destinatario_name_row = cur.fetchone()
                    summary_dict['destinatario_nome'] = destinatario_name_row['nome'] if destinatario_name_row else None
                
                # Remover IDs de FK para a resposta final, se não quiser expô-los
                summary_dict.pop('emitente_id', None)
                summary_dict.pop('destinatario_id', None)

                results.append(self._json_serial(summary_dict))

            service_logger.debug(f"Resultados resumidos finalizados: {len(results)}")
            return results

        except psycopg2.Error as e:
            service_logger.error(f"Erro no banco de dados ao listar NF-es: {e}")
            raise RuntimeError(f"Erro no banco de dados ao listar NF-es: {e}")
        finally:
            if conn:
                conn.close()

