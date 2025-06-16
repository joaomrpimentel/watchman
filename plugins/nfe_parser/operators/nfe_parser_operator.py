import os
import shutil
import xml.etree.ElementTree as ET
from datetime import datetime
from typing import Dict, List, Optional, Any

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
            root = ET.fromstring(xml_content)

            extracted_data = {
                'nfe': {}, 'emitente': {}, 'emitente_endereco': {},
                'destinatario': {}, 'destinatario_endereco': {},
                'itens': [], 'totais': {}, 'transporte': {},
                'transportadora': {}, 'transportadora_endereco': {},
                'volumes': [],
                'informacoes_adicionais': {}, 'xml_original': xml_content
            }

            infNFe = root.find('.//nfe:infNFe', self.ns)
            if infNFe is not None:
                extracted_data['nfe']['chave_acesso'] = infNFe.attrib.get('Id', '')[3:]
                extracted_data['nfe']['versao'] = infNFe.attrib.get('versao', '')

            ide = root.find('.//nfe:ide', self.ns)
            if ide is not None:
                # CORREÇÃO: Adicionado `namespaces=self.ns` em todas as chamadas findtext
                extracted_data['nfe']['codigo_uf'] = int(ide.findtext('nfe:cUF', '0', self.ns))
                extracted_data['nfe']['codigo_nf'] = ide.findtext('nfe:cNF', '', self.ns)
                extracted_data['nfe']['natureza_operacao'] = ide.findtext('nfe:natOp', '', self.ns)
                extracted_data['nfe']['indicador_pagamento'] = self._safe_int(ide.findtext('nfe:indPag', '9', self.ns))
                extracted_data['nfe']['modelo'] = int(ide.findtext('nfe:mod', '0', self.ns))
                extracted_data['nfe']['serie'] = int(ide.findtext('nfe:serie', '0', self.ns))
                extracted_data['nfe']['numero'] = int(ide.findtext('nfe:nNF', '0', self.ns))
                dhEmi_str = ide.findtext('nfe:dhEmi', '', self.ns)
                dt_obj_emi = self.parse_date_to_timestamp(dhEmi_str)
                extracted_data['nfe']['data_emissao'] = dt_obj_emi.date() if dt_obj_emi else None
                dhSaiEnt_str = ide.findtext('nfe:dhSaiEnt', '', self.ns)
                dt_obj_sai = self.parse_date_to_timestamp(dhSaiEnt_str)
                extracted_data['nfe']['data_saida_entrada'] = dt_obj_sai.date() if dt_obj_sai else None
                extracted_data['nfe']['tipo_nf'] = int(ide.findtext('nfe:tpNF', '0', self.ns))
                extracted_data['nfe']['codigo_municipio_fato_gerador'] = ide.findtext('nfe:cMunFG', '', self.ns)
                extracted_data['nfe']['tipo_impressao'] = int(ide.findtext('nfe:tpImp', '0', self.ns))
                extracted_data['nfe']['tipo_emissao'] = int(ide.findtext('nfe:tpEmis', '0', self.ns))
                extracted_data['nfe']['digito_verificador'] = int(ide.findtext('nfe:cDV', '0', self.ns))
                extracted_data['nfe']['ambiente'] = int(ide.findtext('nfe:tpAmb', '0', self.ns))
                extracted_data['nfe']['finalidade_nf'] = int(ide.findtext('nfe:finNFe', '0', self.ns))
                extracted_data['nfe']['processo_emissao'] = int(ide.findtext('nfe:procEmi', '0', self.ns))
                extracted_data['nfe']['versao_processo'] = ide.findtext('nfe:verProc', '', self.ns)

            emit = root.find('.//nfe:emit', self.ns)
            if emit is not None:
                extracted_data['emitente']['tipo_pessoa'] = 'EMITENTE'
                extracted_data['emitente']['cnpj'] = emit.findtext('nfe:CNPJ', None, self.ns)
                extracted_data['emitente']['cpf'] = emit.findtext('nfe:CPF', None, self.ns)
                extracted_data['emitente']['nome'] = emit.findtext('nfe:xNome', None, self.ns)
                extracted_data['emitente']['nome_fantasia'] = emit.findtext('nfe:xFant', None, self.ns)
                extracted_data['emitente']['inscricao_estadual'] = emit.findtext('nfe:IE', None, self.ns)
                extracted_data['emitente']['regime_tributario'] = self._safe_int(emit.findtext('nfe:CRT', '0', self.ns))

                enderEmit = emit.find('nfe:enderEmit', self.ns)
                if enderEmit is not None:
                    extracted_data['emitente_endereco']['tipo_endereco'] = 'PRINCIPAL'
                    extracted_data['emitente_endereco']['logradouro'] = enderEmit.findtext('nfe:xLgr', '', self.ns)
                    extracted_data['emitente_endereco']['numero'] = enderEmit.findtext('nfe:nro', '', self.ns)
                    extracted_data['emitente_endereco']['complemento'] = enderEmit.findtext('nfe:xCpl', None, self.ns)
                    extracted_data['emitente_endereco']['bairro'] = enderEmit.findtext('nfe:xBairro', '', self.ns)
                    extracted_data['emitente_endereco']['codigo_municipio'] = enderEmit.findtext('nfe:cMun', '', self.ns)
                    extracted_data['emitente_endereco']['municipio'] = enderEmit.findtext('nfe:xMun', '', self.ns)
                    extracted_data['emitente_endereco']['uf'] = enderEmit.findtext('nfe:UF', '', self.ns)
                    extracted_data['emitente_endereco']['cep'] = enderEmit.findtext('nfe:CEP', '', self.ns)
                    extracted_data['emitente_endereco']['codigo_pais'] = enderEmit.findtext('nfe:cPais', None, self.ns)
                    extracted_data['emitente_endereco']['pais'] = enderEmit.findtext('nfe:xPais', None, self.ns)
                    extracted_data['emitente_endereco']['telefone'] = enderEmit.findtext('nfe:fone', None, self.ns)

            dest = root.find('.//nfe:dest', self.ns)
            if dest is not None:
                extracted_data['destinatario']['tipo_pessoa'] = 'DESTINATARIO'
                extracted_data['destinatario']['cnpj'] = dest.findtext('nfe:CNPJ', None, self.ns)
                extracted_data['destinatario']['cpf'] = dest.findtext('nfe:CPF', None, self.ns)
                extracted_data['destinatario']['nome'] = dest.findtext('nfe:xNome', '', self.ns)
                extracted_data['destinatario']['email'] = dest.findtext('nfe:email', None, self.ns)

                enderDest = dest.find('nfe:enderDest', self.ns)
                if enderDest is not None:
                    extracted_data['destinatario_endereco']['tipo_endereco'] = 'PRINCIPAL'
                    extracted_data['destinatario_endereco']['logradouro'] = enderDest.findtext('nfe:xLgr', '', self.ns)
                    extracted_data['destinatario_endereco']['numero'] = enderDest.findtext('nfe:nro', '', self.ns)
                    extracted_data['destinatario_endereco']['complemento'] = enderDest.findtext('nfe:xCpl', None, self.ns)
                    extracted_data['destinatario_endereco']['bairro'] = enderDest.findtext('nfe:xBairro', '', self.ns)
                    extracted_data['destinatario_endereco']['codigo_municipio'] = enderDest.findtext('nfe:cMun', '', self.ns)
                    extracted_data['destinatario_endereco']['municipio'] = enderDest.findtext('nfe:xMun', '', self.ns)
                    extracted_data['destinatario_endereco']['uf'] = enderDest.findtext('nfe:UF', '', self.ns)
                    extracted_data['destinatario_endereco']['cep'] = enderDest.findtext('nfe:CEP', '', self.ns)

            for i, det in enumerate(root.findall('.//nfe:det', self.ns)):
                item_nfe = {'numero_item': int(det.attrib.get('nItem', str(i + 1)))}

                prod = det.find('nfe:prod', self.ns)
                if prod is not None:
                    item_nfe.update({
                        'codigo_produto': prod.findtext('nfe:cProd', '', self.ns),
                        'gtin': prod.findtext('nfe:cEAN', None, self.ns),
                        'descricao': prod.findtext('nfe:xProd', '', self.ns),
                        'ncm': prod.findtext('nfe:NCM', None, self.ns),
                        'cfop': prod.findtext('nfe:CFOP', '', self.ns),
                        'unidade_comercial': prod.findtext('nfe:uCom', '', self.ns),
                        'quantidade_comercial': float(prod.findtext('nfe:qCom', '0.0', self.ns)),
                        'valor_unitario_comercial': float(prod.findtext('nfe:vUnCom', '0.0', self.ns)),
                        'valor_total_bruto': float(prod.findtext('nfe:vProd', '0.0', self.ns)),
                        'gtin_tributavel': prod.findtext('nfe:cEANTrib', None, self.ns),
                        'unidade_tributavel': prod.findtext('nfe:uTrib', '', self.ns),
                        'quantidade_tributavel': float(prod.findtext('nfe:qTrib', '0.0', self.ns)),
                        'valor_unitario_tributavel': float(prod.findtext('nfe:vUnTrib', '0.0', self.ns)),
                    })

                imposto_data = []
                imposto_node = det.find('nfe:imposto', self.ns)
                if imposto_node:
                    icms_node = imposto_node.find('nfe:ICMS', self.ns)
                    if icms_node is not None and len(icms_node) > 0:
                        icms_type_node = icms_node[0]
                        item_nfe['origem_mercadoria'] = self._safe_int(icms_type_node.findtext('nfe:orig', None, self.ns))
                        imposto_entry = {
                            'tipo_imposto': 'ICMS',
                            'origem': item_nfe['origem_mercadoria'],
                            'cst': icms_type_node.findtext('nfe:CST', None, self.ns),
                            'modalidade_base_calculo': self._safe_int(icms_type_node.findtext('nfe:modBC', None, self.ns)),
                            'valor_base_calculo': self._safe_float(icms_type_node.findtext('nfe:vBC', None, self.ns)),
                            'aliquota_percentual': self._safe_float(icms_type_node.findtext('nfe:pICMS', None, self.ns)),
                            'valor': self._safe_float(icms_type_node.findtext('nfe:vICMS', None, self.ns)),
                            'percentual_reducao_base_calculo': self._safe_float(icms_type_node.findtext('nfe:pRedBC', None, self.ns)),
                        }
                        imposto_data.append(imposto_entry)

                    ipi_node = imposto_node.find('nfe:IPI', self.ns)
                    if ipi_node is not None and len(ipi_node) > 0:
                        ipi_type_node = ipi_node.find('*', self.ns)
                        imposto_entry = {
                            'tipo_imposto': 'IPI',
                            'cst': ipi_type_node.findtext('nfe:CST', None, self.ns) if ipi_type_node is not None else None,
                            'codigo_enquadramento': ipi_node.findtext('nfe:cEnq', None, self.ns),
                        }
                        imposto_data.append(imposto_entry)

                    pis_node = imposto_node.find('nfe:PIS', self.ns)
                    if pis_node is not None and len(pis_node) > 0:
                        pis_type_node = pis_node[0]
                        imposto_entry = {
                            'tipo_imposto': 'PIS',
                            'cst': pis_type_node.findtext('nfe:CST', None, self.ns),
                            'valor_base_calculo': self._safe_float(pis_type_node.findtext('nfe:vBC', None, self.ns)),
                            'aliquota_percentual': self._safe_float(pis_type_node.findtext('nfe:pPIS', None, self.ns)),
                            'valor': self._safe_float(pis_type_node.findtext('nfe:vPIS', None, self.ns)),
                        }
                        imposto_data.append(imposto_entry)

                    cofins_node = imposto_node.find('nfe:COFINS', self.ns)
                    if cofins_node is not None and len(cofins_node) > 0:
                        cofins_type_node = cofins_node[0]
                        imposto_entry = {
                            'tipo_imposto': 'COFINS',
                            'cst': cofins_type_node.findtext('nfe:CST', None, self.ns),
                            'valor_base_calculo': self._safe_float(cofins_type_node.findtext('nfe:vBC', None, self.ns)),
                            'aliquota_percentual': self._safe_float(cofins_type_node.findtext('nfe:pCOFINS', None, self.ns)),
                            'valor': self._safe_float(cofins_type_node.findtext('nfe:vCOFINS', None, self.ns)),
                        }
                        imposto_data.append(imposto_entry)

                item_nfe['impostos'] = imposto_data
                extracted_data['itens'].append(item_nfe)

            total_node = root.find('.//nfe:ICMSTot', self.ns)
            if total_node is not None:
                extracted_data['totais'] = {
                    'base_calculo_icms': self._safe_float(total_node.findtext('nfe:vBC', None, self.ns)),
                    'valor_icms': self._safe_float(total_node.findtext('nfe:vICMS', None, self.ns)),
                    'base_calculo_icms_st': self._safe_float(total_node.findtext('nfe:vBCST', None, self.ns)),
                    'valor_icms_st': self._safe_float(total_node.findtext('nfe:vST', None, self.ns)),
                    'valor_produtos': self._safe_float(total_node.findtext('nfe:vProd', None, self.ns)),
                    'valor_frete': self._safe_float(total_node.findtext('nfe:vFrete', None, self.ns)),
                    'valor_seguro': self._safe_float(total_node.findtext('nfe:vSeg', None, self.ns)),
                    'valor_desconto': self._safe_float(total_node.findtext('nfe:vDesc', None, self.ns)),
                    'valor_ii': self._safe_float(total_node.findtext('nfe:vII', None, self.ns)),
                    'valor_ipi': self._safe_float(total_node.findtext('nfe:vIPI', None, self.ns)),
                    'valor_pis': self._safe_float(total_node.findtext('nfe:vPIS', None, self.ns)),
                    'valor_cofins': self._safe_float(total_node.findtext('nfe:vCOFINS', None, self.ns)),
                    'valor_outros': self._safe_float(total_node.findtext('nfe:vOutro', None, self.ns)),
                    'valor_total_nfe': self._safe_float(total_node.findtext('nfe:vNF', None, self.ns)),
                }

            transp = root.find('.//nfe:transp', self.ns)
            if transp is not None:
                extracted_data['transporte']['modalidade_frete'] = self._safe_int(transp.findtext('nfe:modFrete', '9', self.ns))
                transporta = transp.find('nfe:transporta', self.ns)
                if transporta is not None:
                    extracted_data['transportadora'] = {
                        'tipo_pessoa': 'TRANSPORTADORA',
                        'cnpj': transporta.findtext('nfe:CNPJ', None, self.ns),
                        'cpf': transporta.findtext('nfe:CPF', None, self.ns),
                        'nome': transporta.findtext('nfe:xNome', '', self.ns),
                        'inscricao_estadual': transporta.findtext('nfe:IE', None, self.ns),
                    }
                    self.log.info("Endereço da transportadora não é estruturado. Usando placeholders.")
                    extracted_data['transportadora_endereco'] = {
                        'tipo_endereco': 'PRINCIPAL',
                        'logradouro': transporta.findtext('nfe:xEnder', 'N/A', self.ns),
                        'numero': 'S/N',
                        'bairro': 'N/A',
                        'codigo_municipio': '0000000',
                        'municipio': transporta.findtext('nfe:xMun', 'N/A', self.ns),
                        'uf': transporta.findtext('nfe:UF', 'NA', self.ns),
                        'cep': '00000000'
                    }
                for vol in transp.findall('.//nfe:vol', self.ns):
                    extracted_data['volumes'].append({
                        'tipo_item': 'VOLUME',
                        'quantidade': self._safe_int(vol.findtext('nfe:qVol', None, self.ns)),
                        'especie': vol.findtext('nfe:esp', None, self.ns),
                        'marca': vol.findtext('nfe:marca', None, self.ns),
                        'numeracao': vol.findtext('nfe:nVol', None, self.ns),
                        'peso_liquido': self._safe_float(vol.findtext('nfe:pesoL', None, self.ns)),
                        'peso_bruto': self._safe_float(vol.findtext('nfe:pesoB', None, self.ns)),
                    })

            infAdic = root.find('.//nfe:infAdic', self.ns)
            if infAdic is not None:
                extracted_data['informacoes_adicionais'] = {
                    'info_contribuinte': infAdic.findtext('nfe:infCpl', None, self.ns),
                    'info_fisco': infAdic.findtext('nfe:infAdFisco', None, self.ns)
                }

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