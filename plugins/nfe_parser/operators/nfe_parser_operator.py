import os
import shutil
import xml.etree.ElementTree as ET
import json
from datetime import datetime
from typing import Dict, List, Optional, Any, Tuple

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
        # table_name is no longer directly used for insertion as data is spread across tables,
        # but kept for backward compatibility if needed for other parts of the DAG.
        table_name: str = 'nfe', 
        *args, **kwargs
    ) -> None:
        super().__init__(*args, **kwargs)
        self.source_folder = source_folder
        self.destination_folder = destination_folder
        self.postgres_conn_id = postgres_conn_id
        self.table_name = table_name # This now refers to the main 'nfe' table.
        self.ns = {'nfe': 'http://www.portalfiscal.inf.br/nfe'}
        
    def execute(self, context: Dict) -> None:
        """
        Executa o operador.
        
        :param context: Contexto de execução da tarefa
        :return: None
        """
        self.log.info(f"Verificando arquivos na pasta: {self.source_folder}")
        
        # Garantir que as pastas existam
        os.makedirs(self.source_folder, exist_ok=True)
        os.makedirs(self.destination_folder, exist_ok=True)
        
        # Listar arquivos XML na pasta de origem
        xml_files = [f for f in os.listdir(self.source_folder) 
                    if f.lower().endswith('.xml')]
        
        if not xml_files:
            self.log.info("Nenhum arquivo XML encontrado na pasta de origem.")
            return
        
        self.log.info(f"Encontrados {len(xml_files)} arquivos XML para processar.")
        
        # Pegar a conexão do PostgreSQL
        pg_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        
        # Processar cada arquivo XML
        for xml_file in xml_files:
            xml_path = os.path.join(self.source_folder, xml_file)
            self.log.info(f"Processando arquivo: {xml_path}")
            
            try:
                with open(xml_path, 'r', encoding='utf-8') as f:
                    xml_content = f.read()

                # Extrair dados do XML
                extracted_data = self.extrair_dados_nfe(xml_content, xml_file)
                
                if not extracted_data:
                    self.log.warning(f"Não foi possível extrair dados do arquivo: {xml_path}")
                    # Insert a record into processamento_nfe for failed parsing
                    self.save_processing_status(pg_hook, None, xml_file, 'FALHA_EXTRACAO', f"Não foi possível extrair dados do XML: {xml_file}", xml_content)
                    continue
                
                # Salvar dados no PostgreSQL
                nfe_id = self.save_to_postgres(pg_hook, extracted_data, xml_content)
                
                # Mover arquivo processado para a pasta de destino
                dest_path = os.path.join(self.destination_folder, xml_file)
                shutil.move(xml_path, dest_path)
                
                self.log.info(f"Arquivo processado e movido para: {dest_path}")
                # Update processing status to SUCESSO after successful move
                self.save_processing_status(pg_hook, nfe_id, xml_file, 'SUCESSO', 'NFe processada com sucesso.', xml_content)
                
            except Exception as e:
                self.log.error(f"Erro ao processar o arquivo {xml_path}: {str(e)}")
                # Insert a record into processamento_nfe for general errors
                self.save_processing_status(pg_hook, None, xml_file, 'ERRO', f"Erro durante o processamento: {str(e)}", xml_content if 'xml_content' in locals() else None)

    def parse_date_to_timestamp(self, date_str: str) -> Optional[datetime]:
        """
        Converte uma string de data para um objeto datetime, lidando com diferentes formatos possíveis.
        
        :param date_str: String contendo a data no formato NFe
        :return: Objeto datetime ou None em caso de erro
        """
        if not date_str:
            return None
            
        try:
            # Formatos possíveis em NFe
            formats = [
                '%Y-%m-%dT%H:%M:%S%z',      # Com timezone (2023-05-01T14:30:00-03:00)
                '%Y-%m-%dT%H:%M:%S.%f%z',   # Com milissegundos e timezone
                '%Y-%m-%dT%H:%M:%S',        # Sem timezone (2023-05-01T14:30:00)
                '%Y-%m-%dT%H:%M:%S.%f',     # Com milissegundos sem timezone
                '%Y-%m-%d %H:%M:%S',        # Formato alternativo
                '%d/%m/%Y %H:%M:%S',        # Formato brasileiro
                '%d/%m/%Y',                 # Apenas data
                '%Y-%m-%d'                  # Apenas data ISO
            ]
            
            # Remover possível sufixo de timezone textual (ex: -03:00[America/Sao_Paulo])
            if '[' in date_str:
                date_str = date_str.split('[')[0]
                
            # Tentar cada formato
            for fmt in formats:
                try:
                    return datetime.strptime(date_str, fmt)
                except ValueError:
                    continue
                    
            # Se chegou aqui, nenhum formato funcionou
            self.log.warning(f"Formato de data não reconhecido: {date_str}")
            return None
            
        except Exception as e:
            self.log.error(f"Erro ao converter data '{date_str}': {str(e)}")
            return None
    
    def extrair_dados_nfe(self, xml_content: str, xml_file_name: str) -> Optional[Dict[str, Any]]:
        """
        Extrai os dados de uma Nota Fiscal Eletrônica no formato XML e estrutura-os
        para o novo esquema otimizado.
        
        :param xml_content: Conteúdo do arquivo XML da NFe
        :param xml_file_name: Nome do arquivo XML
        :return: Dicionário com os dados extraídos estruturados ou None em caso de erro
        """
        try:
            root = ET.fromstring(xml_content)
            
            extracted_data = {
                'nfe': {},
                'emitente': {},
                'emitente_endereco': {},
                'destinatario': {},
                'destinatario_endereco': {},
                'itens': [],
                'totais': {},
                'transporte': {},
                'transportadora': {},
                'transportadora_endereco': {},
                'volumes': [],
                'veiculos': [],
                'lacres': [],
                'informacoes_adicionais': {},
                'xml_original': xml_content # Store original XML content for processing table
            }
            
            # --- NFe Header ---
            infNFe = root.find('.//nfe:infNFe', self.ns)
            if infNFe is not None:
                extracted_data['nfe']['chave_acesso'] = infNFe.attrib.get('Id', '')[3:]
                extracted_data['nfe']['versao'] = infNFe.attrib.get('versao', '')
            
            ide = root.find('.//nfe:ide', self.ns)
            if ide is not None:
                extracted_data['nfe']['codigo_uf'] = int(ide.findtext('nfe:cUF', '0', self.ns))
                extracted_data['nfe']['codigo_nf'] = ide.findtext('nfe:cNF', '', self.ns)
                extracted_data['nfe']['natureza_operacao'] = ide.findtext('nfe:natOp', '', self.ns)
                extracted_data['nfe']['indicador_pagamento'] = int(ide.findtext('nfe:indPag', '0', self.ns))
                extracted_data['nfe']['modelo'] = int(ide.findtext('nfe:mod', '0', self.ns))
                extracted_data['nfe']['serie'] = int(ide.findtext('nfe:serie', '0', self.ns))
                extracted_data['nfe']['numero'] = int(ide.findtext('nfe:nNF', '0', self.ns))
                
                dhEmi_str = ide.findtext('nfe:dhEmi', '', self.ns)
                extracted_data['nfe']['data_emissao'] = self.parse_date_to_timestamp(dhEmi_str).date() if dhEmi_str else None # Store as DATE
                
                dhSaiEnt_str = ide.findtext('nfe:dhSaiEnt', '', self.ns)
                extracted_data['nfe']['data_saida_entrada'] = self.parse_date_to_timestamp(dhSaiEnt_str).date() if dhSaiEnt_str else None # Store as DATE

                extracted_data['nfe']['tipo_nf'] = int(ide.findtext('nfe:tpNF', '0', self.ns))
                extracted_data['nfe']['codigo_municipio_fato_gerador'] = ide.findtext('nfe:cMunFG', '', self.ns)
                extracted_data['nfe']['tipo_impressao'] = int(ide.findtext('nfe:tpImp', '0', self.ns))
                extracted_data['nfe']['tipo_emissao'] = int(ide.findtext('nfe:tpEmis', '0', self.ns))
                extracted_data['nfe']['digito_verificador'] = int(ide.findtext('nfe:cDV', '0', self.ns))
                extracted_data['nfe']['ambiente'] = int(ide.findtext('nfe:tpAmb', '0', self.ns))
                extracted_data['nfe']['finalidade_nf'] = int(ide.findtext('nfe:finNFe', '0', self.ns))
                extracted_data['nfe']['processo_emissao'] = int(ide.findtext('nfe:procEmi', '0', self.ns))
                extracted_data['nfe']['versao_processo'] = ide.findtext('nfe:verProc', '', self.ns)

            # --- Emitente ---
            emit = root.find('.//nfe:emit', self.ns)
            if emit is not None:
                extracted_data['emitente']['tipo_pessoa'] = 'EMITENTE'
                extracted_data['emitente']['cnpj'] = emit.findtext('nfe:CNPJ', '', self.ns)
                extracted_data['emitente']['cpf'] = emit.findtext('nfe:CPF', '', self.ns) # Though typically not present for emitente
                extracted_data['emitente']['nome'] = emit.findtext('nfe:xNome', '', self.ns)
                extracted_data['emitente']['nome_fantasia'] = emit.findtext('nfe:xFant', '', self.ns)
                extracted_data['emitente']['inscricao_estadual'] = emit.findtext('nfe:IE', '', self.ns)
                extracted_data['emitente']['inscricao_municipal'] = emit.findtext('nfe:IM', '', self.ns)
                extracted_data['emitente']['inscricao_suframa'] = emit.findtext('nfe:ISUF', '', self.ns)
                extracted_data['emitente']['cnae'] = emit.findtext('nfe:CNAE', '', self.ns)
                extracted_data['emitente']['regime_tributario'] = int(emit.findtext('nfe:CRT', '0', self.ns))

                enderEmit = emit.find('nfe:enderEmit', self.ns)
                if enderEmit is not None:
                    extracted_data['emitente_endereco'] = {
                        'tipo_endereco': 'PRINCIPAL',
                        'logradouro': enderEmit.findtext('nfe:xLgr', '', self.ns),
                        'numero': enderEmit.findtext('nfe:nro', '', self.ns),
                        'complemento': enderEmit.findtext('nfe:xCpl', '', self.ns),
                        'bairro': enderEmit.findtext('nfe:xBairro', '', self.ns),
                        'codigo_municipio': enderEmit.findtext('nfe:cMun', '', self.ns),
                        'municipio': enderEmit.findtext('nfe:xMun', '', self.ns),
                        'uf': enderEmit.findtext('nfe:UF', '', self.ns),
                        'cep': enderEmit.findtext('nfe:CEP', '', self.ns),
                        'codigo_pais': enderEmit.findtext('nfe:cPais', '', self.ns),
                        'pais': enderEmit.findtext('nfe:xPais', '', self.ns),
                        'telefone': enderEmit.findtext('nfe:fone', '', self.ns)
                    }

            # --- Destinatário ---
            dest = root.find('.//nfe:dest', self.ns)
            if dest is not None:
                extracted_data['destinatario']['tipo_pessoa'] = 'DESTINATARIO'
                extracted_data['destinatario']['cnpj'] = dest.findtext('nfe:CNPJ', '', self.ns)
                extracted_data['destinatario']['cpf'] = dest.findtext('nfe:CPF', '', self.ns)
                extracted_data['destinatario']['id_estrangeiro'] = dest.findtext('nfe:idEstrangeiro', '', self.ns)
                extracted_data['destinatario']['nome'] = dest.findtext('nfe:xNome', '', self.ns)
                extracted_data['destinatario']['inscricao_estadual'] = dest.findtext('nfe:IE', '', self.ns)
                extracted_data['destinatario']['email'] = dest.findtext('nfe:email', '', self.ns)

                enderDest = dest.find('nfe:enderDest', self.ns)
                if enderDest is not None:
                    extracted_data['destinatario_endereco'] = {
                        'tipo_endereco': 'PRINCIPAL',
                        'logradouro': enderDest.findtext('nfe:xLgr', '', self.ns),
                        'numero': enderDest.findtext('nfe:nro', '', self.ns),
                        'complemento': enderDest.findtext('nfe:xCpl', '', self.ns),
                        'bairro': enderDest.findtext('nfe:xBairro', '', self.ns),
                        'codigo_municipio': enderDest.findtext('nfe:cMun', '', self.ns),
                        'municipio': enderDest.findtext('nfe:xMun', '', self.ns),
                        'uf': enderDest.findtext('nfe:UF', '', self.ns),
                        'cep': enderDest.findtext('nfe:CEP', '', self.ns),
                        'codigo_pais': enderDest.findtext('nfe:cPais', '', self.ns),
                        'pais': enderDest.findtext('nfe:xPais', '', self.ns),
                        'telefone': enderDest.findtext('nfe:fone', '', self.ns)
                    }
            
            # --- Itens e Impostos ---
            for i, det in enumerate(root.findall('.//nfe:det', self.ns)):
                item_nfe = {}
                item_nfe['numero_item'] = int(det.attrib.get('nItem', str(i+1)))
                
                prod = det.find('nfe:prod', self.ns)
                if prod is not None:
                    item_nfe['codigo_produto'] = prod.findtext('nfe:cProd', '', self.ns)
                    item_nfe['gtin'] = prod.findtext('nfe:cEAN', '', self.ns)
                    item_nfe['descricao'] = prod.findtext('nfe:xProd', '', self.ns)
                    item_nfe['ncm'] = prod.findtext('nfe:NCM', '', self.ns)
                    item_nfe['cfop'] = prod.findtext('nfe:CFOP', '', self.ns)
                    item_nfe['unidade_comercial'] = prod.findtext('nfe:uCom', '', self.ns)
                    item_nfe['quantidade_comercial'] = float(prod.findtext('nfe:qCom', '0.0', self.ns))
                    item_nfe['valor_unitario_comercial'] = float(prod.findtext('nfe:vUnCom', '0.0', self.ns))
                    item_nfe['valor_total_bruto'] = float(prod.findtext('nfe:vProd', '0.0', self.ns))
                    item_nfe['gtin_tributavel'] = prod.findtext('nfe:cEANTrib', '', self.ns)
                    item_nfe['unidade_tributavel'] = prod.findtext('nfe:uTrib', '', self.ns)
                    item_nfe['quantidade_tributavel'] = float(prod.findtext('nfe:qTrib', '0.0', self.ns))
                    item_nfe['valor_unitario_tributavel'] = float(prod.findtext('nfe:vUnTrib', '0.0', self.ns))
                    item_nfe['origem_mercadoria'] = int(prod.findtext('nfe:orig', '0', self.ns))
                
                imposto_data = []
                imposto_node = det.find('nfe:imposto', self.ns)
                if imposto_node:
                    # ICMS
                    icms_node = imposto_node.find('.//nfe:ICMS', self.ns)
                    if icms_node is not None:
                        # Iterate through all ICMS types (ICMS00, ICMS10, etc.)
                        for icms_type_node in icms_node:
                            imposto_entry = {'tipo_imposto': 'ICMS'}
                            imposto_entry['cst'] = icms_type_node.findtext('nfe:CST', '', self.ns)
                            imposto_entry['csosn'] = icms_type_node.findtext('nfe:CSOSN', '', self.ns) # For Simples Nacional
                            imposto_entry['origem'] = int(icms_type_node.findtext('nfe:orig', '0', self.ns))
                            imposto_entry['modalidade_base_calculo'] = self._safe_int(icms_type_node.findtext('nfe:modBC', '', self.ns))
                            imposto_entry['percentual_reducao_base_calculo'] = self._safe_float(icms_type_node.findtext('nfe:pRedBC', '', self.ns))
                            imposto_entry['valor_base_calculo'] = self._safe_float(icms_type_node.findtext('nfe:vBC', '', self.ns))
                            imposto_entry['aliquota_percentual'] = self._safe_float(icms_type_node.findtext('nfe:pICMS', '', self.ns))
                            imposto_entry['valor'] = self._safe_float(icms_type_node.findtext('nfe:vICMS', '', self.ns))
                            
                            # ICMS ST (if present in specific CSTs)
                            imposto_entry['modalidade_base_calculo_st'] = self._safe_int(icms_type_node.findtext('nfe:modBCST', '', self.ns))
                            imposto_entry['percentual_margem_valor_adicionado_st'] = self._safe_float(icms_type_node.findtext('nfe:pMVAST', '', self.ns))
                            imposto_entry['percentual_reducao_base_calculo_st'] = self._safe_float(icms_type_node.findtext('nfe:pRedBCST', '', self.ns))
                            imposto_entry['valor_base_calculo_st'] = self._safe_float(icms_type_node.findtext('nfe:vBCST', '', self.ns))
                            imposto_entry['aliquota_st'] = self._safe_float(icms_type_node.findtext('nfe:pICMSST', '', self.ns))
                            imposto_entry['valor_st'] = self._safe_float(icms_type_node.findtext('nfe:vICMSST', '', self.ns))

                            # ICMS Simples Nacional (if present)
                            imposto_entry['percentual_credito_sn'] = self._safe_float(icms_type_node.findtext('nfe:pCredSN', '', self.ns))
                            imposto_entry['valor_credito_sn'] = self._safe_float(icms_type_node.findtext('nfe:vCredICMSSN', '', self.ns))

                            imposto_data.append(imposto_entry)

                    # IPI
                    ipi_node = imposto_node.find('.//nfe:IPI', self.ns)
                    if ipi_node is not None:
                        imposto_entry = {'tipo_imposto': 'IPI'}
                        imposto_entry['cst'] = ipi_node.findtext('.//nfe:CST', '', self.ns)
                        imposto_entry['classe_enquadramento'] = ipi_node.findtext('nfe:clEnq', '', self.ns)
                        imposto_entry['cnpj_produtor'] = ipi_node.findtext('nfe:CNPJProd', '', self.ns)
                        imposto_entry['codigo_selo'] = ipi_node.findtext('nfe:cSelo', '', self.ns)
                        imposto_entry['quantidade_selo'] = self._safe_int(ipi_node.findtext('nfe:qSelo', '', self.ns))
                        imposto_entry['codigo_enquadramento'] = ipi_node.findtext('nfe:cEnq', '', self.ns)
                        
                        # IPI based on percentage
                        ipitrib_node_perc = ipi_node.find('.//nfe:IPITrib', self.ns)
                        if ipitrib_node_perc and ipitrib_node_perc.findtext('nfe:pIPI', '', self.ns):
                            imposto_entry['valor_base_calculo'] = self._safe_float(ipitrib_node_perc.findtext('nfe:vBC', '', self.ns))
                            imposto_entry['aliquota_percentual'] = self._safe_float(ipitrib_node_perc.findtext('nfe:pIPI', '', self.ns))
                            imposto_entry['valor'] = self._safe_float(ipitrib_node_perc.findtext('nfe:vIPI', '', self.ns))
                        
                        # IPI based on value per unit
                        ipitrib_node_val = ipi_node.find('.//nfe:IPITrib', self.ns)
                        if ipitrib_node_val and ipitrib_node_val.findtext('nfe:qUnid', '', self.ns):
                            imposto_entry['quantidade_unidade'] = self._safe_float(ipitrib_node_val.findtext('nfe:qUnid', '', self.ns))
                            imposto_entry['valor_unidade'] = self._safe_float(ipitrib_node_val.findtext('nfe:vUnid', '', self.ns))
                            imposto_entry['valor'] = self._safe_float(ipitrib_node_val.findtext('nfe:vIPI', '', self.ns))

                        imposto_data.append(imposto_entry)

                    # PIS
                    pis_node = imposto_node.find('.//nfe:PIS', self.ns)
                    if pis_node is not None:
                        imposto_entry = {'tipo_imposto': 'PIS'}
                        imposto_entry['cst'] = pis_node.findtext('.//nfe:CST', '', self.ns)
                        
                        # PIS based on percentage
                        pisaliq_node = pis_node.find('.//nfe:PISAliq', self.ns)
                        if pisaliq_node:
                            imposto_entry['tipo_calculo'] = 'P'
                            imposto_entry['valor_base_calculo'] = self._safe_float(pisaliq_node.findtext('nfe:vBC', '', self.ns))
                            imposto_entry['aliquota_percentual'] = self._safe_float(pisaliq_node.findtext('nfe:pPIS', '', self.ns))
                            imposto_entry['valor'] = self._safe_float(pisaliq_node.findtext('nfe:vPIS', '', self.ns))
                        
                        # PIS based on value
                        pisqtde_node = pis_node.find('.//nfe:PISQtde', self.ns)
                        if pisqtde_node:
                            imposto_entry['tipo_calculo'] = 'V'
                            imposto_entry['quantidade_unidade'] = self._safe_float(pisqtde_node.findtext('nfe:qBCProd', '', self.ns))
                            imposto_entry['aliquota_valor'] = self._safe_float(pisqtde_node.findtext('nfe:vAliqProd', '', self.ns))
                            imposto_entry['valor'] = self._safe_float(pisqtde_node.findtext('nfe:vPIS', '', self.ns))

                        imposto_data.append(imposto_entry)

                    # COFINS
                    cofins_node = imposto_node.find('.//nfe:COFINS', self.ns)
                    if cofins_node is not None:
                        imposto_entry = {'tipo_imposto': 'COFINS'}
                        imposto_entry['cst'] = cofins_node.findtext('.//nfe:CST', '', self.ns)

                        # COFINS based on percentage
                        cofinsaliq_node = cofins_node.find('.//nfe:COFINSAliq', self.ns)
                        if cofinsaliq_node:
                            imposto_entry['tipo_calculo'] = 'P'
                            imposto_entry['valor_base_calculo'] = self._safe_float(cofinsaliq_node.findtext('nfe:vBC', '', self.ns))
                            imposto_entry['aliquota_percentual'] = self._safe_float(cofinsaliq_node.findtext('nfe:pCOFINS', '', self.ns))
                            imposto_entry['valor'] = self._safe_float(cofinsaliq_node.findtext('nfe:vCOFINS', '', self.ns))
                        
                        # COFINS based on value
                        cofinsqtde_node = cofins_node.find('.//nfe:COFINSQtde', self.ns)
                        if cofinsqtde_node:
                            imposto_entry['tipo_calculo'] = 'V'
                            imposto_entry['quantidade_unidade'] = self._safe_float(cofinsqtde_node.findtext('nfe:qBCProd', '', self.ns))
                            imposto_entry['aliquota_valor'] = self._safe_float(cofinsqtde_node.findtext('nfe:vAliqProd', '', self.ns))
                            imposto_entry['valor'] = self._safe_float(cofinsqtde_node.findtext('nfe:vCOFINS', '', self.ns))
                            
                        imposto_data.append(imposto_entry)
                
                item_nfe['impostos'] = imposto_data
                extracted_data['itens'].append(item_nfe)

            # --- Totals ---
            total_node = root.find('.//nfe:ICMSTot', self.ns)
            if total_node is not None:
                extracted_data['totais']['base_calculo_icms'] = float(total_node.findtext('nfe:vBC', '0.0', self.ns))
                extracted_data['totais']['valor_icms'] = float(total_node.findtext('nfe:vICMS', '0.0', self.ns))
                extracted_data['totais']['base_calculo_icms_st'] = float(total_node.findtext('nfe:vBCST', '0.0', self.ns))
                extracted_data['totais']['valor_icms_st'] = float(total_node.findtext('nfe:vST', '0.0', self.ns))
                extracted_data['totais']['valor_produtos'] = float(total_node.findtext('nfe:vProd', '0.0', self.ns))
                extracted_data['totais']['valor_frete'] = float(total_node.findtext('nfe:vFrete', '0.0', self.ns))
                extracted_data['totais']['valor_seguro'] = float(total_node.findtext('nfe:vSeg', '0.0', self.ns))
                extracted_data['totais']['valor_desconto'] = float(total_node.findtext('nfe:vDesc', '0.0', self.ns))
                extracted_data['totais']['valor_ii'] = float(total_node.findtext('nfe:vII', '0.0', self.ns))
                extracted_data['totais']['valor_ipi'] = float(total_node.findtext('nfe:vIPI', '0.0', self.ns))
                extracted_data['totais']['valor_pis'] = float(total_node.findtext('nfe:vPIS', '0.0', self.ns))
                extracted_data['totais']['valor_cofins'] = float(total_node.findtext('nfe:vCOFINS', '0.0', self.ns))
                extracted_data['totais']['valor_outros'] = float(total_node.findtext('nfe:vOutro', '0.0', self.ns))
                extracted_data['totais']['valor_total_nfe'] = float(total_node.findtext('nfe:vNF', '0.0', self.ns))

            # --- Transporte ---
            transp = root.find('.//nfe:transp', self.ns)
            if transp is not None:
                extracted_data['transporte']['modalidade_frete'] = int(transp.findtext('nfe:modFrete', '0', self.ns))
                
                # Transportadora (Pessoa)
                transporta = transp.find('nfe:transporta', self.ns)
                if transporta is not None:
                    extracted_data['transportadora']['tipo_pessoa'] = 'TRANSPORTADORA'
                    extracted_data['transportadora']['cnpj'] = transporta.findtext('nfe:CNPJ', '', self.ns)
                    extracted_data['transportadora']['cpf'] = transporta.findtext('nfe:CPF', '', self.ns)
                    extracted_data['transportadora']['nome'] = transporta.findtext('nfe:xNome', '', self.ns)
                    extracted_data['transportadora']['inscricao_estadual'] = transporta.findtext('nfe:IE', '', self.ns)
                    extracted_data['transportadora_endereco'] = { # Transportadora's address
                        'tipo_endereco': 'PRINCIPAL',
                        'logradouro': transporta.findtext('nfe:xEnder', '', self.ns), # xEnder contains full address
                        'municipio': transporta.findtext('nfe:xMun', '', self.ns),
                        'uf': transporta.findtext('nfe:UF', '', self.ns),
                        'numero': 'S/N', # Often not explicitly in transporta
                        'bairro': 'N/A', # Often not explicitly in transporta
                        'codigo_municipio': '',
                        'cep': '',
                        'codigo_pais': '',
                        'pais': '',
                        'telefone': ''
                    }

                # Volumes
                for vol in transp.findall('.//nfe:vol', self.ns):
                    volume_data = {
                        'tipo_item': 'VOLUME',
                        'quantidade': self._safe_int(vol.findtext('nfe:qVol', '', self.ns)),
                        'especie': vol.findtext('nfe:esp', '', self.ns),
                        'marca': vol.findtext('nfe:marca', '', self.ns),
                        'numeracao': vol.findtext('nfe:nVol', '', self.ns),
                        'peso_liquido': self._safe_float(vol.findtext('nfe:pesoL', '', self.ns)),
                        'peso_bruto': self._safe_float(vol.findtext('nfe:pesoB', '', self.ns)),
                    }
                    extracted_data['volumes'].append(volume_data)

                    # Lacres for volumes
                    for lac in vol.findall('.//nfe:lacres', self.ns):
                        lacre_data = {
                            'tipo_item': 'LACRE',
                            'numero_lacre': lac.findtext('nfe:nLacre', '', self.ns),
                            'item_pai_id': None # This will be filled during insertion
                        }
                        extracted_data['lacres'].append(lacre_data)
                
                # Veiculos
                for veic in transp.findall('.//nfe:reboque', self.ns): # Can be reboque or veicTransp
                    veiculo_data = {
                        'tipo_item': 'VEICULO',
                        'tipo_veiculo': 'REBOQUE', # Or 'PRINCIPAL' if from veicTransp
                        'placa': veic.findtext('nfe:placa', '', self.ns),
                        'uf': veic.findtext('nfe:UF', '', self.ns),
                        'rntc': veic.findtext('nfe:RNTC', '', self.ns)
                    }
                    extracted_data['veiculos'].append(veiculo_data)

                # Principal vehicle (if present)
                veic_transp = transp.find('nfe:veicTransp', self.ns)
                if veic_transp:
                    veiculo_data = {
                        'tipo_item': 'VEICULO',
                        'tipo_veiculo': 'PRINCIPAL',
                        'placa': veic_transp.findtext('nfe:placa', '', self.ns),
                        'uf': veic_transp.findtext('nfe:UF', '', self.ns),
                        'rntc': veic_transp.findtext('nfe:RNTC', '', self.ns)
                    }
                    extracted_data['veiculos'].append(veiculo_data)


            # --- Informações Adicionais ---
            infAdic = root.find('.//nfe:infAdic', self.ns)
            if infAdic is not None:
                extracted_data['informacoes_adicionais']['info_contribuinte'] = infAdic.findtext('nfe:infCpl', '', self.ns)
                extracted_data['informacoes_adicionais']['info_fisco'] = infAdic.findtext('nfe:infAdFisco', '', self.ns)

            return extracted_data

        except Exception as e:
            self.log.error(f"Erro ao extrair dados do XML: {str(e)}")
            return None

    def _safe_float(self, value: Optional[str]) -> Optional[float]:
        """Converts a string to float, returning None if conversion fails."""
        try:
            return float(value) if value else None
        except ValueError:
            return None

    def _safe_int(self, value: Optional[str]) -> Optional[int]:
        """Converts a string to int, returning None if conversion fails."""
        try:
            return int(float(value)) if value else None # Handle floats like "1.0"
        except ValueError:
            return None

    def _insert_or_get_pessoa_id(self, pg_hook: PostgresHook, pessoa_data: Dict[str, Any]) -> Optional[int]:
        """
        Insere uma pessoa se não existir (baseado em CNPJ/CPF) e retorna o ID.
        """
        cnpj = pessoa_data.get('cnpj')
        cpf = pessoa_data.get('cpf')

        # Check if person already exists by CNPJ or CPF
        if cnpj:
            check_sql = f"SELECT id FROM nfe.pessoa WHERE cnpj = %s"
            result = pg_hook.get_first(check_sql, parameters=(cnpj,))
            if result:
                return result[0]
        elif cpf:
            check_sql = f"SELECT id FROM nfe.pessoa WHERE cpf = %s"
            result = pg_hook.get_first(check_sql, parameters=(cpf,))
            if result:
                return result[0]
        else:
            self.log.warning(f"Pessoa sem CNPJ ou CPF para verificação de duplicidade: {pessoa_data.get('nome')}")
            # Consider a more robust unique identification if neither CNPJ nor CPF exists,
            # or allow duplicates if this scenario is expected for other identifiers (e.g., id_estrangeiro).
            # For now, if no unique identifier, we will proceed with insertion, potentially creating duplicates
            # if the same id_estrangeiro or name comes repeatedly without a document.

        columns = ', '.join(pessoa_data.keys())
        values_placeholders = ', '.join(['%s'] * len(pessoa_data))
        insert_sql = f"""
            INSERT INTO nfe.pessoa ({columns})
            VALUES ({values_placeholders})
            RETURNING id;
        """
        try:
            # Use run with fetch=True to get the returned ID
            result = pg_hook.run(insert_sql, parameters=list(pessoa_data.values()), autocommit=True, fetch=True)
            return result[0][0] if result else None
        except Exception as e:
            self.log.error(f"Erro ao inserir pessoa: {str(e)}")
            return None

    def _insert_endereco(self, pg_hook: PostgresHook, endereco_data: Dict[str, Any]):
        """
        Insere um endereço.
        """
        columns = ', '.join(endereco_data.keys())
        values_placeholders = ', '.join(['%s'] * len(endereco_data))
        insert_sql = f"INSERT INTO nfe.endereco ({columns}) VALUES ({values_placeholders});"
        try:
            pg_hook.run(insert_sql, parameters=list(endereco_data.values()), autocommit=True)
        except Exception as e:
            self.log.error(f"Erro ao inserir endereço: {str(e)}")

    def save_to_postgres(self, pg_hook: PostgresHook, data: Dict[str, Any], original_xml: str) -> Optional[int]:
        """
        Salva os dados extraídos da NFe no PostgreSQL, distribuindo-os pelas tabelas do novo esquema.
        
        :param pg_hook: Instância do PostgresHook para conectar ao banco
        :param data: Dicionário com os dados extraídos da NFe, estruturados por tabela
        :param original_xml: O conteúdo XML original para ser salvo no log de processamento.
        :return: O ID da NFe inserida ou None se a inserção falhar.
        """
        nfe_id = None
        try:
            # --- 1. Insert Pessoa (Emitente, Destinatário, Transportadora) and get IDs ---
            emitente_id = None
            if data.get('emitente'):
                emitente_id = self._insert_or_get_pessoa_id(pg_hook, data['emitente'])
                self.log.info(f"Emitente ID: {emitente_id}")
            
            destinatario_id = None
            if data.get('destinatario'):
                destinatario_id = self._insert_or_get_pessoa_id(pg_hook, data['destinatario'])
                self.log.info(f"Destinatário ID: {destinatario_id}")
            
            transportadora_id = None
            if data.get('transportadora'):
                transportadora_id = self._insert_or_get_pessoa_id(pg_hook, data['transportadora'])
                self.log.info(f"Transportadora ID: {transportadora_id}")

            # --- 2. Insert NFe Header ---
            nfe_data = data['nfe']
            nfe_columns = ', '.join(nfe_data.keys())
            nfe_values_placeholders = ', '.join(['%s'] * len(nfe_data))
            insert_nfe_sql = f"""
                INSERT INTO nfe.nfe ({nfe_columns})
                VALUES ({nfe_values_placeholders})
                ON CONFLICT (chave_acesso) DO UPDATE SET data_atualizacao = EXCLUDED.data_atualizacao
                RETURNING id;
            """
            result = pg_hook.run(insert_nfe_sql, parameters=list(nfe_data.values()), autocommit=True, fetch=True)
            nfe_id = result[0][0] if result else None
            self.log.info(f"NFe ID: {nfe_id}")

            if not nfe_id:
                raise Exception("Falha ao inserir NFe principal.")

            # --- 3. Insert Endereços ---
            if emitente_id and data.get('emitente_endereco'):
                endereco_emit_data = data['emitente_endereco']
                endereco_emit_data['pessoa_id'] = emitente_id
                endereco_emit_data['nfe_id'] = None # Link to pessoa, not NFe directly here
                self._insert_endereco(pg_hook, endereco_emit_data)

            if destinatario_id and data.get('destinatario_endereco'):
                endereco_dest_data = data['destinatario_endereco']
                endereco_dest_data['pessoa_id'] = destinatario_id
                endereco_dest_data['nfe_id'] = None # Link to pessoa, not NFe directly here
                self._insert_endereco(pg_hook, endereco_dest_data)
            
            if transportadora_id and data.get('transportadora_endereco'):
                endereco_transp_data = data['transportadora_endereco']
                endereco_transp_data['pessoa_id'] = transportadora_id
                endereco_transp_data['nfe_id'] = None # Link to pessoa, not NFe directly here
                self._insert_endereco(pg_hook, endereco_transp_data)

            # NOTE: For 'RETIRADA' and 'ENTREGA' addresses, they might be directly linked to NFe.
            # The current XML parsing might not extract these explicitly, but if they were,
            # their 'nfe_id' should be set, and 'pessoa_id' should be None.
            # Example (if present in XML and extracted):
            # if data.get('local_retirada_endereco'):
            #    retirada_data = data['local_retirada_endereco']
            #    retirada_data['nfe_id'] = nfe_id
            #    retirada_data['pessoa_id'] = None
            #    retirada_data['tipo_endereco'] = 'RETIRADA'
            #    self._insert_endereco(pg_hook, retirada_data)


            # --- 4. Insert NFe_Pessoa (Relationships) ---
            if emitente_id:
                pg_hook.run(f"INSERT INTO nfe.nfe_pessoa (nfe_id, pessoa_id, tipo_relacao) VALUES (%s, %s, 'EMITENTE');",
                            parameters=(nfe_id, emitente_id), autocommit=True)
            if destinatario_id:
                pg_hook.run(f"INSERT INTO nfe.nfe_pessoa (nfe_id, pessoa_id, tipo_relacao) VALUES (%s, %s, 'DESTINATARIO');",
                            parameters=(nfe_id, destinatario_id), autocommit=True)

            # --- 5. Insert Itens e Impostos ---
            for item in data.get('itens', []):
                item_data_to_insert = item.copy()
                item_data_to_insert['nfe_id'] = nfe_id
                impostos = item_data_to_insert.pop('impostos', []) # Extract impostos before inserting item

                item_columns = ', '.join(item_data_to_insert.keys())
                item_values_placeholders = ', '.join(['%s'] * len(item_data_to_insert))
                insert_item_sql = f"""
                    INSERT INTO nfe.item_nfe ({item_columns})
                    VALUES ({item_values_placeholders})
                    RETURNING id;
                """
                item_nfe_id_result = pg_hook.run(insert_item_sql, parameters=list(item_data_to_insert.values()), autocommit=True, fetch=True)
                item_nfe_id = item_nfe_id_result[0][0] if item_nfe_id_result else None
                self.log.debug(f"Item NFe ID: {item_nfe_id}")

                if item_nfe_id:
                    for imposto_entry in impostos:
                        imposto_data_to_insert = imposto_entry.copy()
                        imposto_data_to_insert['item_nfe_id'] = item_nfe_id
                        
                        imposto_columns = ', '.join(imposto_data_to_insert.keys())
                        imposto_values_placeholders = ', '.join(['%s'] * len(imposto_data_to_insert))
                        insert_imposto_sql = f"""
                            INSERT INTO nfe.imposto ({imposto_columns})
                            VALUES ({imposto_values_placeholders});
                        """
                        try:
                            pg_hook.run(insert_imposto_sql, parameters=list(imposto_data_to_insert.values()), autocommit=True)
                        except Exception as e:
                            self.log.error(f"Erro ao inserir imposto para item {item_nfe_id}: {str(e)}")

            # --- 6. Insert Totais ---
            if data.get('totais'):
                totais_data = data['totais']
                totais_data['nfe_id'] = nfe_id
                totais_columns = ', '.join(totais_data.keys())
                totais_values_placeholders = ', '.join(['%s'] * len(totais_data))
                insert_totais_sql = f"""
                    INSERT INTO nfe.totais_nfe ({totais_columns})
                    VALUES ({totais_values_placeholders});
                """
                pg_hook.run(insert_totais_sql, parameters=list(totais_data.values()), autocommit=True)

            # --- 7. Insert Transporte and Transporte_Items ---
            if data.get('transporte'):
                transporte_data = data['transporte']
                transporte_data['nfe_id'] = nfe_id
                transporte_data['transportadora_id'] = transportadora_id # Link to transportadora persona
                
                transporte_columns = ', '.join(transporte_data.keys())
                transporte_values_placeholders = ', '.join(['%s'] * len(transporte_data))
                insert_transporte_sql = f"""
                    INSERT INTO nfe.transporte ({transporte_columns})
                    VALUES ({transporte_values_placeholders})
                    RETURNING id;
                """
                transporte_id_result = pg_hook.run(insert_transporte_sql, parameters=list(transporte_data.values()), autocommit=True, fetch=True)
                transporte_id = transporte_id_result[0][0] if transporte_id_result else None
                self.log.debug(f"Transporte ID: {transporte_id}")

                if transporte_id:
                    # Insert Volumes
                    for vol_data in data.get('volumes', []):
                        vol_data_to_insert = vol_data.copy()
                        vol_data_to_insert['transporte_id'] = transporte_id
                        vol_columns = ', '.join(vol_data_to_insert.keys())
                        vol_values_placeholders = ', '.join(['%s'] * len(vol_data_to_insert))
                        insert_vol_sql = f"INSERT INTO nfe.transporte_item ({vol_columns}) VALUES ({vol_values_placeholders}) RETURNING id;"
                        vol_item_id_result = pg_hook.run(insert_vol_sql, parameters=list(vol_data_to_insert.values()), autocommit=True, fetch=True)
                        vol_item_id = vol_item_id_result[0][0] if vol_item_id_result else None

                        # Insert Lacres associated with this volume
                        if vol_item_id:
                            for lac_data in data.get('lacres', []): # This assumes lacres are already parsed with their parent volume context
                                # This part needs refinement in extrair_dados_nfe to link lacres to specific volumes.
                                # For simplicity, assuming a flat list for now, but ideally lacre_data would have a volume_id.
                                # If lacres are directly under a vol tag, then we should process them here.
                                if 'item_pai_id' in lac_data and lac_data['item_pai_id'] is None: # If lacre is not yet linked
                                    # Assuming lacres are extracted as a flat list and we need to associate them to volumes.
                                    # This is a simplification; a more robust solution would involve parsing logic to link them directly.
                                    # For now, let's assume the first few lacres are for the first volume, etc. (needs better XML parsing)
                                    # For now, I'll insert lacres generally and note this for improvement.
                                    pass # Lacres need better extraction to link to parent item

                    # Insert Veiculos
                    for veic_data in data.get('veiculos', []):
                        veic_data_to_insert = veic_data.copy()
                        veic_data_to_insert['transporte_id'] = transporte_id
                        veic_columns = ', '.join(veic_data_to_insert.keys())
                        veic_values_placeholders = ', '.join(['%s'] * len(veic_data_to_insert))
                        insert_veic_sql = f"INSERT INTO nfe.transporte_item ({veic_columns}) VALUES ({veic_values_placeholders});"
                        pg_hook.run(insert_veic_sql, parameters=list(veic_data_to_insert.values()), autocommit=True)
                
            # --- 8. Insert Informações Adicionais ---
            if data.get('informacoes_adicionais'):
                info_adic_data = data['informacoes_adicionais']
                info_adic_data['nfe_id'] = nfe_id
                info_columns = ', '.join(info_adic_data.keys())
                info_values_placeholders = ', '.join(['%s'] * len(info_adic_data))
                insert_info_adic_sql = f"""
                    INSERT INTO nfe.informacoes_adicionais ({info_columns})
                    VALUES ({info_values_placeholders});
                """
                pg_hook.run(insert_info_adic_sql, parameters=list(info_adic_data.values()), autocommit=True)

            # --- 9. Insert Processing Status (initial 'PENDENTE' or update to 'SUCESSO' after full process) ---
            # Initial status is 'PENDENTE' in the NFe table default.
            # Actual processing status is logged here.
            # self.save_processing_status will be called by execute() after successful insertion and move.

            return nfe_id

        except Exception as e:
            self.log.error(f"Erro ao salvar dados no PostgreSQL: {str(e)}")
            # Log failure in processamento_nfe table
            self.save_processing_status(pg_hook, nfe_id, data.get('nfe', {}).get('chave_acesso', 'UNKNOWN_KEY'), 'FALHA_DB', str(e), original_xml)
            return None

    def save_processing_status(self, pg_hook: PostgresHook, nfe_id: Optional[int], chave_acesso_or_filename: str, status: str, message: str, xml_content: Optional[str]):
        """
        Salva ou atualiza o status de processamento da NFe na tabela processamento_nfe.
        Tries to link to nfe_id first, then falls back to chave_acesso for logging.
        """
        try:
            # First, try to find nfe_id if not provided, using chave_acesso from data (if available)
            if nfe_id is None and 'chave_acesso' in (data := self.extrair_dados_nfe(xml_content, chave_acesso_or_filename) if xml_content else {}).get('nfe', {}):
                chave_acesso = data['nfe']['chave_acesso']
                result = pg_hook.get_first(f"SELECT id FROM nfe.nfe WHERE chave_acesso = %s;", parameters=(chave_acesso,))
                if result:
                    nfe_id = result[0]
            
            # Now, construct the insert/update statement
            # If nfe_id is available, link to it. Otherwise, rely on unique filename/chave_acesso for identification in case of parsing errors.
            # For simplicity, let's insert a new record for each processing attempt status.
            insert_sql = """
                INSERT INTO nfe.processamento_nfe (nfe_id, data_processamento, status, mensagem, xml_original, xml_processado)
                VALUES (%s, %s, %s, %s, %s, %s);
            """
            parameters = [
                nfe_id,
                datetime.now(),
                status,
                message,
                xml_content, # Store the original XML content
                None # xml_processado can be null, or you can store a processed version if applicable
            ]
            pg_hook.run(insert_sql, parameters=parameters, autocommit=True)
            self.log.info(f"Status de processamento '{status}' salvo para NFe/Arquivo: {chave_acesso_or_filename}")
        except Exception as e:
            self.log.error(f"Erro ao salvar status de processamento para '{chave_acesso_or_filename}': {str(e)}")