import os
import shutil
import xml.etree.ElementTree as ET
import json
from datetime import datetime
from typing import Dict, List, Optional, Any

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.postgres_hook import PostgresHook


class NFeParserOperator(BaseOperator):
    """
    Operador para extrair dados de arquivos XML de NFe, salvar no PostgreSQL
    e mover arquivos processados para outra pasta.
    
    :param source_folder: Caminho da pasta de origem dos arquivos XML
    :param destination_folder: Caminho da pasta de destino para os arquivos processados
    :param postgres_conn_id: ID da conexão do PostgreSQL no Airflow
    :param table_name: Nome da tabela no PostgreSQL onde os dados serão inseridos
    """
    
    @apply_defaults
    def __init__(
        self,
        source_folder: str,
        destination_folder: str,
        postgres_conn_id: str,
        table_name: str,
        *args, **kwargs
    ) -> None:
        super().__init__(*args, **kwargs)
        self.source_folder = source_folder
        self.destination_folder = destination_folder
        self.postgres_conn_id = postgres_conn_id
        self.table_name = table_name
        
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
                # Extrair dados do XML
                nfe_data = self.extrair_dados_nfe(xml_path)
                
                if not nfe_data:
                    self.log.warning(f"Não foi possível extrair dados do arquivo: {xml_path}")
                    continue
                
                # Salvar dados no PostgreSQL
                self.save_to_postgres(pg_hook, nfe_data)
                
                # Mover arquivo processado para a pasta de destino
                dest_path = os.path.join(self.destination_folder, xml_file)
                shutil.move(xml_path, dest_path)
                
                self.log.info(f"Arquivo processado e movido para: {dest_path}")
                
            except Exception as e:
                self.log.error(f"Erro ao processar o arquivo {xml_path}: {str(e)}")
    
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
    
    def extrair_dados_nfe(self, caminho_xml: str) -> Optional[Dict[str, Any]]:
        """
        Extrai os principais dados de uma Nota Fiscal Eletrônica no formato XML.
        
        :param caminho_xml: Caminho para o arquivo XML da NFe
        :return: Dicionário com os dados extraídos da NFe ou None em caso de erro
        """
        try:
            # Fazer o parse do arquivo XML
            tree = ET.parse(caminho_xml)
            root = tree.getroot()
            
            # Definir o namespace
            ns = {'nfe': 'http://www.portalfiscal.inf.br/nfe'}
            
            # Extrair informações básicas da NFe
            dados_nfe = {}
            
            # Identificação da NFe
            ide = root.find('.//nfe:ide', ns)
            if ide is not None:
                dados_nfe['chave_acesso'] = root.find('.//nfe:infNFe', ns).attrib.get('Id', '')[3:]
                dados_nfe['numero'] = ide.findtext('nfe:nNF', '', ns)
                dados_nfe['serie'] = ide.findtext('nfe:serie', '', ns)
                
                # Converter data de emissão para timestamp
                data_emissao_str = ide.findtext('nfe:dhEmi', '', ns)
                data_emissao = self.parse_date_to_timestamp(data_emissao_str)
                dados_nfe['data_emissao'] = data_emissao if data_emissao else None
                
                dados_nfe['natureza_operacao'] = ide.findtext('nfe:natOp', '', ns)
                
            # Informações do emitente
            emit = root.find('.//nfe:emit', ns)
            if emit is not None:
                dados_nfe['emitente_nome'] = emit.findtext('nfe:xNome', '', ns)
                dados_nfe['emitente_cnpj'] = emit.findtext('nfe:CNPJ', '', ns)
                dados_nfe['emitente_ie'] = emit.findtext('nfe:IE', '', ns)
                
                # Endereço do emitente
                enderEmit = emit.find('nfe:enderEmit', ns)
                if enderEmit is not None:
                    dados_nfe['emitente_endereco'] = {
                        'logradouro': enderEmit.findtext('nfe:xLgr', '', ns),
                        'numero': enderEmit.findtext('nfe:nro', '', ns),
                        'bairro': enderEmit.findtext('nfe:xBairro', '', ns),
                        'municipio': enderEmit.findtext('nfe:xMun', '', ns),
                        'uf': enderEmit.findtext('nfe:UF', '', ns),
                        'cep': enderEmit.findtext('nfe:CEP', '', ns),
                    }
            
            # Informações do destinatário
            dest = root.find('.//nfe:dest', ns)
            if dest is not None:
                dados_nfe['destinatario_nome'] = dest.findtext('nfe:xNome', '', ns)
                dados_nfe['destinatario_cnpj'] = dest.findtext('nfe:CNPJ', '', ns) or dest.findtext('nfe:CPF', '', ns)
                dados_nfe['destinatario_ie'] = dest.findtext('nfe:IE', '', ns)
                
                # Endereço do destinatário
                enderDest = dest.find('nfe:enderDest', ns)
                if enderDest is not None:
                    dados_nfe['destinatario_endereco'] = {
                        'logradouro': enderDest.findtext('nfe:xLgr', '', ns),
                        'numero': enderDest.findtext('nfe:nro', '', ns),
                        'bairro': enderDest.findtext('nfe:xBairro', '', ns),
                        'municipio': enderDest.findtext('nfe:xMun', '', ns),
                        'uf': enderDest.findtext('nfe:UF', '', ns),
                        'cep': enderDest.findtext('nfe:CEP', '', ns),
                    }
            
            # Extrair itens da NFe
            itens = []
            for i, det in enumerate(root.findall('.//nfe:det', ns)):
                item = {}
                item['numero_item'] = det.attrib.get('nItem', str(i+1))
                
                prod = det.find('nfe:prod', ns)
                if prod is not None:
                    item['codigo'] = prod.findtext('nfe:cProd', '', ns)
                    item['descricao'] = prod.findtext('nfe:xProd', '', ns)
                    item['ncm'] = prod.findtext('nfe:NCM', '', ns)
                    item['cfop'] = prod.findtext('nfe:CFOP', '', ns)
                    item['unidade'] = prod.findtext('nfe:uCom', '', ns)
                    item['quantidade'] = prod.findtext('nfe:qCom', '', ns)
                    item['valor_unitario'] = prod.findtext('nfe:vUnCom', '', ns)
                    item['valor_total'] = prod.findtext('nfe:vProd', '', ns)
                
                # Informações sobre impostos
                imposto = det.find('nfe:imposto', ns)
                if imposto is not None:
                    icms = imposto.find('.//nfe:ICMS', ns)
                    if icms is not None:
                        for icms_tipo in icms:
                            if icms_tipo.tag.endswith('}ICMS00') or icms_tipo.tag.endswith('}ICMS10') or \
                               icms_tipo.tag.endswith('}ICMS20') or icms_tipo.tag.endswith('}ICMS30') or \
                               icms_tipo.tag.endswith('}ICMS40') or icms_tipo.tag.endswith('}ICMS51') or \
                               icms_tipo.tag.endswith('}ICMS60') or icms_tipo.tag.endswith('}ICMS70') or \
                               icms_tipo.tag.endswith('}ICMS90'):
                                item['icms_cst'] = icms_tipo.findtext('nfe:CST', '', ns) or icms_tipo.findtext('nfe:CSOSN', '', ns)
                                item['icms_base_calculo'] = icms_tipo.findtext('nfe:vBC', '', ns)
                                item['icms_aliquota'] = icms_tipo.findtext('nfe:pICMS', '', ns)
                                item['icms_valor'] = icms_tipo.findtext('nfe:vICMS', '', ns)
                    
                    ipi = imposto.find('.//nfe:IPI', ns)
                    if ipi is not None:
                        item['ipi_cst'] = ipi.findtext('.//nfe:CST', '', ns)
                        item['ipi_base_calculo'] = ipi.findtext('.//nfe:vBC', '', ns)
                        item['ipi_aliquota'] = ipi.findtext('.//nfe:pIPI', '', ns)
                        item['ipi_valor'] = ipi.findtext('.//nfe:vIPI', '', ns)
                    
                    pis = imposto.find('.//nfe:PIS', ns)
                    if pis is not None:
                        for pis_tipo in pis:
                            if pis_tipo.tag.endswith('}PISAliq') or pis_tipo.tag.endswith('}PISQtde') or \
                               pis_tipo.tag.endswith('}PISNT') or pis_tipo.tag.endswith('}PISOutr'):
                                item['pis_cst'] = pis_tipo.findtext('nfe:CST', '', ns)
                                item['pis_base_calculo'] = pis_tipo.findtext('nfe:vBC', '', ns)
                                item['pis_aliquota'] = pis_tipo.findtext('nfe:pPIS', '', ns)
                                item['pis_valor'] = pis_tipo.findtext('nfe:vPIS', '', ns)
                    
                    cofins = imposto.find('.//nfe:COFINS', ns)
                    if cofins is not None:
                        for cofins_tipo in cofins:
                            if cofins_tipo.tag.endswith('}COFINSAliq') or cofins_tipo.tag.endswith('}COFINSQtde') or \
                               cofins_tipo.tag.endswith('}COFINSNT') or cofins_tipo.tag.endswith('}COFINSOutr'):
                                item['cofins_cst'] = cofins_tipo.findtext('nfe:CST', '', ns)
                                item['cofins_base_calculo'] = cofins_tipo.findtext('nfe:vBC', '', ns)
                                item['cofins_aliquota'] = cofins_tipo.findtext('nfe:pCOFINS', '', ns)
                                item['cofins_valor'] = cofins_tipo.findtext('nfe:vCOFINS', '', ns)
                
                itens.append(item)
            
            dados_nfe['itens'] = itens
            
            # Totais da NFe
            total = root.find('.//nfe:total', ns)
            if total is not None:
                icmstot = total.find('nfe:ICMSTot', ns)
                if icmstot is not None:
                    dados_nfe['total'] = {
                        'base_calculo_icms': icmstot.findtext('nfe:vBC', '', ns),
                        'valor_icms': icmstot.findtext('nfe:vICMS', '', ns),
                        'base_calculo_icms_st': icmstot.findtext('nfe:vBCST', '', ns),
                        'valor_icms_st': icmstot.findtext('nfe:vST', '', ns),
                        'valor_produtos': icmstot.findtext('nfe:vProd', '', ns),
                        'valor_frete': icmstot.findtext('nfe:vFrete', '', ns),
                        'valor_seguro': icmstot.findtext('nfe:vSeg', '', ns),
                        'valor_desconto': icmstot.findtext('nfe:vDesc', '', ns),
                        'valor_ipi': icmstot.findtext('nfe:vIPI', '', ns),
                        'valor_pis': icmstot.findtext('nfe:vPIS', '', ns),
                        'valor_cofins': icmstot.findtext('nfe:vCOFINS', '', ns),
                        'valor_outras_despesas': icmstot.findtext('nfe:vOutro', '', ns),
                        'valor_total': icmstot.findtext('nfe:vNF', '', ns),
                    }
            
            # Informações de pagamento
            pag = root.find('.//nfe:pag', ns)
            if pag is not None:
                dados_nfe['pagamentos'] = []
                for detPag in pag.findall('nfe:detPag', ns):
                    pagamento = {
                        'forma': detPag.findtext('nfe:tPag', '', ns),
                        'valor': detPag.findtext('nfe:vPag', '', ns),
                    }
                    dados_nfe['pagamentos'].append(pagamento)
            
            # Informações adicionais
            infAdic = root.find('.//nfe:infAdic', ns)
            if infAdic is not None:
                dados_nfe['informacoes_adicionais'] = {
                    'informacoes_fisco': infAdic.findtext('nfe:infAdFisco', '', ns),
                    'informacoes_complementares': infAdic.findtext('nfe:infCpl', '', ns),
                }
            
            # Adicionar timestamp de processamento
            dados_nfe['data_processamento'] = datetime.now()
            dados_nfe['nome_arquivo'] = os.path.basename(caminho_xml)
            
            return dados_nfe
        
        except Exception as e:
            self.log.error(f"Erro ao processar o arquivo {caminho_xml}: {str(e)}")
            return None
    
    def save_to_postgres(self, pg_hook: PostgresHook, dados_nfe: Dict[str, Any]) -> None:
        """
        Salva os dados extraídos da NFe no PostgreSQL.
        
        :param pg_hook: Instância do PostgresHook para conectar ao banco
        :param dados_nfe: Dicionário com os dados extraídos da NFe
        :return: None
        """
        # Criar uma cópia do dicionário para não modificar o original
        dados_para_inserir = dados_nfe.copy()
        
        # Converter estruturas aninhadas para JSON
        for key, value in dados_para_inserir.items():
            if isinstance(value, (dict, list)):
                dados_para_inserir[key] = json.dumps(value, ensure_ascii=False)
        
        # Inserir na tabela principal de NFes
        columns = ', '.join(dados_para_inserir.keys())
        values = ', '.join(['%s'] * len(dados_para_inserir))
        
        insert_sql = f"""
        INSERT INTO {self.table_name} ({columns})
        VALUES ({values})
        ON CONFLICT (chave_acesso) 
        DO UPDATE SET 
            data_processamento = EXCLUDED.data_processamento
        """
        
        # pg_hook.run(insert_sql, parameters=list(dados_para_inserir.values()))
        pg_hook.run(insert_sql, parameters=list(dados_para_inserir.values()), autocommit=True)
        self.log.info(f"Dados da NFe {dados_nfe.get('numero')} salvos no PostgreSQL.")