import os
import glob
import tempfile
import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
from xml.etree import ElementTree as ET
from minio import Minio


def extrair_dados_nfe(caminho_xml):
    conn = None
    cur = None
    """Extrai os principais dados de uma Nota Fiscal Eletrônica no formato XML."""
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
            dados_nfe['data_emissao'] = ide.findtext('nfe:dhEmi', '', ns)
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

        itens = []
        for i, det in enumerate(root.findall('.//nfe:det', ns)):
            item = {}
            item['numero_item'] = det.attrib.get('nItem', str(i + 1))

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
                            item['icms_cst'] = icms_tipo.findtext('nfe:CST', '', ns) or icms_tipo.findtext(
                                'nfe:CSOSN', '', ns)
                            item['icms_base_calculo'] = icms_tipo.findtext('nfe:vBC', '', ns)
                            item['icms_aliquota'] = icms_tipo.findtext('nfe:pICMS', '', ns)
                            item['icms_valor'] = icms_tipo.findtext('nfe:vICMS', '', ns)

                ipi = imposto.find('.//nfe:IPI', ns)
                if ipi is not None:
                    item['ipi_cst'] = ipi.findtext('.//nfe:CST', '', ns)
                                icms_tipo.tag.endswith('}ICMS40') or icms_tipo.tag.endswith('}ICMS51') or \
                                        icms_tipo.tag.endswith('}ICMS60') or icms_tipo.tag.endswith('}ICMS70') or \
                                        icms_tipo.tag.endswith('}ICMS90'):
                                    item['icms_cst'] = icms_tipo.findtext('nfe:CST', '', ns) or icms_tipo.findtext(
                                        'nfe:CSOSN', '', ns)
                                    item['icms_base_calculo'] = icms_tipo.findtext('nfe:vBC', '', ns)
                                    item['icms_aliquota'] = icms_tipo.findtext('nfe:pICMS', '', ns)
                                    item['icms_valor'] = icms_tipo.findtext('nfe:vICMS', '', ns)
        return dados_nfe

    except Exception as e:
        print(f"Erro ao processar o arquivo {caminho_xml}: {str(e)}")
        return None


def store_nfe_data(nfe_data):
    """Armazena os dados da NF-E no banco de dados PostgreSQL."""
    conn = None
    try:
        # Recupera os parâmetros de conexão do banco de dados das variáveis do Airflow
        db_host = Variable.get("db_host")
        db_name = Variable.get("db_name")
        db_user = Variable.get("db_user")
        db_password = Variable.get("db_password")

        conn = psycopg2.connect(host=db_host, database=db_name, user=db_user, password=db_password)
        cur = conn.cursor()

        # Insere os dados principais na tabela nfe
        cur.execute("""
            INSERT INTO nfe ( chave_acesso, numero, serie, data_emissao, natureza_operacao, emitente_cnpj, destinatario_cnpj, valor_total)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (chave_acesso) DO NOTHING;
        """, (
            nfe_data.get('chave_acesso'),
            nfe_data.get('numero'),
            nfe_data.get('serie'),
            nfe_data.get('data_emissao'),
            nfe_data.get('natureza_operacao'),
            nfe_data.get('emitente_cnpj'),
            nfe_data.get('destinatario_cnpj'),
            nfe_data.get('total', {}).get('valor_total')
        ))
        #Insere os itens na tabela item
        for item in nfe_data.get('itens', []):
            cur.execute("""
                INSERT INTO item (
                    nfe_chave_acesso, numero_item, codigo, descricao, quantidade, valor_unitario, valor_total, icms_cst, icms_base_calculo, icms_aliquota, icms_valor
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                nfe_data.get('chave_acesso'),
                item.get('numero_item'),
                item.get('codigo'),
                item.get('descricao'),
                item.get('quantidade'),
                item.get('valor_unitario'),
                item.get('valor_total'),
                item.get('icms_cst'),
                item.get('icms_base_calculo'),
                item.get('icms_aliquota'),
                item.get('icms_valor')
            ))
        # Commit das mudanças no banco de dados
        conn.commit()
        print(f"Dados da NFe {nfe_data.get('chave_acesso', 'N/A')} armazenados no banco de dados.")

    except psycopg2.Error as db_error:
        print(f"Database error: {db_error}")
        conn.rollback()
    except Exception as e:
        print(f"Erro ao armazenar dados da NFe: {str(e)}")
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Configuração do DAG
dag = DAG(
    'nfe_processing',
    default_args=default_args,
    description='DAG para processar arquivos NF-E',
    schedule_interval=None,
)

def check_for_new_files(ti, **kwargs):
    file_name = kwargs['dag_run'].conf.get('file_name')
    if file_name:
        return file_name
    return []

def download_file_from_minio(file_name, **kwargs):
    """Baixa o arquivo especificado do MinIO."""
    minio_endpoint = Variable.get('minio_endpoint')
    minio_access_key = Variable.get('minio_access_key')
    minio_secret_key = Variable.get('minio_secret_key')
    bucket_name = 'nfe-inbound'
    client = Minio(minio_endpoint, access_key=minio_access_key, secret_key=minio_secret_key, secure=False)
    temp_dir = tempfile.gettempdir()
    local_file_path = os.path.join(temp_dir, file_name)
    client.fget_object(bucket_name, file_name, local_file_path)
    return local_file_path


def parse_xml_files(ti, **kwargs):
    """Extrai os dados do arquivo XML."""
    file_path = kwargs['ti'].xcom_pull(task_ids='download_file_from_minio')
    try:
        dados_nfe = extrair_dados_nfe(file_path)
        if dados_nfe:
            print(f"\nDados extraídos do arquivo {file_path}:")
            return [dados_nfe]
        else:
            print(f"Erro ao processar o arquivo {file_path}")
            return None
    except Exception as e:
        print(f"Erro durante a extração de dados do arquivo {file_path}: {str(e)}")
        return None
    finally:
        os.remove(file_path)

def store_data_in_db_task(ti):
    """Armazena os dados da NF-E no banco de dados."""
    parsed_data = ti.xcom_pull(task_ids='parse_xml_task')
    try:
        for nfe_data in parsed_data:
            store_nfe_data(nfe_data)
    except Exception as e:
        print(f"Erro ao armazenar dados no banco de dados: {str(e)}")

check_files_task = PythonOperator(task_id='check_for_new_files', python_callable=check_for_new_files, dag=dag)
download_file_task = PythonOperator(task_id='download_file_from_minio', python_callable=download_file_from_minio, op_kwargs={'file_name': '{{ ti.xcom_pull(task_ids="check_for_new_files") }}'}, dag=dag)
parse_xml_task = PythonOperator(task_id='parse_xml_task', python_callable=parse_xml_files, dag=dag)
store_data_task = PythonOperator(task_id='store_data_in_db_task', python_callable=store_data_in_db_task, dag=dag)

check_files_task >> download_file_task >> parse_xml_task >> store_data_task
