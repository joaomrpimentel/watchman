from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from nfe_parser.operators.nfe_parser_operator import NFeParserOperator


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'nfe_parser',
    default_args=default_args,
    description='DAG para processar arquivos XML de Notas Fiscais Eletrônicas',
    schedule_interval=timedelta(minutes=30),  # Execute a cada 30 minutos
    start_date=datetime(2025, 5, 1),
    catchup=False,
    tags=['nfe', 'parser', 'xml'],
) as dag:

    # Tarefa para verificar e criar a tabela no PostgreSQL se não existir
    create_table = PostgresOperator(
        task_id='create_table_if_not_exists',
        postgres_conn_id='postgres_default',
        sql="""
        CREATE TABLE IF NOT EXISTS PESSOA_FISICA (
    cpf VARCHAR(20),
    id SERIAL PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS PESSOA_JURIDICA (
    cnpj VARCHAR(20),
    inscricaoEstadual INTEGER,
    cnae VARCHAR(20),
    id SERIAL PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS ENDERECO_ENTIDADE_SOCIAL (
    cep VARCHAR(20),
    rua VARCHAR(255),
    numero INTEGER,
    bairro VARCHAR(255),
    cidade VARCHAR(255),
    estado CHAR(2),
    id SERIAL PRIMARY KEY,
    pais VARCHAR(20),
    nome VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS NOTA_FISCAL (
    id SERIAL PRIMARY KEY,
    chaveAcesso VARCHAR(255),
    numero INTEGER,
    serie INTEGER,
    dataEmissao TIMESTAMP,
    tributoFederal DECIMAL,
    tributoEstadual DECIMAL,
    idVendedor INTEGER,
    idComprador INTEGER
);

CREATE TABLE IF NOT EXISTS ITEM (
    id SERIAL PRIMARY KEY,
    quantidade DECIMAL,
    valor DECIMAL,
    desconto DECIMAL,
    tributos DECIMAL,
    discriminacao VARCHAR(255),
    idNotaFiscal INTEGER
);

CREATE TABLE IF NOT EXISTS SERVICO (
    id SERIAL PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS PRODUTO (
    cEnqLegal INTEGER,
    cfop INTEGER,
    frete DECIMAL,
    id SERIAL PRIMARY KEY
);
        """
    )

    # Tarefa para processar os arquivos XML e salvar no PostgreSQL
    process_nfe_files = NFeParserOperator(
        task_id='process_nfe_files',
        source_folder='/opt/airflow/data/raw',
        destination_folder='/opt/airflow/data/processed',
        postgres_conn_id='postgres_default',
        table_name='nfe_dados',
    )

    # Tarefa para limpeza de arquivos antigos (opcional)
    def clean_old_files():
        """
        Remove arquivos processados mais antigos que 30 dias
        """
        import os
        import shutil
        from datetime import datetime, timedelta
        
        processed_folder = '/opt/airflow/data/processed'
        cutoff_date = datetime.now() - timedelta(days=30)
        
        for filename in os.listdir(processed_folder):
            filepath = os.path.join(processed_folder, filename)
            file_modified = datetime.fromtimestamp(os.path.getmtime(filepath))
            if file_modified < cutoff_date:
                os.remove(filepath)
                print(f"Removido arquivo antigo: {filepath}")

    clean_old_processed_files = PythonOperator(
        task_id='clean_old_processed_files',
        python_callable=clean_old_files,
    )

    # Definir o fluxo de execução das tarefas
    create_table >> process_nfe_files >> clean_old_processed_files
    # process_nfe_files >> clean_old_processed_files