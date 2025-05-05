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
        CREATE TABLE IF NOT EXISTS nfe_dados (
            id SERIAL PRIMARY KEY,
            chave_acesso VARCHAR(255) UNIQUE,
            numero VARCHAR(50),
            serie VARCHAR(50),
            data_emissao TIMESTAMP,  -- Alterado para TIMESTAMP
            natureza_operacao TEXT,
            emitente_nome TEXT,
            emitente_cnpj VARCHAR(50),
            emitente_ie VARCHAR(50),
            emitente_endereco JSONB,
            destinatario_nome TEXT,
            destinatario_cnpj VARCHAR(50),
            destinatario_ie VARCHAR(50),
            destinatario_endereco JSONB,
            itens JSONB,
            total JSONB,
            pagamentos JSONB,
            informacoes_adicionais JSONB,
            data_processamento TIMESTAMP,
            nome_arquivo VARCHAR(255)
        );
        
        -- Índices para otimizar consultas
        CREATE INDEX IF NOT EXISTS idx_nfe_emitente_cnpj ON nfe_dados(emitente_cnpj);
        CREATE INDEX IF NOT EXISTS idx_nfe_destinatario_cnpj ON nfe_dados(destinatario_cnpj);
        CREATE INDEX IF NOT EXISTS idx_nfe_data_emissao ON nfe_dados(data_emissao);
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