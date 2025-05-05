FROM apache/airflow:2.7.1

USER root

# Instalar dependências do sistema
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Copiar requirements
COPY requirements.txt /requirements.txt

# Instalar pacotes Python
RUN pip install --no-cache-dir -r /requirements.txt

# Criar diretórios de dados
RUN mkdir -p /opt/airflow/data/raw /opt/airflow/data/processed

# Definir permissões
USER root
RUN chown -R airflow:root /opt/airflow/data
USER airflow