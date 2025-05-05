-- Criar banco de dados para armazenar os dados das NFes
CREATE DATABASE nfe_db;

\c nfe_db

-- Criar tabela principal para armazenar os dados das NFes
CREATE TABLE IF NOT EXISTS nfe_dados (
    id SERIAL PRIMARY KEY,
    chave_acesso VARCHAR(255) UNIQUE,
    numero VARCHAR(50),
    serie VARCHAR(50),
    data_emissao TIMESTAMP, 
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
CREATE INDEX idx_nfe_emitente_cnpj ON nfe_dados(emitente_cnpj);
CREATE INDEX idx_nfe_destinatario_cnpj ON nfe_dados(destinatario_cnpj);
CREATE INDEX idx_nfe_data_emissao ON nfe_dados(data_emissao); 

-- Criar usuário para o Airflow acessar o banco
CREATE USER airflow WITH PASSWORD 'airflow';
GRANT ALL PRIVILEGES ON DATABASE nfe_db TO airflow;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO airflow;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO airflow;

-- Criar view para facilitar consultas comuns
CREATE OR REPLACE VIEW vw_nfe_resumo AS
SELECT 
    chave_acesso,
    numero,
    serie,
    data_emissao,
    emitente_nome,
    emitente_cnpj,
    destinatario_nome,
    destinatario_cnpj,
    total->>'valor_total' as valor_total,
    data_processamento
FROM nfe_dados
ORDER BY data_processamento DESC;

-- Comentários para facilitar o entendimento
COMMENT ON TABLE nfe_dados IS 'Tabela principal para armazenar os dados das Notas Fiscais Eletrônicas';
COMMENT ON VIEW vw_nfe_resumo IS 'Visão resumida das Notas Fiscais Eletrônicas';