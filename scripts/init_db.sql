-- Criar banco de dados para armazenar os dados das NFes
CREATE DATABASE nfe_db;

\c nfe_db

-- Criar tabela principal para armazenar os dados das NFes
-- CREATE TABLE IF NOT EXISTS nfe_dados (
--     id SERIAL PRIMARY KEY,
--     chave_acesso VARCHAR(255) UNIQUE,
--     numero VARCHAR(50),
--     serie VARCHAR(50),
--     data_emissao TIMESTAMP, 
--     natureza_operacao TEXT,
--     emitente_nome TEXT,
--     emitente_cnpj VARCHAR(50),
--     emitente_ie VARCHAR(50),
--     emitente_endereco JSONB,
--     destinatario_nome TEXT,
--     destinatario_cnpj VARCHAR(50),
--     destinatario_ie VARCHAR(50),
--     destinatario_endereco JSONB,
--     itens JSONB,
--     total JSONB,
--     pagamentos JSONB,
--     informacoes_adicionais JSONB,
--     data_processamento TIMESTAMP,
--     nome_arquivo VARCHAR(255)
-- );

CREATE TABLE PESSOA_FISICA (
            cpf VARCHAR(20),
            id SERIAL PRIMARY KEY
        );

CREATE TABLE PESSOA_JURIDICA (
    cnpj VARCHAR(20),
    inscricaoEstadual INTEGER,
    cnae VARCHAR(20),
    id SERIAL PRIMARY KEY
);

CREATE TABLE ENDERECO_ENTIDADE_SOCIAL (
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

CREATE TABLE NOTA_FISCAL (
    id SERIAL PRIMARY KEY,
    chaveAcesso INTEGER,
    numero INTEGER,
    serie INTEGER,
    dataEmissao TIMESTAMP,
    tributoFederal DECIMAL,
    tributoEstadual DECIMAL,
    idVendedor SERIAL,
    idComprador SERIAL
);

CREATE TABLE ITEM (
    id SERIAL PRIMARY KEY,
    quantidade DECIMAL,
    valor DECIMAL,
    desconto DECIMAL,
    tributos DECIMAL,
    discriminacao VARCHAR(255),
    idNotaFiscal SERIAL
);

CREATE TABLE SERVICO (
    id SERIAL PRIMARY KEY
);

CREATE TABLE PRODUTO (
    cEnqLegal INTEGER,
    cfop INTEGER,
    frete DECIMAL,
    id SERIAL PRIMARY KEY
);

ALTER TABLE NOTA_FISCAL ADD CONSTRAINT FK_NOTA_FISCAL_ID_VENDEDOR
FOREIGN KEY (idVendedor)
REFERENCES PESSOA_JURIDICA (id)
ON DELETE CASCADE;

ALTER TABLE NOTA_FISCAL ADD CONSTRAINT FK_NOTA_FISCAL_ID_COMPRADOR
FOREIGN KEY (idComprador)
REFERENCES PESSOA_FISICA (id)
ON DELETE CASCADE;

ALTER TABLE ITEM ADD CONSTRAINT FK_ITEM_2
    FOREIGN KEY (idNotaFiscal)
    REFERENCES NOTA_FISCAL (id)
    ON DELETE RESTRICT;

ALTER TABLE SERVICO ADD CONSTRAINT FK_SERVICO_2
    FOREIGN KEY (id)
    REFERENCES ITEM (id)
    ON DELETE CASCADE;

ALTER TABLE PRODUTO ADD CONSTRAINT FK_PRODUTO_2
    FOREIGN KEY (id)
    REFERENCES ITEM (id)
    ON DELETE CASCADE;


-- Índices para otimizar consultas
CREATE INDEX idx_nfe_emitente_cnpj ON PESSOA_JURIDICA(cnpj);
CREATE INDEX idx_nfe_destinatario_cnpj ON PESSOA_FISICA(cpf);
CREATE INDEX idx_nfe_data_emissao ON NOTA_FISCAL(dataEmissao); 

-- Criar usuário para o Airflow acessar o banco
CREATE USER airflow WITH PASSWORD 'airflow';
GRANT ALL PRIVILEGES ON DATABASE nfe_db TO airflow;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO airflow;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO airflow;
