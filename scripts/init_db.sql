-- Script de criação do esquema otimizado de banco de dados para NFe
-- Autor: Análise e Otimização
-- Data: 08/06/2025
-- Mudanças: Generalização e agrupamento de entidades similares

CREATE SCHEMA IF NOT EXISTS nfe;
SET search_path TO nfe, public;

-- ========================================
-- 1. TABELA PESSOA (Generalização)
-- Substitui: emitente, destinatario, transportadora
-- ========================================
CREATE TABLE IF NOT EXISTS pessoa (
    id SERIAL PRIMARY KEY,
    tipo_pessoa VARCHAR(20) NOT NULL, -- 'EMITENTE', 'DESTINATARIO', 'TRANSPORTADORA'
    cnpj VARCHAR(14),
    cpf VARCHAR(11),
    id_estrangeiro VARCHAR(20),
    nome VARCHAR(100) NOT NULL,
    nome_fantasia VARCHAR(100),
    inscricao_estadual VARCHAR(20),
    inscricao_municipal VARCHAR(20),
    inscricao_suframa VARCHAR(20),
    cnae VARCHAR(7),
    regime_tributario SMALLINT,
    email VARCHAR(100),
    data_criacao TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    data_atualizacao TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT ck_pessoa_documento CHECK (
        (cnpj IS NOT NULL AND cpf IS NULL AND id_estrangeiro IS NULL) OR
        (cnpj IS NULL AND cpf IS NOT NULL AND id_estrangeiro IS NULL) OR
        (cnpj IS NULL AND cpf IS NULL AND id_estrangeiro IS NOT NULL)
    ),
    CONSTRAINT ck_pessoa_tipo CHECK (tipo_pessoa IN ('EMITENTE', 'DESTINATARIO', 'TRANSPORTADORA'))
);

CREATE INDEX IF NOT EXISTS idx_pessoa_tipo ON pessoa(tipo_pessoa);
CREATE INDEX IF NOT EXISTS idx_pessoa_cnpj ON pessoa(cnpj);
CREATE INDEX IF NOT EXISTS idx_pessoa_cpf ON pessoa(cpf);

-- ========================================
-- 2. TABELA ENDERECO (Generalização)
-- Substitui: endereco_emitente, endereco_destinatario, local_retirada, local_entrega
-- ========================================
CREATE TABLE IF NOT EXISTS endereco (
    id SERIAL PRIMARY KEY,
    pessoa_id INTEGER REFERENCES pessoa(id) ON DELETE CASCADE,
    nfe_id INTEGER, -- Para locais de retirada/entrega sem pessoa específica
    tipo_endereco VARCHAR(20) NOT NULL, -- 'PRINCIPAL', 'RETIRADA', 'ENTREGA'
    cnpj VARCHAR(14), -- Para locais de retirada/entrega
    cpf VARCHAR(11),  -- Para locais de retirada/entrega
    logradouro VARCHAR(100) NOT NULL,
    numero VARCHAR(20) NOT NULL,
    complemento VARCHAR(100),
    bairro VARCHAR(100) NOT NULL,
    codigo_municipio VARCHAR(7) NOT NULL,
    municipio VARCHAR(100) NOT NULL,
    uf CHAR(2) NOT NULL,
    cep VARCHAR(8) NOT NULL,
    codigo_pais VARCHAR(4),
    pais VARCHAR(100),
    telefone VARCHAR(20),
    data_criacao TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    data_atualizacao TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT ck_endereco_referencia CHECK (
        (pessoa_id IS NOT NULL AND nfe_id IS NULL) OR
        (pessoa_id IS NULL AND nfe_id IS NOT NULL)
    ),
    CONSTRAINT ck_endereco_tipo CHECK (tipo_endereco IN ('PRINCIPAL', 'RETIRADA', 'ENTREGA'))
);

CREATE INDEX IF NOT EXISTS idx_endereco_pessoa_id ON endereco(pessoa_id);
CREATE INDEX IF NOT EXISTS idx_endereco_nfe_id ON endereco(nfe_id);
CREATE INDEX IF NOT EXISTS idx_endereco_tipo ON endereco(tipo_endereco);

-- ========================================
-- 3. TABELA NFE_PESSOA (Relacionamento)
-- Liga NFe com pessoas (emitente, destinatário)
-- ========================================
CREATE TABLE IF NOT EXISTS nfe_pessoa (
    id SERIAL PRIMARY KEY,
    nfe_id INTEGER NOT NULL,
    pessoa_id INTEGER NOT NULL REFERENCES pessoa(id) ON DELETE CASCADE,
    tipo_relacao VARCHAR(20) NOT NULL, -- 'EMITENTE', 'DESTINATARIO'
    data_criacao TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uk_nfe_pessoa_tipo UNIQUE (nfe_id, tipo_relacao),
    CONSTRAINT ck_nfe_pessoa_tipo CHECK (tipo_relacao IN ('EMITENTE', 'DESTINATARIO'))
);

CREATE INDEX IF NOT EXISTS idx_nfe_pessoa_nfe_id ON nfe_pessoa(nfe_id);
CREATE INDEX IF NOT EXISTS idx_nfe_pessoa_pessoa_id ON nfe_pessoa(pessoa_id);

-- ========================================
-- 4. TABELA IMPOSTO (Generalização)
-- Substitui: imposto_icms, imposto_pis, imposto_cofins, imposto_ipi
-- ========================================
CREATE TABLE IF NOT EXISTS imposto (
    id SERIAL PRIMARY KEY,
    item_nfe_id INTEGER NOT NULL,
    tipo_imposto VARCHAR(10) NOT NULL, -- 'ICMS', 'PIS', 'COFINS', 'IPI'
    
    -- Campos comuns
    cst VARCHAR(3),
    csosn VARCHAR(3),
    valor_base_calculo NUMERIC(16,2),
    aliquota_percentual NUMERIC(5,2),
    valor NUMERIC(16,2),
    
    -- Campos específicos ICMS
    origem SMALLINT,
    modalidade_base_calculo SMALLINT,
    percentual_reducao_base_calculo NUMERIC(5,2),
    modalidade_base_calculo_st SMALLINT,
    percentual_margem_valor_adicionado_st NUMERIC(5,2),
    percentual_reducao_base_calculo_st NUMERIC(5,2),
    valor_base_calculo_st NUMERIC(16,2),
    aliquota_st NUMERIC(5,2),
    valor_st NUMERIC(16,2),
    percentual_credito_sn NUMERIC(5,2),
    valor_credito_sn NUMERIC(16,2),
    
    -- Campos específicos PIS/COFINS
    tipo_calculo CHAR(1), -- 'P' para percentual, 'V' para valor
    aliquota_valor NUMERIC(16,2),
    
    -- Campos específicos IPI
    classe_enquadramento VARCHAR(5),
    cnpj_produtor VARCHAR(14),
    codigo_selo VARCHAR(60),
    quantidade_selo INTEGER,
    codigo_enquadramento VARCHAR(3),
    quantidade_unidade NUMERIC(16,4),
    valor_unidade NUMERIC(16,4),
    
    data_criacao TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    data_atualizacao TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT ck_imposto_tipo CHECK (tipo_imposto IN ('ICMS', 'PIS', 'COFINS', 'IPI'))
);

CREATE INDEX IF NOT EXISTS idx_imposto_item_nfe_id ON imposto(item_nfe_id);
CREATE INDEX IF NOT EXISTS idx_imposto_tipo ON imposto(tipo_imposto);
CREATE INDEX IF NOT EXISTS idx_imposto_item_tipo ON imposto(item_nfe_id, tipo_imposto);

-- ========================================
-- 5. TABELA TRANSPORTE_ITEM (Generalização)
-- Substitui: volume_transporte, veiculo_transporte, lacre_volume
-- ========================================
CREATE TABLE IF NOT EXISTS transporte_item (
    id SERIAL PRIMARY KEY,
    transporte_id INTEGER NOT NULL,
    tipo_item VARCHAR(20) NOT NULL, -- 'VOLUME', 'VEICULO', 'LACRE'
    item_pai_id INTEGER REFERENCES transporte_item(id) ON DELETE CASCADE,
    
    -- Campos para VOLUME
    quantidade INTEGER,
    especie VARCHAR(60),
    marca VARCHAR(60),
    numeracao VARCHAR(60),
    peso_liquido NUMERIC(16,3),
    peso_bruto NUMERIC(16,3),
    
    -- Campos para VEICULO
    placa VARCHAR(7),
    uf CHAR(2),
    rntc VARCHAR(20),
    tipo_veiculo VARCHAR(20), -- 'PRINCIPAL', 'REBOQUE'
    
    -- Campos para LACRE
    numero_lacre VARCHAR(60),
    
    data_criacao TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    data_atualizacao TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT ck_transporte_item_tipo CHECK (tipo_item IN ('VOLUME', 'VEICULO', 'LACRE'))
);

CREATE INDEX IF NOT EXISTS idx_transporte_item_transporte_id ON transporte_item(transporte_id);
CREATE INDEX IF NOT EXISTS idx_transporte_item_tipo ON transporte_item(tipo_item);
CREATE INDEX IF NOT EXISTS idx_transporte_item_pai_id ON transporte_item(item_pai_id);

-- ========================================
-- TABELAS MANTIDAS (com ajustes mínimos)
-- ========================================

-- Tabela principal da NFe (mantida)
CREATE TABLE IF NOT EXISTS nfe (
    id SERIAL PRIMARY KEY,
    chave_acesso VARCHAR(44) UNIQUE NOT NULL,
    versao VARCHAR(10) NOT NULL,
    codigo_uf INTEGER NOT NULL,
    codigo_nf VARCHAR(20) NOT NULL,
    natureza_operacao VARCHAR(100) NOT NULL,
    indicador_pagamento SMALLINT NOT NULL,
    modelo INTEGER NOT NULL,
    serie INTEGER NOT NULL,
    numero INTEGER NOT NULL,
    data_emissao DATE NOT NULL,
    data_saida_entrada DATE,
    tipo_nf SMALLINT NOT NULL,
    codigo_municipio_fato_gerador VARCHAR(7) NOT NULL,
    tipo_impressao SMALLINT NOT NULL,
    tipo_emissao SMALLINT NOT NULL,
    digito_verificador INTEGER NOT NULL,
    ambiente SMALLINT NOT NULL,
    finalidade_nf SMALLINT NOT NULL,
    processo_emissao SMALLINT NOT NULL,
    versao_processo VARCHAR(20) NOT NULL,
    data_criacao TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    data_atualizacao TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    status_processamento VARCHAR(20) DEFAULT 'PENDENTE'
);

CREATE INDEX IF NOT EXISTS idx_nfe_chave_acesso ON nfe(chave_acesso);
CREATE INDEX IF NOT EXISTS idx_nfe_data_emissao ON nfe(data_emissao);

-- Tabela de itens da NFe (mantida)
CREATE TABLE IF NOT EXISTS item_nfe (
    id SERIAL PRIMARY KEY,
    nfe_id INTEGER NOT NULL REFERENCES nfe(id) ON DELETE CASCADE,
    numero_item INTEGER NOT NULL,
    codigo_produto VARCHAR(60) NOT NULL,
    gtin VARCHAR(14),
    descricao VARCHAR(120) NOT NULL,
    ncm VARCHAR(8),
    cfop VARCHAR(4) NOT NULL,
    unidade_comercial VARCHAR(6) NOT NULL,
    quantidade_comercial NUMERIC(16,4) NOT NULL,
    valor_unitario_comercial NUMERIC(16,4) NOT NULL,
    valor_total_bruto NUMERIC(16,2) NOT NULL,
    gtin_tributavel VARCHAR(14),
    unidade_tributavel VARCHAR(6) NOT NULL,
    quantidade_tributavel NUMERIC(16,4) NOT NULL,
    valor_unitario_tributavel NUMERIC(16,4) NOT NULL,
    origem_mercadoria SMALLINT,
    data_criacao TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    data_atualizacao TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uk_item_nfe UNIQUE (nfe_id, numero_item)
);

CREATE INDEX IF NOT EXISTS idx_item_nfe_nfe_id ON item_nfe(nfe_id);
CREATE INDEX IF NOT EXISTS idx_item_nfe_codigo_produto ON item_nfe(codigo_produto);

-- Tabela de totais da NFe (mantida)
CREATE TABLE IF NOT EXISTS totais_nfe (
    id SERIAL PRIMARY KEY,
    nfe_id INTEGER NOT NULL REFERENCES nfe(id) ON DELETE CASCADE,
    base_calculo_icms NUMERIC(16,2) NOT NULL,
    valor_icms NUMERIC(16,2) NOT NULL,
    base_calculo_icms_st NUMERIC(16,2) NOT NULL,
    valor_icms_st NUMERIC(16,2) NOT NULL,
    valor_produtos NUMERIC(16,2) NOT NULL,
    valor_frete NUMERIC(16,2) NOT NULL,
    valor_seguro NUMERIC(16,2) NOT NULL,
    valor_desconto NUMERIC(16,2) NOT NULL,
    valor_ii NUMERIC(16,2) NOT NULL,
    valor_ipi NUMERIC(16,2) NOT NULL,
    valor_pis NUMERIC(16,2) NOT NULL,
    valor_cofins NUMERIC(16,2) NOT NULL,
    valor_outros NUMERIC(16,2) NOT NULL,
    valor_total_nfe NUMERIC(16,2) NOT NULL,
    data_criacao TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    data_atualizacao TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_totais_nfe_nfe_id ON totais_nfe(nfe_id);

-- Tabela de transporte (ajustada)
CREATE TABLE IF NOT EXISTS transporte (
    id SERIAL PRIMARY KEY,
    nfe_id INTEGER NOT NULL REFERENCES nfe(id) ON DELETE CASCADE,
    modalidade_frete SMALLINT NOT NULL,
    transportadora_id INTEGER REFERENCES pessoa(id), -- FK para pessoa
    data_criacao TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    data_atualizacao TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_transporte_nfe_id ON transporte(nfe_id);
CREATE INDEX IF NOT EXISTS idx_transporte_transportadora_id ON transporte(transportadora_id);

-- Tabela de informações adicionais (mantida)
CREATE TABLE IF NOT EXISTS informacoes_adicionais (
    id SERIAL PRIMARY KEY,
    nfe_id INTEGER NOT NULL REFERENCES nfe(id) ON DELETE CASCADE,
    info_contribuinte TEXT,
    info_fisco TEXT,
    data_criacao TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    data_atualizacao TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_informacoes_adicionais_nfe_id ON informacoes_adicionais(nfe_id);

-- Tabela de processamento da NFe (mantida)
CREATE TABLE IF NOT EXISTS processamento_nfe (
    id SERIAL PRIMARY KEY,
    nfe_id INTEGER NOT NULL REFERENCES nfe(id) ON DELETE CASCADE,
    data_processamento TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(20) NOT NULL,
    mensagem TEXT,
    xml_original TEXT,
    xml_processado TEXT,
    data_criacao TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    data_atualizacao TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_processamento_nfe_nfe_id ON processamento_nfe(nfe_id);
CREATE INDEX IF NOT EXISTS idx_processamento_nfe_status ON processamento_nfe(status);
CREATE INDEX IF NOT EXISTS idx_processamento_nfe_data_processamento ON processamento_nfe(data_processamento);

-- ========================================
-- ADIÇÃO DAS FOREIGN KEYS APÓS CRIAÇÃO
-- ========================================
DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_nfe_pessoa_nfe' AND conrelid = 'nfe_pessoa'::regclass) THEN ALTER TABLE nfe_pessoa ADD CONSTRAINT fk_nfe_pessoa_nfe FOREIGN KEY (nfe_id) REFERENCES nfe(id) ON DELETE CASCADE; END IF; END $$;
DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_endereco_nfe' AND conrelid = 'endereco'::regclass) THEN ALTER TABLE endereco ADD CONSTRAINT fk_endereco_nfe FOREIGN KEY (nfe_id) REFERENCES nfe(id) ON DELETE CASCADE; END IF; END $$;
DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_imposto_item_nfe' AND conrelid = 'imposto'::regclass) THEN ALTER TABLE imposto ADD CONSTRAINT fk_imposto_item_nfe FOREIGN KEY (item_nfe_id) REFERENCES item_nfe(id) ON DELETE CASCADE; END IF; END $$;
DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_transporte_item_transporte' AND conrelid = 'transporte_item'::regclass) THEN ALTER TABLE transporte_item ADD CONSTRAINT fk_transporte_item_transporte FOREIGN KEY (transporte_id) REFERENCES transporte(id) ON DELETE CASCADE; END IF; END $$;

-- ========================================
-- COMENTÁRIOS NAS TABELAS OTIMIZADAS
-- ========================================
COMMENT ON TABLE pessoa IS 'Tabela generalizada para armazenar emitentes, destinatários e transportadoras';
COMMENT ON TABLE endereco IS 'Tabela generalizada para todos os tipos de endereços (principal, retirada, entrega)';
COMMENT ON TABLE nfe_pessoa IS 'Tabela de relacionamento entre NFe e pessoas (emitente/destinatário)';
COMMENT ON TABLE imposto IS 'Tabela generalizada para todos os tipos de impostos (ICMS, PIS, COFINS, IPI)';
COMMENT ON TABLE transporte_item IS 'Tabela generalizada para volumes, veículos e lacres de transporte';

-- ========================================
-- VIEWS PARA FACILITAR CONSULTAS
-- ========================================

-- View para emitentes
CREATE OR REPLACE VIEW v_emitentes AS
SELECT p.*, e.logradouro, e.numero, e.bairro, e.municipio, e.uf, e.cep
FROM pessoa p
LEFT JOIN endereco e ON p.id = e.pessoa_id AND e.tipo_endereco = 'PRINCIPAL'
WHERE p.tipo_pessoa = 'EMITENTE';

-- View para destinatários
CREATE OR REPLACE VIEW v_destinatarios AS
SELECT p.*, e.logradouro, e.numero, e.bairro, e.municipio, e.uf, e.cep
FROM pessoa p
LEFT JOIN endereco e ON p.id = e.pessoa_id AND e.tipo_endereco = 'PRINCIPAL'
WHERE p.tipo_pessoa = 'DESTINATARIO';

-- View para impostos por tipo
CREATE OR REPLACE VIEW v_impostos_icms AS
SELECT * FROM imposto WHERE tipo_imposto = 'ICMS';

CREATE OR REPLACE VIEW v_impostos_pis AS
SELECT * FROM imposto WHERE tipo_imposto = 'PIS';

CREATE OR REPLACE VIEW v_impostos_cofins AS
SELECT * FROM imposto WHERE tipo_imposto = 'COFINS';

CREATE OR REPLACE VIEW v_impostos_ipi AS
SELECT * FROM imposto WHERE tipo_imposto = 'IPI';