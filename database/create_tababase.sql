/*
drop table categorias_gastos CASCADE;
drop table gastos CASCADE;
drop table fornecedores CASCADE;
drop table deputados CASCADE;
drop table proposicao CASCADE;
drop table deputado_proposicao CASCADE;

select id_deputado_api, count(*) from deputados group by id_deputado_api

select * from deputados where id_deputado_api = 133439
*/

CREATE TABLE fornecedores(
    id SERIAL PRIMARY KEY,
    nome VARCHAR(255) NOT NULL,
    cnpjCPF VARCHAR(20) NOT NULL,
    UNIQUE(cnpjCPF)
);

-- Tabela categoria_gasto
CREATE TABLE categorias_gastos (
    id SERIAL PRIMARY KEY,
    descricao VARCHAR(255) NOT NULL,
    UNIQUE(descricao)
);

CREATE TABLE deputados (
    id SERIAL PRIMARY KEY,
    id_deputado_api INTEGER,
    nome VARCHAR(255) NOT NULL,
	id_deputado_arq INTEGER,
    UNIQUE(id_deputado_api)
);

-- Tabela gasto
CREATE TABLE gastos (
    id SERIAL PRIMARY KEY,
    id_documento INTEGER NOT NULL,
    id_deputado INTEGER REFERENCES deputados(id) NOT NULL,
    categoria_gasto_id INTEGER REFERENCES categorias_gastos(id),
    fornecedor_id INTEGER REFERENCES fornecedores(id),
    numero VARCHAR(255),
    data_emissao TIMESTAMP,
    valor_documento NUMERIC(10, 2),
    valor_glosa NUMERIC(10, 2),
    valor_liquido NUMERIC(10, 2)
);

CREATE TABLE IF NOT EXISTS proposicoes
(
    id SERIAL PRIMARY KEY,
    id_proposicao INTEGER NOT NULL,
    ementa VARCHAR(1000) NOT NULL,
	UNIQUE(id_proposicao)
);

CREATE TABLE IF NOT EXISTS deputados_proposicoes
(
    id SERIAL PRIMARY KEY,
    id_proposicao INTEGER NOT NULL,
    id_deputado INTEGER NOT NULL,
    proponente BOOLEAN NOT NULL,
    FOREIGN KEY (id_proposicao) REFERENCES proposicoes(id_proposicao),
    FOREIGN KEY (id_deputado) REFERENCES deputados(id_deputado_api)
);

CREATE VIEW deputado_gastos_agregados AS
SELECT
    d.id_deputado_api AS deputado_id_api,
    SUM(g.valor_documento) AS total_documentos,
    SUM(g.valor_glosa) AS total_glosas,
    SUM(g.valor_liquido) AS total_liquido
FROM
    public.deputados d
    JOIN public.gastos g ON d.id = g.id_deputado
GROUP BY
    d.id_deputado_api;



CREATE TABLE IF NOT EXISTS data_files
(
    id SERIAL PRIMARY KEY,
    file_name VARCHAR(255) NOT NULL,
    content_length numeric(10,2),
    etag characterVARCHAR(255) NOT NULL,
    processing_datetime TIMESTAMP NOT NULL,
    UNIQUE (file_name)
);