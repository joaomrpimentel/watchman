#!/usr/bin/env bash

# Adiciona a conexão ao PostgreSQL automaticamente
airflow connections add 'postgres_default' \
    --conn-type 'postgres' \
    --conn-login "${POSTGRES_USER}" \
    --conn-password "${POSTGRES_PASSWORD}" \
    --conn-host "${POSTGRES_HOST}" \
    --conn-port "${POSTGRES_PORT}" \
    --conn-schema "${POSTGRES_DB}"

# Executa o comando original do entrypoint
exec /entrypoint "$@"