# Estrutura do Projeto

## Fluxo padrão

Request → Route → Service → Banco → Service → Route → Template

## Routes

Responsáveis por:
- receber request
- validar sessão (paroquia)
- chamar services
- renderizar templates ou retornar JSON

## Services

Responsáveis por:
- consultas SQL
- regras de negócio
- paginação

## Banco

- MySQL
- uso de pool em db.py
- acesso via db_conn()

## Templates

- base.html central
- templates comuns herdam base
- templates de impressão são independentes
