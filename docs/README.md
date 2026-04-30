# Sistema do ECC

Sistema web para gerenciamento de Encontro de Casais com Cristo (ECC).

## Tecnologias

- Python 3
- Flask
- MySQL
- Jinja2

## Estrutura

- routes/ → rotas da aplicação
- services/ → regras de negócio e acesso ao banco
- templates/ → páginas HTML
- static/ → CSS e JS
- db.py → conexão com banco (pool)

## Execução

(Coloque aqui como você executa no Render/Railway)

## Status atual

- Multi-paróquia funcional
- Pool de conexões implementado
- Services centralizados (encontristas/encontreiros)
- Pronto para iniciar controle de acesso

## Deploy

O sistema está hospedado em ambiente cloud (Render/Railway).

Configurações importantes:

- Variáveis de ambiente:
  - DB_HOST
  - DB_USER
  - DB_PASSWORD
  - DB_NAME
  - SECRET_KEY

- Uso de pool de conexões em `db.py`

Observação:
Evitar conexões diretas com mysql.connector fora de db.py.
