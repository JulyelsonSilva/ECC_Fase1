# Plano de Migração - Controle de Acesso, Multi-Paróquia e PostgreSQL

## Estado inicial

Sistema atual funcionando com:
- Flask
- MySQL
- Uma única paróquia
- Sem login real por usuário
- Controle administrativo simples por token

## Objetivo

Evoluir o sistema para:
- Controle de acesso por usuário
- Perfis de autorização
- Suporte a múltiplas paróquias
- Migração futura para PostgreSQL

## Ordem definida

1. Backup do banco MySQL atual
2. Criação de branch de desenvolvimento
3. Criação da tabela `paroquias`
4. Inclusão de `paroquia_id` nas tabelas principais
5. Ajuste das consultas para filtrar por paróquia
6. Criação de autenticação real
7. Criação de perfis de acesso
8. Testes completos ainda em MySQL
9. Migração para PostgreSQL