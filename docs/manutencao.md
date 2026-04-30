# Manutenção do Projeto

## Regras importantes

- Sempre alterar o mínimo possível
- Preservar lógica existente
- Preferir alterar services antes de routes
- Sempre testar após cada mudança

## Padrão de desenvolvimento

1. Ajustar service
2. Ajustar route
3. Testar
4. Só então seguir

## Observações

- helpers de paróquia estão em utils.py
- não duplicar lógica de sessão
- não usar mysql.connector direto fora de db.py
