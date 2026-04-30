# Manutenção do Sistema ECC

## Princípios

1. Alterar o mínimo possível
2. Preservar estrutura existente
3. Testar sempre após mudanças

---

## Fluxo de alteração

### Backend

1. Alterar Service
2. Ajustar Route (se necessário)
3. Testar

---

### Banco

1. Criar coluna/tabela
2. Popular dados
3. Ajustar código
4. Remover legado

---

## Inserção de novas funcionalidades

- Criar Service primeiro
- Depois integrar na Route
- Depois ajustar Template

---

## Cuidados importantes

- Sempre usar `paroquia_id`
- Evitar duplicação de lógica
- Preferir `casal_id`

---

## Refatoração

Permitido apenas quando:
- código estiver duplicado
- difícil manutenção
- risco de erro alto

---

## Problemas comuns

### Erro de conexão
- Verificar DB_HOST, DB_USER, DB_PASSWORD

### Dados inconsistentes
- Verificar relacionamento com casal_id

### Página não carrega
- Verificar erro de indentação
- Verificar template

---

## Deploy

- Usar Railway / Render
- Banco pode ser MySQL ou Postgres (futuro)

---

## Checklist antes de finalizar alteração

- [ ] Backend funcionando
- [ ] Frontend funcionando
- [ ] Banco consistente
- [ ] Teste manual realizado
