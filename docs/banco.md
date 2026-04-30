# Banco de Dados – Sistema ECC

## Visão Geral

O sistema utiliza MySQL como banco de dados principal, com estrutura preparada para múltiplas paróquias (multi-tenant simples via `paroquia_id`).

---

## Tabelas Principais

### paroquias
Armazena as paróquias do sistema.

- id (PK)
- nome
- diocese

---

### usuarios
Controle de acesso ao sistema.

- id (PK)
- paroquia_id (FK → paroquias.id)
- nome
- login
- senha_hash
- perfil (admin, operador, etc.)
- ativo (0/1)

---

### encontristas
Base principal de casais.

- id (PK)
- paroquia_id (FK)
- nome_completo_ele
- nome_completo_ela
- nome_usual_ele
- nome_usual_ela
- data_casamento (DATE)
- ano
- num_ecc
- telefone_ele
- telefone_ela
- endereco
- observacao
- observacao_extra

### Datas de etapas
- data_1_etapa
- data_2_etapa
- data_3_etapa

---

### encontreiros
Registros de trabalho nos encontros.

- id (PK)
- paroquia_id (FK)
- ano
- equipe
- casal_id (FK → encontristas.id)
- nome_ele (LEGADO)
- nome_ela (LEGADO)
- coordenador ('Sim'/'Não')
- telefone
- endereco
- observacao
- status

⚠️ Campos nome_ele/nome_ela são legado e devem ser removidos futuramente.

---

### circulos
Círculos derivados da equipe de círculos.

- id (PK)
- paroquia_id (FK)
- ano
- nome_circulo
- cor_circulo
- integrantes_original
- integrantes_atual
- coord_orig_casal_id (FK)
- coord_atual_casal_id (FK)

---

### palestras
Controle de palestras.

- id (PK)
- paroquia_id (FK)
- ano
- id_casal (FK → encontristas.id)
- palestrante (quando individual)
- tema

---

### geocoding_cache (opcional)
Cache de geolocalização.

---

### encontristas_geo (opcional)
Armazena coordenadas geográficas.

---

## Índices recomendados

```sql
CREATE INDEX idx_encontristas_nome ON encontristas(nome_completo_ele, nome_completo_ela);
CREATE INDEX idx_encontristas_usual ON encontristas(nome_usual_ele, nome_usual_ela);

CREATE INDEX idx_encontreiros_casal ON encontreiros(casal_id);
CREATE INDEX idx_encontreiros_ano ON encontreiros(ano);

CREATE INDEX idx_circulos_ano ON circulos(ano);
