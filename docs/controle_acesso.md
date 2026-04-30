# Controle de acesso

## Objetivo

O sistema exige login por usuário e senha. O acesso é definido pelo perfil do usuário e, quando aplicável, pela paróquia vinculada ao cadastro.

## Perfis

| Perfil | Permissão |
|---|---|
| `super` | Acessa todo o sistema, sem restrições. Pode selecionar/trocar paróquia, criar paróquias e gerenciar usuários. |
| `admin` | Pode criar/alterar paróquias e usuários, mas não acessa dados pastorais como encontristas, encontreiros, círculos e palestras. |
| `admin_paroquia` | Acessa a própria paróquia, consulta dados pastorais e gerencia usuários da paróquia. Não insere/altera dados pastorais. |
| `montagem` | Acessa a própria paróquia, gerencia usuários da paróquia e pode inserir/alterar/consultar dados pastorais. |
| `palestras` | Pode inserir/alterar/consultar palestras da própria paróquia. Nas demais áreas, consulta. |
| `fichas` | Pode inserir/alterar/consultar encontristas da própria paróquia. Nas demais áreas, consulta. |
| `pos_encontro` | Pode inserir/alterar/consultar círculos da própria paróquia. Nas demais áreas, consulta. |
| `financas` | Consulta dados da própria paróquia. |
| `usuario_comum` | Consulta dados da própria paróquia. |

## Seleção de paróquia

Somente `super` e `admin` acessam a tela de seleção de paróquia e podem trocar a paróquia ativa na sessão.

Os demais perfis entram diretamente na paróquia vinculada ao próprio usuário. Para esses perfis, a troca de paróquia não é exibida nem permitida.

## Banco de dados

A tabela `usuarios` faz parte do schema oficial do sistema e é garantida por `services/schema_service.py`, na função `ensure_database_schema()`.

Estrutura esperada:

```sql
CREATE TABLE usuarios (
  id INT NOT NULL AUTO_INCREMENT,
  paroquia_id INT NULL,
  nome VARCHAR(150) NOT NULL,
  login VARCHAR(80) NOT NULL,
  senha_hash VARCHAR(255) NOT NULL,
  perfil ENUM('super','admin','admin_paroquia','montagem','palestras','fichas','pos_encontro','financas','usuario_comum') NOT NULL DEFAULT 'usuario_comum',
  ativo TINYINT(1) NOT NULL DEFAULT 1,
  created_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  UNIQUE KEY uq_usuarios_login (login),
  KEY idx_usuarios_paroquia (paroquia_id),
  KEY idx_usuarios_perfil (perfil),
  KEY idx_usuarios_ativo (ativo),
  CONSTRAINT fk_usuarios_paroquia FOREIGN KEY (paroquia_id) REFERENCES paroquias(id) ON DELETE SET NULL
);
```

Não há necessidade de executar arquivo SQL separado para a tabela `usuarios` quando `/__init_db__` estiver funcionando. O banco atual já possui `senha_hash` e `ativo`.

## Arquivos principais

- `auth.py`: autenticação, sessão, perfis, hash de senha e bloqueio global de permissões.
- `routes/admin.py`: administração de paróquias e usuários.
- `services/schema_service.py`: garantia da estrutura do banco, incluindo `usuarios`.
- `templates/login.html`: tela inicial de login usando o `base.html`.
- `templates/minha_conta.html`: alteração de nome e senha do próprio usuário.
- `templates/admin_usuarios.html`: CRUD administrativo de usuários.
- `templates/base.html`: navegação condicional, usuário logado, administração, troca de paróquia e logout.

## Minha conta

A rota `/minha-conta` permite ao usuário alterar o próprio nome e senha.

Regras:

- exige senha atual para salvar qualquer alteração;
- não permite alterar login;
- não permite alterar perfil;
- não permite alterar paróquia;
- após alterar o nome, a sessão é atualizada para refletir o novo nome no topo da página.

## Segurança básica

As senhas são armazenadas em `senha_hash`, não em texto puro.

O sistema usa o formato interno:

```text
sha256$salt$digest
```

A comparação é feita por hash, usando `hmac.compare_digest`.

A tabela também possui o campo `ativo`, permitindo bloquear o acesso de um usuário sem apagar seu histórico.

## Manutenção

As permissões foram centralizadas em `auth.py`, na função `registrar_controle_acesso(app)`. Isso evita espalhar verificações manuais por todas as rotas e preserva a estrutura atual do projeto:

- `routes` para fluxo;
- `services` para regras e SQL;
- `utils` para helpers;
- `db.py` para conexão.

O backend continua sendo a proteção principal. O frontend apenas evita exibir menus, cards e botões indevidos.
