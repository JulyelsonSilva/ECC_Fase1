# Controle de acesso

## Objetivo

O sistema passa a exigir login por usuário e senha. O acesso é definido pelo perfil do usuário e, quando aplicável, pela paróquia vinculada ao cadastro.

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

## Arquivos principais

- `auth.py`: autenticação, sessão, perfis e bloqueio global de permissões.
- `routes/admin.py`: administração de paróquias e usuários.
- `templates/login.html`: tela inicial de login usando o `base.html`.
- `templates/admin_usuarios.html`: CRUD administrativo de usuários.
- `templates/base.html`: exibição condicional de navegação, administração, troca de paróquia e logout.
- `sql/controle_acesso.sql`: criação da tabela `usuarios` e usuário inicial.

## Primeiro acesso

Execute `sql/controle_acesso.sql` no Adminer.

Usuário inicial:

- Login: `super`
- Senha: `admin123`

Depois do primeiro acesso, recomenda-se alterar a senha do usuário `super` ou criar outro usuário `super` e desativar o inicial.

## Observação de manutenção

As permissões foram centralizadas em `auth.py`, na função `registrar_controle_acesso(app)`. Isso evita espalhar verificações manuais por todas as rotas e preserva a estrutura atual do projeto: `routes` para fluxo, `services` para regras/SQL, `utils` para helpers e `db.py` para conexão.
