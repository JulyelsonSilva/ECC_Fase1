from functools import wraps
import hashlib
import hmac
import time

from flask import (
    flash,
    jsonify,
    redirect,
    render_template,
    request,
    session,
    url_for,
)

from db import db_conn

PERFIS = (
    "super",
    "admin",
    "admin_paroquia",
    "montagem",
    "palestras",
    "fichas",
    "pos_encontro",
    "financas",
    "usuario_comum",
)

PERFIS_SELECIONAM_PAROQUIA = {"super", "admin"}
PERFIS_GERENCIAM_PAROQUIAS = {"super", "admin"}
PERFIS_GERENCIAM_USUARIOS = {"super", "admin", "admin_paroquia", "montagem"}
PERFIS_SEM_DADOS_PASTORAIS = {"admin"}

# Perfis que podem gravar em cada área.
PERMISSOES_ESCRITA = {
    "encontristas": {"super", "montagem", "fichas"},
    "encontreiros": {"super", "montagem"},
    "montagem": {"super", "montagem"},
    "palestras": {"super", "montagem", "palestras"},
    "circulos": {"super", "montagem", "pos_encontro"},
}

# Endpoints que usam POST apenas para consulta/validação, sem alterar banco.
POSTS_DE_CONSULTA = {
    "api_buscar_casal",
    "api_buscar_cg",
    "api_check_casal_equipe",
    "api_validar_montagem_ano",
    "api_palestras_validate",
    "api_palestras_buscar",
    "api_circulos_buscar_encontrista",
}

ROTAS_PUBLICAS = {
    "static",
    "login",
    "logout",
}

ROTAS_ADMINISTRATIVAS = {
    "admin_home",
    "admin_paroquias",
    "admin_paroquias_criar",
    "admin_paroquias_editar",
    "admin_paroquias_atualizar",
    "admin_paroquias_alternar",
    "admin_usuarios",
    "admin_usuarios_criar",
    "admin_usuarios_editar",
    "admin_usuarios_atualizar",
    "admin_usuarios_alternar",
    "init_db_route",
}

ROTAS_PAROQUIA = {
    "selecionar_paroquia",
    "definir_paroquia",
    "trocar_paroquia",
}


def _hash_senha(senha: str) -> str:
    """Hash simples e compatível sem depender de bibliotecas externas."""
    senha = senha or ""
    salt_base = f"{time.time_ns()}:{id(senha)}"
    salt = hashlib.sha256(salt_base.encode("utf-8")).hexdigest()[:32]
    digest = hashlib.sha256((salt + senha).encode("utf-8")).hexdigest()
    return f"sha256${salt}${digest}"


def _verificar_senha(senha: str, senha_hash: str) -> bool:
    senha = senha or ""
    senha_hash = senha_hash or ""

    # Formato usado por este sistema: sha256$salt$digest
    if senha_hash.startswith("sha256$"):
        try:
            _, salt, digest = senha_hash.split("$", 2)
        except ValueError:
            return False
        calc = hashlib.sha256((salt + senha).encode("utf-8")).hexdigest()
        return hmac.compare_digest(calc, digest)

    # Compatibilidade futura/caso algum hash Werkzeug seja usado manualmente.
    try:
        from werkzeug.security import check_password_hash
        return check_password_hash(senha_hash, senha)
    except Exception:
        return False


def gerar_hash_senha(senha: str) -> str:
    return _hash_senha(senha)


def usuario_logado():
    return session.get("usuario")


def usuario_id_atual():
    usuario = usuario_logado() or {}
    return usuario.get("id")


def perfil_atual():
    usuario = usuario_logado() or {}
    return usuario.get("perfil")


def usuario_nome_atual():
    usuario = usuario_logado() or {}
    return usuario.get("nome") or usuario.get("login")


def usuario_paroquia_id():
    usuario = usuario_logado() or {}
    return usuario.get("paroquia_id")


def esta_logado():
    return bool(usuario_logado())


def perfil_em(*perfis):
    return perfil_atual() in set(perfis)


def pode_selecionar_paroquia():
    return perfil_atual() in PERFIS_SELECIONAM_PAROQUIA


def pode_gerenciar_paroquias():
    return perfil_atual() in PERFIS_GERENCIAM_PAROQUIAS


def pode_gerenciar_usuarios():
    return perfil_atual() in PERFIS_GERENCIAM_USUARIOS


def pode_ver_dados_pastorais():
    return perfil_atual() not in PERFIS_SEM_DADOS_PASTORAIS


def pode_escrever(area: str) -> bool:
    perfil = perfil_atual()
    if perfil == "super":
        return True
    return perfil in PERMISSOES_ESCRITA.get(area, set())


def login_required(view_func):
    @wraps(view_func)
    def wrapper(*args, **kwargs):
        if not esta_logado():
            return redirect(url_for("login", next=request.path))
        return view_func(*args, **kwargs)
    return wrapper


def carregar_usuario_por_login(login: str):
    conn = db_conn()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute("""
            SELECT
                u.id,
                u.paroquia_id,
                u.nome,
                u.login,
                u.senha_hash,
                u.perfil,
                u.ativo,
                p.nome AS paroquia_nome
            FROM usuarios u
            LEFT JOIN paroquias p ON p.id = u.paroquia_id
            WHERE u.login = %s
            LIMIT 1
        """, (login,))
        return cur.fetchone()
    finally:
        cur.close()
        conn.close()


def autenticar_usuario(login: str, senha: str):
    usuario = carregar_usuario_por_login(login)
    if not usuario:
        return None
    if not usuario.get("ativo"):
        return None
    if not _verificar_senha(senha, usuario.get("senha_hash")):
        return None
    return usuario


def iniciar_sessao_usuario(usuario):
    session.clear()
    session["usuario"] = {
        "id": usuario["id"],
        "nome": usuario.get("nome"),
        "login": usuario.get("login"),
        "perfil": usuario.get("perfil"),
        "paroquia_id": usuario.get("paroquia_id"),
        "paroquia_nome": usuario.get("paroquia_nome"),
    }

    if usuario.get("perfil") not in PERFIS_SELECIONAM_PAROQUIA:
        session["paroquia_id"] = usuario.get("paroquia_id")
        session["paroquia_nome"] = usuario.get("paroquia_nome")


def encerrar_sessao_usuario():
    session.clear()


def _resposta_acesso_negado(mensagem="Acesso negado."):
    if request.path.startswith("/api/") or request.is_json:
        return jsonify({"ok": False, "msg": mensagem}), 403
    flash(mensagem, "error")
    return redirect(url_for("index"))


def _area_do_endpoint(endpoint: str):
    if endpoint in {"fichas", "editar_encontrista", "normalizar_geocodificar"}:
        return "encontristas"
    if endpoint.startswith("api_encontristas"):
        return "encontristas"

    if endpoint in {
        "api_adicionar_dirigente",
        "api_adicionar_cg",
        "api_add_membro_equipe",
        "api_marcar_status_dirigente",
        "api_marcar_status_membro",
        "api_concluir_montagem_ano",
    }:
        return "montagem"

    if endpoint.startswith("api_palestras"):
        return "palestras"

    if endpoint.startswith("api_circulos"):
        return "circulos"

    return None


def _endpoint_eh_dado_pastoral(endpoint: str) -> bool:
    if not endpoint:
        return False

    if endpoint in ROTAS_ADMINISTRATIVAS or endpoint in ROTAS_PAROQUIA or endpoint in ROTAS_PUBLICAS:
        return False

    return True


def registrar_auth_routes(app):
    @app.route("/login", methods=["GET", "POST"])
    def login():
        if esta_logado() and request.method == "GET":
            if perfil_atual() in {"admin"}:
                return redirect(url_for("admin_home"))
            if pode_selecionar_paroquia() and not session.get("paroquia_id"):
                return redirect(url_for("selecionar_paroquia"))
            return redirect(url_for("index"))

        erro = None
        login_form = ""

        if request.method == "POST":
            login_form = (request.form.get("login") or "").strip()
            senha = request.form.get("senha") or ""

            usuario = autenticar_usuario(login_form, senha)
            if not usuario:
                erro = "Usuário ou senha inválidos."
            else:
                iniciar_sessao_usuario(usuario)

                if usuario.get("perfil") == "admin":
                    return redirect(url_for("admin_home"))

                if usuario.get("perfil") in PERFIS_SELECIONAM_PAROQUIA:
                    return redirect(url_for("selecionar_paroquia"))

                return redirect(url_for("index"))

        return render_template("login.html", erro=erro, login=login_form)

    @app.route("/logout")
    def logout():
        encerrar_sessao_usuario()
        return redirect(url_for("login"))


def registrar_controle_acesso(app):
    @app.context_processor
    def inject_usuario_atual():
        return {
            "usuario_atual": usuario_logado(),
            "usuario_nome": usuario_nome_atual(),
            "usuario_perfil": perfil_atual(),
            "pode_trocar_paroquia": pode_selecionar_paroquia(),
            "pode_admin_paroquias": pode_gerenciar_paroquias(),
            "pode_admin_usuarios": pode_gerenciar_usuarios(),
            "pode_ver_dados": pode_ver_dados_pastorais(),
        }

    @app.before_request
    def verificar_acesso_global():
        endpoint = request.endpoint or ""

        if endpoint in ROTAS_PUBLICAS or endpoint.startswith("static"):
            return None

        if not esta_logado():
            return redirect(url_for("login", next=request.path))

        perfil = perfil_atual()

        # Admin sem acesso pastoral: só administração e seleção de paróquia.
        if perfil == "admin" and _endpoint_eh_dado_pastoral(endpoint):
            return redirect(url_for("admin_home"))

        # Seleção/troca de paróquia somente para super/admin.
        if endpoint in ROTAS_PAROQUIA and not pode_selecionar_paroquia():
            return redirect(url_for("index"))

        if endpoint == "admin_home" and not pode_gerenciar_paroquias() and not pode_gerenciar_usuarios():
            return redirect(url_for("index"))

        # Administração de paróquias somente super/admin.
        if endpoint in {
            "admin_paroquias",
            "admin_paroquias_criar",
            "admin_paroquias_editar",
            "admin_paroquias_atualizar",
            "admin_paroquias_alternar",
            "init_db_route",
        } and not pode_gerenciar_paroquias():
            return _resposta_acesso_negado("Seu perfil não permite gerenciar paróquias.")

        # Administração de usuários conforme perfil.
        if endpoint in {
            "admin_usuarios",
            "admin_usuarios_criar",
            "admin_usuarios_editar",
            "admin_usuarios_atualizar",
            "admin_usuarios_alternar",
        } and not pode_gerenciar_usuarios():
            return _resposta_acesso_negado("Seu perfil não permite gerenciar usuários.")

        # Bloqueio de escrita por área.
        if request.method not in {"GET", "HEAD", "OPTIONS"} and endpoint not in POSTS_DE_CONSULTA:
            area = _area_do_endpoint(endpoint)
            if area and not pode_escrever(area):
                return _resposta_acesso_negado("Seu perfil permite consultar, mas não alterar estes dados.")

        return None
