from flask import flash, jsonify, render_template, request, redirect, url_for, session

from db import db_conn
from services.schema_service import ensure_database_schema
from auth import (
    PERFIS,
    gerar_hash_senha,
    perfil_atual,
    usuario_paroquia_id,
    pode_gerenciar_paroquias,
    pode_gerenciar_usuarios,
)


def register_admin_routes(app):

    def _paroquias_disponiveis_para_usuario():
        conn = db_conn()
        cur = conn.cursor(dictionary=True)
        try:
            if perfil_atual() in {"super", "admin"}:
                cur.execute("""
                    SELECT id, nome, cidade, estado, diocese
                    FROM paroquias
                    WHERE ativa = 1
                    ORDER BY nome
                """)
                return cur.fetchall() or []

            cur.execute("""
                SELECT id, nome, cidade, estado, diocese
                FROM paroquias
                WHERE id = %s
                LIMIT 1
            """, (usuario_paroquia_id(),))
            row = cur.fetchone()
            return [row] if row else []
        finally:
            cur.close()
            conn.close()

    def _perfis_disponiveis_para_usuario():
        perfil = perfil_atual()
        if perfil == "super":
            return list(PERFIS)
        if perfil == "admin":
            return [p for p in PERFIS if p != "super"]
        return [
            "admin_paroquia",
            "montagem",
            "palestras",
            "fichas",
            "pos_encontro",
            "financas",
            "usuario_comum",
        ]

    def _pode_manipular_usuario(usuario):
        perfil = perfil_atual()
        if perfil == "super":
            return True
        if perfil == "admin":
            return usuario.get("perfil") != "super"
        return int(usuario.get("paroquia_id") or 0) == int(usuario_paroquia_id() or 0) and usuario.get("perfil") not in {"super", "admin"}

    @app.route("/__init_db__")
    def init_db_route():
        if not pode_gerenciar_paroquias():
            return jsonify({"ok": False, "msg": "Acesso negado"}), 403

        try:
            resultado = ensure_database_schema()
            return jsonify(resultado)
        except Exception as e:
            return jsonify({"ok": False, "msg": str(e)}), 500

    @app.route("/admin")
    def admin_home():
        return render_template("admin_home.html")

    @app.route("/admin/paroquias")
    def admin_paroquias():
        conn = db_conn()
        cur = conn.cursor(dictionary=True)

        try:
            cur.execute("""
                SELECT id, nome, cidade, estado, diocese, ativa, created_at, updated_at
                FROM paroquias
                ORDER BY ativa DESC, nome ASC
            """)
            paroquias = cur.fetchall() or []
        finally:
            cur.close()
            conn.close()

        return render_template(
            "admin_paroquias.html",
            paroquias=paroquias,
            editando=None,
        )

    @app.route("/admin/paroquias/criar", methods=["POST"])
    def admin_paroquias_criar():
        nome = (request.form.get("nome") or "").strip()
        cidade = (request.form.get("cidade") or "").strip()
        estado = (request.form.get("estado") or "").strip().upper()
        diocese = (request.form.get("diocese") or "").strip()

        if not nome:
            flash("Nome da paróquia é obrigatório.", "error")
            return redirect(url_for("admin_paroquias"))

        conn = db_conn()
        cur = conn.cursor()

        try:
            cur.execute("""
                INSERT INTO paroquias (nome, cidade, estado, diocese, ativa)
                VALUES (%s, %s, %s, %s, 1)
            """, (nome, cidade or None, estado or None, diocese or None))
            conn.commit()
            flash("Paróquia cadastrada com sucesso.", "success")
        finally:
            cur.close()
            conn.close()

        return redirect(url_for("admin_paroquias"))

    @app.route("/admin/paroquias/<int:paroquia_id>/editar")
    def admin_paroquias_editar(paroquia_id):
        conn = db_conn()
        cur = conn.cursor(dictionary=True)

        try:
            cur.execute("""
                SELECT id, nome, cidade, estado, diocese, ativa
                FROM paroquias
                WHERE id = %s
            """, (paroquia_id,))
            editando = cur.fetchone()

            if not editando:
                return "Paróquia não encontrada.", 404

            cur.execute("""
                SELECT id, nome, cidade, estado, diocese, ativa, created_at, updated_at
                FROM paroquias
                ORDER BY ativa DESC, nome ASC
            """)
            paroquias = cur.fetchall() or []
        finally:
            cur.close()
            conn.close()

        return render_template(
            "admin_paroquias.html",
            paroquias=paroquias,
            editando=editando,
        )

    @app.route("/admin/paroquias/<int:paroquia_id>/atualizar", methods=["POST"])
    def admin_paroquias_atualizar(paroquia_id):
        nome = (request.form.get("nome") or "").strip()
        cidade = (request.form.get("cidade") or "").strip()
        estado = (request.form.get("estado") or "").strip().upper()
        diocese = (request.form.get("diocese") or "").strip()

        if not nome:
            flash("Nome da paróquia é obrigatório.", "error")
            return redirect(url_for("admin_paroquias_editar", paroquia_id=paroquia_id))

        conn = db_conn()
        cur = conn.cursor()

        try:
            cur.execute("""
                UPDATE paroquias
                SET nome = %s,
                    cidade = %s,
                    estado = %s,
                    diocese = %s
                WHERE id = %s
            """, (nome, cidade or None, estado or None, diocese or None, paroquia_id))
            conn.commit()
            flash("Paróquia atualizada com sucesso.", "success")
        finally:
            cur.close()
            conn.close()

        return redirect(url_for("admin_paroquias"))

    @app.route("/admin/paroquias/<int:paroquia_id>/alternar", methods=["POST"])
    def admin_paroquias_alternar(paroquia_id):
        conn = db_conn()
        cur = conn.cursor()

        try:
            cur.execute("""
                UPDATE paroquias
                SET ativa = CASE WHEN ativa = 1 THEN 0 ELSE 1 END
                WHERE id = %s
            """, (paroquia_id,))
            conn.commit()
            flash("Status da paróquia alterado com sucesso.", "success")
        finally:
            cur.close()
            conn.close()

        return redirect(url_for("admin_paroquias"))

    @app.route("/admin/usuarios")
    def admin_usuarios():
        paroquia_filtro = request.args.get("paroquia_id", type=int)
        conn = db_conn()
        cur = conn.cursor(dictionary=True)

        try:
            sql = """
                SELECT
                    u.id,
                    u.paroquia_id,
                    u.nome,
                    u.login,
                    u.perfil,
                    u.ativo,
                    u.created_at,
                    p.nome AS paroquia_nome
                FROM usuarios u
                LEFT JOIN paroquias p ON p.id = u.paroquia_id
            """
            params = []

            if perfil_atual() not in {"super", "admin"}:
                sql += " WHERE u.paroquia_id = %s AND u.perfil NOT IN ('super', 'admin')"
                params.append(usuario_paroquia_id())
            elif paroquia_filtro:
                sql += " WHERE u.paroquia_id = %s"
                params.append(paroquia_filtro)

            sql += " ORDER BY p.nome, u.nome, u.login"

            cur.execute(sql, params)
            usuarios = cur.fetchall() or []
        finally:
            cur.close()
            conn.close()

        return render_template(
            "admin_usuarios.html",
            usuarios=usuarios,
            paroquias=_paroquias_disponiveis_para_usuario(),
            perfis=_perfis_disponiveis_para_usuario(),
            editando=None,
            paroquia_filtro=paroquia_filtro,
        )

    @app.route("/admin/usuarios/criar", methods=["POST"])
    def admin_usuarios_criar():
        nome = (request.form.get("nome") or "").strip()
        login = (request.form.get("login") or "").strip().lower()
        senha = request.form.get("senha") or ""
        perfil = (request.form.get("perfil") or "").strip()
        paroquia_id = request.form.get("paroquia_id", type=int)

        if perfil_atual() not in {"super", "admin"}:
            paroquia_id = usuario_paroquia_id()

        if perfil not in _perfis_disponiveis_para_usuario():
            flash("Perfil não permitido para o seu usuário.", "error")
            return redirect(url_for("admin_usuarios"))

        if not nome or not login or not senha or not perfil:
            flash("Preencha nome, login, senha e perfil.", "error")
            return redirect(url_for("admin_usuarios"))

        if perfil not in {"super", "admin"} and not paroquia_id:
            flash("Informe a paróquia do usuário.", "error")
            return redirect(url_for("admin_usuarios"))

        conn = db_conn()
        cur = conn.cursor()

        try:
            cur.execute("""
                INSERT INTO usuarios (paroquia_id, nome, login, senha_hash, perfil, ativo)
                VALUES (%s, %s, %s, %s, %s, 1)
            """, (
                paroquia_id if perfil not in {"super", "admin"} else None,
                nome,
                login,
                gerar_hash_senha(senha),
                perfil,
            ))
            conn.commit()
            flash("Usuário cadastrado com sucesso.", "success")
        except Exception as e:
            conn.rollback()
            flash(f"Erro ao cadastrar usuário: {e}", "error")
        finally:
            cur.close()
            conn.close()

        return redirect(url_for("admin_usuarios"))

    @app.route("/admin/usuarios/<int:usuario_id>/editar")
    def admin_usuarios_editar(usuario_id):
        conn = db_conn()
        cur = conn.cursor(dictionary=True)

        try:
            cur.execute("""
                SELECT id, paroquia_id, nome, login, perfil, ativo
                FROM usuarios
                WHERE id = %s
            """, (usuario_id,))
            editando = cur.fetchone()

            if not editando:
                return "Usuário não encontrado.", 404

            if not _pode_manipular_usuario(editando):
                return "Acesso negado.", 403

            cur.execute("""
                SELECT
                    u.id,
                    u.paroquia_id,
                    u.nome,
                    u.login,
                    u.perfil,
                    u.ativo,
                    u.created_at,
                    p.nome AS paroquia_nome
                FROM usuarios u
                LEFT JOIN paroquias p ON p.id = u.paroquia_id
                ORDER BY p.nome, u.nome, u.login
            """)
            usuarios = cur.fetchall() or []

            if perfil_atual() not in {"super", "admin"}:
                usuarios = [u for u in usuarios if int(u.get("paroquia_id") or 0) == int(usuario_paroquia_id() or 0) and u.get("perfil") not in {"super", "admin"}]
        finally:
            cur.close()
            conn.close()

        return render_template(
            "admin_usuarios.html",
            usuarios=usuarios,
            paroquias=_paroquias_disponiveis_para_usuario(),
            perfis=_perfis_disponiveis_para_usuario(),
            editando=editando,
            paroquia_filtro=None,
        )

    @app.route("/admin/usuarios/<int:usuario_id>/atualizar", methods=["POST"])
    def admin_usuarios_atualizar(usuario_id):
        nome = (request.form.get("nome") or "").strip()
        login = (request.form.get("login") or "").strip().lower()
        senha = request.form.get("senha") or ""
        perfil = (request.form.get("perfil") or "").strip()
        paroquia_id = request.form.get("paroquia_id", type=int)

        if perfil_atual() not in {"super", "admin"}:
            paroquia_id = usuario_paroquia_id()

        conn = db_conn()
        cur = conn.cursor(dictionary=True)

        try:
            cur.execute("SELECT id, paroquia_id, perfil FROM usuarios WHERE id = %s", (usuario_id,))
            atual = cur.fetchone()
            if not atual:
                return "Usuário não encontrado.", 404
            if not _pode_manipular_usuario(atual):
                return "Acesso negado.", 403

            if perfil not in _perfis_disponiveis_para_usuario():
                flash("Perfil não permitido para o seu usuário.", "error")
                return redirect(url_for("admin_usuarios_editar", usuario_id=usuario_id))

            if senha:
                cur.execute("""
                    UPDATE usuarios
                    SET paroquia_id = %s,
                        nome = %s,
                        login = %s,
                        senha_hash = %s,
                        perfil = %s
                    WHERE id = %s
                """, (
                    paroquia_id if perfil not in {"super", "admin"} else None,
                    nome,
                    login,
                    gerar_hash_senha(senha),
                    perfil,
                    usuario_id,
                ))
            else:
                cur.execute("""
                    UPDATE usuarios
                    SET paroquia_id = %s,
                        nome = %s,
                        login = %s,
                        perfil = %s
                    WHERE id = %s
                """, (
                    paroquia_id if perfil not in {"super", "admin"} else None,
                    nome,
                    login,
                    perfil,
                    usuario_id,
                ))

            conn.commit()
            flash("Usuário atualizado com sucesso.", "success")
        except Exception as e:
            conn.rollback()
            flash(f"Erro ao atualizar usuário: {e}", "error")
        finally:
            cur.close()
            conn.close()

        return redirect(url_for("admin_usuarios"))

    @app.route("/admin/usuarios/<int:usuario_id>/alternar", methods=["POST"])
    def admin_usuarios_alternar(usuario_id):
        conn = db_conn()
        cur = conn.cursor(dictionary=True)

        try:
            cur.execute("SELECT id, paroquia_id, perfil FROM usuarios WHERE id = %s", (usuario_id,))
            usuario = cur.fetchone()
            if not usuario:
                return "Usuário não encontrado.", 404
            if not _pode_manipular_usuario(usuario):
                return "Acesso negado.", 403

            cur.execute("""
                UPDATE usuarios
                SET ativo = CASE WHEN ativo = 1 THEN 0 ELSE 1 END
                WHERE id = %s
            """, (usuario_id,))
            conn.commit()
            flash("Status do usuário alterado com sucesso.", "success")
        finally:
            cur.close()
            conn.close()

        return redirect(url_for("admin_usuarios"))
