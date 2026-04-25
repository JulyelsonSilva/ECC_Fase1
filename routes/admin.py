from flask import jsonify, render_template, request, redirect, url_for

from db import db_conn
from services.schema_service import ensure_database_schema


def register_admin_routes(app, _admin_ok):

    def admin_token():
        return request.args.get("token") or request.form.get("token") or ""

    # =========================
    # INIT DB
    # =========================
    @app.route("/__init_db__")
    def init_db_route():
        if not _admin_ok():
            return jsonify({"ok": False, "msg": "Acesso negado"}), 403

        try:
            resultado = ensure_database_schema()
            return jsonify(resultado)
        except Exception as e:
            return jsonify({"ok": False, "msg": str(e)}), 500

    # =========================
    # ADMINISTRAÇÃO DE PARÓQUIAS
    # =========================
    @app.route("/admin/paroquias")
    def admin_paroquias():
        if not _admin_ok():
            return "Acesso negado. Informe o token administrativo.", 403

        token = admin_token()

        conn = db_conn()
        cur = conn.cursor(dictionary=True)

        try:
            cur.execute("""
                SELECT
                    id,
                    nome,
                    cidade,
                    estado,
                    diocese,
                    ativa,
                    created_at,
                    updated_at
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
            token=token
        )

    @app.route("/admin/paroquias/criar", methods=["POST"])
    def admin_paroquias_criar():
        if not _admin_ok():
            return "Acesso negado.", 403

        token = admin_token()

        nome = (request.form.get("nome") or "").strip()
        cidade = (request.form.get("cidade") or "").strip()
        estado = (request.form.get("estado") or "").strip().upper()
        diocese = (request.form.get("diocese") or "").strip()

        if not nome:
            return "Nome da paróquia é obrigatório.", 400

        conn = db_conn()
        cur = conn.cursor()

        try:
            cur.execute("""
                INSERT INTO paroquias
                    (nome, cidade, estado, diocese, ativa)
                VALUES
                    (%s, %s, %s, %s, 1)
            """, (
                nome,
                cidade or None,
                estado or None,
                diocese or None
            ))
            conn.commit()
        finally:
            cur.close()
            conn.close()

        return redirect(url_for("admin_paroquias", token=token))

    @app.route("/admin/paroquias/<int:paroquia_id>/editar")
    def admin_paroquias_editar(paroquia_id):
        if not _admin_ok():
            return "Acesso negado.", 403

        token = admin_token()

        conn = db_conn()
        cur = conn.cursor(dictionary=True)

        try:
            cur.execute("""
                SELECT
                    id,
                    nome,
                    cidade,
                    estado,
                    diocese,
                    ativa
                FROM paroquias
                WHERE id = %s
            """, (paroquia_id,))
            editando = cur.fetchone()

            if not editando:
                return "Paróquia não encontrada.", 404

            cur.execute("""
                SELECT
                    id,
                    nome,
                    cidade,
                    estado,
                    diocese,
                    ativa,
                    created_at,
                    updated_at
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
            token=token
        )

    @app.route("/admin/paroquias/<int:paroquia_id>/atualizar", methods=["POST"])
    def admin_paroquias_atualizar(paroquia_id):
        if not _admin_ok():
            return "Acesso negado.", 403

        token = admin_token()

        nome = (request.form.get("nome") or "").strip()
        cidade = (request.form.get("cidade") or "").strip()
        estado = (request.form.get("estado") or "").strip().upper()
        diocese = (request.form.get("diocese") or "").strip()

        if not nome:
            return "Nome da paróquia é obrigatório.", 400

        conn = db_conn()
        cur = conn.cursor()

        try:
            cur.execute("""
                UPDATE paroquias
                SET
                    nome = %s,
                    cidade = %s,
                    estado = %s,
                    diocese = %s
                WHERE id = %s
            """, (
                nome,
                cidade or None,
                estado or None,
                diocese or None,
                paroquia_id
            ))
            conn.commit()
        finally:
            cur.close()
            conn.close()

        return redirect(url_for("admin_paroquias", token=token))

    @app.route("/admin/paroquias/<int:paroquia_id>/alternar", methods=["POST"])
    def admin_paroquias_alternar(paroquia_id):
        if not _admin_ok():
            return "Acesso negado.", 403

        token = admin_token()

        conn = db_conn()
        cur = conn.cursor()

        try:
            cur.execute("""
                UPDATE paroquias
                SET ativa = CASE WHEN ativa = 1 THEN 0 ELSE 1 END
                WHERE id = %s
            """, (paroquia_id,))
            conn.commit()
        finally:
            cur.close()
            conn.close()

        return redirect(url_for("admin_paroquias", token=token))