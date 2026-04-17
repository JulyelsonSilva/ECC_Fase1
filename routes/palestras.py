from flask import render_template, request, jsonify

from db import db_conn


def register_palestras_routes(
    app,
    PALESTRAS_TITULOS,
    PALESTRAS_SOLO,
):
    # =========================
    # PALESTRAS (listagem)
    # =========================
    @app.route('/palestras')
    def palestras():
        conn = db_conn()
        cursor = conn.cursor(dictionary=True)

        nome_ele = (request.args.get('nome_ele') or '').strip()
        nome_ela = (request.args.get('nome_ela') or '').strip()
        ano = (request.args.get('ano') or '').strip()

        query = """
            SELECT id, ano, palestra, nome_ele, nome_ela, observacao, status
              FROM palestras
             WHERE 1=1
        """
        params = []

        if nome_ele:
            query += " AND nome_ele LIKE %s"
            params.append(f"%{nome_ele}%")

        if nome_ela:
            query += " AND nome_ela LIKE %s"
            params.append(f"%{nome_ela}%")

        if ano:
            query += " AND ano = %s"
            params.append(ano)

        query += " ORDER BY ano DESC, palestra ASC"

        cursor.execute(query, params)
        dados = cursor.fetchall()

        cursor.close()
        conn.close()

        return render_template(
            'palestras.html',
            dados=dados
        )

    # =========================
    # API – Contagem por ano
    # =========================
    @app.route("/api/palestras_por_ano")
    def api_palestras_por_ano():
        conn = db_conn()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute("""
                SELECT ano, COUNT(*) AS qtd
                  FROM palestras
                 WHERE ano IS NOT NULL
                 GROUP BY ano
                 ORDER BY ano ASC
            """)
            rows = cur.fetchall() or []

            out = []
            for r in rows:
                if r.get("ano") is not None:
                    out.append({
                        "ano": int(r["ano"]),
                        "qtd": int(r.get("qtd") or 0)
                    })

            return jsonify(out)
        finally:
            try:
                cur.close()
                conn.close()
            except Exception:
                pass

    # =========================
    # API – Buscar palestra
    # =========================
    @app.route("/api/palestras/busca")
    def api_palestras_busca():
        nome_ele = (request.args.get("nome_ele") or "").strip()
        nome_ela = (request.args.get("nome_ela") or "").strip()

        if not nome_ele:
            return jsonify({"ok": False, "msg": "Informe pelo menos o nome_ele."}), 400

        conn = db_conn()
        cur = conn.cursor(dictionary=True)
        try:
            query = """
                SELECT ano, palestra, nome_ele, nome_ela
                  FROM palestras
                 WHERE LOWER(nome_ele) LIKE LOWER(%s)
            """
            params = [f"%{nome_ele}%"]

            if nome_ela:
                query += " AND LOWER(nome_ela) LIKE LOWER(%s)"
                params.append(f"%{nome_ela}%")

            query += " ORDER BY ano DESC"

            cur.execute(query, params)
            rows = cur.fetchall() or []

            return jsonify({
                "ok": True,
                "resultados": rows
            })
        finally:
            try:
                cur.close()
                conn.close()
            except Exception:
                pass

    # =========================
    # API – Inserir palestra
    # =========================
    @app.route('/api/palestras/add', methods=['POST'])
    def api_add_palestra():
        data = request.get_json(silent=True) or {}

        ano = data.get("ano")
        palestra = (data.get("palestra") or "").strip()
        nome_ele = (data.get("nome_ele") or "").strip()
        nome_ela = (data.get("nome_ela") or "").strip()
        observacao = (data.get("observacao") or "").strip()

        if not (ano and palestra and nome_ele):
            return jsonify({"ok": False, "msg": "Campos obrigatórios faltando."}), 400

        # validação simples: palestra existe na lista padrão
        if palestra not in PALESTRAS_TITULOS:
            return jsonify({"ok": False, "msg": "Palestra inválida."}), 400

        # validação: se for SOLO, não pode ter nome_ela
        if palestra in PALESTRAS_SOLO:
            nome_ela = ""

        conn = db_conn()
        cur = conn.cursor()
        try:
            cur.execute("""
                INSERT INTO palestras
                    (ano, palestra, nome_ele, nome_ela, observacao, status)
                VALUES
                    (%s,  %s,      %s,       %s,       %s,         'Aberto')
            """, (int(ano), palestra, nome_ele, nome_ela, observacao))

            conn.commit()

            return jsonify({"ok": True})
        finally:
            try:
                cur.close()
                conn.close()
            except Exception:
                pass

    # =========================
    # API – Concluir palestra
    # =========================
    @app.route('/api/palestras/concluir', methods=['POST'])
    def api_concluir_palestra():
        data = request.get_json(silent=True) or {}

        _id = data.get("id")

        if not _id:
            return jsonify({"ok": False, "msg": "ID obrigatório."}), 400

        conn = db_conn()
        cur = conn.cursor()
        try:
            cur.execute("""
                UPDATE palestras
                   SET status = 'Concluido'
                 WHERE id = %s
            """, (int(_id),))

            conn.commit()

            return jsonify({"ok": True})
        finally:
            try:
                cur.close()
                conn.close()
            except Exception:
                pass
