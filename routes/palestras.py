from flask import render_template, request, jsonify

from db import db_conn


def register_palestras_routes(
    app,
    PALESTRAS_TITULOS,
    PALESTRAS_SOLO,
):
    @app.route('/palestras')
    def palestras_painel():
        """Painel: anos em Aberto x Concluído na tabela 'palestras'."""
        conn = db_conn()
        cur = conn.cursor(dictionary=True)
        cur.execute("""
            SELECT ano,
                   SUM(CASE WHEN UPPER(TRIM(status))='CONCLUIDO' THEN 1 ELSE 0 END) AS qtd_concluido,
                   COUNT(*) AS total,
                   SUM(CASE WHEN UPPER(TRIM(status))='ABERTO' THEN 1 ELSE 0 END) AS qtd_aberto
              FROM palestras
             GROUP BY ano
             ORDER BY ano DESC
        """)
        rows = cur.fetchall() or []
        cur.close(); conn.close()

        anos_concluidos, anos_aberto = [], []
        for r in rows:
            item = {
                "ano": r["ano"],
                "qtd_concluido": int(r["qtd_concluido"] or 0),
                "total": int(r["total"] or 0),
                "qtd_aberto": int(r["qtd_aberto"] or 0)
            }
            if item["total"] > 0 and item["qtd_aberto"] == 0:
                anos_concluidos.append(item)
            else:
                anos_aberto.append(item)

        return render_template(
            'palestras_painel.html',
            anos_aberto=anos_aberto,
            anos_concluidos=anos_concluidos
        )

    @app.route('/palestras/nova')
    def palestras_nova():
        """
        Tela de montagem das palestras para um ano.
        Querystring opcional: ?ano=YYYY (pré-seleciona o ano).
        """
        ano_preselecionado = request.args.get('ano', type=int)
        existentes = {}
        tem_abertos = False

        if ano_preselecionado:
            conn = db_conn()
            cur = conn.cursor(dictionary=True)
            try:
                cur.execute("""
                    SELECT id, palestra, nome_ele, nome_ela, status
                      FROM palestras
                     WHERE ano = %s
                     ORDER BY id DESC
                """, (ano_preselecionado,))
                rows = cur.fetchall() or []

                for r in rows:
                    t = r.get("palestra") or ""
                    if t and t not in existentes:
                        existentes[t] = {
                            "id": r.get("id"),
                            "nome_ele": r.get("nome_ele"),
                            "nome_ela": r.get("nome_ela"),
                            "status": r.get("status"),
                        }
                    st = (r.get("status") or "").strip().lower()
                    if st == "aberto":
                        tem_abertos = True
            finally:
                try:
                    cur.close()
                    conn.close()
                except Exception:
                    pass

        return render_template(
            'nova_palestras.html',
            ano_preselecionado=ano_preselecionado,
            titulos=PALESTRAS_TITULOS,
            solo_titulos=list(PALESTRAS_SOLO),
            existentes=existentes,
            tem_abertos=tem_abertos
        )

    # Compatibilidade com templates antigos que usam url_for('nova_palestra')
    app.add_url_rule('/palestras/nova', endpoint='nova_palestra', view_func=palestras_nova)

    @app.route('/api/palestras/validate', methods=['POST'], endpoint='api_palestras_validate')
    def api_palestras_validate_compat():
        """Compatibilidade com templates antigos que enviam {ano, titulo, nome_ele, nome_ela}."""
        data = request.get_json(silent=True) or {}
        palestra = (data.get('titulo') or data.get('palestra') or '').strip()
        nome_ele = (data.get('nome_ele') or '').strip()
        nome_ela = (data.get('nome_ela') or '').strip()

        if not (palestra and nome_ele):
            return jsonify({"ok": False, "msg": "Parâmetros insuficientes."}), 400

        solo = palestra in PALESTRAS_SOLO
        if (not solo) and not nome_ela:
            return jsonify({"ok": False, "msg": "Informe Nome (Ela)."}), 400

        conn = db_conn()
        cur = conn.cursor(dictionary=True)
        try:
            eligible = False
            cur.execute("""
                SELECT 1 FROM encontristas
                 WHERE nome_usual_ele = %s AND nome_usual_ela = %s
                 LIMIT 1
            """, (nome_ele, nome_ela if not solo else ''))
            if cur.fetchone():
                eligible = True
            else:
                cur.execute("""
                    SELECT 1 FROM encontreiros
                     WHERE nome_ele = %s AND nome_ela = %s
                     LIMIT 1
                """, (nome_ele, nome_ela if not solo else ''))
                eligible = cur.fetchone() is not None

            if solo:
                eligible = True

            telefones, endereco = '', ''
            if not solo:
                cur.execute("""
                    SELECT telefones, endereco
                      FROM encontreiros
                     WHERE nome_ele = %s AND nome_ela = %s
                     ORDER BY ano DESC
                     LIMIT 1
                """, (nome_ele, nome_ela))
                r = cur.fetchone()
                if r:
                    telefones = (r.get('telefones') or '').strip()
                    endereco = r.get('endereco') or ''
                else:
                    cur.execute("""
                        SELECT telefone_ele, telefone_ela, endereco
                          FROM encontristas
                         WHERE nome_usual_ele = %s AND nome_usual_ela = %s
                         ORDER BY ano DESC
                         LIMIT 1
                    """, (nome_ele, nome_ela))
                    r2 = cur.fetchone()
                    if r2:
                        tel_ele = (r2.get('telefone_ele') or '').strip()
                        tel_ela = (r2.get('telefone_ela') or '').strip()
                        telefones = " / ".join([t for t in [tel_ele, tel_ela] if t])
                        endereco = r2.get('endereco') or ''

            if solo:
                cur.execute("""
                    SELECT COUNT(*) AS n
                      FROM palestras
                     WHERE palestra = %s AND nome_ele = %s
                """, (palestra, nome_ele))
            else:
                cur.execute("""
                    SELECT COUNT(*) AS n
                      FROM palestras
                     WHERE palestra = %s AND nome_ele = %s AND nome_ela = %s
                """, (palestra, nome_ele, nome_ela))
            n = int(((cur.fetchone() or {}).get('n', 0) or 0))

            if not eligible:
                return jsonify({
                    "ok": False,
                    "eligible": False,
                    "cap": 5,
                    "repeticoes": n,
                    "telefones": telefones,
                    "endereco": endereco,
                    "msg": "Casal precisa ser encontrista ou já ter trabalhado no ECC."
                }), 403

            return jsonify({
                "ok": True,
                "eligible": True,
                "cap": 5,
                "repeticoes": n,
                "telefones": telefones,
                "endereco": endereco
            })
        finally:
            try:
                cur.close()
                conn.close()
            except Exception:
                pass

    @app.route('/api/palestras/save', methods=['POST'], endpoint='api_palestras_save')
    def api_palestras_save_compat():
        """Compatibilidade com templates antigos; faz update do título/ano se já existir."""
        data = request.get_json(silent=True) or {}
        ano = data.get('ano')
        palestra = (data.get('titulo') or data.get('palestra') or '').strip()
        nome_ele = (data.get('nome_ele') or '').strip()
        nome_ela = (data.get('nome_ela') or '').strip()

        if not (ano and palestra and nome_ele):
            return jsonify({"ok": False, "msg": "Parâmetros insuficientes."}), 400

        solo = palestra in PALESTRAS_SOLO
        if (not solo) and not nome_ela:
            return jsonify({"ok": False, "msg": "Informe Nome (Ela)."}), 400

        conn = db_conn()
        cur = conn.cursor(dictionary=True)
        try:
            if solo:
                cur.execute("""
                    SELECT COUNT(*) AS n
                      FROM palestras
                     WHERE palestra = %s AND nome_ele = %s
                """, (palestra, nome_ele))
            else:
                cur.execute("""
                    SELECT COUNT(*) AS n
                      FROM palestras
                     WHERE palestra = %s AND nome_ele = %s AND nome_ela = %s
                """, (palestra, nome_ele, nome_ela))
            repeticoes = int(((cur.fetchone() or {}).get('n', 0) or 0))
            if repeticoes >= 5:
                return jsonify({"ok": False, "msg": "Limite de 5 repetições atingido para esta palestra."}), 409

            cur.execute("""
                SELECT id
                  FROM palestras
                 WHERE ano = %s AND palestra = %s
                 ORDER BY id DESC
                 LIMIT 1
            """, (int(ano), palestra))
            existing = cur.fetchone()

            cur2 = conn.cursor()
            if existing:
                if solo:
                    cur2.execute("""
                        UPDATE palestras
                           SET nome_ele = %s,
                               nome_ela = '',
                               status = 'Aberto'
                         WHERE id = %s
                    """, (nome_ele, existing['id']))
                else:
                    cur2.execute("""
                        UPDATE palestras
                           SET nome_ele = %s,
                               nome_ela = %s,
                               status = 'Aberto'
                         WHERE id = %s
                    """, (nome_ele, nome_ela, existing['id']))
                action = 'update'
            else:
                if solo:
                    cur2.execute("""
                        INSERT INTO palestras (ano, palestra, nome_ele, nome_ela, status)
                        VALUES (%s, %s, %s, '', 'Aberto')
                    """, (int(ano), palestra, nome_ele))
                else:
                    cur2.execute("""
                        INSERT INTO palestras (ano, palestra, nome_ele, nome_ela, status)
                        VALUES (%s, %s, %s, %s, 'Aberto')
                    """, (int(ano), palestra, nome_ele, nome_ela))
                action = 'insert'

            conn.commit()
            cur2.close()
            return jsonify({"ok": True, "action": action})
        finally:
            try:
                cur.close()
                conn.close()
            except Exception:
                pass

    @app.route('/api/palestras/buscar', methods=['POST'])
    def api_palestras_buscar():
        """
        Body: { palestra, nome_ele, nome_ela }
        """
        data = request.get_json(silent=True) or {}
        palestra = (data.get('palestra') or '').strip()
        nome_ele = (data.get('nome_ele') or '').strip()
        nome_ela = (data.get('nome_ela') or '').strip()

        if not (palestra and nome_ele and nome_ela):
            return jsonify({"ok": False, "msg": "Parâmetros insuficientes."}), 400

        conn = db_conn()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute("""
                SELECT 1 FROM encontristas
                 WHERE nome_usual_ele = %s AND nome_usual_ela = %s
                 LIMIT 1
            """, (nome_ele, nome_ela))
            e1 = cur.fetchone() is not None

            if not e1:
                cur.execute("""
                    SELECT 1 FROM encontreiros
                     WHERE nome_ele = %s AND nome_ela = %s
                     LIMIT 1
                """, (nome_ele, nome_ela))
                e2 = cur.fetchone() is not None
            else:
                e2 = True

            if not e2:
                return jsonify({"ok": False, "msg": "Casal precisa ser encontrista ou já ter trabalhado no ECC."}), 403

            telefones, endereco = '', ''
            cur.execute("""
                SELECT telefones, endereco
                  FROM encontreiros
                 WHERE nome_ele = %s AND nome_ela = %s
                 ORDER BY ano DESC
                 LIMIT 1
            """, (nome_ele, nome_ela))
            r = cur.fetchone()
            if r:
                telefones = (r.get('telefones') or '').strip()
                endereco = r.get('endereco') or ''
            else:
                cur.execute("""
                    SELECT telefone_ele, telefone_ela, endereco
                      FROM encontristas
                     WHERE nome_usual_ele = %s AND nome_usual_ela = %s
                     ORDER BY ano DESC
                     LIMIT 1
                """, (nome_ele, nome_ela))
                r2 = cur.fetchone()
                if r2:
                    tel_ele = (r2.get('telefone_ele') or '').strip()
                    tel_ela = (r2.get('telefone_ela') or '').strip()
                    telefones = " / ".join([t for t in [tel_ele, tel_ela] if t])
                    endereco = r2.get('endereco') or ''

            cur.execute("""
                SELECT COUNT(*) AS n
                  FROM palestras
                 WHERE palestra = %s AND nome_ele = %s AND nome_ela = %s
            """, (palestra, nome_ele, nome_ela))
            n = (cur.fetchone() or {}).get("n", 0) or 0
            n = int(n)

            if n >= 5:
                return jsonify({
                    "ok": False,
                    "repeticoes": n,
                    "msg": "Limite de 5 repetições atingido para esta palestra."
                }), 409

            return jsonify({
                "ok": True,
                "telefones": telefones,
                "endereco": endereco,
                "repeticoes": n
            })
        finally:
            try:
                cur.close()
                conn.close()
            except Exception:
                pass

    @app.route('/api/palestras/adicionar', methods=['POST'])
    def api_palestras_adicionar():
        """
        Body (solo): { ano, palestra, nome_ele }
        Body (casal): { ano, palestra, nome_ele, nome_ela, telefones?, endereco? }
        """
        data = request.get_json(silent=True) or {}
        ano = data.get('ano')
        palestra = (data.get('palestra') or '').strip()
        nome_ele = (data.get('nome_ele') or '').strip()
        nome_ela = (data.get('nome_ela') or '').strip()

        if not (ano and palestra and nome_ele):
            return jsonify({"ok": False, "msg": "Parâmetros insuficientes."}), 400

        solo = palestra in PALESTRAS_SOLO
        if (not solo) and not nome_ela:
            return jsonify({"ok": False, "msg": "Informe Nome (Ela)."}), 400

        conn = db_conn()
        cur = conn.cursor(dictionary=True)
        try:
            if not solo:
                cur.execute("""
                    SELECT COUNT(*) AS n
                      FROM palestras
                     WHERE palestra = %s AND nome_ele = %s AND nome_ela = %s
                """, (palestra, nome_ele, nome_ela))
                n = (cur.fetchone() or {}).get("n", 0) or 0
                if int(n) >= 5:
                    return jsonify({"ok": False, "msg": "Limite de 5 repetições atingido para esta palestra."}), 409

            cur2 = conn.cursor()
            if solo:
                cur2.execute("""
                    INSERT INTO palestras (ano, palestra, nome_ele, status, observacao)
                    VALUES (%s, %s, %s, 'Aberto', NULL)
                """, (int(ano), palestra, nome_ele))
            else:
                cur2.execute("""
                    INSERT INTO palestras (ano, palestra, nome_ele, nome_ela, status, observacao)
                    VALUES (%s, %s, %s, %s, 'Aberto', NULL)
                """, (int(ano), palestra, nome_ele, nome_ela))
            conn.commit()
            cur2.close()
            return jsonify({"ok": True})
        finally:
            try:
                cur.close()
                conn.close()
            except Exception:
                pass

    @app.route('/api/palestras/encerrar', methods=['POST'])
    def api_palestras_encerrar():
        """
        Fecha o ano de palestras: tudo que estiver 'Aberto' vira 'Concluido'.
        Body: { "ano": 2025 }
        """
        data = request.get_json(silent=True) or {}
        ano = data.get('ano')
        if not ano:
            return jsonify({"ok": False, "msg": "Ano obrigatório."}), 400

        conn = db_conn()
        cur = conn.cursor()
        try:
            cur.execute("""
                UPDATE palestras
                   SET status = 'Concluido'
                 WHERE ano = %s
                   AND UPPER(status) = 'ABERTO'
            """, (int(ano),))
            conn.commit()
            return jsonify({"ok": True, "alterados": cur.rowcount})
        finally:
            try:
                cur.close()
                conn.close()
            except Exception:
                pass

    # Alias retrocompatível
    app.add_url_rule(
        '/api/palestras/encerrar-ano',
        endpoint='api_palestras_encerrar_ano',
        view_func=api_palestras_encerrar,
        methods=['POST']
    )

    @app.route('/api/palestras/marcar-status', methods=['POST'])
    def api_palestras_marcar_status():
        """
        Marca um registro de palestra como Recusou/Desistiu com justificativa.
        """
        data = request.get_json(silent=True) or {}
        _id = data.get('id')
        novo_status = (data.get('novo_status') or '').strip().title()
        observacao = (data.get('observacao') or '').strip()

        if novo_status not in ('Recusou', 'Desistiu'):
            return jsonify({"ok": False, "msg": "novo_status deve ser 'Recusou' ou 'Desistiu'."}), 400
        if not observacao:
            return jsonify({"ok": False, "msg": "Observação é obrigatória."}), 400

        conn = db_conn()
        cur = conn.cursor()
        try:
            if _id:
                cur.execute("""
                    UPDATE palestras
                       SET status = %s, observacao = %s
                     WHERE id = %s
                       AND (status IS NULL OR UPPER(status) IN ('ABERTO','ACEITO'))
                     LIMIT 1
                """, (novo_status, observacao, int(_id)))
                conn.commit()
                if cur.rowcount == 0:
                    return jsonify({"ok": False, "msg": "Registro não encontrado ou não alterável."}), 404
                return jsonify({"ok": True})

            ano = data.get('ano')
            palestra = (data.get('palestra') or '').strip()
            if not (ano and palestra):
                return jsonify({"ok": False, "msg": "Informe id, ou ano e palestra."}), 400

            nome_ele = (data.get('nome_ele') or '').strip()
            nome_ela = (data.get('nome_ela') or '').strip()

            clauses = [
                "UPDATE palestras SET status=%s, observacao=%s",
                "WHERE ano=%s AND palestra=%s",
                "AND (status IS NULL OR UPPER(status) IN ('ABERTO','ACEITO'))"
            ]
            params = [novo_status, observacao, int(ano), palestra]

            if nome_ele:
                clauses.append("AND nome_ele=%s")
                params.append(nome_ele)
            if nome_ela:
                clauses.append("AND nome_ela=%s")
                params.append(nome_ela)

            clauses.append("ORDER BY id DESC LIMIT 1")
            sql = "\n".join(clauses)

            cur.execute(sql, tuple(params))
            conn.commit()
            if cur.rowcount == 0:
                return jsonify({"ok": False, "msg": "Registro não encontrado ou não alterável com os critérios informados."}), 404
            return jsonify({"ok": True})
        finally:
            try:
                cur.close()
                conn.close()
            except Exception:
                pass
