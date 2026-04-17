from flask import render_template, request, jsonify

from db import db_conn


def register_implantacao_routes(
    app,
    TEAM_MAP,
    TEAM_LIMITS,
    TEAM_CHOICES,
    _team_label,
):
    @app.route('/implantacao')
    def implantacao_painel():
        conn = db_conn()
        cur = conn.cursor(dictionary=True)
        cur.execute("""
            SELECT 
                ano,
                SUM(CASE WHEN UPPER(TRIM(status))='CONCLUIDO' THEN 1 ELSE 0 END) AS qtd_concluido,
                COUNT(*) AS total
            FROM implantacao
            GROUP BY ano
            ORDER BY ano DESC
        """)
        rows = cur.fetchall() or []
        cur.close(); conn.close()

        anos_concluidos, anos_aberto = [], []
        for r in rows:
            item = {"ano": r["ano"], "qtd_concluido": int(r["qtd_concluido"] or 0), "total": int(r["total"] or 0)}
            if item["total"] > 0 and item["qtd_concluido"] == item["total"]:
                anos_concluidos.append(item)
            else:
                anos_aberto.append(item)

        return render_template('implantacao.html',
                               anos_aberto=anos_aberto,
                               anos_concluidos=anos_concluidos)

    @app.route('/implantacao/nova')
    def implantacao_nova():
        ano_pre = request.args.get('ano', type=int)

        # sugestões dos encontristas do ano anterior que AINDA não estão na implantacao do ano
        sugestoes_prev_ano = []
        if ano_pre:
            conn = db_conn(); cur = conn.cursor(dictionary=True)
            try:
                cur.execute("""
                    SELECT e.nome_usual_ele, e.nome_usual_ela, e.telefone_ele, e.telefone_ela, e.endereco
                      FROM encontristas e
                     WHERE e.ano = %s
                       AND NOT EXISTS (
                             SELECT 1 FROM implantacao i
                              WHERE i.ano = %s
                                AND i.nome_ele = e.nome_usual_ele
                                AND i.nome_ela = e.nome_usual_ela
                                AND (i.status IS NULL OR UPPER(TRIM(i.status)) NOT IN ('RECUSOU','DESISTIU'))
                       )
                     ORDER BY e.nome_usual_ele, e.nome_usual_ela
                """, (ano_pre - 1, ano_pre))
                for r in cur.fetchall():
                    tel_ele = (r.get('telefone_ele') or '').strip()
                    tel_ela = (r.get('telefone_ela') or '').strip()
                    tels = " / ".join([t for t in [tel_ele, tel_ela] if t])
                    sugestoes_prev_ano.append({
                        "nome_ele": r.get('nome_usual_ele') or '',
                        "nome_ela": r.get('nome_usual_ela') or '',
                        "telefones": tels,
                        "endereco": r.get('endereco') or ''
                    })
            finally:
                try: cur.close(); conn.close()
                except Exception: pass

        return render_template(
            'implantacao.html',
            ano_preselecionado=ano_pre,
            team_map=TEAM_MAP,
            team_limits=TEAM_LIMITS,
            team_choices=TEAM_CHOICES,
            sugestoes_prev_ano=sugestoes_prev_ano
        )

    @app.route('/api/implantacao/equipe-counts')
    def api_implantacao_equipe_counts():
        ano = request.args.get('ano', type=int)
        if not ano:
            return jsonify({"ok": False, "msg": "Ano obrigatório.", "counts": {}}), 400

        conn = db_conn(); cur = conn.cursor(dictionary=True)
        try:
            cur.execute("""
                SELECT equipe, coordenador
                  FROM implantacao
                 WHERE ano = %s
                   AND (status IS NULL OR UPPER(TRIM(status)) NOT IN ('RECUSOU','DESISTIU'))
            """, (ano,))
            rows = cur.fetchall() or []
        finally:
            try: cur.close(); conn.close()
            except Exception: pass

        rotulo_to_filtro = {info["rotulo"]: info["filtro"] for info in TEAM_MAP.values()}
        counts = {info["filtro"]: 0 for info in TEAM_MAP.values()}

        for r in rows:
            eq = (r.get("equipe") or "").strip()
            is_coord = (r.get("coordenador") or "").strip().upper() == "SIM"
            if is_coord:
                continue
            filtro = rotulo_to_filtro.get(eq)
            if filtro:
                counts[filtro] = counts.get(filtro, 0) + 1

        return jsonify({"ok": True, "counts": counts})

    @app.route('/api/implantacao/check-casal', methods=['POST'])
    def api_implantacao_check_casal():
        data = request.get_json(silent=True) or {}
        ano = data.get('ano')
        equipe = _team_label((data.get('equipe') or '').strip())
        nome_ele = (data.get('nome_ele') or '').strip()
        nome_ela = (data.get('nome_ela') or '').strip()
        if not (ano and equipe and nome_ele and nome_ela):
            return jsonify({"ok": False, "msg": "Parâmetros insuficientes."}), 400

        conn = db_conn(); cur = conn.cursor(dictionary=True)
        try:
            # Já está montado neste ano (na própria implantacao)?
            cur.execute("""
                SELECT 1 FROM implantacao
                 WHERE ano = %s AND nome_ele = %s AND nome_ela = %s
                   AND (status IS NULL OR UPPER(status) NOT IN ('RECUSOU','DESISTIU'))
                 LIMIT 1
            """, (int(ano), nome_ele, nome_ela))
            ja_no_ano = cur.fetchone() is not None

            # Já trabalhou nesta equipe (qualquer ano)?
            cur.execute("""
                SELECT 1 FROM implantacao
                 WHERE nome_ele = %s AND nome_ela = %s AND equipe = %s
                 LIMIT 1
            """, (nome_ele, nome_ela, equipe))
            trabalhou_antes = cur.fetchone() is not None

            # Já foi coordenador nesta equipe?
            cur.execute("""
                SELECT 1 FROM implantacao
                 WHERE nome_ele = %s AND nome_ela = %s AND equipe = %s
                   AND UPPER(coordenador)='SIM'
                 LIMIT 1
            """, (nome_ele, nome_ela, equipe))
            ja_coord = cur.fetchone() is not None

            # Telefones/endereço mais recentes: implantacao -> encontreiros -> encontristas
            telefones, endereco = '', ''
            cur.execute("""
                SELECT telefones, endereco
                  FROM implantacao
                 WHERE nome_ele = %s AND nome_ela = %s
                 ORDER BY ano DESC, id DESC
                 LIMIT 1
            """, (nome_ele, nome_ela))
            r = cur.fetchone()
            if r:
                telefones = (r.get('telefones') or '').strip()
                endereco = r.get('endereco') or ''
            else:
                cur.execute("""
                    SELECT telefones, endereco
                      FROM encontreiros
                     WHERE nome_ele = %s AND nome_ela = %s
                     ORDER BY ano DESC, id DESC
                     LIMIT 1
                """, (nome_ele, nome_ela))
                r2 = cur.fetchone()
                if r2:
                    telefones = (r2.get('telefones') or '').strip()
                    endereco = r2.get('endereco') or ''
                else:
                    cur.execute("""
                        SELECT telefone_ele, telefone_ela, endereco
                          FROM encontristas
                         WHERE nome_usual_ele = %s AND nome_usual_ela = %s
                         ORDER BY ano DESC
                         LIMIT 1
                    """, (nome_ele, nome_ela))
                    r3 = cur.fetchone()
                    if r3:
                        tel_ele = (r3.get('telefone_ele') or '').strip()
                        tel_ela = (r3.get('telefone_ela') or '').strip()
                        telefones = " / ".join([t for t in [tel_ele, tel_ela] if t])
                        endereco = r3.get('endereco') or ''

            return jsonify({
                "ok": True,
                "ja_no_ano": ja_no_ano,
                "trabalhou_antes": trabalhou_antes,
                "ja_coordenador": ja_coord,
                "telefones": telefones,
                "endereco": endereco
            })
        finally:
            try: cur.close(); conn.close()
            except Exception: pass

    @app.route('/api/implantacao/add-membro', methods=['POST'])
    def api_implantacao_add_membro():
        data = request.get_json(silent=True) or {}
        ano = data.get('ano')
        equipe = _team_label((data.get('equipe') or '').strip())
        nome_ele = (data.get('nome_ele') or '').strip()
        nome_ela = (data.get('nome_ela') or '').strip()
        telefones = (data.get('telefones') or '').strip()
        endereco  = (data.get('endereco') or '').strip()
        coord_sim = str(data.get('coordenador') or 'Não').strip()
        confirmar_repeticao = bool(data.get('confirmar_repeticao'))

        if not (ano and equipe and nome_ele and nome_ela):
            return jsonify({"ok": False, "msg": "Parâmetros insuficientes."}), 400

        if coord_sim.lower() in ('true', '1', 'sim', 's'):
            coord_val = 'Sim'
        else:
            coord_val = 'Não'

        conn = db_conn(); cur = conn.cursor(dictionary=True)
        try:
            # já no ano?
            cur.execute("""
                SELECT 1 FROM implantacao
                 WHERE ano = %s AND nome_ele = %s AND nome_ela = %s
                   AND (status IS NULL OR UPPER(status) IN ('ABERTO','ACEITO','CONCLUIDO'))
                 LIMIT 1
            """, (int(ano), nome_ele, nome_ela))
            if cur.fetchone():
                return jsonify({"ok": False, "msg": "Casal já está lançado neste ano (Implantação)."}), 409

            # já trabalhou nesta equipe antes?
            cur.execute("""
                SELECT 1 FROM implantacao
                 WHERE nome_ele = %s AND nome_ela = %s AND equipe = %s
                 LIMIT 1
            """, (nome_ele, nome_ela, equipe))
            if cur.fetchone() and not confirmar_repeticao:
                return jsonify({"ok": False, "needs_confirm": True,
                                "msg": "Casal já trabalhou nesta equipe (Implantação). Confirmar para lançar novamente?"})

            cur2 = conn.cursor()
            cur2.execute("""
                INSERT INTO implantacao
                    (ano, equipe, nome_ele, nome_ela, telefones, endereco, coordenador, status)
                VALUES
                    (%s,  %s,     %s,       %s,       %s,         %s,       %s,           'Aberto')
            """, (int(ano), equipe, nome_ele, nome_ela, telefones, endereco, coord_val))
            conn.commit()
            new_id = cur2.lastrowid
            cur2.close()
            return jsonify({"ok": True, "id": new_id})
        finally:
            try: cur.close(); conn.close()
            except Exception: pass

    @app.route('/api/implantacao/marcar-status', methods=['POST'])
    def api_implantacao_marcar_status():
        data = request.get_json(silent=True) or {}
        _id = data.get('id')
        novo_status = (data.get('novo_status') or '').strip().title()
        observacao  = (data.get('observacao') or '').strip()

        if not (_id and novo_status in ('Recusou', 'Desistiu') and observacao):
            return jsonify({"ok": False, "msg": "Parâmetros inválidos. Observação é obrigatória."}), 400

        conn = db_conn(); cur = conn.cursor()
        try:
            cur.execute("""
                UPDATE implantacao
                   SET status=%s, observacao=%s
                 WHERE id=%s
                   AND (status IS NULL OR UPPER(status) IN ('ABERTO','ACEITO'))
                 LIMIT 1
            """, (novo_status, observacao, int(_id)))
            conn.commit()
            if cur.rowcount == 0:
                return jsonify({"ok": False, "msg": "Registro não encontrado ou não alterável."}), 404
            return jsonify({"ok": True})
        finally:
            try: cur.close(); conn.close()
            except Exception: pass

    @app.route('/api/implantacao/concluir-ano', methods=['POST'])
    def api_implantacao_concluir_ano():
        data = request.get_json(silent=True) or {}
        ano = data.get('ano')
        if not ano:
            return jsonify({"ok": False, "msg": "Ano obrigatório."}), 400

        conn = db_conn(); cur = conn.cursor()
        try:
            cur.execute("""
                UPDATE implantacao
                   SET status='Concluido'
                 WHERE ano=%s
                   AND UPPER(status)='ABERTO'
            """, (int(ano),))
            conn.commit()
            return jsonify({"ok": True, "alterados": cur.rowcount})
        finally:
            try: cur.close(); conn.close()
            except Exception: pass
