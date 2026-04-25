from flask import render_template, request, jsonify, redirect, url_for

from db import db_conn


def register_core_routes(
    app,
    TEAM_MAP,
    TEAM_LIMITS,
    _q,
):
    # --- KPI: contagem de integrantes por equipe (exclui Coordenador; exclui Recusou/Desistiu) ---
    @app.route('/api/team-kpis')
    def api_team_kpis():
        ano = request.args.get('ano', type=int)
        if not ano:
            return jsonify({"ok": False, "msg": "Ano obrigatório."}), 400

        rotulo_to_filtro = {}
        for k, v in TEAM_MAP.items():
            rotulo_to_filtro[v["rotulo"]] = v["filtro"]

        data = {}
        conn = db_conn()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute("""
                SELECT equipe, COUNT(*) AS n
                  FROM encontreiros
                 WHERE ano = %s
                   AND (coordenador IS NULL OR UPPER(TRIM(coordenador)) <> 'SIM')
                   AND (status IS NULL OR UPPER(TRIM(status)) NOT IN ('RECUSOU','DESISTIU'))
                 GROUP BY equipe
            """, (ano,))
            for r in cur.fetchall():
                rot = (r.get("equipe") or "").strip()
                n = int(r.get("n") or 0)
                filtro = rotulo_to_filtro.get(rot)
                if filtro:
                    data[filtro] = n
        finally:
            try:
                cur.close()
                conn.close()
            except Exception:
                pass

        return jsonify({"ok": True, "counts": data})

    # =========================
    # Rotas principais
    # =========================
    @app.route('/')
    def index():
        return render_template('index.html')

    @app.route('/fichas', methods=['GET', 'POST'])
    def fichas():
        form = {
            "ano": "",
            "num_ecc": "",
            "data_casamento": "",
            "nome_completo_ele": "",
            "nome_completo_ela": "",
            "nome_usual_ele": "",
            "nome_usual_ela": "",
            "telefone_ele": "",
            "telefone_ela": "",
            "endereco": "",
            "casal_visitacao": "",
            "ficha_num": "",
            "aceitou": "",
            "observacao": "",
            "observacao_extra": "",
        }

        error = None

        if request.method == 'POST':
            for k in form.keys():
                form[k] = (request.form.get(k) or "").strip()

            if not form["ano"]:
                error = "Informe o ano do encontro."
            else:
                try:
                    ano = int(form["ano"])
                except ValueError:
                    ano = None
                    error = "Ano inválido."

            data_casamento = form["data_casamento"] or None

            if not error:
                conn = db_conn()
                cur = conn.cursor()
                try:
                    cur.execute("""
                        INSERT INTO encontristas (
                            ano,
                            num_ecc,
                            data_casamento,
                            nome_completo_ele,
                            nome_completo_ela,
                            nome_usual_ele,
                            nome_usual_ela,
                            telefone_ele,
                            telefone_ela,
                            endereco,
                            casal_visitacao,
                            ficha_num,
                            aceitou,
                            observacao,
                            observacao_extra
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        ano,
                        form["num_ecc"],
                        data_casamento,
                        form["nome_completo_ele"],
                        form["nome_completo_ela"],
                        form["nome_usual_ele"],
                        form["nome_usual_ela"],
                        form["telefone_ele"],
                        form["telefone_ela"],
                        form["endereco"],
                        form["casal_visitacao"],
                        form["ficha_num"],
                        form["aceitou"],
                        form["observacao"],
                        form["observacao_extra"],
                    ))
                    conn.commit()
                finally:
                    try:
                        cur.close()
                        conn.close()
                    except Exception:
                        pass

                return redirect(url_for('fichas', saved=1))

        conn = db_conn()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute("""
                SELECT id, ano, num_ecc, nome_usual_ele, nome_usual_ela
                FROM encontristas
                ORDER BY id DESC
                LIMIT 15
            """)
            ultimos = cur.fetchall() or []
        finally:
            try:
                cur.close()
                conn.close()
            except Exception:
                pass

        saved = request.args.get('saved')

        return render_template(
            'fichas.html',
            form=form,
            error=error,
            saved=saved,
            ultimos=ultimos
        )

    @app.route('/palestrantes')
    def palestrantes():
        nome_ele = (request.args.get('nome_ele') or '').strip()
        nome_ela = (request.args.get('nome_ela') or '').strip()
        ano_filtro = (request.args.get('ano') or '').strip()

        conn = db_conn()
        cur = conn.cursor(dictionary=True)

        sql = """
            SELECT id, ano, palestra, nome_ele, nome_ela
              FROM palestras
             WHERE 1=1
        """
        params = []

        if nome_ele:
            sql += " AND LOWER(nome_ele) LIKE LOWER(%s)"
            params.append(f"%{nome_ele}%")

        if nome_ela:
            sql += " AND LOWER(COALESCE(nome_ela,'')) LIKE LOWER(%s)"
            params.append(f"%{nome_ela}%")

        if ano_filtro:
            sql += " AND ano = %s"
            params.append(ano_filtro)

        sql += " ORDER BY ano DESC, id ASC"

        cur.execute(sql, params)
        rows = cur.fetchall() or []
        cur.close()
        conn.close()

        from collections import defaultdict

        titulo_ordem = {
            "Plano de Deus": 0,
            "Testem.Plano de Deus": 1,
            "Harmonia Conjugal": 2,
            "Diálogo c/ filhos": 3,
            "Penitência": 4,
            "Testem. Jovem": 5,
            "Ceia Eucarística": 6,
            "N.SrªVida da Família": 7,
            "Testem. Ceia Eucarística": 8,
            "Fé Revezes da Vida": 9,
            "Sentido da Vida": 10,
            "Oração": 11,
            "Corresponsabilidade": 12,
            "Vivência do Sacramento do Matrimônio": 13,
            "O casal Cristão no Mundo de Hoje": 14,
        }

        por_ano = defaultdict(list)

        for r in rows:
            item = {
                "palestra": r.get("palestra") or "",
                "nome_ele": (r.get("nome_ele") or "").title(),
                "nome_ela": (r.get("nome_ela") or "").title() if r.get("nome_ela") else ""
            }
            por_ano[r["ano"]].append(item)

        for ano in list(por_ano.keys()):
            por_ano[ano].sort(key=lambda x: titulo_ordem.get(x["palestra"], 9999))

        colunas = ["palestra", "nome_ele", "nome_ela"]

        return render_template(
            "palestrantes.html",
            por_ano=por_ano,
            colunas=colunas
        )

    # ============================================
    # RELATÓRIOS / IMPRESSÕES
    # ============================================
    @app.route("/relatorios")
    def relatorios():
        conn = db_conn()
        cur = conn.cursor(dictionary=True)

        try:
            cur.execute("""
                SELECT DISTINCT ano
                FROM encontreiros
                WHERE ano IS NOT NULL
                  AND ano NOT IN (2020, 2021)
                ORDER BY ano DESC
            """)
            anos = [r["ano"] for r in cur.fetchall()]
        finally:
            try:
                cur.close()
                conn.close()
            except Exception:
                pass

        return render_template("relatorios.html", anos=anos)

    @app.route("/api/trabalhos_por_ano")
    def api_trabalhos_por_ano():
        conn = db_conn()
        cur = conn.cursor(dictionary=True)

        try:
            cur.execute("""
                SELECT
                    ano,
                    COUNT(DISTINCT casal_id) AS qtd
                FROM encontreiros
                WHERE casal_id IS NOT NULL
                  AND (status IS NULL OR UPPER(TRIM(status)) NOT IN ('RECUSOU','DESISTIU'))
                GROUP BY ano
                ORDER BY ano
            """)
            data = cur.fetchall()
        finally:
            try:
                cur.close()
                conn.close()
            except Exception:
                pass

        return jsonify(data)

    @app.route("/api/ano_origem_dos_trabalhadores")
    def api_ano_origem_dos_trabalhadores():
        ano = request.args.get("ano", type=int)

        if not ano:
            return jsonify({"dist": []})

        conn = db_conn()
        cur = conn.cursor(dictionary=True)

        try:
            cur.execute("""
                SELECT
                    i.ano AS ano_encontro,
                    COUNT(DISTINCT e.casal_id) AS qtd
                FROM encontreiros e
                JOIN encontristas i ON i.id = e.casal_id
                WHERE e.ano = %s
                  AND e.casal_id IS NOT NULL
                  AND (e.status IS NULL OR UPPER(TRIM(e.status)) NOT IN ('RECUSOU','DESISTIU'))
                GROUP BY i.ano
                ORDER BY i.ano
            """, (ano,))
            dist = cur.fetchall()
        finally:
            try:
                cur.close()
                conn.close()
            except Exception:
                pass

        return jsonify({"dist": dist})

    @app.route("/api/encontreiros_por_ano")
    def api_encontreiros_por_ano():
        conn = db_conn()
        cur = conn.cursor(dictionary=True)

        try:
            cur.execute("""
                SELECT ano, COUNT(*) AS qtd
                FROM encontreiros
                WHERE (status IS NULL OR UPPER(TRIM(status)) NOT IN ('RECUSOU','DESISTIU'))
                GROUP BY ano
                ORDER BY ano
            """)
            data = cur.fetchall()
        finally:
            try:
                cur.close()
                conn.close()
            except Exception:
                pass

        return jsonify(data)

    @app.route("/docs")
    def docs_index():
        conn = db_conn()
        cur = conn.cursor(dictionary=True)

        try:
            cur.execute("""
                SELECT DISTINCT ano
                FROM encontreiros
                WHERE ano IS NOT NULL
                  AND ano NOT IN (2020, 2021)
                ORDER BY ano DESC
            """)
            anos = [r["ano"] for r in cur.fetchall()]

            cur.execute("""
                SELECT DISTINCT equipe
                FROM encontreiros
                WHERE equipe IS NOT NULL AND equipe <> ''
                ORDER BY equipe
            """)
            equipes = [r["equipe"] for r in cur.fetchall()]

        finally:
            try:
                cur.close()
                conn.close()
            except Exception:
                pass

        return render_template("docs.html", anos=anos, equipes=equipes)

    @app.get("/imprimir/coordenadores")
    def imprimir_coordenadores():
        ano = request.args.get("ano", type=int)

        if not ano:
            return "Informe ?ano=YYYY", 400

        conn = db_conn()
        cur = conn.cursor(dictionary=True)

        rows = _q(cur, """
            SELECT
                e.equipe,
                i.nome_usual_ele AS ele,
                i.nome_usual_ela AS ela,
                CONCAT_WS(' / ',
                    NULLIF(TRIM(i.telefone_ele), ''),
                    NULLIF(TRIM(i.telefone_ela), '')
                ) AS telefones,
                i.endereco AS endereco
            FROM encontreiros e
            JOIN encontristas i ON i.id = e.casal_id
            WHERE e.ano = %s
              AND e.casal_id IS NOT NULL
              AND LOWER(TRIM(COALESCE(e.status,''))) NOT IN ('desistiu','recusou')
              AND LOWER(TRIM(COALESCE(e.coordenador,''))) IN (
                    'sim',
                    's',
                    'coordenador',
                    'coordenadora',
                    'sim coordenador',
                    'sim - coordenador'
              )
            ORDER BY e.equipe, ele, ela
        """, [ano])

        cur.close()
        conn.close()

        return render_template("print_coordenadores.html", ano=ano, rows=rows)

    @app.get("/imprimir/equipes")
    def imprimir_equipes():
        ano = request.args.get("ano", type=int)
        equipe = request.args.get("equipe")

        if not ano:
            return "Informe ?ano=YYYY", 400

        params = [ano]
        where_equipe = ""

        if equipe:
            where_equipe = " AND e.equipe = %s "
            params.append(equipe)

        conn = db_conn()
        cur = conn.cursor(dictionary=True)

        rows = _q(cur, f"""
            SELECT
                e.equipe,
                i.nome_usual_ele AS ele,
                i.nome_usual_ela AS ela,
                CASE
                    WHEN LOWER(TRIM(COALESCE(e.coordenador,''))) IN (
                        'sim',
                        's',
                        'coordenador',
                        'coordenadora',
                        'sim coordenador',
                        'sim - coordenador'
                    )
                    THEN 1 ELSE 0
                END AS is_coord,
                CONCAT_WS(' / ',
                    NULLIF(TRIM(i.telefone_ele), ''),
                    NULLIF(TRIM(i.telefone_ela), '')
                ) AS telefones,
                i.endereco AS endereco
            FROM encontreiros e
            JOIN encontristas i ON i.id = e.casal_id
            WHERE e.ano = %s
              {where_equipe}
              AND e.casal_id IS NOT NULL
              AND LOWER(TRIM(COALESCE(e.status,''))) NOT IN ('desistiu','recusou')
            ORDER BY e.equipe, is_coord DESC, ele, ela
        """, params)

        cur.close()
        conn.close()

        return render_template("print_equipes.html", ano=ano, equipe=equipe, rows=rows)

    @app.get("/imprimir/vigilia")
    def imprimir_vigilia():
        ano = request.args.get("ano", type=int)
        qtd = request.args.get("qtd", default=56, type=int)
        ids = request.args.get("ids")
        seed = request.args.get("seed", default="vigilia")

        if not ano:
            return "Informe ?ano=YYYY", 400

        conn = db_conn()
        cur = conn.cursor(dictionary=True)

        if ids:
            lista_ids = [int(x) for x in ids.split(",") if x.strip().isdigit()]

            if not lista_ids:
                return "Parâmetro ids inválido.", 400

            placeholders = ",".join(["%s"] * len(lista_ids))

            rows = _q(cur, f"""
                SELECT
                    id,
                    nome_usual_ele AS nome_ele,
                    nome_usual_ela AS nome_ela,
                    nome_usual_ele,
                    nome_usual_ela,
                    endereco,
                    telefone_ele,
                    telefone_ela
                FROM encontristas
                WHERE id IN ({placeholders})
                ORDER BY FIELD(id, {placeholders})
            """, lista_ids + lista_ids)

        else:
            rows = _q(cur, """
                SELECT
                    i.id,
                    i.nome_usual_ele AS nome_ele,
                    i.nome_usual_ela AS nome_ela,
                    i.nome_usual_ele,
                    i.nome_usual_ela,
                    i.endereco,
                    i.telefone_ele,
                    i.telefone_ela
                FROM encontristas i
                LEFT JOIN encontreiros e
                  ON e.casal_id = i.id
                 AND e.ano = %s
                 AND LOWER(TRIM(COALESCE(e.status,''))) NOT IN ('desistiu','recusou')
                WHERE e.id IS NULL
                ORDER BY MD5(CONCAT_WS('#', i.id, %s))
                LIMIT %s
            """, [ano, seed, qtd])

        cur.close()
        conn.close()

        return render_template(
            "print_vigilia.html",
            ano=ano,
            qtd=qtd,
            seed=seed,
            rows=rows
        )

    @app.route('/api/team-limits')
    def api_team_limits():
        return jsonify({"ok": True, "limits": TEAM_LIMITS})