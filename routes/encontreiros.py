from flask import render_template, request, redirect, url_for
from collections import defaultdict
import re
import mysql.connector
from mysql.connector import errors as mysql_errors

from db import db_conn


def register_encontreiros_routes(
    app,
    PALESTRAS_TITULOS,
    PALESTRAS_SOLO,
    DB_CONFIG,
    safe_fetch_one
):
    # =========================
    # ENCONTREIROS (listagem – sem edição)
    # =========================
    @app.route('/encontreiros')
    def encontreiros():
        conn = db_conn()
        cursor = conn.cursor(dictionary=True)

        nome_ele = (request.args.get('nome_ele', '') or '').strip()
        nome_ela = (request.args.get('nome_ela', '') or '').strip()
        ano_filtro = (request.args.get('ano', '') or '').strip()

        query = """
            SELECT id, ano, equipe, nome_ele, nome_ela, telefones, endereco, coordenador
              FROM encontreiros
             WHERE (status IS NULL OR UPPER(TRIM(status)) IN ('ABERTO','CONCLUIDO'))
        """
        params = []

        if nome_ele:
            query += " AND nome_ele LIKE %s"
            params.append(f"%{nome_ele}%")
        if nome_ela:
            query += " AND nome_ela LIKE %s"
            params.append(f"%{nome_ela}%")
        if ano_filtro:
            query += " AND ano = %s"
            params.append(ano_filtro)

        query += " ORDER BY ano DESC, equipe ASC, id ASC"

        cursor.execute(query, params)
        todos = cursor.fetchall()
        cursor.close()
        conn.close()

        por_ano = defaultdict(list)
        for row in todos:
            por_ano[row['ano']].append(row)

        colunas_visiveis = ['equipe', 'nome_ele', 'nome_ela', 'telefones', 'endereco', 'coordenador']

        return render_template(
            'encontreiros.html',
            por_ano=por_ano,
            colunas_visiveis=colunas_visiveis
        )

    # =========================
    # VISÃO DE EQUIPES
    # =========================
    @app.route('/visao-equipes')
    def visao_equipes():
        equipe = request.args.get('equipe', '')
        target = request.args.get('target', '')
        ano_montagem = request.args.get('ano_montagem', '')
        tabela = {}
        colunas = []

        if equipe:
            conn = db_conn()
            cursor = conn.cursor(dictionary=True)

            if equipe == 'Dirigentes':
                colunas = ['Montagem', 'Fichas', 'Palestra', 'Finanças', 'Pós Encontro']
                dados = {col: {} for col in colunas}

                for pasta in colunas:
                    cursor.execute(
                        "SELECT * FROM encontreiros WHERE equipe LIKE '%DIRIGENTE%' AND equipe LIKE %s",
                        (f"%{pasta.upper()}%",)
                    )
                    for row in cursor.fetchall():
                        ano = row['ano']
                        nome = f"*{row['nome_ele']} e {row['nome_ela']}" if (row.get('coordenador') or '').strip().lower() == 'sim' else f"{row['nome_ele']} e {row['nome_ela']}"
                        dados[pasta].setdefault(ano, nome)

                anos = sorted({ano for pasta_data in dados.values() for ano in pasta_data})
                for a in anos:
                    linha = [dados[col].get(a, '') for col in colunas]
                    tabela[a] = linha

            else:
                cursor.execute("SELECT * FROM encontreiros WHERE equipe LIKE %s", (f"%{equipe}%",))
                all_rows = cursor.fetchall()

                coordenadores_globais = set(
                    f"{row['nome_ele']} e {row['nome_ela']}" for row in all_rows if (row.get('coordenador') or '').strip().lower() == 'sim'
                )

                if equipe == 'Sala':
                    colunas = ['Coordenador', 'Boa Vontade', 'Canto 1', 'Canto 2', 'Som e Projeção 1', 'Som e Projeção 2', 'Recepção de Palestras']
                    dados_ano = defaultdict(lambda: {col: '' for col in colunas})

                    for row in all_rows:
                        a = row['ano']
                        equipe_txt = (row['equipe'] or '').upper()
                        nome_chave = f"{row['nome_ele']} e {row['nome_ela']}"
                        coordenador = (row.get('coordenador') or '').strip().lower() == 'sim'

                        if coordenador and all(x not in equipe_txt for x in ['BOA VONTADE', 'CANTO', 'SOM E PROJEÇÃO', 'RECEPÇÃO']):
                            dados_ano[a]['Coordenador'] = f"*{nome_chave}"
                        elif 'BOA VONTADE' in equipe_txt:
                            dados_ano[a]['Boa Vontade'] = nome_chave
                        elif 'CANTO' in equipe_txt:
                            if not dados_ano[a]['Canto 1']:
                                dados_ano[a]['Canto 1'] = nome_chave
                            elif not dados_ano[a]['Canto 2']:
                                dados_ano[a]['Canto 2'] = nome_chave
                        elif 'SOM E PROJEÇÃO' in equipe_txt:
                            if not dados_ano[a]['Som e Projeção 1']:
                                dados_ano[a]['Som e Projeção 1'] = nome_chave
                            elif not dados_ano[a]['Som e Projeção 2']:
                                dados_ano[a]['Som e Projeção 2'] = nome_chave
                        elif 'RECEPÇÃO' in equipe_txt:
                            dados_ano[a]['Recepção de Palestras'] = nome_chave

                    for a, linha_dict in dados_ano.items():
                        linha = []
                        for col in colunas:
                            nome = linha_dict[col]
                            if nome.startswith("*"):
                                linha.append(nome)
                            elif nome in coordenadores_globais:
                                linha.append(f"~{nome}")
                            else:
                                linha.append(nome)
                        tabela[a] = linha

                else:
                    colunas = ['Coordenador'] + [f'Integrante {i}' for i in range(1, 10)]
                    por_ano = defaultdict(list)

                    for row in all_rows:
                        a = row['ano']
                        nome_chave = f"{row['nome_ele']} e {row['nome_ela']}"
                        if (row.get('coordenador') or '').strip().lower() == 'sim':
                            por_ano[a].insert(0, f"*{nome_chave}")
                        else:
                            por_ano[a].append(nome_chave)

                    for a, nomes in por_ano.items():
                        linha = []
                        for nome in nomes:
                            if nome.startswith("*"):
                                linha.append(nome)
                            elif nome in coordenadores_globais:
                                linha.append(f"~{nome}")
                            else:
                                linha.append(nome)
                        while len(linha) < len(colunas):
                            linha.append('')
                        tabela[a] = linha

            cursor.close()
            conn.close()

        return render_template(
            'visao_equipes.html',
            equipe_selecionada=equipe,
            tabela=tabela,
            colunas=colunas,
            target=target,
            ano_montagem=ano_montagem
        )

    @app.route('/visao-equipes/select')
    def visao_equipes_select():
        # Aceita 'ano_montagem' OU 'ano'
        ano_montagem = request.args.get('ano_montagem', type=int) or request.args.get('ano', type=int)

        # Aceita 'target' OU 'ret_target'
        target = (request.args.get('target') or request.args.get('ret_target') or '').strip()

        # Aceita múltiplos nomes para os selecionados
        ele = (request.args.get('ele')
               or request.args.get('selecionar_ele')
               or request.args.get('nome_ele') or '').strip()
        ela = (request.args.get('ela')
               or request.args.get('selecionar_ela')
               or request.args.get('nome_ela') or '').strip()

        # Se algo essencial faltar, volta para a visão de equipes (com o que tiver)
        if not (ano_montagem and target and ele and ela):
            return redirect(url_for('visao_equipes', target=target, ano_montagem=ano_montagem))

        # Redireciona de volta pra Nova Montagem com os nomes selecionados
        return redirect(url_for(
            'nova_montagem',
            ano=ano_montagem,
            target=target,
            selecionar_ele=ele,
            selecionar_ela=ela
        ))

    # =========================
    # Visão do Casal (ATUALIZADA: inclui palestras do casal)
    # =========================
    @app.route('/visao-casal')
    def visao_casal():
        nome_ele = (request.args.get("nome_ele") or "").strip()
        nome_ela = (request.args.get("nome_ela") or "").strip()

        dados_encontrista = {}
        dados_encontreiros = []
        dados_palestras = []
        erro = None

        if not nome_ele or not nome_ela:
            erro = "Informe ambos os nomes para realizar a busca."
            return render_template(
                "visao_casal.html",
                nome_ele=nome_ele, nome_ela=nome_ela,
                dados_encontrista=None, dados_encontreiros=[],
                dados_palestras=[],
                erro=erro
            )

        conn = db_conn()
        cursor = conn.cursor(dictionary=True)

        try:
            # ENCONTRISTA (nomes usuais)
            cursor.execute("""
                SELECT ano, endereco, telefone_ele, telefone_ela
                  FROM encontristas
                 WHERE nome_usual_ele = %s AND nome_usual_ela = %s
                 ORDER BY ano DESC
                 LIMIT 1
            """, (nome_ele, nome_ela))
            e = cursor.fetchone()
            if e:
                dados_encontrista = {
                    "ano_encontro": e["ano"],
                    "endereco": e.get("endereco") or "",
                    "telefones": f"{e.get('telefone_ele') or ''} / {e.get('telefone_ela') or ''}".strip(" /")
                }

            # ENCONTREIROS (histórico de trabalho)
            cursor.execute("""
                SELECT ano, equipe, coordenador, endereco, telefones, status, observacao
                  FROM encontreiros
                 WHERE nome_ele = %s AND nome_ela = %s
                 ORDER BY ano DESC, equipe ASC
            """, (nome_ele, nome_ela))
            dados_encontreiros = cursor.fetchall() or []

            # PALESTRAS do CASAL (exclui títulos solo – pois não são “casal”)
            # Considera equivalência por nome exato (mesmo padrão de cadastro).
            format_titles = tuple(t for t in PALESTRAS_TITULOS if t not in PALESTRAS_SOLO)
            if format_titles:
                in_clause = ", ".join(["%s"] * len(format_titles))
                sql = f"""
                    SELECT ano, palestra
                      FROM palestras
                     WHERE LOWER(nome_ele) = LOWER(%s)
                       AND LOWER(COALESCE(nome_ela,'')) = LOWER(%s)
                       AND palestra IN ({in_clause})
                     ORDER BY ano DESC
                """
                params = [nome_ele, nome_ela] + list(format_titles)
                cursor.execute(sql, params)
                dados_palestras = cursor.fetchall() or []

            # Se nada encontrado em nenhuma das duas tabelas, sinaliza
            if not e and not dados_encontreiros and not dados_palestras:
                erro = "Casal não encontrado."

        finally:
            cursor.close()
            conn.close()

        # --------- Monta estruturas para o template novo (anos / por_ano_*) ----------
        por_ano_trabalhos = defaultdict(list)
        for r in (dados_encontreiros or []):
            por_ano_trabalhos[r["ano"]].append({
                "equipe": r.get("equipe") or "",
                "coordenador": r.get("coordenador") or "",
                "status": r.get("status") or "",
                "observacao": r.get("observacao") or ""
            })

        por_ano_palestras = defaultdict(list)
        for p in (dados_palestras or []):
            por_ano_palestras[p["ano"]].append({"palestra": p.get("palestra") or ""})

        anos_set = set(por_ano_trabalhos.keys()) | set(por_ano_palestras.keys())
        anos = sorted(anos_set, reverse=True)

        return render_template(
            "visao_casal.html",
            nome_ele=nome_ele,
            nome_ela=nome_ela,
            dados_encontrista=dados_encontrista if dados_encontrista else None,
            dados_encontreiros=dados_encontreiros,
            dados_palestras=dados_palestras,
            anos=anos,
            por_ano_trabalhos=por_ano_trabalhos,
            por_ano_palestras=por_ano_palestras,
            erro=erro
        )

    # =========================
    # Relatório de Casais (mantido)
    # =========================
    @app.route('/relatorio-casais', methods=['GET', 'POST'])
    def relatorio_casais():
        def split_casal(line: str):
            raw = (line or '').strip()
            if not raw:
                return None, None
            if ";" in raw:
                a, b = raw.split(";", 1)
                return a.strip(), b.strip()
            if re.search(r"\s+e\s+", raw, flags=re.I):
                a, b = re.split(r"\s+e\s+", raw, maxsplit=1, flags=re.I)
                return a.strip(), b.strip()
            if " " in raw:
                a, b = raw.split(" ", 1)
                return a.strip(), b.strip()
            return None, None

        resultados_ok, resultados_fail = [], []
        titulo = (request.form.get("titulo") or "Relatório de Casais") if request.method == 'POST' else "Relatório de Casais"
        entrada = (request.form.get("lista_nomes", "") or "") if request.method == 'POST' else ""

        if request.method == 'POST' and entrada.strip():
            linhas = [l.strip() for l in entrada.splitlines() if l.strip()]

            try:
                conn = mysql.connector.connect(
                    host=DB_CONFIG['host'],
                    user=DB_CONFIG['user'],
                    password=DB_CONFIG['password'],
                    database=DB_CONFIG['database'],
                    connection_timeout=5
                )
                cur = conn.cursor(dictionary=True)
            except mysql_errors.Error:
                for linha in linhas:
                    resultados_fail.append({"nome": linha, "endereco": "Erro de conexão com o banco", "telefones": "— / —"})
                return render_template("relatorio_casais.html", resultados=resultados_fail, titulo=titulo, entrada=entrada)

            try:
                def consulta_prefix_like(a: str, b: str):
                    a = (a or "").strip()
                    b = (b or "").strip()
                    if not a or not b:
                        return None
                    a_pref, b_pref = f"{a}%", f"{b}%"

                    work = safe_fetch_one(
                        cur,
                        "SELECT endereco, telefones "
                        "FROM encontreiros "
                        "WHERE (nome_ele LIKE %s AND nome_ela LIKE %s) "
                        "   OR (nome_ele LIKE %s AND nome_ela LIKE %s) "
                        "ORDER BY ano DESC LIMIT 1",
                        (a_pref, b_pref, b_pref, a_pref)
                    )
                    if work is None:
                        work = safe_fetch_one(
                            cur,
                            "SELECT endereco, telefones "
                            "FROM encontreiros "
                            "WHERE (nome_usual_ele LIKE %s AND nome_usual_ela LIKE %s) "
                            "   OR (nome_usual_ele LIKE %s AND nome_usual_ela LIKE %s) "
                            "ORDER BY ano DESC LIMIT 1",
                            (a_pref, b_pref, b_pref, a_pref)
                        )

                    base = safe_fetch_one(
                        cur,
                        "SELECT endereco, telefone_ele, telefone_ela "
                        "FROM encontristas "
                        "WHERE (nome_usual_ele LIKE %s AND nome_usual_ela LIKE %s) "
                        "   OR (nome_usual_ele LIKE %s AND nome_usual_ela LIKE %s) "
                        "   OR (nome_ele LIKE %s AND nome_ela LIKE %s) "
                        "   OR (nome_ele LIKE %s AND nome_ela LIKE %s) "
                        "LIMIT 1",
                        (a_pref, b_pref, b_pref, a_pref, a_pref, b_pref, b_pref, a_pref)
                    )
                    if base is None:
                        base = safe_fetch_one(
                            cur,
                            "SELECT endereco, telefone_ele, telefone_ela "
                            "FROM encontristas "
                            "WHERE (nome_ele LIKE %s AND nome_ela LIKE %s) "
                            "   OR (nome_ele LIKE %s AND nome_ela LIKE %s) "
                            "LIMIT 1",
                            (a_pref, b_pref, b_pref, a_pref)
                        )

                    if work or base:
                        if work:
                            endereco = (work.get('endereco') if work else None) or (base.get('endereco') if base else "")
                            telefones = work.get('telefones')
                            if not telefones:
                                if base:
                                    telefones = f"{(base.get('telefone_ele') or '—')} / {(base.get('telefone_ela') or '—')}"
                                else:
                                    telefones = "— / —"
                        else:
                            endereco = base.get('endereco') or "—"
                            telefones = f"{(base.get('telefone_ele') or '—')} / {(base.get('telefone_ela') or '—')}"
                        return {"endereco": endereco or "—", "telefones": telefones or "— / —"}

                    return None

                for linha in linhas:
                    ele, ela = split_casal(linha)
                    if not ele or not ela:
                        resultados_fail.append({"nome": linha, "endereco": "Formato não reconhecido", "telefones": "— / —"})
                        continue

                    dados = consulta_prefix_like(ele, ela)
                    if dados:
                        resultados_ok.append({"nome": f"{ele} e {ela}", **dados})
                    else:
                        resultados_fail.append({"nome": f"{ele} e {ela}", "endereco": "Não encontrado", "telefones": "— / —"})
            finally:
                try:
                    cur.close()
                    conn.close()
                except Exception:
                    pass

        resultados = resultados_ok + resultados_fail
        return render_template("relatorio_casais.html", resultados=resultados, titulo=titulo, entrada=entrada)
