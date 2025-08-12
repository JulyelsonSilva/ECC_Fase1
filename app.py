from flask import Flask, render_template, request, jsonify, redirect, url_for
import mysql.connector
from collections import defaultdict
import math
import re

app = Flask(__name__)

DB_CONFIG = {
    'host': 'db4free.net',
    'user': 'eccdivino2',
    'password': 'eccdivino2025',
    'database': 'eccdivinomcz2'
}

# -----------------------------
# Rotas principais
# -----------------------------
@app.route('/')
def index():
    return render_template('index.html')

@app.route('/encontristas')
def encontristas():
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor(dictionary=True)

    nome_ele = request.args.get('nome_usual_ele', '')
    nome_ela = request.args.get('nome_usual_ela', '')
    ano = request.args.get('ano', '')
    pagina = int(request.args.get('pagina', 1))
    por_pagina = 50

    query = "SELECT * FROM encontristas WHERE 1=1"
    params = []
    if nome_ele:
        query += " AND nome_usual_ele LIKE %s"
        params.append(f"%{nome_ele}%")
    if nome_ela:
        query += " AND nome_usual_ela LIKE %s"
        params.append(f"%{nome_ela}%")
    if ano:
        query += " AND ano = %s"
        params.append(ano)

    cursor.execute(query, params)
    todos = cursor.fetchall()
    total_paginas = max(1, math.ceil(len(todos) / por_pagina))
    dados = todos[(pagina-1)*por_pagina : pagina*por_pagina]

    # flags opcionais para feedback no template
    updated = request.args.get('updated')
    notfound = request.args.get('notfound')

    cursor.close()
    conn.close()

    return render_template(
        'encontristas.html',
        dados=dados,
        pagina=pagina,
        total_paginas=total_paginas,
        updated=updated,
        notfound=notfound
    )

# -----------------------------
# Edição de Encontrista (GET/POST)
# -----------------------------
@app.route('/encontristas/<int:encontrista_id>/editar', methods=['GET', 'POST'])
def editar_encontrista(encontrista_id):
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor(dictionary=True)

    if request.method == 'POST':
        # Campos conforme estrutura informada
        nome_completo_ele = request.form.get('nome_completo_ele', '').strip()
        nome_completo_ela = request.form.get('nome_completo_ela', '').strip()
        nome_usual_ele = request.form.get('nome_usual_ele', '').strip()
        nome_usual_ela = request.form.get('nome_usual_ela', '').strip()
        telefone_ele = request.form.get('telefone_ele', '').strip()
        telefone_ela = request.form.get('telefone_ela', '').strip()
        endereco = request.form.get('endereco', '').strip()
        ecc_num = request.form.get('ecc_num', '').strip()
        ano_raw = request.form.get('ano', '').strip()
        anos_casados = request.form.get('anos_casados', '').strip()
        cor_circulo = request.form.get('cor_circulo', '').strip()
        casal_visitacao = request.form.get('casal_visitacao', '').strip()
        ficha_num = request.form.get('ficha_num', '').strip()
        aceitou = request.form.get('aceitou', '').strip()
        observacao = request.form.get('observacao', '').strip()
        observacao_extra = request.form.get('observacao_extra', '').strip()

        try:
            ano = int(ano_raw) if ano_raw else None
        except ValueError:
            ano = None

        sql = """
            UPDATE encontristas SET
                nome_completo_ele = %s,
                nome_completo_ela = %s,
                nome_usual_ele = %s,
                nome_usual_ela = %s,
                telefone_ele = %s,
                telefone_ela = %s,
                endereco = %s,
                ecc_num = %s,
                ano = %s,
                anos_casados = %s,
                cor_circulo = %s,
                casal_visitacao = %s,
                ficha_num = %s,
                aceitou = %s,
                observacao = %s,
                observacao_extra = %s
            WHERE id = %s
        """
        cursor.execute(sql, (
            nome_completo_ele, nome_completo_ela, nome_usual_ele, nome_usual_ela,
            telefone_ele, telefone_ela, endereco, ecc_num, ano, anos_casados,
            cor_circulo, casal_visitacao, ficha_num, aceitou, observacao,
            observacao_extra, encontrista_id
        ))
        conn.commit()
        cursor.close()
        conn.close()
        # volta para listagem com flag de sucesso
        return redirect(url_for('encontristas') + '?updated=1')

    # GET: carrega registro para o formulário
    cursor.execute("SELECT * FROM encontristas WHERE id = %s", (encontrista_id,))
    registro = cursor.fetchone()
    cursor.close()
    conn.close()

    if not registro:
        return redirect(url_for('encontristas') + '?notfound=1')

    return render_template('editar_encontrista.html', r=registro)

@app.route('/encontreiros')
def encontreiros():
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor(dictionary=True)

    nome_ele = request.args.get('nome_ele', '')
    nome_ela = request.args.get('nome_ela', '')

    query = "SELECT * FROM encontreiros WHERE 1=1"
    params = []
    if nome_ele:
        query += " AND nome_ele LIKE %s"
        params.append(f"%{nome_ele}%")
    if nome_ela:
        query += " AND nome_ela LIKE %s"
        params.append(f"%{nome_ela}%")

    query += " ORDER BY ano DESC"
    cursor.execute(query, params)
    todos = cursor.fetchall()
    cursor.close()
    conn.close()

    por_ano = defaultdict(list)
    for row in todos:
        por_ano[row['ano']].append(row)

    return render_template('encontreiros.html', por_ano=por_ano)

@app.route('/visao-equipes')
def visao_equipes():
    equipe = request.args.get('equipe', '')
    tabela = {}
    colunas = []

    if equipe:
        conn = mysql.connector.connect(**DB_CONFIG)
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
            for ano in anos:
                linha = [dados[col].get(ano, '') for col in colunas]
                tabela[ano] = linha

        else:
            cursor.execute("SELECT * FROM encontreiros WHERE equipe LIKE %s", (f"%{equipe}%",))
            all_rows = cursor.fetchall()

            # Coordenadores históricos da equipe
            coordenadores_globais = set(
                f"{row['nome_ele']} e {row['nome_ela']}" for row in all_rows if (row.get('coordenador') or '').strip().lower() == 'sim'
            )

            if equipe == 'Sala':
                colunas = ['Coordenador', 'Boa Vontade', 'Canto 1', 'Canto 2', 'Som e Projeção 1', 'Som e Projeção 2', 'Recepção de Palestras']
                dados_ano = defaultdict(lambda: {col: '' for col in colunas})

                for row in all_rows:
                    ano = row['ano']
                    equipe_txt = (row['equipe'] or '').upper()
                    nome_chave = f"{row['nome_ele']} e {row['nome_ela']}"
                    coordenador = (row.get('coordenador') or '').strip().lower() == 'sim'

                    if coordenador and all(x not in equipe_txt for x in ['BOA VONTADE', 'CANTO', 'SOM E PROJEÇÃO', 'RECEPÇÃO']):
                        dados_ano[ano]['Coordenador'] = f"*{nome_chave}"
                    elif 'BOA VONTADE' in equipe_txt:
                        dados_ano[ano]['Boa Vontade'] = nome_chave
                    elif 'CANTO' in equipe_txt:
                        if not dados_ano[ano]['Canto 1']:
                            dados_ano[ano]['Canto 1'] = nome_chave
                        elif not dados_ano[ano]['Canto 2']:
                            dados_ano[ano]['Canto 2'] = nome_chave
                    elif 'SOM E PROJEÇÃO' in equipe_txt:
                        if not dados_ano[ano]['Som e Projeção 1']:
                            dados_ano[ano]['Som e Projeção 1'] = nome_chave
                        elif not dados_ano[ano]['Som e Projeção 2']:
                            dados_ano[ano]['Som e Projeção 2'] = nome_chave
                    elif 'RECEPÇÃO' in equipe_txt:
                        dados_ano[ano]['Recepção de Palestras'] = nome_chave

                for ano, linha_dict in dados_ano.items():
                    linha = []
                    for col in colunas:
                        nome = linha_dict[col]
                        if nome.startswith("*"):
                            linha.append(nome)
                        elif nome in coordenadores_globais:
                            linha.append(f"~{nome}")
                        else:
                            linha.append(nome)
                    tabela[ano] = linha

            else:
                colunas = ['Coordenador'] + [f'Integrante {i}' for i in range(1, 10)]
                por_ano = defaultdict(list)

                for row in all_rows:
                    ano = row['ano']
                    nome_chave = f"{row['nome_ele']} e {row['nome_ela']}"
                    if (row.get('coordenador') or '').strip().lower() == 'sim':
                        por_ano[ano].insert(0, f"*{nome_chave}")
                    else:
                        por_ano[ano].append(nome_chave)

                for ano, nomes in por_ano.items():
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
                    tabela[ano] = linha

        cursor.close()
        conn.close()

    return render_template('visao_equipes.html', equipe_selecionada=equipe, tabela=tabela, colunas=colunas)

# -----------------------------
# ROTA RECUPERADA: /visao-casal
# -----------------------------
@app.route('/visao-casal')
def visao_casal():
    nome_ele = request.args.get("nome_ele", "").strip()
    nome_ela = request.args.get("nome_ela", "").strip()

    dados_encontrista = {}
    dados_encontreiros = []
    erro = None

    # Só continua se ambos os nomes forem informados
    if not nome_ele or not nome_ela:
        erro = "Informe ambos os nomes para realizar a busca."
        return render_template("visao_casal.html",
                               nome_ele=nome_ele,
                               nome_ela=nome_ela,
                               dados_encontrista=None,
                               dados_encontreiros=[],
                               erro=erro)

    # Conectar ao banco de dados
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor(dictionary=True)

    try:
        # ENCONTRISTAS
        cursor.execute("""
            SELECT ano, endereco, telefone_ele, telefone_ela
            FROM encontristas 
            WHERE nome_usual_ele = %s AND nome_usual_ela = %s
        """, (nome_ele, nome_ela))
        resultado_encontrista = cursor.fetchone()

        while cursor.nextset():  # Evita erro "Unread result found"
            pass

        if resultado_encontrista:
            dados_encontrista = {
                "ano_encontro": resultado_encontrista["ano"],
                "endereco": resultado_encontrista["endereco"],
                "telefones": f"{resultado_encontrista['telefone_ele']} / {resultado_encontrista['telefone_ela']}"
            }

        # ENCONTREIROS
        cursor.execute("""
            SELECT ano, equipe, coordenador, endereco, telefones
            FROM encontreiros 
            WHERE nome_ele = %s AND nome_ela = %s
        """, (nome_ele, nome_ela))
        resultados_encontreiros = cursor.fetchall()

        if resultados_encontreiros:
            dados_encontreiros = [{
                "ano": r["ano"],
                "equipe": r["equipe"],
                "coordenador": r["coordenador"]
            } for r in resultados_encontreiros]

            if "ano_encontro" not in dados_encontrista:
                dados_encontrista["ano_encontro"] = "-"

            # Pega endereço e telefone do ano mais recente
            mais_recente = max(resultados_encontreiros, key=lambda x: x["ano"])
            dados_encontrista["endereco"] = mais_recente["endereco"]
            dados_encontrista["telefones"] = mais_recente["telefones"]

        if not resultado_encontrista and not resultados_encontreiros:
            erro = "Casal não encontrado."

    finally:
        cursor.close()
        conn.close()

    return render_template("visao_casal.html",
                           nome_ele=nome_ele,
                           nome_ela=nome_ela,
                           dados_encontrista=dados_encontrista,
                           dados_encontreiros=dados_encontreiros,
                           erro=erro)

# -----------------------------
# Organograma
# -----------------------------
@app.route('/organograma')
def organograma():
    return render_template('organograma.html')

@app.route('/dados-organograma')
def dados_organograma():
    ano = request.args.get("ano")
    if not ano:
        return jsonify([])

    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT equipe, nome_ele, nome_ela, coordenador FROM encontreiros WHERE ano = %s", (ano,))
    dados = cursor.fetchall()
    cursor.close()
    conn.close()
    return jsonify(dados)

# -----------------------------
# Relatório de Casais (LIKE + inversão + não encontrados por último)
# -----------------------------
@app.route('/relatorio-casais', methods=['GET', 'POST'])
def relatorio_casais():
    def split_casal(line: str):
        """Divide em (ele, ela) aceitando ';' | ' e ' | 1º espaço."""
        raw = (line or '').strip()
        if not raw:
            return None, None
        if ";" in raw:
            a, b = raw.split(";", 1); return a.strip(), b.strip()
        if re.search(r"\s+e\s+", raw, flags=re.I):
            a, b = re.split(r"\s+e\s+", raw, maxsplit=1, flags=re.I); return a.strip(), b.strip()
        if " " in raw:
            a, b = raw.split(" ", 1); return a.strip(), b.strip()
        return None, None

    def get_table_columns(conn, table_name: str) -> set:
        cur = conn.cursor()
        try:
            cur.execute("""
                SELECT COLUMN_NAME
                FROM information_schema.columns
                WHERE table_schema=%s AND table_name=%s
            """, (DB_CONFIG['database'], table_name))
            return {row[0] for row in cur.fetchall()}
        finally:
            cur.close()

    def escolher_par(colunas: set, prefer_usual=True):
        pares = [
            ('nome_usual_ele', 'nome_usual_ela'),
            ('nome_ele', 'nome_ela'),
        ] if prefer_usual else [
            ('nome_ele', 'nome_ela'),
            ('nome_usual_ele', 'nome_usual_ela'),
        ]
        for a, b in pares:
            if a in colunas and b in colunas:
                return a, b
        return None, None

    resultados_ok = []
    resultados_fail = []

    if request.method == 'POST':
        nomes_input = (request.form.get("lista_nomes", "") or "").strip()
        if nomes_input:
            linhas = [l.strip() for l in nomes_input.splitlines() if l.strip()]

            conn = mysql.connector.connect(
                host=DB_CONFIG['host'],
                user=DB_CONFIG['user'],
                password=DB_CONFIG['password'],
                database=DB_CONFIG['database'],
                connection_timeout=10,
            )
            try:
                cols_work = get_table_columns(conn, 'encontreiros')
                cols_base = get_table_columns(conn, 'encontristas')
                work_a, work_b = escolher_par(cols_work, prefer_usual=False)
                base_a, base_b = escolher_par(cols_base, prefer_usual=True)
                cur = conn.cursor(dictionary=True)

                def consulta_like(a: str, b: str):
                    """Consulta com LIKE nas duas tabelas; consolida preferindo ENCONTREIROS mais recente."""
                    work = None
                    if work_a and work_b:
                        cur.execute(
                            f"SELECT * FROM encontreiros WHERE {work_a} LIKE %s AND {work_b} LIKE %s ORDER BY ano DESC LIMIT 1",
                            (f"%{a}%", f"%{b}%")
                        )
                        work = cur.fetchone()

                    base = None
                    if base_a and base_b:
                        where_parts = [f"({base_a} LIKE %s AND {base_b} LIKE %s)"]
                        params = [f"%{a}%", f"%{b}%"]
                        # Só adiciona OR se as colunas existirem e forem diferentes
                        if 'nome_ele' in cols_base and 'nome_ela' in cols_base and (base_a, base_b) != ('nome_ele', 'nome_ela'):
                            where_parts.append("(nome_ele LIKE %s AND nome_ela LIKE %s)")
                            params += [f"%{a}%", f"%{b}%"]
                        cur.execute(
                            "SELECT endereco, telefone_ele, telefone_ela FROM encontristas WHERE "
                            + " OR ".join(where_parts) + " LIMIT 1",
                            tuple(params)
                        )
                        base = cur.fetchone()

                    if work:
                        endereco = work.get('endereco') or (base.get('endereco') if base else "")
                        if 'telefones' in work and work.get('telefones'):
                            telefones = work['telefones']
                        else:
                            tel_ele = work.get('telefone_ele')
                            tel_ela = work.get('telefone_ela')
                            if tel_ele or tel_ela:
                                telefones = f"{tel_ele or '—'} / {tel_ela or '—'}"
                            elif base:
                                telefones = f"{(base.get('telefone_ele') or '—')} / {(base.get('telefone_ela') or '—')}"
                            else:
                                telefones = "— / —"
                        return {"endereco": (endereco or "—"), "telefones": (telefones or "— / —")}

                    if base:
                        telefones = f"{(base.get('telefone_ele') or '—')} / {(base.get('telefone_ela') or '—')}"
                        return {"endereco": (base.get('endereco') or '—'), "telefones": telefones}

                    return None

                for linha in linhas:
                    try:
                        ele, ela = split_casal(linha)
                        if not ele or not ela:
                            resultados_fail.append({"nome": linha, "endereco": "Formato não reconhecido", "telefones": "— / —"})
                            continue

                        dados = consulta_like(ele, ela) or consulta_like(ela, ele)
                        if dados:
                            resultados_ok.append({"nome": f"{ele} e {ela}", "endereco": dados["endereco"], "telefones": dados["telefones"]})
                        else:
                            resultados_fail.append({"nome": f"{ele} e {ela}", "endereco": "Não encontrado", "telefones": "— / —"})
                    except Exception as e:
                        app.logger.exception(f"Falha ao processar linha: {linha}")
                        resultados_fail.append({"nome": linha, "endereco": "Erro ao processar", "telefones": str(e)})

                cur.close()
            finally:
                try:
                    conn.close()
                except Exception:
                    pass

    # Junta: encontrados primeiro, não-encontrados por último
    resultados = resultados_ok + resultados_fail
    return render_template("relatorio_casais.html", resultados=resultados)

# -----------------------------
# Main
# -----------------------------
if __name__ == "__main__":
    app.run(debug=True)

