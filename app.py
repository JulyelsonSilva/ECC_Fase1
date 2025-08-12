from flask import Flask, render_template, request, jsonify
import mysql.connector
from collections import defaultdict
import math

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
    total_paginas = math.ceil(len(todos) / por_pagina)
    dados = todos[(pagina-1)*por_pagina : pagina*por_pagina]
    conn.close()

    return render_template('encontristas.html', dados=dados, pagina=pagina, total_paginas=total_paginas)

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
                    nome = f"*{row['nome_ele']} e {row['nome_ela']}" if row['coordenador'].strip().lower() == 'sim' else f"{row['nome_ele']} e {row['nome_ela']}"
                    dados[pasta].setdefault(ano, nome)

            anos = sorted({ano for pasta_data in dados.values() for ano in pasta_data})
            for ano in anos:
                linha = [dados[col].get(ano, '') for col in colunas]
                tabela[ano] = linha

        else:
            cursor.execute("SELECT * FROM encontreiros WHERE equipe LIKE %s", (f"%{equipe}%",))
            all_rows = cursor.fetchall()

            coordenadores_globais = set(
                f"{row['nome_ele']} e {row['nome_ela']}" for row in all_rows if row['coordenador'].strip().lower() == 'sim'
            )

            if equipe == 'Sala':
                colunas = ['Coordenador', 'Boa Vontade', 'Canto 1', 'Canto 2', 'Som e Projeção 1', 'Som e Projeção 2', 'Recepção de Palestras']
                dados_ano = defaultdict(lambda: {col: '' for col in colunas})

                for row in all_rows:
                    ano = row['ano']
                    equipe_txt = row['equipe'].upper()
                    nome_chave = f"{row['nome_ele']} e {row['nome_ela']}"
                    coordenador = row['coordenador'].strip().lower() == 'sim'

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
                coordenadores_ano = {}

                for row in all_rows:
                    ano = row['ano']
                    nome_chave = f"{row['nome_ele']} e {row['nome_ela']}"
                    if row['coordenador'].strip().lower() == 'sim':
                        coordenadores_ano[ano] = nome_chave
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
        # 🔍 Buscar na tabela ENCONTRISTAS
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

        # 🔍 Buscar na tabela ENCONTREIROS
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

            # Se não encontrou na tabela encontristas, adiciona valor padrão
            if "ano_encontro" not in dados_encontrista:
                dados_encontrista["ano_encontro"] = "-"

            # Pega endereço e telefone do ano mais recente
            mais_recente = max(resultados_encontreiros, key=lambda x: x["ano"])
            dados_encontrista["endereco"] = mais_recente["endereco"]
            dados_encontrista["telefones"] = mais_recente["telefones"]

        # Se nenhum dado encontrado em nenhuma tabela
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
    conn.close()
    return jsonify(dados)

# -----------------------------
# Relatório de Casais (impressão nativa do navegador)
# -----------------------------
@app.route('/relatorio-casais', methods=['GET', 'POST'])
def relatorio_casais():
    def split_casal(line: str):
        """Divide a linha em (ele, ela) aceitando ; | ' e ' | espaço único.
           Regra do espaço: divide no primeiro espaço (útil quando são só primeiros nomes)."""
        raw = (line or '').strip()
        if not raw:
            return None, None
        # Prioridade 1: ponto e vírgula
        if ";" in raw:
            a, b = raw.split(";", 1)
            return a.strip(), b.strip()
        # Prioridade 2: ' e ' (com espaços, case-insensitive)
        import re
        if re.search(r"\s+e\s+", raw, flags=re.I):
            parts = re.split(r"\s+e\s+", raw, maxsplit=1, flags=re.I)
            if len(parts) >= 2:
                return parts[0].strip(), parts[1].strip()
        # Prioridade 3: espaço único (divide no primeiro espaço)
        if " " in raw:
            a, b = raw.split(" ", 1)
            return a.strip(), b.strip()
        # Nada reconhecido
        return None, None

    def buscar_consolidado(nome_a: str, nome_b: str, cursor):
        """Busca em ENCONTREIROS (mais recente) e ENCONTRISTAS.
           Tenta (A,B); se não achar nada, tenta (B,A). Consolida telefones/endereço do registro mais recente."""
        def _consulta(a: str, b: str):
            # ENCONTREIROS (prioriza mais recente). Tenta por nome_ele/nome_ela OU nome_usual_ele/nome_usual_ela.
            cursor.execute("""
                SELECT *
                FROM encontreiros
                WHERE (nome_ele = %s AND nome_ela = %s)
                   OR (nome_usual_ele = %s AND nome_usual_ela = %s)
                ORDER BY ano DESC
                LIMIT 1
            """, (a, b, a, b))
            work = cursor.fetchone()

            # ENCONTRISTAS (base)
            cursor.execute("""
                SELECT ano, endereco, telefone_ele, telefone_ela
                FROM encontristas
                WHERE (nome_usual_ele = %s AND nome_usual_ela = %s)
                   OR (nome_ele = %s AND nome_ela = %s)
                LIMIT 1
            """, (a, b, a, b))
            base = cursor.fetchone()

            # Consolidação
            if work:
                endereco = work.get("endereco") or (base["endereco"] if base else "")
                # Telefones: se houver campo 'telefones', usa; senão tenta ele/dela
                if "telefones" in work and work.get("telefones"):
                    telefones = work["telefones"]
                else:
                    tel_ele = work.get("telefone_ele")
                    tel_ela = work.get("telefone_ela")
                    if tel_ele or tel_ela:
                        telefones = f"{tel_ele or '—'} / {tel_ela or '—'}"
                    elif base:
                        telefones = f"{base.get('telefone_ele','') or '—'} / {base.get('telefone_ela','') or '—'}"
                    else:
                        telefones = "— / —"
                return {"endereco": endereco or "—", "telefones": telefones or "— / —"}
            elif base:
                telefones = f"{base.get('telefone_ele','') or '—'} / {base.get('telefone_ela','') or '—'}"
                return {"endereco": base.get("endereco") or "—", "telefones": telefones}
            else:
                return None

        # Tenta na ordem dada
        res = _consulta(nome_a, nome_b)
        if res:
            return res
        # Tenta invertido
        return _consulta(nome_b, nome_a)

    resultados = []

    if request.method == 'POST':
        nomes_input = request.form.get("lista_nomes", "").strip()
        if nomes_input:
            linhas = [l.strip() for l in nomes_input.split("\n") if l.strip()]

            conn = mysql.connector.connect(**DB_CONFIG)
            cursor = conn.cursor(dictionary=True)

            for linha in linhas:
                ele, ela = split_casal(linha)
                if not ele or not ela:
                    resultados.append({
                        "nome": linha,
                        "endereco": "Formato não reconhecido",
                        "telefones": "— / —"
                    })
                    continue

                dados = buscar_consolidado(ele, ela, cursor)
                if not dados:
                    resultados.append({
                        "nome": f"{ele} e {ela}",
                        "endereco": "Não encontrado",
                        "telefones": "— / —"
                    })
                else:
                    resultados.append({
                        "nome": f"{ele} e {ela}",
                        "endereco": dados["endereco"],
                        "telefones": dados["telefones"]
                    })

            conn.close()

    return render_template("relatorio_casais.html", resultados=resultados)
# -----------------------------
# Main
# -----------------------------
if __name__ == "__main__":
    app.run(debug=True)
