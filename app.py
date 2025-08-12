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
    resultados = []

    if request.method == 'POST':
        nomes_input = request.form.get("lista_nomes", "").strip()
        if nomes_input:
            linhas = [l.strip() for l in nomes_input.split("\n") if l.strip()]

            conn = mysql.connector.connect(**DB_CONFIG)
            cursor = conn.cursor(dictionary=True)

            for linha in linhas:
                partes = linha.split(";")
                if len(partes) == 2:
                    nome_ele, nome_ela = partes[0].strip(), partes[1].strip()

                    # ENCONTRISTAS
                    cursor.execute("""
                        SELECT ano, endereco, telefone_ele, telefone_ela 
                        FROM encontristas 
                        WHERE nome_usual_ele = %s AND nome_usual_ela = %s
                    """, (nome_ele, nome_ela))
                    dado_encontrista = cursor.fetchone()

                    # ENCONTREIROS (mais recente)
                    cursor.execute("""
                        SELECT * 
                        FROM encontreiros 
                        WHERE nome_ele = %s AND nome_ela = %s
                        ORDER BY ano DESC
                        LIMIT 1
                    """, (nome_ele, nome_ela))
                    dado_encontreiros = cursor.fetchone()

                    # Consolidar dados mais recentes
                    if dado_encontreiros:
                        ano = dado_encontreiros.get("ano", "")
                        endereco = dado_encontreiros.get("endereco") or (dado_encontrista["endereco"] if dado_encontrista else "")
                        # Telefones: tenta campo 'telefones', senão combina ele/dela se existir
                        if "telefones" in dado_encontreiros and dado_encontreiros.get("telefones"):
                            telefones = dado_encontreiros.get("telefones")
                        else:
                            tel_ele = dado_encontreiros.get("telefone_ele") if "telefone_ele" in dado_encontreiros else None
                            tel_ela = dado_encontreiros.get("telefone_ela") if "telefone_ela" in dado_encontreiros else None
                            if tel_ele or tel_ela:
                                telefones = f"{tel_ele or '—'} / {tel_ela or '—'}"
                            elif dado_encontrista:
                                telefones = f"{dado_encontrista.get('telefone_ele','') or '—'} / {dado_encontrista.get('telefone_ela','') or '—'}"
                            else:
                                telefones = "— / —"
                    elif dado_encontrista:
                        ano = dado_encontrista["ano"]
                        endereco = dado_encontrista["endereco"]
                        telefones = f"{dado_encontrista.get('telefone_ele','') or '—'} / {dado_encontrista.get('telefone_ela','') or '—'}"
                    else:
                        ano = "Não encontrado"
                        endereco = "Não encontrado"
                        telefones = "Não encontrado"

                    resultados.append({
                        "nome": f"{nome_ele} e {nome_ela}",
                        "ano": ano,
                        "endereco": endereco,
                        "telefones": telefones
                    })

            conn.close()

    return render_template("relatorio_casais.html", resultados=resultados)

# -----------------------------
# Main
# -----------------------------
if __name__ == "__main__":
    app.run(debug=True)
