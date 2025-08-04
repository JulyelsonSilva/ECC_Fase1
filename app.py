from flask import Flask, render_template, request
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
            colunas = ['Montagem', 'Fichas', 'Palestras', 'Finanças', 'Pós Encontro']
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

        elif equipe == 'Sala':
            colunas = ['Boa Vontade', 'Canto 1', 'Canto 2', 'Som e Projeção 1', 'Som e Projeção 2', 'Recepção de Palestras']
            dados = {col: {} for col in colunas}

            for subt in colunas:
                cursor.execute(
                    "SELECT * FROM encontreiros WHERE equipe LIKE '%SALA%' AND equipe LIKE %s",
                    (f"%{subt.upper()}%",)
                )
                for row in cursor.fetchall():
                    ano = row['ano']
                    nome = f"*{row['nome_ele']} e {row['nome_ela']}" if row['coordenador'].strip().lower() == 'sim' else f"{row['nome_ele']} e {row['nome_ela']}"
                    dados[subt].setdefault(ano, nome)

            anos = sorted({ano for subt_data in dados.values() for ano in subt_data})
            for ano in anos:
                linha = [dados[col].get(ano, '') for col in colunas]
                tabela[ano] = linha

        else:
            colunas = ['Coordenador'] + [f'Integrante {i}' for i in range(1, 10)]
            cursor.execute("SELECT * FROM encontreiros WHERE equipe LIKE %s", (f"%{equipe}%",))
            rows = cursor.fetchall()
            por_ano = defaultdict(list)
            for row in rows:
                ano = row['ano']
                nome = f"*{row['nome_ele']} e {row['nome_ela']}" if row['coordenador'].strip().lower() == 'sim' else f"{row['nome_ele']} e {row['nome_ela']}"
                por_ano[ano].append(nome)

            for ano, nomes in por_ano.items():
                linha = nomes[:len(colunas)]
                while len(linha) < len(colunas):
                    linha.append('')
                tabela[ano] = linha

        conn.close()

    return render_template('visao_equipes.html', equipe_selecionada=equipe, tabela=tabela, colunas=colunas)

def visao_equipes():
    equipe = request.args.get('equipe', '')
    tabela = {}
    colunas = []

    if equipe:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT * FROM encontreiros WHERE equipe LIKE %s", (f"%{equipe}%",))
        rows = cursor.fetchall()
        conn.close()

        por_ano = defaultdict(list)
        for row in rows:
            ano = row['ano']
            nome = f"*{row['nome_ele']} e {row['nome_ela']}" if row['coordenador'].strip().lower() == 'sim' else f"{row['nome_ele']} e {row['nome_ela']}"
            por_ano[ano].append(nome)

        if equipe == 'Dirigentes':
            colunas = ['Montagem', 'Fichas', 'Palestras', 'Finanças', 'Pós Encontro']
        elif equipe == 'Sala':
            colunas = ['Boa Vontade', 'Canto 1', 'Canto 2', 'Som e Projeção 1', 'Som e Projeção 2', 'Recepção de Palestras']
        else:
            colunas = ['Coordenador'] + [f'Integrante {i}' for i in range(1, 10)]

        for ano, nomes in por_ano.items():
            linha = nomes[:len(colunas)]
            while len(linha) < len(colunas):
                linha.append('')
            tabela[ano] = linha

    return render_template('visao_equipes.html', equipe_selecionada=equipe, tabela=tabela, colunas=colunas)

if __name__ == '__main__':
    app.run(debug=True)
