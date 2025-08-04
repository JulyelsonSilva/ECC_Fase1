from flask import Flask, render_template, request
import mysql.connector
from collections import defaultdict

app = Flask(__name__)

DB_CONFIG = {
    'host': 'db4free.net',
    'user': 'eccdivino2',
    'password': 'eccdivino2025',
    'database': 'eccdivinomcz2'
}

@app.route('/visao-equipes')
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
