from flask import Flask, render_template, request
import mysql.connector
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
    page = int(request.args.get('page', 1))
    nome_ele = request.args.get('nome_usual_ele', '')
    nome_ela = request.args.get('nome_usual_ela', '')
    ano = request.args.get('ano', '')

    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor(dictionary=True)

    filters = []
    params = []
    if nome_ele:
        filters.append("nome_usual_ele LIKE %s")
        params.append(f"%{nome_ele}%")
    if nome_ela:
        filters.append("nome_usual_ela LIKE %s")
        params.append(f"%{nome_ela}%")
    if ano:
        filters.append("ano = %s")
        params.append(ano)

    where_clause = "WHERE " + " AND ".join(filters) if filters else ""
    limit = 50
    offset = (page - 1) * limit

    cursor.execute(f"SELECT COUNT(*) as total FROM encontristas {where_clause}", params)
    total = cursor.fetchone()['total']
    total_pages = math.ceil(total / limit)

    cursor.execute(f"SELECT * FROM encontristas {where_clause} LIMIT %s OFFSET %s", params + [limit, offset])
    data = cursor.fetchall()

    conn.close()
    return render_template('encontristas.html', dados=data, page=page, total_pages=total_pages)

@app.route('/encontreiros')
def encontreiros():
    nome_ele = request.args.get('nome_ele', '')
    nome_ela = request.args.get('nome_ela', '')

    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor(dictionary=True)

    filters = []
    params = []
    if nome_ele:
        filters.append("nome_ele LIKE %s")
        params.append(f"%{nome_ele}%")
    if nome_ela:
        filters.append("nome_ela LIKE %s")
        params.append(f"%{nome_ela}%")

    where_clause = "WHERE " + " AND ".join(filters) if filters else ""

    cursor.execute(f"SELECT DISTINCT ano FROM encontreiros ORDER BY ano DESC")
    anos = [row['ano'] for row in cursor.fetchall()]
    ano_dados = {}

    for ano in anos:
        cursor.execute(f"SELECT * FROM encontreiros {where_clause} AND ano = %s" if where_clause else "SELECT * FROM encontreiros WHERE ano = %s", params + [ano])
        ano_dados[ano] = cursor.fetchall()

    conn.close()
    return render_template('encontreiros.html', dados_por_ano=ano_dados)

if __name__ == '__main__':
    app.run(debug=True)
