
from flask import Flask, render_template, request
import mysql.connector

app = Flask(__name__)

DB_CONFIG = {
    'host': 'db4free.net',
    'user': 'eccdivino2',
    'password': 'eccdivino2025',
    'database': 'eccdivinomcz2'
}

@app.route('/')
def index():
    return render_template("index.html")

@app.route('/visao-casal')
def visao_casal():
    nome_ele = request.args.get("nome_ele", "")
    nome_ela = request.args.get("nome_ela", "")

    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor(dictionary=True)

    dados_encontrista = None
    dados_encontreiros = []

    try:
        cursor.execute("""
            SELECT ano 
            FROM encontristas 
            WHERE nome_usual_ele LIKE %s OR nome_usual_ela LIKE %s
        """, (f"%{nome_ele}%", f"%{nome_ela}%"))
        dados_encontrista = cursor.fetchone()

        while cursor.nextset():
            pass

        cursor.execute("""
            SELECT ano, equipe, pasta, coordenador 
            FROM encontreiros 
            WHERE nome_ele LIKE %s OR nome_ela LIKE %s
        """, (f"%{nome_ele}%", f"%{nome_ela}%"))
        dados_encontreiros = cursor.fetchall()
    finally:
        cursor.close()
        conn.close()

    return render_template("visao_casal.html",
                           nome_ele=nome_ele,
                           nome_ela=nome_ela,
                           dados_encontrista=dados_encontrista,
                           dados_encontreiros=dados_encontreiros)

if __name__ == "__main__":
    app.run(debug=True)
