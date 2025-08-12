from flask import Flask, render_template, request, jsonify, send_file, make_response, url_for
import mysql.connector
from collections import defaultdict
import math
import re
from datetime import datetime
from xhtml2pdf import pisa
import io
import os

app = Flask(__name__)

DB_CONFIG = {
    'host': 'db4free.net',
    'user': 'eccdivino2',
    'password': 'eccdivino2025',
    'database': 'eccdivinomcz2'
}

# -----------------------------
# Funções utilitárias
# -----------------------------
def get_db():
    return mysql.connector.connect(**DB_CONFIG)

def _norm(s: str) -> str:
    return re.sub(r'\s+', ' ', (s or '').strip())

def parse_pasted_couples(raw: str):
    out = []
    if not raw:
        return out
    for line in raw.splitlines():
        L = _norm(line)
        if not L:
            continue
        if '\t' in L:
            parts = [ _norm(p) for p in L.split('\t') if _norm(p) ]
        else:
            parts = re.split(r'\s*;\s*|\s*,\s*|\s*-\s*|\s*\|\s*', L)
            parts = [ _norm(p) for p in parts if _norm(p) ]
        if len(parts) >= 2:
            out.append({'ele': parts[0], 'ela': parts[1], 'orig': L})
    return out

def buscar_encontristas(ele_like, ela_like):
    conn = get_db()
    cur = conn.cursor(dictionary=True)
    like_ele = f"%{ele_like}%"
    like_ela = f"%{ela_like}%"
    cur.execute("""
        SELECT 
          id,
          nome_usual_ele, nome_usual_ela,
          nome_ele, nome_ela,
          telefone_ele, telefone_ela,
          endereco,
          ano AS ano_encontro,
          encontro
        FROM encontristas
        WHERE
          (nome_usual_ele LIKE %s AND nome_usual_ela LIKE %s)
          OR (nome_ele LIKE %s AND nome_ela LIKE %s)
    """, (like_ele, like_ela, like_ele, like_ela))
    rows = cur.fetchall()
    cur.close(); conn.close()
    return rows

def buscar_encontreiros(ele_like, ela_like):
    conn = get_db()
    cur = conn.cursor(dictionary=True)
    like_ele = f"%{ele_like}%"
    like_ela = f"%{ela_like}%"
    cur.execute("""
        SELECT 
          ano,
          equipe,
          coordenador,
          telefone_ele,
          telefone_ela,
          endereco,
          nome_usual_ele, nome_usual_ela,
          nome_ele, nome_ela
        FROM encontreiros
        WHERE
          (nome_usual_ele LIKE %s AND nome_usual_ela LIKE %s)
          OR (nome_ele LIKE %s AND nome_ela LIKE %s)
        ORDER BY ano DESC
    """, (like_ele, like_ela, like_ele, like_ela))
    rows = cur.fetchall()
    cur.close(); conn.close()
    return rows

def consolidar_registro(base_rows, work_rows):
    base = base_rows[0] if base_rows else None
    latest = work_rows[0] if work_rows else None

    tel_ele = (latest or {}).get('telefone_ele') or (base or {}).get('telefone_ele')
    tel_ela = (latest or {}).get('telefone_ela') or (base or {}).get('telefone_ela')
    endereco = (latest or {}).get('endereco') or (base or {}).get('endereco')

    historico = []
    for r in work_rows:
        historico.append({
            'ano': r.get('ano'),
            'equipe': r.get('equipe'),
            'coordenador': r.get('coordenador')
        })

    return {
        'base_existe': base is not None,
        'nome_usual_ele': (base or {}).get('nome_usual_ele') or (latest or {}).get('nome_usual_ele') or (base or {}).get('nome_ele'),
        'nome_usual_ela': (base or {}).get('nome_usual_ela') or (latest or {}).get('nome_usual_ela') or (base or {}).get('nome_ela'),
        'nome_ele': (base or {}).get('nome_ele'),
        'nome_ela': (base or {}).get('nome_ela'),
        'ano_encontro': (base or {}).get('ano_encontro'),
        'encontro': (base or {}).get('encontro'),
        'telefone_ele': tel_ele,
        'telefone_ela': tel_ela,
        'endereco': endereco,
        'historico': historico,
        'fonte_contato': 'encontreiros' if latest else ('encontristas' if base else None),
        'ano_mais_recente': (latest or {}).get('ano') or (base or {}).get('ano_encontro')
    }

def _montar_registros(entrada: str):
    pares = parse_pasted_couples(entrada)
    registros = []
    for p in pares:
        ele, ela = p['ele'], p['ela']
        base_rows = buscar_encontristas(ele, ela)
        work_rows = buscar_encontreiros(ele, ela)
        reg = consolidar_registro(base_rows, work_rows)
        reg['entrada'] = p['orig']
        reg['matches_base'] = len(base_rows)
        reg['matches_work'] = len(work_rows)
        registros.append(reg)
    return registros

def link_callback(uri, rel):
    """Resolve caminho de imagens/CSS locais para o xhtml2pdf."""
    if uri.startswith('/'):
        if uri.startswith('/static/'):
            path = os.path.join(app.root_path, uri.lstrip('/'))
            return path
        return os.path.join(app.root_path, uri.lstrip('/'))
    return uri

# -----------------------------
# Rotas existentes
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

# -----------------------------
# NOVO: Relatório consolidado
# -----------------------------
@app.route("/relatorio-casais", methods=["GET", "POST"])
def relatorio_casais():
    if request.method == "GET":
        return render_template("relatorio_casais.html", registros=None, entrada="")
    entrada = request.form.get("lista", "") or ""
    registros = _montar_registros(entrada)
    return render_template("relatorio_casais.html", registros=registros, entrada=entrada)

@app.route("/relatorio-casais.pdf", methods=["POST"])
def relatorio_casais_pdf():
    entrada = request.form.get("lista", "") or ""
    registros = _montar_registros(entrada)

    html = render_template(
        "relatorio_casais_pdf.html",
        registros=registros,
        data_geracao=datetime.now().strftime("%d/%m/%Y %H:%M")
    )

    pdf_io = io.BytesIO()
    pisa_status = pisa.CreatePDF(io.StringIO(html), dest=pdf_io, encoding='utf-8', link_callback=link_callback)
    if pisa_status.err:
        return make_response("Erro ao gerar PDF", 500)

    pdf_io.seek(0)
    return send_file(
        pdf_io,
        as_attachment=True,
        download_name="relatorio_casais.pdf",
        mimetype="application/pdf"
    )

if __name__ == "__main__":
    app.run(debug=True)
