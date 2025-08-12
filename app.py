from flask import Flask, render_template, request, jsonify
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
# Utilidades / DB
# -----------------------------
def get_db():
    return mysql.connector.connect(**DB_CONFIG)

def _norm(s: str) -> str:
    return re.sub(r'\s+', ' ', (s or '').strip())

def parse_pasted_couples(raw: str):
    """
    Cada linha deve conter ELE e ELA (tab, ponto-e-vírgula, vírgula, hífen ou pipe).
    Retorna: [{'ele':..., 'ela':..., 'orig':...}, ...]
    """
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
    """Busca casal base em ENCONTRISTAS por nomes usuais e completos."""
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
    """Busca histórico em ENCONTREIROS (pode ter vários anos)."""
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
    """
    Consolida dados:
    - Telefones/Endereço do registro mais recente em ENCONTREIROS; se não houver, de ENCONTRISTAS.
    - Ano do encontro (de ENCONTRISTAS).
    - Histórico (ENCONTREIROS).
    """
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

            # Coordenadores históricos da equipe
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

@app.route('/autocomplete-nomes')
def autocomplete_nomes():
    termo = request.args.get("q", "").strip()
    resultados = set()
    if termo and len(termo) >= 3:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        cursor.execute("""
            SELECT nome_ele, nome_ela 
            FROM encontristas 
            WHERE nome_ele LIKE %s OR nome_ela LIKE %s
        """, (f"%{termo}%", f"%{termo}%"))
        for nome_ele, nome_ela in cursor.fetchall():
            if nome_ele: resultados.add(nome_ele)
            if nome_ela: resultados.add(nome_ela)
        conn.close()
    return jsonify(sorted(resultados))

@app.route('/visao-casal')
def visao_casal():
    nome_ele = request.args.get("nome_ele", "").strip()
    nome_ela = request.args.get("nome_ela", "").strip()

    dados_encontrista = {}
    dados_encontreiros = []
    erro = None

    if not nome_ele or not nome_ela:
        erro = "Informe ambos os nomes para realizar a busca."
        return render_template("visao_casal.html",
                               nome_ele=nome_ele,
                               nome_ela=nome_ela,
                               dados_encontrista=None,
                               dados_encontreiros=[],
                               erro=erro)

    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor(dictionary=True)

    try:
        # ENCONTRISTAS (exato por nomes usuais)
        cursor.execute("""
            SELECT ano, endereco, telefone_ele, telefone_ela, nome_usual_ele, nome_usual_ela
            FROM encontristas 
            WHERE nome_usual_ele = %s AND nome_usual_ela = %s
        """, (nome_ele, nome_ela))
        resultado_encontrista = cursor.fetchone()

        if resultado_encontrista:
            dados_encontrista = {
                "ano_encontro": resultado_encontrista["ano"],
                "endereco": resultado_encontrista["endereco"],
                "telefones": f"{resultado_encontrista.get('telefone_ele','') or '—'} / {resultado_encontrista.get('telefone_ela','') or '—'}"
            }

        # ENCONTREIROS (todos os anos; consolidar mais recente)
        cursor.execute("""
            SELECT ano, equipe, coordenador, endereco, telefone_ele, telefone_ela
            FROM encontreiros 
            WHERE nome_usual_ele = %s AND nome_usual_ela = %s
            ORDER BY ano DESC
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

            # Telefones/Endereço do ano mais recente
            mais_recente = resultados_encontreiros[0]
            tel_ele = mais_recente.get("telefone_ele") or ""
            tel_ela = mais_recente.get("telefone_ela") or ""
            dados_encontrista["endereco"] = mais_recente.get("endereco") or dados_encontrista.get("endereco")
            dados_encontrista["telefones"] = f"{tel_ele or '—'} / {tel_ela or '—'}"

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
# Relatório consolidado (tela + impressão nativa)
# -----------------------------
@app.route("/relatorio-casais", methods=["GET", "POST"])
def relatorio_casais():
    if request.method == "GET":
        return render_template("relatorio_casais.html", registros=None, entrada="")
    entrada = request.form.get("lista", "") or ""
    registros = _montar_registros(entrada)
    return render_template("relatorio_casais.html", registros=registros, entrada=entrada)

if __name__ == "__main__":
    app.run(debug=True)
