from flask import Flask, render_template, request, jsonify, redirect, url_for
import mysql.connector
from collections import defaultdict
import math
import re
from werkzeug.routing import BuildError

app = Flask(__name__)

DB_CONFIG = {
    'host': 'db4free.net',
    'user': 'eccdivino2',
    'password': 'eccdivino2025',
    'database': 'eccdivinomcz2'
}

# Mapeamento das equipes (título -> string do banco)
TEAM_MAP = {
    "Sala": "Equipe de Sala - Coordenador/Apresentador",
    "Círculos": "Equipe de Círculos",
    "Circulos": "Equipe de Círculos",
    "Café e Minimercado": "Equipe Café e Minimercado",
    "Compras": "Equipe Compras",
    "Acolhida": "Equipe Acolhida",
    "Ordem e Limpeza": "Equipe Ordem e Limpeza",
    "Liturgia e Vigília": "Equipe Liturgia e Vigília",
    "Liturgia e Vigilia": "Equipe Liturgia e Vigília",
    "Secretaria": "Equipe Secretaria",
    "Cozinha": "Equipe Cozinha",
    "Visitação": "Equipe Visitação",
    "Visitacao": "Equipe Visitação",
}

# Limites por equipe (min, max)
TEAM_LIMITS = {
    "Círculos": (5, 5),
    "Circulos": (5, 5),
    "Café e Minimercado": (3, 7),
    "Compras": (0, 1),
    "Acolhida": (4, 6),
    "Ordem e Limpeza": (3, 7),
    "Liturgia e Vigília": (2, 6),
    "Liturgia e Vigilia": (2, 6),
    "Secretaria": (3, 5),
    "Cozinha": (7, 9),
    "Visitação": (6, 10),
    "Visitacao": (6, 10),
}

# -----------------------------
# Rotas principais
# -----------------------------
@app.route('/', endpoint='index')
def index():
    return render_template('index.html')

# -----------------------------
# ENCONTRISTAS (listagem + edição)
# -----------------------------
@app.route('/encontristas', endpoint='encontristas')
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

@app.route('/encontristas/<int:encontrista_id>/editar', methods=['GET', 'POST'], endpoint='editar_encontrista')
def editar_encontrista(encontrista_id):
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor(dictionary=True)

    if request.method == 'POST':
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
        return redirect(url_for('encontristas') + '?updated=1')

    cursor.execute("SELECT * FROM encontristas WHERE id = %s", (encontrista_id,))
    registro = cursor.fetchone()
    cursor.close()
    conn.close()

    if not registro:
        return redirect(url_for('encontristas') + '?notfound=1')

    return render_template('editar_encontrista.html', r=registro)

# -----------------------------
# MONTAGEM (Aberto x Concluído)
# -----------------------------
@app.route('/montagem', endpoint='montagem')
def montagem():
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor(dictionary=True)
    cursor.execute("""
        SELECT 
            ano,
            SUM(CASE WHEN TRIM(LOWER(status)) = 'concluido' THEN 1 ELSE 0 END) AS qtd_concluido,
            COUNT(*) AS total
        FROM encontreiros
        GROUP BY ano
        ORDER BY ano DESC
    """)
    rows = cursor.fetchall()
    cursor.close()
    conn.close()

    anos_concluidos = []
    anos_aberto = []

    for r in rows:
        item = {"ano": r["ano"], "qtd_concluido": int(r["qtd_concluido"]), "total": int(r["total"])}
        if item["total"] > 0 and item["qtd_concluido"] == item["total"]:
            anos_concluidos.append(item)
        else:
            anos_aberto.append(item)

    return render_template('montagem.html',
                           anos_aberto=anos_aberto,
                           anos_concluidos=anos_concluidos)

# -----------------------------
# Página Nova Montagem
# -----------------------------
@app.route('/montagem/nova', endpoint='nova_montagem')
def nova_montagem():
    ano_preselecionado = request.args.get('ano', type=int)
    initial_data = {"dirigentes": {}, "cg": None, "coords": {}}

    # presets opcionais vindos da seleção na visão de equipes
    pre_target = (request.args.get('target') or '').strip()
    pre_ele = (request.args.get('ele') or '').strip()
    pre_ela = (request.args.get('ela') or '').strip()
    pre_tel = (request.args.get('tel') or '').strip()
    pre_end = (request.args.get('end') or '').strip()
    notfound = request.args.get('notfound')

    if ano_preselecionado:
        equipes_dir = [
            "Equipe Dirigente - MONTAGEM",
            "Equipe Dirigente -FICHAS",
            "Equipe Dirigente - FINANÇAS",
            "Equipe Dirigente - PALESTRA",
            "Equipe Dirigente - PÓS ENCONTRO",
        ]
        conn = mysql.connector.connect(**DB_CONFIG)
        cur = conn.cursor(dictionary=True)
        try:
            # Dirigentes (somente ABERTOS)
            for equipe in equipes_dir:
                cur.execute("""
                    SELECT id, nome_ele, nome_ela, telefones, endereco
                      FROM encontreiros
                     WHERE ano = %s AND equipe = %s AND TRIM(LOWER(status)) = 'aberto'
                     ORDER BY id ASC
                     LIMIT 1
                """, (ano_preselecionado, equipe))
                r = cur.fetchone()
                if r:
                    initial_data["dirigentes"][equipe] = {
                        "id": r.get("id"),
                        "nome_ele": r.get("nome_ele") or "",
                        "nome_ela": r.get("nome_ela") or "",
                        "telefones": r.get("telefones") or "",
                        "endereco": r.get("endereco") or ""
                    }

            # Coordenador Geral ABERTO
            cur.execute("""
                SELECT id, nome_ele, nome_ela, telefones, endereco
                  FROM encontreiros
                 WHERE ano = %s
                   AND UPPER(equipe) = 'CASAL COORDENADOR GERAL'
                   AND TRIM(LOWER(status)) = 'aberto'
                 LIMIT 1
            """, (ano_preselecionado,))
            r_cg = cur.fetchone()
            if r_cg:
                initial_data["cg"] = {
                    "id": r_cg.get("id"),
                    "nome_ele": r_cg.get("nome_ele") or "",
                    "nome_ela": r_cg.get("nome_ela") or "",
                    "telefones": r_cg.get("telefones") or "",
                    "endereco": r_cg.get("endereco") or "",
                }

            # Coordenadores de equipe (somente ABERTOS)
            for team_key, equipe_str in TEAM_MAP.items():
                cur.execute("""
                    SELECT id, nome_ele, nome_ela, telefones, endereco
                      FROM encontreiros
                     WHERE ano = %s AND equipe = %s AND TRIM(LOWER(status)) = 'aberto'
                     LIMIT 1
                """, (ano_preselecionado, equipe_str))
                r = cur.fetchone()
                if r:
                    initial_data["coords"][team_key] = {
                        "id": r.get("id"),
                        "nome_ele": r.get("nome_ele") or "",
                        "nome_ela": r.get("nome_ela") or "",
                        "telefones": r.get("telefones") or "",
                        "endereco": r.get("endereco") or "",
                    }
        finally:
            cur.close(); conn.close()

    # injeta preset clicado na visão de equipes (se houver)
    initial_data["preset_from_visao"] = {
        "target": pre_target,
        "ele": pre_ele,
        "ela": pre_ela,
        "telefones": pre_tel,
        "endereco": pre_end
    } if pre_target and pre_ele and pre_ela else None

    return render_template('nova_montagem.html',
                           ano_preselecionado=ano_preselecionado,
                           initial_data=initial_data,
                           team_map=TEAM_MAP,
                           notfound=notfound)

# -----------------------------
# Seleção via Visão de Equipes (preenche e volta)
# -----------------------------
@app.route('/visao-equipes/select', endpoint='visao_equipes_select')
def visao_equipes_select():
    ano_montagem = request.args.get('ano_montagem', type=int)
    target = (request.args.get('target') or '').strip()
    ele = (request.args.get('ele') or '').strip()
    ela = (request.args.get('ela') or '').strip()

    if not (ano_montagem and target and ele and ela):
        return redirect(url_for('nova_montagem', ano=ano_montagem or '', target=target, ele=ele, ela=ela, notfound=1))

    conn = mysql.connector.connect(**DB_CONFIG)
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute("""
            SELECT telefones, endereco
              FROM encontreiros
             WHERE nome_ele = %s AND nome_ela = %s
             ORDER BY ano DESC LIMIT 1
        """, (ele, ela))
        r = cur.fetchone()
    finally:
        cur.close(); conn.close()

    params = {'ano': ano_montagem, 'target': target, 'ele': ele, 'ela': ela}
    if r:
        if r.get('telefones'): params['tel'] = r['telefones']
        if r.get('endereco'):  params['end'] = r['endereco']
    return redirect(url_for('nova_montagem', **params))

# -----------------------------
# APIs da Montagem – Dirigentes/Status/CG
# -----------------------------
@app.route('/api/buscar-casal', methods=['POST'], endpoint='api_buscar_casal')
def api_buscar_casal():
    data = request.get_json(silent=True) or {}
    nome_ele = (data.get('nome_ele') or '').strip()
    nome_ela = (data.get('nome_ela') or '').strip()
    if not nome_ele or not nome_ela:
        return jsonify({"ok": False, "msg": "Informe nome_ele e nome_ela."}), 400

    conn = mysql.connector.connect(**DB_CONFIG)
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute("""
            SELECT ano, telefones, endereco
              FROM encontreiros
             WHERE nome_ele = %s AND nome_ela = %s
             ORDER BY ano DESC LIMIT 1
        """, (nome_ele, nome_ela))
        r = cur.fetchone()
        if r:
            return jsonify({"ok": True, "origem": "encontreiros", "ano": r["ano"],
                            "telefones": r.get("telefones") or "", "endereco": r.get("endereco") or ""})

        cur.execute("""
            SELECT telefone_ele, telefone_ela, endereco, ano
              FROM encontristas
             WHERE nome_usual_ele = %s AND nome_usual_ela = %s
             ORDER BY ano DESC LIMIT 1
        """, (nome_ele, nome_ela))
        r2 = cur.fetchone()
        if r2:
            tels = " / ".join([t for t in [(r2.get('telefone_ele') or '').strip(), (r2.get('telefone_ela') or '').strip()] if t])
            return jsonify({"ok": True, "origem":"encontristas","ano":r2.get("ano"),
                            "telefones":tels or "", "endereco": r2.get("endereco") or ""})
        return jsonify({"ok": False, "msg": "Casal não participou do ECC."}), 404
    finally:
        cur.close(); conn.close()

@app.route('/api/adicionar-dirigente', methods=['POST'], endpoint='api_adicionar_dirigente')
def api_adicionar_dirigente():
    data = request.get_json(silent=True) or {}
    ano = (str(data.get('ano') or '')).strip()
    equipe = (data.get('equipe') or '').strip()
    nome_ele = (data.get('nome_ele') or '').strip()
    nome_ela = (data.get('nome_ela') or '').strip()
    telefones = (data.get('telefones') or '').strip()
    endereco = (data.get('endereco') or '').strip()

    if not ano.isdigit() or len(ano) != 4:
        return jsonify({"ok": False, "msg": "Ano inválido."}), 400
    if not equipe:
        return jsonify({"ok": False, "msg": "Equipe obrigatória."}), 400
    if not nome_ele or not nome_ela:
        return jsonify({"ok": False, "msg": "Preencha nome_ele e nome_ela."}), 400

    conn = mysql.connector.connect(**DB_CONFIG)
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO encontreiros
                (ano, equipe, nome_ele, nome_ela, telefones, endereco, coordenador, status)
            VALUES
                (%s,  %s,     %s,       %s,       %s,         %s,       'Sim',      'Aberto')
        """, (int(ano), equipe, nome_ele, nome_ela, telefones, endereco))
        conn.commit()
        return jsonify({"ok": True, "id": cur.lastrowid})
    finally:
        cur.close(); conn.close()

@app.route('/api/marcar-status', methods=['POST'], endpoint='api_marcar_status')
def api_marcar_status():
    data = request.get_json(silent=True) or {}
    rec_id = data.get('id')
    status = (data.get('status') or '').strip()
    observacao = (data.get('observacao') or '').strip()

    if not rec_id or status not in ('Recusou', 'Desistiu', 'Concluido', 'Aberto'):
        return jsonify({"ok": False, "msg": "Parâmetros inválidos."}), 400

    # Justificativa obrigatória para Recusou/Desistiu
    if status in ('Recusou', 'Desistiu') and not observacao:
        return jsonify({"ok": False, "msg": "Justificativa é obrigatória para Recusou/Desistiu."}), 400

    conn = mysql.connector.connect(**DB_CONFIG)
    cur = conn.cursor()
    try:
        if status in ('Recusou', 'Desistiu'):
            cur.execute("UPDATE encontreiros SET status=%s, observacao=%s WHERE id=%s",
                        (status, observacao, rec_id))
        else:
            # Para Aberto/Concluido não exigimos observação; não alteramos observacao existente
            cur.execute("UPDATE encontreiros SET status=%s WHERE id=%s",
                        (status, rec_id))
        conn.commit()
        return jsonify({"ok": True})
    finally:
        cur.close(); conn.close()

@app.route('/api/buscar-cg', methods=['POST'], endpoint='api_buscar_cg')
def api_buscar_cg():
    data = request.get_json(silent=True) or {}
    nome_ele = (data.get('nome_ele') or '').strip()
    nome_ela = (data.get('nome_ela') or '').strip()
    if not nome_ele or not nome_ela:
        return jsonify({"ok": False, "msg": "Informe nome_ele e nome_ela."}), 400

    conn = mysql.connector.connect(**DB_CONFIG)
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute("""
            SELECT ano, telefones, endereco
              FROM encontreiros
             WHERE nome_ele = %s AND nome_ela = %s
             ORDER BY ano DESC LIMIT 1
        """, (nome_ele, nome_ela))
        r = cur.fetchone()
        if not r:
            return jsonify({"ok": False, "msg": "Casal nunca trabalhou no ECC."}), 404

        cur.execute("""
            SELECT 1 FROM encontreiros
             WHERE nome_ele=%s AND nome_ela=%s AND UPPER(equipe)='CASAL COORDENADOR GERAL' LIMIT 1
        """, (nome_ele, nome_ela))
        if cur.fetchone():
            return jsonify({"ok": False, "msg": "Casal já foi Coordenador Geral."}), 409

        return jsonify({"ok": True, "ano_ref": r["ano"],
                        "telefones": r.get("telefones") or "", "endereco": r.get("endereco") or ""})
    finally:
        cur.close(); conn.close()

@app.route('/api/adicionar-cg', methods=['POST'], endpoint='api_adicionar_cg')
def api_adicionar_cg():
    data = request.get_json(silent=True) or {}
    ano = (str(data.get('ano') or '')).strip()
    nome_ele = (data.get('nome_ele') or '').strip()
    nome_ela = (data.get('nome_ela') or '').strip()
    telefones = (data.get('telefones') or '').strip()
    endereco = (data.get('endereco') or '').strip()

    if not ano.isdigit() or len(ano) != 4:
        return jsonify({"ok": False, "msg": "Ano inválido."}), 400
    if not nome_ele or not nome_ela:
        return jsonify({"ok": False, "msg": "Preencha nome_ele e nome_ela."}), 400

    conn = mysql.connector.connect(**DB_CONFIG)
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute("SELECT 1 FROM encontreiros WHERE nome_ele=%s AND nome_ela=%s LIMIT 1", (nome_ele, nome_ela))
        if not cur.fetchone():
            return jsonify({"ok": False, "msg": "Casal nunca trabalhou no ECC."}), 404

        cur.execute("""
            SELECT 1 FROM encontreiros
             WHERE nome_ele=%s AND nome_ela=%s AND UPPER(equipe)='CASAL COORDENADOR GERAL' LIMIT 1
        """, (nome_ele, nome_ela))
        if cur.fetchone():
            return jsonify({"ok": False, "msg": "Casal já foi Coordenador Geral."}), 409

        cur2 = conn.cursor()
        cur2.execute("""
            INSERT INTO encontreiros
                (ano, equipe, nome_ele, nome_ela, telefones, endereco, coordenador, status)
            VALUES
                (%s, 'Casal Coordenador Geral', %s, %s, %s, %s, 'Sim', 'Aberto')
        """, (int(ano), nome_ele, nome_ela, telefones, endereco))
        conn.commit()
        new_id = cur2.lastrowid
        cur2.close()
        return jsonify({"ok": True, "id": new_id})
    finally:
        cur.close(); conn.close()

# -----------------------------
# ENCONTREIROS (listagem – só Aberto/Concluido)
# -----------------------------
@app.route('/encontreiros', endpoint='encontreiros')
def encontreiros():
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor(dictionary=True)

    nome_ele = request.args.get('nome_ele', '')
    nome_ela = request.args.get('nome_ela', '')
    ano = request.args.get('ano', '')

    query = """
        SELECT * FROM encontreiros
        WHERE LOWER(TRIM(status)) IN ('aberto','concluido')
    """
    params = []
    if nome_ele:
        query += " AND nome_ele LIKE %s"; params.append(f"%{nome_ele}%")
    if nome_ela:
        query += " AND nome_ela LIKE %s"; params.append(f"%{nome_ela}%")
    if ano:
        query += " AND ano = %s"; params.append(ano)

    query += " ORDER BY ano DESC, equipe ASC"
    cursor.execute(query, params)
    todos = cursor.fetchall()
    cursor.close(); conn.close()

    por_ano = defaultdict(list)
    for row in todos:
        por_ano[row['ano']].append(row)

    return render_template('encontreiros.html', por_ano=por_ano)

# -----------------------------
# Visão Equipes (com/sem links)
# -----------------------------
@app.route('/visao-equipes', endpoint='visao_equipes')
def visao_equipes():
    equipe = request.args.get('equipe', '')
    target = request.args.get('target', '')
    ano_montagem = request.args.get('ano_montagem', type=int)

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

            anos = sorted({a for pasta_data in dados.values() for a in pasta_data})
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

                for a, linha_dict in dados_ano.items():
                    linha = []
                    for col in colunas:
                        nome = linha_dict[col]
                        if nome.startswith("*"): linha.append(nome)
                        elif nome in coordenadores_globais: linha.append(f"~{nome}")
                        else: linha.append(nome)
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
                        if nome.startswith("*"): linha.append(nome)
                        elif nome in coordenadores_globais: linha.append(f"~{nome}")
                        else: linha.append(nome)
                    while len(linha) < len(colunas): linha.append('')
                    tabela[a] = linha

        cursor.close(); conn.close()

    return render_template('visao_equipes.html',
                           equipe_selecionada=equipe,
                           tabela=tabela,
                           colunas=colunas,
                           target=target,
                           ano_montagem=ano_montagem)

# -----------------------------
# MONTAGEM DE EQUIPE
# -----------------------------
def _normalize_team_key(k: str) -> str:
    k = (k or '').strip()
    return {"Circulos":"Círculos", "Liturgia e Vigilia":"Liturgia e Vigília", "Visitacao":"Visitação"}.get(k, k)

@app.route('/montagem/equipe', endpoint='montagem_equipe')
def montagem_equipe():
    ano = request.args.get('ano', type=int)
    equipe_key = _normalize_team_key(request.args.get('equipe', ''))
    if not (ano and equipe_key and equipe_key in TEAM_MAP):
        return redirect(url_for('montagem'))

    equipe_final = TEAM_MAP[equipe_key]
    min_qtd, max_qtd = TEAM_LIMITS[equipe_key]

    conn = mysql.connector.connect(**DB_CONFIG)
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute("""
            SELECT 1 FROM encontreiros
             WHERE ano=%s AND equipe=%s AND LOWER(TRIM(coordenador))='sim'
               AND LOWER(TRIM(status)) IN ('aberto','concluido')
             LIMIT 1
        """, (ano, equipe_final))
        if not cur.fetchone():
            return redirect(url_for('nova_montagem', ano=ano))

        cur.execute("""
            SELECT id, nome_ele, nome_ela, telefones, endereco
              FROM encontreiros
             WHERE ano=%s AND equipe=%s
               AND LOWER(TRIM(coordenador))='não'
               AND LOWER(TRIM(status))='aberto'
             ORDER BY id ASC
             LIMIT %s
        """, (ano, equipe_final, max_qtd))
        integrantes = cur.fetchall()

        cur.execute("""
            SELECT DISTINCT nome_ele, nome_ela
              FROM encontreiros
             WHERE ano=%s AND LOWER(TRIM(status)) IN ('aberto','concluido')
        """, (ano,))
        usados = {(r['nome_ele'], r['nome_ela']) for r in cur.fetchall()}

        prev_year = (ano - 1) if ano else None
        candidatos = []
        if prev_year:
            cur.execute("""
                SELECT nome_usual_ele, nome_usual_ela, telefone_ele, telefone_ela, endereco
                  FROM encontristas
                 WHERE ano=%s
                 ORDER BY nome_usual_ele, nome_usual_ela
            """, (prev_year,))
            for r in cur.fetchall():
                pair = (r['nome_usual_ele'], r['nome_usual_ela'])
                if pair in usados:
                    continue
                tel_ele = (r.get('telefone_ele') or '').strip()
                tel_ela = (r.get('telefone_ela') or '').strip()
                tels = " / ".join([t for t in [tel_ele, tel_ela] if t])
                candidatos.append({
                    "ele": r['nome_usual_ele'] or '',
                    "ela": r['nome_usual_ela'] or '',
                    "telefones": tels,
                    "endereco": r.get('endereco') or ''
                })
    finally:
        cur.close(); conn.close()

    slots = []
    for i in range(max_qtd):
        if i < len(integrantes):
            slots.append(integrantes[i])
        else:
            slots.append(None)

    return render_template('equipe_montagem.html',
                           ano=ano,
                           equipe_key=equipe_key,
                           equipe_final=equipe_final,
                           min_qtd=min_qtd,
                           max_qtd=max_qtd,
                           slots=slots,
                           candidatos=candidatos)

# --- APIs de membros da equipe ---
@app.route('/api/buscar-integrante', methods=['POST'], endpoint='api_buscar_integrante')
def api_buscar_integrante():
    data = request.get_json(silent=True) or {}
    nome_ele = (data.get('nome_ele') or '').strip()
    nome_ela = (data.get('nome_ela') or '').strip()
    equipe_final = (data.get('equipe_final') or '').strip()

    if not nome_ele or not nome_ela or not equipe_final:
        return jsonify({"ok": False, "msg": "Parâmetros obrigatórios ausentes."}), 400

    conn = mysql.connector.connect(**DB_CONFIG)
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute("""
            SELECT 1 FROM encontreiros
             WHERE nome_ele=%s AND nome_ela=%s AND equipe=%s
               AND LOWER(TRIM(coordenador))='sim' LIMIT 1
        """, (nome_ele, nome_ela, equipe_final))
        was_coord = bool(cur.fetchone())

        cur.execute("""
            SELECT 1 FROM encontreiros
             WHERE nome_ele=%s AND nome_ela=%s AND equipe=%s
             LIMIT 1
        """, (nome_ele, nome_ela, equipe_final))
        worked_before = bool(cur.fetchone())

        cur.execute("""
            SELECT telefones, endereco FROM encontreiros
             WHERE nome_ele=%s AND nome_ela=%s
             ORDER BY ano DESC LIMIT 1
        """, (nome_ele, nome_ela))
        r = cur.fetchone()
        telefones = r.get('telefones') if r else ''
        endereco = r.get('endereco') if r else ''

        if not r:
            cur.execute("""
                SELECT telefone_ele, telefone_ela, endereco
                  FROM encontristas
                 WHERE nome_usual_ele=%s AND nome_usual_ela=%s
                 ORDER BY ano DESC LIMIT 1
            """, (nome_ele, nome_ela))
            r2 = cur.fetchone()
            if r2:
                tel_ele = (r2.get('telefone_ele') or '').strip()
                tel_ela = (r2.get('telefone_ela') or '').strip()
                telefones = " / ".join([t for t in [tel_ele, tel_ela] if t])
                endereco = r2.get('endereco') or ''

        return jsonify({"ok": True, "was_coord": was_coord, "worked_before": worked_before,
                        "telefones": telefones or "", "endereco": endereco or ""})
    finally:
        cur.close(); conn.close()

@app.route('/api/adicionar-integrante', methods=['POST'], endpoint='api_adicionar_integrante')
def api_adicionar_integrante():
    data = request.get_json(silent=True) or {}
    ano = data.get('ano')
    equipe_final = (data.get('equipe_final') or '').strip()
    nome_ele = (data.get('nome_ele') or '').strip()
    nome_ela = (data.get('nome_ela') or '').strip()
    telefones = (data.get('telefones') or '').strip()
    endereco = (data.get('endereco') or '').strip()
    force = bool(data.get('force'))

    if not (isinstance(ano, int) and 1900 <= ano <= 2100):
        return jsonify({"ok": False, "msg": "Ano inválido."}), 400
    if not equipe_final:
        return jsonify({"ok": False, "msg": "Equipe obrigatória."}), 400
    if not nome_ele or not nome_ela:
        return jsonify({"ok": False, "msg": "Preencha nome_ele e nome_ela."}), 400

    conn = mysql.connector.connect(**DB_CONFIG)
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute("""
            SELECT 1 FROM encontreiros
             WHERE nome_ele=%s AND nome_ela=%s AND equipe=%s
               AND LOWER(TRIM(coordenador))='sim' LIMIT 1
        """, (nome_ele, nome_ela, equipe_final))
        if cur.fetchone():
            return jsonify({"ok": False, "msg": "Casal já foi coordenador desta equipe."}), 409

        cur.execute("""
            SELECT 1 FROM encontreiros
             WHERE nome_ele=%s AND nome_ela=%s AND equipe=%s
             LIMIT 1
        """, (nome_ele, nome_ela, equipe_final))
        worked_before = bool(cur.fetchone())
        if worked_before and not force:
            return jsonify({"ok": False, "need_confirm": True,
                            "msg": "Casal já trabalhou na equipe. Confirmar para montar novamente?"}), 409

        cur.execute("""
            SELECT 1 FROM encontreiros
             WHERE ano=%s AND nome_ele=%s AND nome_ela=%s
               AND LOWER(TRIM(status)) IN ('aberto','concluido')
             LIMIT 1
        """, (ano, nome_ele, nome_ela))
        if cur.fetchone():
            return jsonify({"ok": False, "msg": "Casal já foi montado em alguma equipe neste ano."}), 409

        cur2 = conn.cursor()
        cur2.execute("""
            INSERT INTO encontreiros
                (ano, equipe, nome_ele, nome_ela, telefones, endereco, coordenador, status)
            VALUES
                (%s,  %s,     %s,       %s,       %s,         %s,       'Não',      'Aberto')
        """, (ano, equipe_final, nome_ele, nome_ela, telefones, endereco))
        conn.commit()
        new_id = cur2.lastrowid
        cur2.close()
        return jsonify({"ok": True, "id": new_id})
    finally:
        cur.close(); conn.close()

@app.route('/api/concluir-equipe', methods=['POST'], endpoint='api_concluir_equipe')
def api_concluir_equipe():
    """
    ATENÇÃO: Agora NÃO altera status. Apenas valida e retorna OK.
    A conclusão real da montagem é por ano (endpoint api_concluir_montagem_ano).
    """
    data = request.get_json(silent=True) or {}
    ano = data.get('ano')
    equipe_final = (data.get('equipe_final') or '').strip()
    if not (isinstance(ano, int) and equipe_final):
        return jsonify({"ok": False, "msg": "Parâmetros inválidos."}), 400
    # Nenhuma alteração em banco.
    return jsonify({"ok": True, "note": "Equipe validada. Status permanece 'Aberto'."})

# ---- NOVO: Concluir Montagem do Ano (troca todos 'Aberto' -> 'Concluido') ----
@app.route('/api/concluir-montagem-ano', methods=['POST'], endpoint='api_concluir_montagem_ano')
def api_concluir_montagem_ano():
    data = request.get_json(silent=True) or {}
    ano = data.get('ano')
    try:
        ano = int(ano)
    except Exception:
        return jsonify({"ok": False, "msg": "Ano inválido."}), 400

    conn = mysql.connector.connect(**DB_CONFIG)
    cur = conn.cursor()
    try:
        cur.execute("""
            UPDATE encontreiros
               SET status='Concluido'
             WHERE ano=%s AND LOWER(TRIM(status))='aberto'
        """, (ano,))
        conn.commit()
        return jsonify({"ok": True, "updated": cur.rowcount})
    finally:
        cur.close(); conn.close()

# -----------------------------
# Visão do Casal
# -----------------------------
@app.route('/visao-casal', endpoint='visao_casal')
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
        cursor.execute("""
            SELECT ano, endereco, telefone_ele, telefone_ela
            FROM encontristas 
            WHERE nome_usual_ele = %s AND nome_usual_ela = %s
        """, (nome_ele, nome_ela))
        resultado_encontrista = cursor.fetchone()

        while cursor.nextset():
            pass

        if resultado_encontrista:
            dados_encontrista = {
                "ano_encontro": resultado_encontrista["ano"],
                "endereco": resultado_encontrista["endereco"],
                "telefones": f"{resultado_encontrista['telefone_ele']} / {resultado_encontrista['telefone_ela']}"
            }

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
@app.route('/organograma', endpoint='organograma')
def organograma():
    return render_template('organograma.html')

@app.route('/dados-organograma', endpoint='dados_organograma')
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
# Relatório de Casais
# -----------------------------
@app.route('/relatorio-casais', methods=['GET', 'POST'], endpoint='relatorio_casais')
def relatorio_casais():
    def split_casal(line: str):
        raw = (line or '').strip()
        if not raw: return None, None
        if ";" in raw:
            a,b = raw.split(";",1); return a.strip(), b.strip()
        if re.search(r"\s+e\s+", raw, flags=re.I):
            a,b = re.split(r"\s+e\s+", raw, maxsplit=1, flags=re.I); return a.strip(), b.strip()
        if " " in raw:
            a,b = raw.split(" ",1); return a.strip(), b.strip()
        return None, None

    def get_table_columns(conn, table_name: str) -> set:
        cur = conn.cursor()
        cur.execute("""
            SELECT COLUMN_NAME
            FROM information_schema.columns
            WHERE table_schema=%s AND table_name=%s
        """, (DB_CONFIG['database'], table_name))
        cols = {row[0] for row in cur.fetchall()}
        cur.close()
        return cols

    def escolher_par(colunas: set, prefer_usual=True):
        pares = [('nome_usual_ele','nome_usual_ela'), ('nome_ele','nome_ela')] if prefer_usual else [('nome_ele','nome_ela'), ('nome_usual_ele','nome_usual_ela')]
        for a,b in pares:
            if a in colunas and b in colunas: return a,b
        return None, None

    resultados_ok, resultados_fail = [], []

    if request.method == 'POST':
        nomes_input = (request.form.get("lista_nomes","") or "").strip()
        if nomes_input:
            linhas = [l.strip() for l in nomes_input.splitlines() if l.strip()]
            conn = mysql.connector.connect(**DB_CONFIG, connection_timeout=10)
            try:
                cols_work = get_table_columns(conn, 'encontreiros')
                cols_base = get_table_columns(conn, 'encontristas')
                work_a, work_b = escolher_par(cols_work, prefer_usual=False)
                base_a, base_b = escolher_par(cols_base, prefer_usual=True)
                cur = conn.cursor(dictionary=True)

                def consulta_like(a: str, b: str):
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
                        if 'nome_ele' in cols_base and 'nome_ela' in cols_base and (base_a, base_b) != ('nome_ele','nome_ela'):
                            where_parts.append("(nome_ele LIKE %s AND nome_ela LIKE %s)")
                            params += [f"%{a}%", f"%{b}%"]
                        cur.execute("SELECT endereco, telefone_ele, telefone_ela FROM encontristas WHERE " + " OR ".join(where_parts) + " LIMIT 1", tuple(params))
                        base = cur.fetchone()

                    if work:
                        endereco = work.get('endereco') or (base.get('endereco') if base else "")
                        if 'telefones' in work and work.get('telefones'):
                            telefones = work['telefones']
                        else:
                            tel_ele = work.get('telefone_ele'); tel_ela = work.get('telefone_ela')
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
                            resultados_fail.append({"nome": linha, "endereco":"Formato não reconhecido","telefones":"— / —"})
                            continue
                        dados = consulta_like(ele, ela) or consulta_like(ela, ele)
                        if dados:
                            resultados_ok.append({"nome": f"{ele} e {ela}", "endereco": dados["endereco"], "telefones": dados["telefones"]})
                        else:
                            resultados_fail.append({"nome": f"{ele} e {ela}", "endereco":"Não encontrado","telefones":"— / —"})
                    except Exception as e:
                        app.logger.exception(f"Falha ao processar linha: {linha}")
                        resultados_fail.append({"nome": linha, "endereco":"Erro ao processar", "telefones": str(e)})
                cur.close()
            finally:
                conn.close()

    resultados = resultados_ok + resultados_fail
    return render_template("relatorio_casais.html", resultados=resultados)

# -----------------------------
# (Opcional) Handler para BuildError em url_for
# -----------------------------
@app.errorhandler(BuildError)
def handle_build_error(e):
    app.logger.error("BuildError em url_for: endpoint=%s, values=%s", getattr(e, 'endpoint', None), getattr(e, 'values', None))
    return redirect(url_for('index'))

# -----------------------------
# Main
# -----------------------------
if __name__ == "__main__":
    app.run(debug=True)

