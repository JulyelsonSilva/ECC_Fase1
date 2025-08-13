from flask import Flask, render_template, request, jsonify, redirect, url_for
import mysql.connector
from collections import defaultdict
import math
import re
from mysql.connector import errors as mysql_errors

app = Flask(__name__)

DB_CONFIG = {
    'host': 'db4free.net',
    'user': 'eccdivino2',
    'password': 'eccdivino2025',
    'database': 'eccdivinomcz2'
}

# -----------------------------
# Mapeamentos de Equipes
# -----------------------------
TEAM_MAP = {
    "sala":      {"rotulo": "Equipe de Sala - Coordenador/Apresentador", "filtro": "Sala"},
    "circulos":  {"rotulo": "Equipe de Círculos", "filtro": "Circulos"},
    "cafe":      {"rotulo": "Equipe Café e Minimercado", "filtro": "Café e Minimercado"},
    "compras":   {"rotulo": "Equipe Compras", "filtro": "Compras"},
    "acolhida":  {"rotulo": "Equipe Acolhida", "filtro": "Acolhida"},
    "ordem":     {"rotulo": "Equipe Ordem e Limpeza", "filtro": "Ordem e Limpeza"},
    "liturgia":  {"rotulo": "Equipe Liturgia e Vigilia", "filtro": "Liturgia e Vigilia"},
    "secretaria":{"rotulo": "Equipe Secretaria", "filtro": "Secretaria"},
    "cozinha":   {"rotulo": "Equipe Cozinha", "filtro": "Cozinha"},
    "visitacao": {"rotulo": "Equipe Visitação", "filtro": "Visitação"},
}

TEAM_LIMITS = {
    "Circulos": {"min": 5, "max": 5},
    "Café e Minimercado": {"min": 3, "max": 7},
    "Compras": {"min": 0, "max": 1},
    "Acolhida": {"min": 4, "max": 6},
    "Ordem e Limpeza": {"min": 3, "max": 7},
    "Liturgia e Vigilia": {"min": 2, "max": 6},
    "Secretaria": {"min": 3, "max": 5},
    "Cozinha": {"min": 7, "max": 9},
    "Visitação": {"min": 6, "max": 10},
}

# -----------------------------
# Rotas principais
# -----------------------------
@app.route('/')
def index():
    return render_template('index.html')

# -----------------------------
# ENCONTRISTAS (listagem + edição)
# -----------------------------
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

@app.route('/encontristas/<int:encontrista_id>/editar', methods=['GET', 'POST'])
def editar_encontrista(encontrista_id):
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor(dictionary=True)

    if request.method == 'POST':
        nome_completo_ele  = request.form.get('nome_completo_ele', '').strip()
        nome_completo_ela  = request.form.get('nome_completo_ela', '').strip()
        nome_usual_ele     = request.form.get('nome_usual_ele', '').strip()
        nome_usual_ela     = request.form.get('nome_usual_ela', '').strip()
        telefone_ele       = request.form.get('telefone_ele', '').strip()
        telefone_ela       = request.form.get('telefone_ela', '').strip()
        endereco           = request.form.get('endereco', '').strip()
        ecc_num            = request.form.get('ecc_num', '').strip()
        ano_raw            = request.form.get('ano', '').strip()
        anos_casados       = request.form.get('anos_casados', '').strip()
        cor_circulo        = request.form.get('cor_circulo', '').strip()
        casal_visitacao    = request.form.get('casal_visitacao', '').strip()
        ficha_num          = request.form.get('ficha_num', '').strip()
        aceitou            = request.form.get('aceitou', '').strip()
        observacao         = request.form.get('observacao', '').strip()
        observacao_extra   = request.form.get('observacao_extra', '').strip()

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
@app.route('/montagem')
def montagem():
    """
    Esquerda: 'Aberto' -> anos com pelo menos 1 registro status != 'Concluido'
    Direita:  'Concluído' -> anos onde TODOS têm status = 'Concluido'
    """
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
# Nova Montagem (pré-preenche dirigentes/CG/coord_equipes)
# -----------------------------
@app.route('/montagem/nova')
def nova_montagem():
    ano_preselecionado = request.args.get('ano', type=int)

    initial_data = {
        "dirigentes": {},   # {nome_equipe: {nome_ele, nome_ela, telefones, endereco}}
        "cg": None,         # {nome_ele, nome_ela, telefones, endereco}
        "coord_teams": {}   # { key_do_TEAM_MAP: {nome_ele, nome_ela, telefones, endereco} }
    }

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
            # Dirigentes (qualquer status exceto Recusou/Desistiu)
            for equipe in equipes_dir:
                cur.execute("""
                    SELECT nome_ele, nome_ela, telefones, endereco
                      FROM encontreiros
                     WHERE ano = %s AND equipe = %s
                       AND (status IS NULL OR UPPER(status) NOT IN ('RECUSOU','DESISTIU'))
                     ORDER BY id ASC
                     LIMIT 1
                """, (ano_preselecionado, equipe))
                r = cur.fetchone()
                if r:
                    initial_data["dirigentes"][equipe] = {
                        "nome_ele": r.get("nome_ele") or "",
                        "nome_ela": r.get("nome_ela") or "",
                        "telefones": r.get("telefones") or "",
                        "endereco": r.get("endereco") or ""
                    }

            # Coordenador Geral (apenas ABERTO)
            cur.execute("""
                SELECT nome_ele, nome_ela, telefones, endereco
                  FROM encontreiros
                 WHERE ano = %s
                   AND UPPER(equipe) = 'CASAL COORDENADOR GERAL'
                   AND UPPER(status) = 'ABERTO'
                 ORDER BY id DESC
                 LIMIT 1
            """, (ano_preselecionado,))
            r_cg = cur.fetchone()
            if r_cg:
                initial_data["cg"] = {
                    "nome_ele": r_cg.get("nome_ele") or "",
                    "nome_ela": r_cg.get("nome_ela") or "",
                    "telefones": r_cg.get("telefones") or "",
                    "endereco": r_cg.get("endereco") or "",
                }

            # Coordenadores de Equipe existentes (status ABERTO)
            for key, info in TEAM_MAP.items():
                rotulo = info["rotulo"]
                cur.execute("""
                    SELECT nome_ele, nome_ela, telefones, endereco
                      FROM encontreiros
                     WHERE ano = %s
                       AND equipe = %s
                       AND UPPER(coordenador) = 'SIM'
                       AND UPPER(status) = 'ABERTO'
                     ORDER BY id DESC
                     LIMIT 1
                """, (ano_preselecionado, rotulo))
                r_team = cur.fetchone()
                if r_team:
                    initial_data["coord_teams"][key] = {
                        "nome_ele": r_team.get("nome_ele") or "",
                        "nome_ela": r_team.get("nome_ela") or "",
                        "telefones": r_team.get("telefones") or "",
                        "endereco": r_team.get("endereco") or "",
                    }
        finally:
            try:
                cur.close()
                conn.close()
            except Exception:
                pass

    return render_template(
        'nova_montagem.html',
        ano_preselecionado=ano_preselecionado,
        initial_data=initial_data,
        team_map=TEAM_MAP
    )

# -----------------------------
# APIs da Montagem – Dirigentes / CG
# -----------------------------
@app.route('/api/buscar-casal', methods=['POST'])
def api_buscar_casal():
    """
    Busca p/ Dirigentes e Coord. de Equipe:
    1) encontreiros (mais recente) -> 2) encontristas (nomes usuais).
    """
    data = request.get_json(silent=True) or {}
    nome_ele = (data.get('nome_ele') or '').strip()
    nome_ela = (data.get('nome_ela') or '').strip()
    if not nome_ele or not nome_ela:
        return jsonify({"ok": False, "msg": "Informe nome_ele e nome_ela."}), 400

    conn = mysql.connector.connect(**DB_CONFIG)
    cur = conn.cursor(dictionary=True)
    try:
        # 1) ENCONTREIROS (mais recente)
        cur.execute("""
            SELECT ano, telefones, endereco
              FROM encontreiros
             WHERE nome_ele = %s AND nome_ela = %s
             ORDER BY ano DESC
             LIMIT 1
        """, (nome_ele, nome_ela))
        r = cur.fetchone()
        if r:
            return jsonify({
                "ok": True,
                "origem": "encontreiros",
                "ano": r["ano"],
                "telefones": r.get("telefones") or "",
                "endereco": r.get("endereco") or ""
            })

        # 2) ENCONTRISTAS (nomes usuais)
        cur.execute("""
            SELECT telefone_ele, telefone_ela, endereco, ano
              FROM encontristas
             WHERE nome_usual_ele = %s AND nome_usual_ela = %s
             ORDER BY ano DESC
             LIMIT 1
        """, (nome_ele, nome_ela))
        r2 = cur.fetchone()
        if r2:
            tel_ele = (r2.get('telefone_ele') or '').strip()
            tel_ela = (r2.get('telefone_ela') or '').strip()
            tels = " / ".join([t for t in [tel_ele, tel_ela] if t])
            return jsonify({
                "ok": True,
                "origem": "encontristas",
                "ano": r2.get("ano"),
                "telefones": tels or "",
                "endereco": r2.get("endereco") or ""
            })

        # 3) não encontrado
        return jsonify({"ok": False, "msg": "Casal não participou do ECC."}), 404
    finally:
        try:
            cur.close()
            conn.close()
        except Exception:
            pass

@app.route('/api/adicionar-dirigente', methods=['POST'])
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
        return jsonify({"ok": True})
    finally:
        try:
            cur.close()
            conn.close()
        except Exception:
            pass

@app.route('/api/buscar-cg', methods=['POST'])
def api_buscar_cg():
    data = request.get_json(silent=True) or {}
    nome_ele = (data.get('nome_ele') or '').strip()
    nome_ela = (data.get('nome_ela') or '').strip()
    if not nome_ele or not nome_ela:
        return jsonify({"ok": False, "msg": "Informe nome_ele e nome_ela."}), 400

    conn = mysql.connector.connect(**DB_CONFIG)
    cur = conn.cursor(dictionary=True)
    try:
        # Já trabalhou no ECC? (qualquer equipe)
        cur.execute("""
            SELECT ano, telefones, endereco
              FROM encontreiros
             WHERE nome_ele = %s AND nome_ela = %s
             ORDER BY ano DESC
             LIMIT 1
        """, (nome_ele, nome_ela))
        r = cur.fetchone()
        if not r:
            return jsonify({"ok": False, "msg": "Casal nunca trabalhou no ECC."}), 404

        # Já foi Coordenador Geral?
        cur.execute("""
            SELECT 1
              FROM encontreiros
             WHERE nome_ele = %s AND nome_ela = %s
               AND UPPER(equipe) = 'CASAL COORDENADOR GERAL'
             LIMIT 1
        """, (nome_ele, nome_ela))
        r2 = cur.fetchone()
        if r2:
            return jsonify({"ok": False, "msg": "Casal já foi Coordenador Geral."}), 409

        return jsonify({
            "ok": True,
            "ano_ref": r["ano"],
            "telefones": r.get("telefones") or "",
            "endereco": r.get("endereco") or ""
        })
    finally:
        try:
            cur.close()
            conn.close()
        except Exception:
            pass

@app.route('/api/adicionar-cg', methods=['POST'])
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
        # 1) valida "já trabalhou"
        cur.execute("""
            SELECT 1 FROM encontreiros
             WHERE nome_ele = %s AND nome_ela = %s
             LIMIT 1
        """, (nome_ele, nome_ela))
        if not cur.fetchone():
            return jsonify({"ok": False, "msg": "Casal nunca trabalhou no ECC."}), 404

        # 2) valida "nunca foi CG"
        cur.execute("""
            SELECT 1 FROM encontreiros
             WHERE nome_ele = %s AND nome_ela = %s
               AND UPPER(equipe) = 'CASAL COORDENADOR GERAL'
             LIMIT 1
        """, (nome_ele, nome_ela))
        if cur.fetchone():
            return jsonify({"ok": False, "msg": "Casal já foi Coordenador Geral."}), 409

        # 3) insere CG para o ano informado
        cur2 = conn.cursor()
        cur2.execute("""
            INSERT INTO encontreiros
                (ano, equipe, nome_ele, nome_ela, telefones, endereco, coordenador, status)
            VALUES
                (%s,  'Casal Coordenador Geral', %s, %s, %s, %s, 'Sim', 'Aberto')
        """, (int(ano), nome_ele, nome_ela, telefones, endereco))
        conn.commit()
        cur2.close()
        return jsonify({"ok": True})
    finally:
        try:
            cur.close()
            conn.close()
        except Exception:
            pass

# -----------------------------
# ENCONTREIROS (listagem – sem edição)
# -----------------------------
@app.route('/encontreiros')
def encontreiros():
    """
    Lista Encontreiros (somente registros ativos):
      - Inclui: status NULL, Aberto, Concluido
      - Exclui: Recusou, Desistiu
    Filtros opcionais: nome_ele, nome_ela, ano
    Agrupa por ano (desc) e ordena por equipe.
    """
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor(dictionary=True)

    nome_ele = (request.args.get('nome_ele', '') or '').strip()
    nome_ela = (request.args.get('nome_ela', '') or '').strip()
    ano_filtro = (request.args.get('ano', '') or '').strip()

    query = """
        SELECT id, ano, equipe, nome_ele, nome_ela, telefones, endereco, coordenador
          FROM encontreiros
         WHERE (status IS NULL OR UPPER(TRIM(status)) IN ('ABERTO','CONCLUIDO'))
    """
    params = []

    if nome_ele:
        query += " AND nome_ele LIKE %s"
        params.append(f"%{nome_ele}%")
    if nome_ela:
        query += " AND nome_ela LIKE %s"
        params.append(f"%{nome_ela}%")
    if ano_filtro:
        query += " AND ano = %s"
        params.append(ano_filtro)

    query += " ORDER BY ano DESC, equipe ASC, id ASC"

    cursor.execute(query, params)
    todos = cursor.fetchall()
    cursor.close()
    conn.close()

    # Agrupa por ano
    por_ano = defaultdict(list)
    for row in todos:
        por_ano[row['ano']].append(row)

    # Colunas visíveis (sem 'status')
    colunas_visiveis = ['equipe', 'nome_ele', 'nome_ela', 'telefones', 'endereco', 'coordenador']

    return render_template('encontreiros.html',
                           por_ano=por_ano,
                           colunas_visiveis=colunas_visiveis)

# -----------------------------
# Visão Equipes (com/sem links)
# -----------------------------
@app.route('/visao-equipes')
def visao_equipes():
    equipe = request.args.get('equipe', '')
    target = request.args.get('target', '')
    ano_montagem = request.args.get('ano_montagem', '')
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
                    a = row['ano']
                    equipe_txt = (row['equipe'] or '').upper()
                    nome_chave = f"{row['nome_ele']} e {row['nome_ela']}"
                    coordenador = (row.get('coordenador') or '').strip().lower() == 'sim'

                    if coordenador and all(x not in equipe_txt for x in ['BOA VONTADE', 'CANTO', 'SOM E PROJEÇÃO', 'RECEPÇÃO']):
                        dados_ano[a]['Coordenador'] = f"*{nome_chave}"
                    elif 'BOA VONTADE' in equipe_txt:
                        dados_ano[a]['Boa Vontade'] = nome_chave
                    elif 'CANTO' in equipe_txt:
                        if not dados_ano[a]['Canto 1']:
                            dados_ano[a]['Canto 1'] = nome_chave
                        elif not dados_ano[a]['Canto 2']:
                            dados_ano[a]['Canto 2'] = nome_chave
                    elif 'SOM E PROJEÇÃO' in equipe_txt:
                        if not dados_ano[a]['Som e Projeção 1']:
                            dados_ano[a]['Som e Projeção 1'] = nome_chave
                        elif not dados_ano[a]['Som e Projeção 2']:
                            dados_ano[a]['Som e Projeção 2'] = nome_chave
                    elif 'RECEPÇÃO' in equipe_txt:
                        dados_ano[a]['Recepção de Palestras'] = nome_chave

                for a, linha_dict in dados_ano.items():
                    linha = []
                    for col in colunas:
                        nome = linha_dict[col]
                        if nome.startswith("*"):
                            linha.append(nome)
                        elif nome in coordenadores_globais:
                            linha.append(f"~{nome}")
                        else:
                            linha.append(nome)
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
                        if nome.startswith("*"):
                            linha.append(nome)
                        elif nome in coordenadores_globais:
                            linha.append(f"~{nome}")
                        else:
                            linha.append(nome)
                    while len(linha) < len(colunas):
                        linha.append('')
                    tabela[a] = linha

        cursor.close()
        conn.close()

    # target/ano_montagem vão para o template para decidir se mostra links
    return render_template(
        'visao_equipes.html',
        equipe_selecionada=equipe,
        tabela=tabela,
        colunas=colunas,
        target=target,
        ano_montagem=ano_montagem
    )

# Link a partir da visão de equipes de volta para nova_montagem
@app.route('/visao-equipes/select')
def visao_equipes_select():
    ano_montagem = request.args.get('ano_montagem', type=int)
    target = request.args.get('target', '')
    ele = request.args.get('ele', '')
    ela = request.args.get('ela', '')
    if not (ano_montagem and target and ele and ela):
        return redirect(url_for('visao_equipes'))
    # volta para nova_montagem preenchendo o alvo
    return redirect(url_for('nova_montagem', ano=ano_montagem, target=target,
                            selecionar_ele=ele, selecionar_ela=ela))

# -----------------------------
# Visão do Casal
# -----------------------------
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
# Relatório de Casais
# -----------------------------
@app.route('/relatorio-casais', methods=['GET', 'POST'])
def relatorio_casais():
    def split_casal(line: str):
        """Aceita ';' | ' e ' | 1º espaço."""
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

    # --- helpers robustos para diferenças de esquema ---
    def fetch_encontreiros(cur, a_pref, b_pref):
        """
        Tenta consultar ENCONTREIROS usando (nome_ele, nome_ela).
        Se a coluna não existir, tenta (nome_usual_ele, nome_usual_ela).
        Retorna dict com chaves padrão: endereco, telefones, telefone_ele, telefone_ela (ou None).
        """
        # 1) nome_ele / nome_ela
        try:
            cur.execute(
                "SELECT endereco, telefones, telefone_ele, telefone_ela "
                "FROM encontreiros "
                "WHERE (nome_ele LIKE %s AND nome_ela LIKE %s) "
                "   OR (nome_ele LIKE %s AND nome_ela LIKE %s) "
                "ORDER BY ano DESC LIMIT 1",
                (a_pref, b_pref, b_pref, a_pref)
            )
            row = cur.fetchone()
            return row
        except mysql_errors.ProgrammingError as e:
            # 2) fallback: nome_usual_ele / nome_usual_ela
            if e.errno != 1054:
                raise
            try:
                cur.execute(
                    "SELECT endereco, telefones, telefone_ele, telefone_ela "
                    "FROM encontreiros "
                    "WHERE (nome_usual_ele LIKE %s AND nome_usual_ela LIKE %s) "
                    "   OR (nome_usual_ele LIKE %s AND nome_usual_ela LIKE %s) "
                    "ORDER BY ano DESC LIMIT 1",
                    (a_pref, b_pref, b_pref, a_pref)
                )
                row = cur.fetchone()
                return row
            except mysql_errors.ProgrammingError:
                return None  # esquema muito diferente
        except mysql_errors.Error:
            return None

    def fetch_encontristas(cur, a_pref, b_pref):
        """
        Tenta consultar ENCONTRISTAS preferindo (nome_usual_ele, nome_usual_ela),
        caindo para (nome_ele, nome_ela) se necessário.
        Retorna dict com chaves: endereco, telefone_ele, telefone_ela (ou None).
        """
        # 1) nome_usual_ele / nome_usual_ela + nome_ele/nome_ela
        try:
            cur.execute(
                "SELECT endereco, telefone_ele, telefone_ela "
                "FROM encontristas "
                "WHERE (nome_usual_ele LIKE %s AND nome_usual_ela LIKE %s) "
                "   OR (nome_usual_ele LIKE %s AND nome_usual_ela LIKE %s) "
                "   OR (nome_ele LIKE %s AND nome_ela LIKE %s) "
                "   OR (nome_ele LIKE %s AND nome_ela LIKE %s) "
                "LIMIT 1",
                (a_pref, b_pref, b_pref, a_pref, a_pref, b_pref, b_pref, a_pref)
            )
            row = cur.fetchone()
            return row
        except mysql_errors.ProgrammingError as e:
            # 2) fallback: só nome_ele / nome_ela
            if e.errno != 1054:
                raise
            try:
                cur.execute(
                    "SELECT endereco, telefone_ele, telefone_ela "
                    "FROM encontristas "
                    "WHERE (nome_ele LIKE %s AND nome_ela LIKE %s) "
                    "   OR (nome_ele LIKE %s AND nome_ela LIKE %s) "
                    "LIMIT 1",
                    (a_pref, b_pref, b_pref, a_pref)
                )
                row = cur.fetchone()
                return row
            except mysql_errors.Error:
                return None
        except mysql_errors.Error:
            return None

    resultados_ok, resultados_fail = [], []
    titulo  = (request.form.get("titulo") or "Relatório de Casais") if request.method == 'POST' else "Relatório de Casais"
    entrada = (request.form.get("lista_nomes", "") or "") if request.method == 'POST' else ""

    if request.method == 'POST' and entrada.strip():
        linhas = [l.strip() for l in entrada.splitlines() if l.strip()]

        conn = mysql.connector.connect(**DB_CONFIG)
        cur = conn.cursor(dictionary=True)
        try:
            for linha in linhas:
                try:
                    ele, ela = split_casal(linha)
                    if not ele or not ela:
                        resultados_fail.append({"nome": linha, "endereco": "Formato não reconhecido", "telefones": "— / —"})
                        continue

                    # LIKE de prefixo (rápido e aceita primeiros nomes)
                    a_pref = f"{ele.strip()}%"
                    b_pref = f"{ela.strip()}%"

                    work = fetch_encontreiros(cur, a_pref, b_pref)
                    base = fetch_encontristas(cur, a_pref, b_pref)

                    # Consolidação (prefere dados do 'work'; senão usa 'base')
                    if work or base:
                        if work:
                            endereco = (work.get('endereco') if work else None) or (base.get('endereco') if base else "")
                            if work.get('telefones'):
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
                        else:
                            endereco = base.get('endereco') or "—"
                            telefones = f"{(base.get('telefone_ele') or '—')} / {(base.get('telefone_ela') or '—')}"
                        resultados_ok.append({"nome": f"{ele} e {ela}", "endereco": endereco or "—", "telefones": telefones or "— / —"})
                    else:
                        resultados_fail.append({"nome": f"{ele} e {ela}", "endereco": "Não encontrado", "telefones": "— / —"})
                except Exception as e:
                    app.logger.exception(f"Falha ao processar linha: {linha}")
                    resultados_fail.append({"nome": linha, "endereco": "Erro ao processar", "telefones": str(e)})
        finally:
            try:
                cur.close()
                conn.close()
            except Exception:
                pass

    # Encontrados primeiro; não encontrados por último
    resultados = resultados_ok + resultados_fail
    return render_template("relatorio_casais.html", resultados=resultados, titulo=titulo, entrada=entrada)


# -----------------------------
# Montagem de Equipe (integrantes)
# -----------------------------
@app.route('/equipe-montagem')
def equipe_montagem():
    """
    Tela de montagem de equipe (sempre renderiza TODAS as caixas até o máximo teórico da equipe).
    Parâmetros:
      - ?ano=YYYY
      - ?equipe=<Circulos|Cozinha|...>  (chave de filtro da TEAM_MAP)
    """
    ano = request.args.get('ano', type=int)
    equipe_filtro = (request.args.get('equipe') or '').strip()

    # Resolve rótulo que é gravado no banco a partir do filtro curto
    equipe_final = None
    for _key, info in TEAM_MAP.items():
        if info['filtro'].lower() == equipe_filtro.lower():
            equipe_final = info['rotulo']
            break
    if not equipe_final:
        equipe_final = equipe_filtro or 'Equipe'

    # Limites FIXOS definidos no dicionário
    limites_cfg = TEAM_LIMITS.get(equipe_filtro, TEAM_LIMITS.get(equipe_final, {}))
    limites = {
        "min": int(limites_cfg.get('min', 0)),
        "max": int(limites_cfg.get('max', 8)),
    }

    # Membros já montados (não coordenadores) no ano/equipe
    # Inclui todos os ativos (qualquer status) EXCETO Recusou/Desistiu
    conn = mysql.connector.connect(**DB_CONFIG)
    cur = conn.cursor(dictionary=True)
    membros_existentes = []
    try:
        cur.execute("""
            SELECT id, nome_ele, nome_ela, telefones, endereco, status
              FROM encontreiros
             WHERE ano = %s
               AND equipe = %s
               AND (coordenador IS NULL OR UPPER(TRIM(coordenador)) <> 'SIM')
               AND (status IS NULL OR UPPER(TRIM(status)) NOT IN ('RECUSOU','DESISTIU'))
             ORDER BY id ASC
        """, (ano, equipe_final))
        membros_existentes = cur.fetchall()
    finally:
        try:
            cur.close(); conn.close()
        except Exception:
            pass

    # Sugestões do ano anterior (encontristas ano-1) — EXCLUI quem já está montado no ano atual em QUALQUER equipe
    # (considera montado se status != Recusou/Desistiu)
    sugestoes_prev_ano = []
    if ano:
        conn = mysql.connector.connect(**DB_CONFIG)
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute("""
                SELECT e.nome_usual_ele, e.nome_usual_ela, e.telefone_ele, e.telefone_ela, e.endereco
                  FROM encontristas e
                 WHERE e.ano = %s
                   AND NOT EXISTS (
                         SELECT 1
                           FROM encontreiros w
                          WHERE w.ano = %s
                            AND w.nome_ele = e.nome_usual_ele
                            AND w.nome_ela = e.nome_usual_ela
                            AND (w.status IS NULL OR UPPER(TRIM(w.status)) NOT IN ('RECUSOU','DESISTIU'))
                       )
                 ORDER BY e.nome_usual_ele, e.nome_usual_ela
            """, (ano - 1, ano))
            for r in cur.fetchall():
                tel_ele = (r.get('telefone_ele') or '').strip()
                tel_ela = (r.get('telefone_ela') or '').strip()
                tels = " / ".join([t for t in [tel_ele, tel_ela] if t])
                sugestoes_prev_ano.append({
                    "nome_ele": r.get('nome_usual_ele') or '',
                    "nome_ela": r.get('nome_usual_ela') or '',
                    "telefones": tels,
                    "endereco": r.get('endereco') or ''
                })
        finally:
            try:
                cur.close(); conn.close()
            except Exception:
                pass

    return render_template(
        'equipe_montagem.html',
        ano=ano,
        equipe=equipe_filtro,          # ex.: 'Circulos'
        equipe_final=equipe_final,     # ex.: 'Equipe de Círculos'
        limites=limites,               # {"min":x,"max":y} (FIXO)
        membros_existentes=membros_existentes,
        sugestoes_prev_ano=sugestoes_prev_ano
    )



# --- APIs auxiliares da montagem de equipe ---

def _casal_ja_no_ano(conn, ano:int, nome_ele:str, nome_ela:str) -> bool:
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT 1 FROM encontreiros
             WHERE ano = %s AND nome_ele = %s AND nome_ela = %s
               AND (status IS NULL OR UPPER(status) NOT IN ('RECUSOU','DESISTIU'))
             LIMIT 1
        """, (ano, nome_ele, nome_ela))
        return cur.fetchone() is not None
    finally:
        cur.close()

@app.route('/api/check-casal-equipe', methods=['POST'])
def api_check_casal_equipe():
    data = request.get_json(silent=True) or {}
    ano = data.get('ano')
    equipe_final = (data.get('equipe_final') or '').strip()
    nome_ele = (data.get('nome_ele') or '').strip()
    nome_ela = (data.get('nome_ela') or '').strip()
    if not (ano and equipe_final and nome_ele and nome_ela):
        return jsonify({"ok": False, "msg": "Parâmetros insuficientes."}), 400

    conn = mysql.connector.connect(**DB_CONFIG)
    cur = conn.cursor(dictionary=True)
    try:
        # Já coordenou essa equipe?
        cur.execute("""
            SELECT 1 FROM encontreiros
             WHERE nome_ele = %s AND nome_ela = %s
               AND equipe = %s
               AND UPPER(coordenador) = 'SIM'
             LIMIT 1
        """, (nome_ele, nome_ela, equipe_final))
        ja_coord = cur.fetchone() is not None

        # Já trabalhou nessa equipe (qualquer ano)?
        cur.execute("""
            SELECT 1 FROM encontreiros
             WHERE nome_ele = %s AND nome_ela = %s
               AND equipe = %s
             LIMIT 1
        """, (nome_ele, nome_ela, equipe_final))
        trabalhou_antes = cur.fetchone() is not None

        # Já está montado no ano atual (qualquer equipe)?
        ja_no_ano = _casal_ja_no_ano(conn, int(ano), nome_ele, nome_ela)

        # Telefones/endereço mais recentes
        # 1) encontreiros
        cur.execute("""
            SELECT telefones, endereco
              FROM encontreiros
             WHERE nome_ele = %s AND nome_ela = %s
             ORDER BY ano DESC
             LIMIT 1
        """, (nome_ele, nome_ela))
        r = cur.fetchone()
        telefones = (r.get('telefones') if r else '') or ''
        endereco = (r.get('endereco') if r else '') or ''

        if not r:
            # 2) encontristas
            cur.execute("""
                SELECT telefone_ele, telefone_ela, endereco
                  FROM encontristas
                 WHERE nome_usual_ele = %s AND nome_usual_ela = %s
                 ORDER BY ano DESC
                 LIMIT 1
            """, (nome_ele, nome_ela))
            r2 = cur.fetchone()
            if r2:
                tel_ele = (r2.get('telefone_ele') or '').strip()
                tel_ela = (r2.get('telefone_ela') or '').strip()
                telefones = " / ".join([t for t in [tel_ele, tel_ela] if t])
                endereco = r2.get('endereco') or ''

        return jsonify({
            "ok": True,
            "ja_coordenador": ja_coord,
            "trabalhou_antes": trabalhou_antes,
            "ja_no_ano": ja_no_ano,
            "telefones": telefones,
            "endereco": endereco
        })
    finally:
        try:
            cur.close()
            conn.close()
        except Exception:
            pass

@app.route('/api/add-membro-equipe', methods=['POST'])
def api_add_membro_equipe():
    data = request.get_json(silent=True) or {}
    ano = data.get('ano')
    equipe_final = (data.get('equipe_final') or '').strip()
    nome_ele = (data.get('nome_ele') or '').strip()
    nome_ela = (data.get('nome_ela') or '').strip()
    telefones = (data.get('telefones') or '').strip()
    endereco = (data.get('endereco') or '').strip()
    confirmar_repeticao = bool(data.get('confirmar_repeticao'))

    if not (ano and equipe_final and nome_ele and nome_ela):
        return jsonify({"ok": False, "msg": "Parâmetros insuficientes."}), 400

    conn = mysql.connector.connect(**DB_CONFIG)
    cur = conn.cursor(dictionary=True)
    try:
        # Já coordenador dessa equipe? Bloqueia.
        cur.execute("""
            SELECT 1 FROM encontreiros
             WHERE nome_ele = %s AND nome_ela = %s
               AND equipe = %s
               AND UPPER(coordenador) = 'SIM'
             LIMIT 1
        """, (nome_ele, nome_ela, equipe_final))
        if cur.fetchone():
            return jsonify({"ok": False, "msg": "Casal já foi coordenador desta equipe."}), 409

        # Já montado no ano atual? Bloqueia.
        if _casal_ja_no_ano(conn, int(ano), nome_ele, nome_ela):
            return jsonify({"ok": False, "msg": "Casal já está montado neste ano."}), 409

        # Já trabalhou nessa equipe antes (como membro)? Solicita confirmação.
        cur.execute("""
            SELECT 1 FROM encontreiros
             WHERE nome_ele = %s AND nome_ela = %s
               AND equipe = %s
             LIMIT 1
        """, (nome_ele, nome_ela, equipe_final))
        if cur.fetchone() and not confirmar_repeticao:
            return jsonify({"ok": False, "needs_confirm": True,
                            "msg": "Casal já trabalhou na equipe. Confirmar para montar novamente?"})

        # Insere membro
        cur2 = conn.cursor()
        cur2.execute("""
            INSERT INTO encontreiros
                (ano, equipe, nome_ele, nome_ela, telefones, endereco, coordenador, status)
            VALUES
                (%s,  %s,     %s,       %s,       %s,         %s,       'Não',      'Aberto')
        """, (int(ano), equipe_final, nome_ele, nome_ela, telefones, endereco))
        conn.commit()
        cur2.close()
        return jsonify({"ok": True})
    finally:
        try:
            cur.close()
            conn.close()
        except Exception:
            pass

# Alterar status (Recusou/Desistiu) com justificativa obrigatória
@app.route('/api/marcar-status-dirigente', methods=['POST'])
def api_marcar_status_dirigente():
    """
    Marca o registro ABERTO de um ano/equipe (dirigente ou coord. de equipe) como Recusou/Desistiu com observação.
    Body: {ano, equipe, novo_status, observacao}
    """
    data = request.get_json(silent=True) or {}
    ano = data.get('ano')
    equipe = (data.get('equipe') or '').strip()
    novo_status = (data.get('novo_status') or '').strip()
    observacao = (data.get('observacao') or '').strip()

    if not (ano and equipe and novo_status in ('Recusou','Desistiu') and observacao):
        return jsonify({"ok": False, "msg": "Parâmetros inválidos. Observação é obrigatória."}), 400

    conn = mysql.connector.connect(**DB_CONFIG)
    cur = conn.cursor()
    try:
        cur.execute("""
            UPDATE encontreiros
               SET status = %s, observacao = %s
             WHERE ano = %s
               AND equipe = %s
               AND UPPER(status) = 'ABERTO'
             ORDER BY id DESC
             LIMIT 1
        """, (novo_status, observacao, int(ano), equipe))
        conn.commit()
        if cur.rowcount == 0:
            return jsonify({"ok": False, "msg": "Nenhum registro ABERTO encontrado para alterar."}), 404
        return jsonify({"ok": True})
    finally:
        try:
            cur.close()
            conn.close()
        except Exception:
            pass

@app.route('/api/marcar-status-membro', methods=['POST'])
def api_marcar_status_membro():
    """
    Marca um membro específico por ID como Recusou/Desistiu com observação.
    Body: {id, novo_status, observacao}
    """
    data = request.get_json(silent=True) or {}
    _id = data.get('id')
    novo_status = (data.get('novo_status') or '').strip()
    observacao = (data.get('observacao') or '').strip()

    if not (_id and novo_status in ('Recusou','Desistiu') and observacao):
        return jsonify({"ok": False, "msg": "Parâmetros inválidos. Observação é obrigatória."}), 400

    conn = mysql.connector.connect(**DB_CONFIG)
    cur = conn.cursor()
    try:
        cur.execute("""
            UPDATE encontreiros
               SET status = %s, observacao = %s
             WHERE id = %s
               AND (status IS NULL OR UPPER(status) IN ('ABERTO','ACEITO'))
             LIMIT 1
        """, (novo_status, observacao, int(_id)))
        conn.commit()
        if cur.rowcount == 0:
            return jsonify({"ok": False, "msg": "Registro não encontrado ou não alterável."}), 404
        return jsonify({"ok": True})
    finally:
        try:
            cur.close()
            conn.close()
        except Exception:
            pass

# (Opcional futuro) concluir montagem do ANO inteiro
@app.route('/api/concluir-montagem-ano', methods=['POST'])
def api_concluir_montagem_ano():
    """
    Marca TODOS os registros 'Aberto' do ano como 'Concluido'.
    """
    data = request.get_json(silent=True) or {}
    ano = data.get('ano')
    if not ano:
        return jsonify({"ok": False, "msg": "Ano obrigatório."}), 400

    conn = mysql.connector.connect(**DB_CONFIG)
    cur = conn.cursor()
    try:
        cur.execute("""
            UPDATE encontreiros
               SET status = 'Concluido'
             WHERE ano = %s
               AND UPPER(status) = 'ABERTO'
        """, (int(ano),))
        conn.commit()
        return jsonify({"ok": True, "alterados": cur.rowcount})
    finally:
        try:
            cur.close()
            conn.close()
        except Exception:
            pass

# -----------------------------
# Main
# -----------------------------
if __name__ == "__main__":
    app.run(debug=True)
