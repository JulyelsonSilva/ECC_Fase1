from flask import Flask, render_template, request, jsonify, redirect, url_for
import mysql.connector
from collections import defaultdict
import math
import os, re
from difflib import SequenceMatcher
from mysql.connector import errors as mysql_errors

app = Flask(__name__)

# =========================
# Config do Banco
# =========================
DB_CONFIG = {
    'host': 'db4free.net',
    'user': 'eccdivino2',
    'password': 'eccdivino2025',
    'database': 'eccdivinomcz2'
}

def db_conn():
    return mysql.connector.connect(**DB_CONFIG)

def safe_fetch_one(cur, sql, params):
    """SELECT ... LIMIT 1 com tratamento; retorna dict ou None."""
    try:
        cur.execute(sql, params)
        return cur.fetchone()
    except (mysql_errors.ProgrammingError, mysql_errors.DatabaseError,
            mysql_errors.InterfaceError, mysql_errors.OperationalError):
        return None

# =========================
# Admin helpers
# =========================
def _get_db():
    # Alias para reutilizar o que você já tem
    return db_conn()

def _admin_ok():
    token = request.args.get("token") or request.form.get("token")
    return bool(token) and token == os.environ.get("ADMIN_TOKEN", "")

# =========================
# Constantes de Equipes
# =========================
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
    "Sala": {"min": 4, "max": 6},
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

# --- KPI: contagem de integrantes por equipe (exclui Coordenador; exclui Recusou/Desistiu) ---
@app.route('/api/team-kpis')
def api_team_kpis():
    ano = request.args.get('ano', type=int)
    if not ano:
        return jsonify({"ok": False, "msg": "Ano obrigatório."}), 400

    # rotulo -> filtro (ex.: 'Equipe de Círculos' -> 'Circulos')
    rotulo_to_filtro = {}
    for k, v in TEAM_MAP.items():
        rotulo_to_filtro[v["rotulo"]] = v["filtro"]

    data = {}
    conn = mysql.connector.connect(**DB_CONFIG)
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute("""
            SELECT equipe, COUNT(*) AS n
              FROM encontreiros
             WHERE ano = %s
               AND (coordenador IS NULL OR UPPER(TRIM(coordenador)) <> 'SIM')
               AND (status IS NULL OR UPPER(TRIM(status)) NOT IN ('RECUSOU','DESISTIU'))
             GROUP BY equipe
        """, (ano,))
        for r in cur.fetchall():
            rot = (r.get("equipe") or "").strip()
            n = int(r.get("n") or 0)
            filtro = rotulo_to_filtro.get(rot)
            if filtro:
                data[filtro] = n
    finally:
        try:
            cur.close()
            conn.close()
        except Exception:
            pass

    return jsonify({"ok": True, "counts": data})

# Equipes como opções de <select> (rótulos usados no banco)
TEAM_CHOICES = [info["rotulo"] for info in TEAM_MAP.values()]

# =========================
# Constantes de Palestras
# =========================
PALESTRAS_TITULOS = [
    "Plano de Deus",
    "Testem.Plano de Deus",
    "Harmonia Conjugal",
    "Diálogo c/ filhos",
    "Penitência",
    "Testem. Jovem",
    "Ceia Eucarística",
    "N.SrªVida da Família",
    "Testem. Ceia Eucarística",
    "Fé Revezes da Vida",
    "Sentido da Vida",
    "Oração",
    "Corresponsabilidade",
    "Vivência do Sacramento do Matrimônio",
    "O casal Cristão no Mundo de Hoje",
]
# Palestras "solo" (não-casal)
PALESTRAS_SOLO = {"Penitência", "Testem. Jovem", "Ceia Eucarística"}

# =========================
# Rotas principais
# =========================
@app.route('/')
def index():
    return render_template('index.html')

# =========================
# ENCONTRISTAS (listagem + edição)
# =========================
@app.route('/encontristas')
def encontristas():
    conn = db_conn()
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
    conn = db_conn()
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

# =========================
# MONTAGEM (Aberto x Concluído)
# =========================
@app.route('/montagem')
def montagem():
    conn = db_conn()
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

    anos_concluidos, anos_aberto = [], []
    for r in rows:
        item = {"ano": r["ano"], "qtd_concluido": int(r["qtd_concluido"]), "total": int(r["total"])}
        if item["total"] > 0 and item["qtd_concluido"] == item["total"]:
            anos_concluidos.append(item)
        else:
            anos_aberto.append(item)

    return render_template('montagem.html',
                           anos_aberto=anos_aberto,
                           anos_concluidos=anos_concluidos)

# =========================
# Nova Montagem + APIs Dirigentes/CG (mesmo conteúdo que você já tinha)
# =========================
@app.route('/montagem/nova')
def nova_montagem():
    ano_preselecionado = request.args.get('ano', type=int)

    initial_data = {
        "dirigentes": {},
        "cg": None,
        "coord_teams": {}
    }

    if ano_preselecionado:
        equipes_dir = [
            "Equipe Dirigente - MONTAGEM",
            "Equipe Dirigente -FICHAS",          # <- manter exatamente assim
            "Equipe Dirigente - FINANÇAS",
            "Equipe Dirigente - PALESTRA",
            "Equipe Dirigente - PÓS ENCONTRO",
        ]
        conn = db_conn()
        cur = conn.cursor(dictionary=True)
        try:
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

@app.route('/api/buscar-casal', methods=['POST'])
def api_buscar_casal():
    data = request.get_json(silent=True) or {}
    nome_ele = (data.get('nome_ele') or '').strip()
    nome_ela = (data.get('nome_ela') or '').strip()
    if not nome_ele or not nome_ela:
        return jsonify({"ok": False, "msg": "Informe nome_ele e nome_ela."}), 400

    conn = db_conn()
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

    conn = db_conn()
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

    conn = db_conn()
    cur = conn.cursor(dictionary=True)
    try:
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

    conn = db_conn()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute("""
            SELECT 1 FROM encontreiros
             WHERE nome_ele = %s AND nome_ela = %s
             LIMIT 1
        """, (nome_ele, nome_ela))
        if not cur.fetchone():
            return jsonify({"ok": False, "msg": "Casal nunca trabalhou no ECC."}), 404

        cur.execute("""
            SELECT 1 FROM encontreiros
             WHERE nome_ele = %s AND nome_ela = %s
               AND UPPER(equipe) = 'CASAL COORDENADOR GERAL'
             LIMIT 1
        """, (nome_ele, nome_ela))
        if cur.fetchone():
            return jsonify({"ok": False, "msg": "Casal já foi Coordenador Geral."}), 409

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
# -----------------------------------------
# API: contagem de integrantes por equipe
# usado por nova_montagem (sem contar coordenador)
# -----------------------------------------
@app.route('/api/equipe-counts')
def api_equipe_counts():
    ano = request.args.get('ano', type=int)
    if not ano:
        return jsonify({"ok": False, "msg": "Ano obrigatório.", "counts": {}}), 400

    # Vamos carregar todas as linhas do ano, ativas,
    # e agrupar no Python para mapear corretamente a equipe "Sala"
    # (que possui subfunções como Canto, Boa Vontade etc).
    conn = mysql.connector.connect(**DB_CONFIG)
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute("""
            SELECT equipe, coordenador
            FROM encontreiros
            WHERE ano = %s
              AND (status IS NULL OR UPPER(TRIM(status)) NOT IN ('RECUSOU','DESISTIU'))
        """, (ano,))
        rows = cur.fetchall()
    finally:
        try:
            cur.close()
            conn.close()
        except Exception:
            pass

    # Mapa rotulo -> filtro (igual ao TEAM_MAP do app)
    rotulo_to_filtro = {info["rotulo"]: info["filtro"] for info in TEAM_MAP.values()}

    # Inicializa todas as equipes com zero
    counts = {info["filtro"]: 0 for info in TEAM_MAP.values()}

    for r in rows:
        equipe_txt = (r.get("equipe") or "").strip()
        is_coord = (r.get("coordenador") or "").strip().upper() == "SIM"

        # Não contar coordenadores no total de integrantes
        if is_coord:
            continue

        eq_upper = equipe_txt.upper()

        # Regra especial para SALA: qualquer equipe que contenha "SALA"
        # (inclui Boa Vontade, Canto, Som/Projeção, Recepção etc.)
        if "SALA" in eq_upper:
            counts["Sala"] = counts.get("Sala", 0) + 1
            continue

        # Demais equipes: tentar casar exatamente pelo rótulo gravado
        filtro = rotulo_to_filtro.get(equipe_txt)
        if filtro:
            counts[filtro] = counts.get(filtro, 0) + 1
        # Se não casou, ignoramos a linha (pode ser alguma variação não mapeada)

    return jsonify({"ok": True, "counts": counts})

# =========================
# IMLANTAÇÃO
# =========================

@app.route('/implantacao')
def implantacao_painel():
    conn = db_conn()
    cur = conn.cursor(dictionary=True)
    cur.execute("""
        SELECT 
            ano,
            SUM(CASE WHEN UPPER(TRIM(status))='CONCLUIDO' THEN 1 ELSE 0 END) AS qtd_concluido,
            COUNT(*) AS total
        FROM implantacao
        GROUP BY ano
        ORDER BY ano DESC
    """)
    rows = cur.fetchall() or []
    cur.close(); conn.close()

    anos_concluidos, anos_aberto = [], []
    for r in rows:
        item = {"ano": r["ano"], "qtd_concluido": int(r["qtd_concluido"] or 0), "total": int(r["total"] or 0)}
        if item["total"] > 0 and item["qtd_concluido"] == item["total"]:
            anos_concluidos.append(item)
        else:
            anos_aberto.append(item)

    return render_template('implantacao_painel.html',
                           anos_aberto=anos_aberto,
                           anos_concluidos=anos_concluidos)
@app.route('/implantacao/nova')
def implantacao_nova():
    ano_pre = request.args.get('ano', type=int)

    # sugestões dos encontristas do ano anterior que AINDA não estão na implantacao do ano
    sugestoes_prev_ano = []
    if ano_pre:
        conn = db_conn(); cur = conn.cursor(dictionary=True)
        try:
            cur.execute("""
                SELECT e.nome_usual_ele, e.nome_usual_ela, e.telefone_ele, e.telefone_ela, e.endereco
                  FROM encontristas e
                 WHERE e.ano = %s
                   AND NOT EXISTS (
                         SELECT 1 FROM implantacao i
                          WHERE i.ano = %s
                            AND i.nome_ele = e.nome_usual_ele
                            AND i.nome_ela = e.nome_usual_ela
                            AND (i.status IS NULL OR UPPER(TRIM(i.status)) NOT IN ('RECUSOU','DESISTIU'))
                   )
                 ORDER BY e.nome_usual_ele, e.nome_usual_ela
            """, (ano_pre - 1, ano_pre))
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
            try: cur.close(); conn.close()
            except Exception: pass

    return render_template(
        'implantacao.html',
        ano_preselecionado=ano_pre,
        team_map=TEAM_MAP,
        team_limits=TEAM_LIMITS,
        team_choices=TEAM_CHOICES,
        sugestoes_prev_ano=sugestoes_prev_ano
    )

@app.route('/api/implantacao/equipe-counts')
def api_implantacao_equipe_counts():
    ano = request.args.get('ano', type=int)
    if not ano:
        return jsonify({"ok": False, "msg": "Ano obrigatório.", "counts": {}}), 400

    conn = db_conn(); cur = conn.cursor(dictionary=True)
    try:
        cur.execute("""
            SELECT equipe, coordenador
              FROM implantacao
             WHERE ano = %s
               AND (status IS NULL OR UPPER(TRIM(status)) NOT IN ('RECUSOU','DESISTIU'))
        """, (ano,))
        rows = cur.fetchall() or []
    finally:
        try: cur.close(); conn.close()
        except Exception: pass

    rotulo_to_filtro = {info["rotulo"]: info["filtro"] for info in TEAM_MAP.values()}
    counts = {info["filtro"]: 0 for info in TEAM_MAP.values()}

    for r in rows:
        eq = (r.get("equipe") or "").strip()
        is_coord = (r.get("coordenador") or "").strip().upper() == "SIM"
        if is_coord:
            continue
        filtro = rotulo_to_filtro.get(eq)
        if filtro:
            counts[filtro] = counts.get(filtro, 0) + 1

    return jsonify({"ok": True, "counts": counts})

@app.route('/api/implantacao/check-casal', methods=['POST'])
def api_implantacao_check_casal():
    data = request.get_json(silent=True) or {}
    ano = data.get('ano')
    equipe = (data.get('equipe') or '').strip()
    nome_ele = (data.get('nome_ele') or '').strip()
    nome_ela = (data.get('nome_ela') or '').strip()
    if not (ano and equipe and nome_ele and nome_ela):
        return jsonify({"ok": False, "msg": "Parâmetros insuficientes."}), 400

    conn = db_conn(); cur = conn.cursor(dictionary=True)
    try:
        # Já está montado neste ano (na própria implantacao)?
        cur.execute("""
            SELECT 1 FROM implantacao
             WHERE ano = %s AND nome_ele = %s AND nome_ela = %s
               AND (status IS NULL OR UPPER(status) NOT IN ('RECUSOU','DESISTIU'))
             LIMIT 1
        """, (int(ano), nome_ele, nome_ela))
        ja_no_ano = cur.fetchone() is not None

        # Já trabalhou nesta equipe (qualquer ano)?
        cur.execute("""
            SELECT 1 FROM implantacao
             WHERE nome_ele = %s AND nome_ela = %s AND equipe = %s
             LIMIT 1
        """, (nome_ele, nome_ela, equipe))
        trabalhou_antes = cur.fetchone() is not None

        # Já foi coordenador nesta equipe?
        cur.execute("""
            SELECT 1 FROM implantacao
             WHERE nome_ele = %s AND nome_ela = %s AND equipe = %s
               AND UPPER(coordenador)='SIM'
             LIMIT 1
        """, (nome_ele, nome_ela, equipe))
        ja_coord = cur.fetchone() is not None

        # Telefones/endereço mais recentes: implantacao -> encontreiros -> encontristas
        telefones, endereco = '', ''
        cur.execute("""
            SELECT telefones, endereco
              FROM implantacao
             WHERE nome_ele = %s AND nome_ela = %s
             ORDER BY ano DESC, id DESC
             LIMIT 1
        """, (nome_ele, nome_ela))
        r = cur.fetchone()
        if r:
            telefones = (r.get('telefones') or '').strip()
            endereco = r.get('endereco') or ''
        else:
            cur.execute("""
                SELECT telefones, endereco
                  FROM encontreiros
                 WHERE nome_ele = %s AND nome_ela = %s
                 ORDER BY ano DESC, id DESC
                 LIMIT 1
            """, (nome_ele, nome_ela))
            r2 = cur.fetchone()
            if r2:
                telefones = (r2.get('telefones') or '').strip()
                endereco = r2.get('endereco') or ''
            else:
                cur.execute("""
                    SELECT telefone_ele, telefone_ela, endereco
                      FROM encontristas
                     WHERE nome_usual_ele = %s AND nome_usual_ela = %s
                     ORDER BY ano DESC
                     LIMIT 1
                """, (nome_ele, nome_ela))
                r3 = cur.fetchone()
                if r3:
                    tel_ele = (r3.get('telefone_ele') or '').strip()
                    tel_ela = (r3.get('telefone_ela') or '').strip()
                    telefones = " / ".join([t for t in [tel_ele, tel_ela] if t])
                    endereco = r3.get('endereco') or ''

        return jsonify({
            "ok": True,
            "ja_no_ano": ja_no_ano,
            "trabalhou_antes": trabalhou_antes,
            "ja_coordenador": ja_coord,
            "telefones": telefones,
            "endereco": endereco
        })
    finally:
        try: cur.close(); conn.close()
        except Exception: pass
@app.route('/api/implantacao/add-membro', methods=['POST'])
def api_implantacao_add_membro():
    data = request.get_json(silent=True) or {}
    ano = data.get('ano')
    equipe = (data.get('equipe') or '').strip()
    nome_ele = (data.get('nome_ele') or '').strip()
    nome_ela = (data.get('nome_ela') or '').strip()
    telefones = (data.get('telefones') or '').strip()
    endereco  = (data.get('endereco') or '').strip()
    coord_sim = str(data.get('coordenador') or 'Não').strip()   # aceita True/False ou 'Sim'/'Não'
    confirmar_repeticao = bool(data.get('confirmar_repeticao'))

    if not (ano and equipe and nome_ele and nome_ela):
        return jsonify({"ok": False, "msg": "Parâmetros insuficientes."}), 400

    if coord_sim.lower() in ('true','1','sim','s'):
        coord_val = 'Sim'
    else:
        coord_val = 'Não'

    conn = db_conn(); cur = conn.cursor(dictionary=True)
    try:
        # já no ano?
        cur.execute("""
            SELECT 1 FROM implantacao
             WHERE ano = %s AND nome_ele = %s AND nome_ela = %s
               AND (status IS NULL OR UPPER(status) IN ('ABERTO','ACEITO','CONCLUIDO'))
             LIMIT 1
        """, (int(ano), nome_ele, nome_ela))
        if cur.fetchone():
            return jsonify({"ok": False, "msg": "Casal já está lançado neste ano (Implantação)."}), 409

        # já trabalhou nesta equipe antes?
        cur.execute("""
            SELECT 1 FROM implantacao
             WHERE nome_ele = %s AND nome_ela = %s AND equipe = %s
             LIMIT 1
        """, (nome_ele, nome_ela, equipe))
        if cur.fetchone() and not confirmar_repeticao:
            return jsonify({"ok": False, "needs_confirm": True,
                            "msg": "Casal já trabalhou nesta equipe (Implantação). Confirmar para lançar novamente?"})

        cur2 = conn.cursor()
        cur2.execute("""
            INSERT INTO implantacao
                (ano, equipe, nome_ele, nome_ela, telefones, endereco, coordenador, status)
            VALUES
                (%s,  %s,     %s,       %s,       %s,         %s,       %s,           'Aberto')
        """, (int(ano), equipe, nome_ele, nome_ela, telefones, endereco, coord_val))
        conn.commit()
        new_id = cur2.lastrowid
        cur2.close()
        return jsonify({"ok": True, "id": new_id})
    finally:
        try: cur.close(); conn.close()
        except Exception: pass
@app.route('/api/implantacao/marcar-status', methods=['POST'])
def api_implantacao_marcar_status():
    data = request.get_json(silent=True) or {}
    _id = data.get('id')
    novo_status = (data.get('novo_status') or '').strip().title()
    observacao  = (data.get('observacao') or '').strip()

    if not (_id and novo_status in ('Recusou','Desistiu') and observacao):
        return jsonify({"ok": False, "msg": "Parâmetros inválidos. Observação é obrigatória."}), 400

    conn = db_conn(); cur = conn.cursor()
    try:
        cur.execute("""
            UPDATE implantacao
               SET status=%s, observacao=%s
             WHERE id=%s
               AND (status IS NULL OR UPPER(status) IN ('ABERTO','ACEITO'))
             LIMIT 1
        """, (novo_status, observacao, int(_id)))
        conn.commit()
        if cur.rowcount == 0:
            return jsonify({"ok": False, "msg": "Registro não encontrado ou não alterável."}), 404
        return jsonify({"ok": True})
    finally:
        try: cur.close(); conn.close()
        except Exception: pass
@app.route('/api/implantacao/concluir-ano', methods=['POST'])
def api_implantacao_concluir_ano():
    data = request.get_json(silent=True) or {}
    ano = data.get('ano')
    if not ano:
        return jsonify({"ok": False, "msg": "Ano obrigatório."}), 400

    conn = db_conn(); cur = conn.cursor()
    try:
        cur.execute("""
            UPDATE implantacao
               SET status='Concluido'
             WHERE ano=%s
               AND UPPER(status)='ABERTO'
        """, (int(ano),))
        conn.commit()
        return jsonify({"ok": True, "alterados": cur.rowcount})
    finally:
        try: cur.close(); conn.close()
        except Exception: pass


# =========================
# ENCONTREIROS (listagem – sem edição)
# =========================
@app.route('/encontreiros')
def encontreiros():
    conn = db_conn()
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

    por_ano = defaultdict(list)
    for row in todos:
        por_ano[row['ano']].append(row)

    colunas_visiveis = ['equipe', 'nome_ele', 'nome_ela', 'telefones', 'endereco', 'coordenador']

    return render_template('encontreiros.html',
                           por_ano=por_ano,
                           colunas_visiveis=colunas_visiveis)

# =========================
# VISÃO DE EQUIPES
# =========================
@app.route('/visao-equipes')
def visao_equipes():
    equipe = request.args.get('equipe', '')
    target = request.args.get('target', '')
    ano_montagem = request.args.get('ano_montagem', '')
    tabela = {}
    colunas = []

    if equipe:
        conn = db_conn()
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

    return render_template(
        'visao_equipes.html',
        equipe_selecionada=equipe,
        tabela=tabela,
        colunas=colunas,
        target=target,
        ano_montagem=ano_montagem
    )

@app.route('/visao-equipes/select')
def visao_equipes_select():
    # Aceita 'ano_montagem' OU 'ano'
    ano_montagem = request.args.get('ano_montagem', type=int) or request.args.get('ano', type=int)

    # Aceita 'target' OU 'ret_target'
    target = (request.args.get('target') or request.args.get('ret_target') or '').strip()

    # Aceita múltiplos nomes para os selecionados
    ele = (request.args.get('ele')
           or request.args.get('selecionar_ele')
           or request.args.get('nome_ele') or '').strip()
    ela = (request.args.get('ela')
           or request.args.get('selecionar_ela')
           or request.args.get('nome_ela') or '').strip()

    # Se algo essencial faltar, volta para a visão de equipes (com o que tiver)
    if not (ano_montagem and target and ele and ela):
        return redirect(url_for('visao_equipes', target=target, ano_montagem=ano_montagem))

    # Redireciona de volta pra Nova Montagem com os nomes selecionados
    return redirect(url_for('nova_montagem',
                            ano=ano_montagem,
                            target=target,
                            selecionar_ele=ele,
                            selecionar_ela=ela))

# =========================
# Visão do Casal (ATUALIZADA: inclui palestras do casal)
# =========================
@app.route('/visao-casal')
def visao_casal():
    nome_ele = (request.args.get("nome_ele") or "").strip()
    nome_ela = (request.args.get("nome_ela") or "").strip()

    dados_encontrista = {}
    dados_encontreiros = []
    dados_palestras = []
    erro = None

    if not nome_ele or not nome_ela:
        erro = "Informe ambos os nomes para realizar a busca."
        return render_template(
            "visao_casal.html",
            nome_ele=nome_ele, nome_ela=nome_ela,
            dados_encontrista=None, dados_encontreiros=[],
            dados_palestras=[],
            erro=erro
        )

    conn = db_conn()
    cursor = conn.cursor(dictionary=True)

    try:
        # ENCONTRISTA (nomes usuais)
        cursor.execute("""
            SELECT ano, endereco, telefone_ele, telefone_ela
              FROM encontristas 
             WHERE nome_usual_ele = %s AND nome_usual_ela = %s
             ORDER BY ano DESC
             LIMIT 1
        """, (nome_ele, nome_ela))
        e = cursor.fetchone()
        if e:
            dados_encontrista = {
                "ano_encontro": e["ano"],
                "endereco": e.get("endereco") or "",
                "telefones": f"{e.get('telefone_ele') or ''} / {e.get('telefone_ela') or ''}".strip(" /")
            }

        # ENCONTREIROS (histórico de trabalho)
        cursor.execute("""
            SELECT ano, equipe, coordenador, endereco, telefones
              FROM encontreiros 
             WHERE nome_ele = %s AND nome_ela = %s
             ORDER BY ano DESC, equipe ASC
        """, (nome_ele, nome_ela))
        dados_encontreiros = cursor.fetchall() or []

        # PALESTRAS do CASAL (exclui títulos solo – pois não são “casal”)
        # Considera equivalência por nome exato (mesmo padrão de cadastro).
        format_titles = tuple(t for t in PALESTRAS_TITULOS if t not in PALESTRAS_SOLO)
        if format_titles:
            in_clause = ", ".join(["%s"] * len(format_titles))
            sql = f"""
                SELECT ano, palestra
                  FROM palestras
                 WHERE LOWER(nome_ele) = LOWER(%s)
                   AND LOWER(COALESCE(nome_ela,'')) = LOWER(%s)
                   AND palestra IN ({in_clause})
                 ORDER BY ano DESC
            """
            params = [nome_ele, nome_ela] + list(format_titles)
            cursor.execute(sql, params)
            dados_palestras = cursor.fetchall() or []

        # Se nada encontrado em nenhuma das duas tabelas, sinaliza
        if not e and not dados_encontreiros and not dados_palestras:
            erro = "Casal não encontrado."

    finally:
        cursor.close()
        conn.close()

    # --------- Monta estruturas para o template novo (anos / por_ano_*) ----------
    por_ano_trabalhos = defaultdict(list)
    for r in (dados_encontreiros or []):
        por_ano_trabalhos[r["ano"]].append({
            "equipe": r.get("equipe") or "",
            "coordenador": r.get("coordenador") or ""
        })

    por_ano_palestras = defaultdict(list)
    for p in (dados_palestras or []):
        por_ano_palestras[p["ano"]].append({"palestra": p.get("palestra") or ""})

    anos_set = set(por_ano_trabalhos.keys()) | set(por_ano_palestras.keys())
    anos = sorted(anos_set, reverse=True)

    return render_template(
        "visao_casal.html",
        nome_ele=nome_ele,
        nome_ela=nome_ela,
        dados_encontrista=dados_encontrista if dados_encontrista else None,
        dados_encontreiros=dados_encontreiros,
        dados_palestras=dados_palestras,
        anos=anos,
        por_ano_trabalhos=por_ano_trabalhos,
        por_ano_palestras=por_ano_palestras,
        erro=erro
    )

# =========================
# Relatório de Casais (mantido)
# =========================
@app.route('/relatorio-casais', methods=['GET', 'POST'])
def relatorio_casais():
    def split_casal(line: str):
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

    resultados_ok, resultados_fail = [], []
    titulo  = (request.form.get("titulo") or "Relatório de Casais") if request.method == 'POST' else "Relatório de Casais"
    entrada = (request.form.get("lista_nomes", "") or "") if request.method == 'POST' else ""

    if request.method == 'POST' and entrada.strip():
        linhas = [l.strip() for l in entrada.splitlines() if l.strip()]

        try:
            conn = mysql.connector.connect(
                host=DB_CONFIG['host'],
                user=DB_CONFIG['user'],
                password=DB_CONFIG['password'],
                database=DB_CONFIG['database'],
                connection_timeout=5
            )
            cur = conn.cursor(dictionary=True)
        except mysql_errors.Error:
            for linha in linhas:
                resultados_fail.append({"nome": linha, "endereco": "Erro de conexão com o banco", "telefones": "— / —"})
            return render_template("relatorio_casais.html", resultados=resultados_fail, titulo=titulo, entrada=entrada)

        try:
            def consulta_prefix_like(a: str, b: str):
                a = (a or "").strip(); b = (b or "").strip()
                if not a or not b:
                    return None
                a_pref, b_pref = f"{a}%", f"{b}%"

                work = safe_fetch_one(
                    cur,
                    "SELECT endereco, telefones "
                    "FROM encontreiros "
                    "WHERE (nome_ele LIKE %s AND nome_ela LIKE %s) "
                    "   OR (nome_ele LIKE %s AND nome_ela LIKE %s) "
                    "ORDER BY ano DESC LIMIT 1",
                    (a_pref, b_pref, b_pref, a_pref)
                )
                if work is None:
                    work = safe_fetch_one(
                        cur,
                        "SELECT endereco, telefones "
                        "FROM encontreiros "
                        "WHERE (nome_usual_ele LIKE %s AND nome_usual_ela LIKE %s) "
                        "   OR (nome_usual_ele LIKE %s AND nome_usual_ela LIKE %s) "
                        "ORDER BY ano DESC LIMIT 1",
                        (a_pref, b_pref, b_pref, a_pref)
                    )

                base = safe_fetch_one(
                    cur,
                    "SELECT endereco, telefone_ele, telefone_ela "
                    "FROM encontristas "
                    "WHERE (nome_usual_ele LIKE %s AND nome_usual_ela LIKE %s) "
                    "   OR (nome_usual_ele LIKE %s AND nome_usual_ela LIKE %s) "
                    "   OR (nome_ele LIKE %s AND nome_ela LIKE %s) "
                    "   OR (nome_ele LIKE %s AND nome_ela LIKE %s) "
                    "LIMIT 1",
                    (a_pref, b_pref, b_pref, a_pref, a_pref, b_pref, b_pref, a_pref)
                )
                if base is None:
                    base = safe_fetch_one(
                        cur,
                        "SELECT endereco, telefone_ele, telefone_ela "
                        "FROM encontristas "
                        "WHERE (nome_ele LIKE %s AND nome_ela LIKE %s) "
                        "   OR (nome_ele LIKE %s AND nome_ela LIKE %s) "
                        "LIMIT 1",
                        (a_pref, b_pref, b_pref, a_pref)
                    )

                if work or base:
                    if work:
                        endereco = (work.get('endereco') if work else None) or (base.get('endereco') if base else "")
                        telefones = work.get('telefones')
                        if not telefones:
                            if base:
                                telefones = f"{(base.get('telefone_ele') or '—')} / {(base.get('telefone_ela') or '—')}"
                            else:
                                telefones = "— / —"
                    else:
                        endereco = base.get('endereco') or "—"
                        telefones = f"{(base.get('telefone_ele') or '—')} / {(base.get('telefone_ela') or '—')}"
                    return {"endereco": endereco or "—", "telefones": telefones or "— / —"}

                return None

            for linha in linhas:
                ele, ela = split_casal(linha)
                if not ele or not ela:
                    resultados_fail.append({"nome": linha, "endereco": "Formato não reconhecido", "telefones": "— / —"})
                    continue

                dados = consulta_prefix_like(ele, ela)
                if dados:
                    resultados_ok.append({"nome": f"{ele} e {ela}", **dados})
                else:
                    resultados_fail.append({"nome": f"{ele} e {ela}", "endereco": "Não encontrado", "telefones": "— / —"})
        finally:
            try:
                cur.close()
                conn.close()
            except Exception:
                pass

    resultados = resultados_ok + resultados_fail
    return render_template("relatorio_casais.html", resultados=resultados, titulo=titulo, entrada=entrada)

# =========================
# Montagem de Equipe (integrantes) + APIs auxiliares
# =========================
@app.route('/equipe-montagem')
def equipe_montagem():
    """
    Tela de montagem de equipe.
    Parâmetros:
      - ?ano=YYYY
      - ?equipe=<Circulos|Cozinha|Sala|...>  (chave de filtro do TEAM_MAP ou nome direto)
    """
    ano = request.args.get('ano', type=int)
    equipe_filtro = (request.args.get('equipe') or '').strip()

    # Resolve rótulo padrão via TEAM_MAP (ex.: Circulos -> "Equipe de Círculos")
    equipe_final = None
    for _key, info in TEAM_MAP.items():
        if info['filtro'].lower() == equipe_filtro.lower():
            equipe_final = info['rotulo']
            break
    if not equipe_final:
        equipe_final = equipe_filtro or 'Equipe'

    # Caso especial: SALA tem subequipes fixas
    if equipe_filtro.lower() == 'sala':
        SALA_DB = {
            "Canto": "Equipe de Sala - Canto",
            "Som e Projeção": "Equipe de Sala - Som e Projeção",
            "Boa Vontade": "Equipe de Sala - Boa Vontade",
            "Recepção Palestrantes": "Equipe de Sala - Recepção Palestrantes",
        }
        # Ordem fixa dos 6 slots
        sala_order = [
            ("Canto 1", "Canto"),
            ("Canto 2", "Canto"),
            ("Som e Projeção 1", "Som e Projeção"),
            ("Som e Projeção 2", "Som e Projeção"),
            ("Boa Vontade", "Boa Vontade"),
            ("Recepção Palestrantes", "Recepção Palestrantes"),
        ]

        # Carrega existentes do ano para QUALQUER subequipe de Sala (exceto coordenador e Recusou/Desistiu)
        conn = mysql.connector.connect(**DB_CONFIG)
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(f"""
                SELECT id, ano, equipe, nome_ele, nome_ela, telefones, endereco, status
                  FROM encontreiros
                 WHERE ano = %s
                   AND equipe IN (%s, %s, %s, %s)
                   AND (coordenador IS NULL OR UPPER(TRIM(coordenador)) <> 'SIM')
                   AND (status IS NULL OR UPPER(TRIM(status)) NOT IN ('RECUSOU','DESISTIU'))
                 ORDER BY id ASC
            """, (
                ano,
                SALA_DB["Canto"],
                SALA_DB["Som e Projeção"],
                SALA_DB["Boa Vontade"],
                SALA_DB["Recepção Palestrantes"],
            ))
            rows = cur.fetchall()

            # Agrupa por subequipe
            buckets = {k: [] for k in SALA_DB.keys()}
            for r in rows:
                eq = (r.get("equipe") or "")
                for k, dbname in SALA_DB.items():
                    if eq == dbname:
                        buckets[k].append(r)
                        break

            # Sugestão para Recepção Palestrantes: Dirigente - PALESTRA do mesmo ano
            cur.execute("""
                SELECT nome_ele, nome_ela, telefones, endereco
                  FROM encontreiros
                 WHERE ano = %s
                   AND UPPER(equipe) = 'EQUIPE DIRIGENTE - PALESTRA'
                   AND (status IS NULL OR UPPER(TRIM(status)) NOT IN ('RECUSOU','DESISTIU'))
                 ORDER BY id DESC
                 LIMIT 1
            """, (ano,))
            pref_recepcao = cur.fetchone() or {}
        finally:
            try:
                cur.close(); conn.close()
            except Exception:
                pass

        # Monta a lista de slots com existing por posição (Canto tem 2, Som tem 2)
        sala_slots = []
        use_index = {"Canto": 0, "Som e Projeção": 0, "Boa Vontade": 0, "Recepção Palestrantes": 0}
        for label, kind in sala_order:
            lst = buckets.get(kind, [])
            idx = use_index[kind]
            existing = lst[idx] if idx < len(lst) else None
            use_index[kind] = idx + 1
            sala_slots.append({
                "label": label,                # ex.: "Canto 1"
                "kind": kind,                  # ex.: "Canto"
                "equipe_db": SALA_DB[kind],    # ex.: "Equipe de Sala - Canto"
                "existing": existing or None,
            })

        # Sugestões do ano anterior (igual às outras equipes)
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
                           -- sem filtro de status: apareceu no ano => não sugerir
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

        # Limites fixos para Sala: 6 slots
        limites = {"min": 6, "max": 6}

        return render_template(
            'equipe_montagem_sala.html',
            ano=ano,
            limites=limites,
            sala_slots=sala_slots,
            sugestoes_prev_ano=sugestoes_prev_ano,
            pref_recepcao=pref_recepcao
        )

    # ------------------ fluxo padrão p/ outras equipes ------------------
    limites_cfg = TEAM_LIMITS.get(equipe_filtro, TEAM_LIMITS.get(equipe_final, {}))
    limites = {
        "min": int(limites_cfg.get('min', 0)),
        "max": int(limites_cfg.get('max', 8)),
    }

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

    # Sugestões do ano anterior (idem de antes)
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
        equipe=equipe_filtro,
        equipe_final=equipe_final,
        limites=limites,
        membros_existentes=membros_existentes,
        sugestoes_prev_ano=sugestoes_prev_ano
    )


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

    conn = db_conn()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute("""
            SELECT 1 FROM encontreiros
             WHERE nome_ele = %s AND nome_ela = %s
               AND equipe = %s
               AND UPPER(coordenador) = 'SIM'
             LIMIT 1
        """, (nome_ele, nome_ela, equipe_final))
        ja_coord = cur.fetchone() is not None

        cur.execute("""
            SELECT 1 FROM encontreiros
             WHERE nome_ele = %s AND nome_ela = %s
               AND equipe = %s
             LIMIT 1
        """, (nome_ele, nome_ela, equipe_final))
        trabalhou_antes = cur.fetchone() is not None

        ja_no_ano = _casal_ja_no_ano(conn, int(ano), nome_ele, nome_ela)

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

    conn = db_conn()
    cur = conn.cursor(dictionary=True)
    try:
        # bloqueios (iguais aos seus)
        cur.execute("""
            SELECT 1 FROM encontreiros
             WHERE nome_ele = %s AND nome_ela = %s
               AND equipe = %s
               AND UPPER(coordenador) = 'SIM'
             LIMIT 1
        """, (nome_ele, nome_ela, equipe_final))
        if cur.fetchone():
            return jsonify({"ok": False, "msg": "Casal já foi coordenador desta equipe."}), 409

        if _casal_ja_no_ano(conn, int(ano), nome_ele, nome_ela):
            return jsonify({"ok": False, "msg": "Casal já está montado neste ano."}), 409

        cur.execute("""
            SELECT 1 FROM encontreiros
             WHERE nome_ele = %s AND nome_ela = %s
               AND equipe = %s
             LIMIT 1
        """, (nome_ele, nome_ela, equipe_final))
        if cur.fetchone() and not confirmar_repeticao:
            return jsonify({"ok": False, "needs_confirm": True,
                            "msg": "Casal já trabalhou na equipe. Confirmar para montar novamente?"})

        cur2 = conn.cursor()
        cur2.execute("""
            INSERT INTO encontreiros
                (ano, equipe, nome_ele, nome_ela, telefones, endereco, coordenador, status)
            VALUES
                (%s,  %s,     %s,       %s,       %s,         %s,       'Não',      'Aberto')
        """, (int(ano), equipe_final, nome_ele, nome_ela, telefones, endereco))
        conn.commit()
        new_id = cur2.lastrowid
        cur2.close()
        return jsonify({"ok": True, "id": new_id})   # <<<<<< devolve o id
    finally:
        try:
            cur.close()
            conn.close()
        except Exception:
            pass
# Alterar status (Recusou/Desistiu) para DIRIGENTES/CG com justificativa obrigatória
@app.route('/api/marcar-status-dirigente', methods=['POST'])
def api_marcar_status_dirigente():
    """
    Marca o registro ABERTO de um ano/equipe (dirigente ou coord. de equipe) como Recusou/Desistiu com observação.
    Body JSON: { "ano": 2025, "equipe": "<rótulo exato>", "novo_status": "Recusou"|"Desistiu", "observacao": "texto" }
    """
    data = request.get_json(silent=True) or {}
    ano = data.get('ano')
    equipe = (data.get('equipe') or '').strip()
    novo_status = (data.get('novo_status') or '').strip()
    observacao = (data.get('observacao') or '').strip()

    if not (ano and equipe and novo_status in ('Recusou', 'Desistiu') and observacao):
        return jsonify({"ok": False, "msg": "Parâmetros inválidos. Observação é obrigatória."}), 400

    conn = mysql.connector.connect(**DB_CONFIG)
    cur = conn.cursor()
    try:
        # Atualiza o registro mais recente ABERTO daquele ano/equipe
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
    data = request.get_json(silent=True) or {}
    _id = data.get('id')
    novo_status = (data.get('novo_status') or '').strip()
    observacao = (data.get('observacao') or '').strip()

    if not (_id and novo_status in ('Recusou','Desistiu') and observacao):
        return jsonify({"ok": False, "msg": "Parâmetros inválidos. Observação é obrigatória."}), 400

    conn = db_conn()
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

@app.route('/api/concluir-montagem-ano', methods=['POST'])
def api_concluir_montagem_ano():
    data = request.get_json(silent=True) or {}
    ano = data.get('ano')
    if not ano:
        return jsonify({"ok": False, "msg": "Ano obrigatório."}), 400

    conn = db_conn()
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

# =========================
# Organograma
# =========================
@app.route('/organograma')
def organograma():
    return render_template('organograma.html')

@app.route('/dados-organograma')
def dados_organograma():
    ano = request.args.get("ano", type=int)
    if not ano:
        return jsonify([])
    conn = db_conn()
    cursor = conn.cursor(dictionary=True)
    try:
        cursor.execute("""
            SELECT equipe, nome_ele, nome_ela, coordenador
              FROM encontreiros
             WHERE ano = %s
               AND (status IS NULL OR UPPER(TRIM(status)) IN ('ABERTO','CONCLUIDO'))
        """, (ano,))
        dados = cursor.fetchall()
    finally:
        cursor.close()
        conn.close()
    return jsonify(dados)

# =========================
# Palestras – Painel / Nova / APIs / Palestrantes (mantido conforme combinado)
# =========================
@app.route('/palestras')
def palestras_painel():
    """Painel: anos em Aberto x Concluído na tabela 'palestras'."""
    conn = db_conn()
    cur = conn.cursor(dictionary=True)
    cur.execute("""
        SELECT ano,
               SUM(CASE WHEN UPPER(TRIM(status))='CONCLUIDO' THEN 1 ELSE 0 END) AS qtd_concluido,
               COUNT(*) AS total,
               SUM(CASE WHEN UPPER(TRIM(status))='ABERTO' THEN 1 ELSE 0 END) AS qtd_aberto
          FROM palestras
         GROUP BY ano
         ORDER BY ano DESC
    """)
    rows = cur.fetchall() or []
    cur.close(); conn.close()

    anos_concluidos, anos_aberto = [], []
    for r in rows:
        item = {
            "ano": r["ano"],
            "qtd_concluido": int(r["qtd_concluido"] or 0),
            "total": int(r["total"] or 0),
            "qtd_aberto": int(r["qtd_aberto"] or 0)
        }
        if item["total"] > 0 and item["qtd_aberto"] == 0:
            anos_concluidos.append(item)
        else:
            anos_aberto.append(item)

    return render_template('palestras_painel.html',
                           anos_aberto=anos_aberto,
                           anos_concluidos=anos_concluidos)

@app.route('/palestras/nova')
def palestras_nova():
    """
    Tela de montagem das palestras para um ano.
    Querystring opcional: ?ano=YYYY (pré-seleciona o ano).
    Passa ao template:
      - ano_preselecionado (int|None)
      - titulos (lista)
      - solo_titulos (lista)
      - existentes (dict por título com dados já cadastrados no ano)
      - tem_abertos (bool) -> se há registros com status 'Aberto' no ano
    """
    ano_preselecionado = request.args.get('ano', type=int)
    existentes = {}
    tem_abertos = False

    if ano_preselecionado:
        conn = mysql.connector.connect(**DB_CONFIG)
        cur = conn.cursor(dictionary=True)
        try:
            # Carrega registros do ano; OBS: a coluna é 'palestra' (não 'titulo')
            cur.execute("""
                SELECT id, palestra, nome_ele, nome_ela, status
                  FROM palestras
                 WHERE ano = %s
                 ORDER BY id DESC
            """, (ano_preselecionado,))
            rows = cur.fetchall() or []

            for r in rows:
                t = r.get("palestra") or ""
                if t and t not in existentes:
                    existentes[t] = {
                        "id": r.get("id"),
                        "nome_ele": r.get("nome_ele"),
                        "nome_ela": r.get("nome_ela"),
                        "status": r.get("status"),
                    }
                st = (r.get("status") or "").strip().lower()
                if st == "aberto":
                    tem_abertos = True
        finally:
            try:
                cur.close()
                conn.close()
            except Exception:
                pass

    return render_template(
        'nova_palestras.html',
        ano_preselecionado=ano_preselecionado,
        titulos=PALESTRAS_TITULOS,
        solo_titulos=list(PALESTRAS_SOLO),
        existentes=existentes,
        tem_abertos=tem_abertos
    )


@app.route('/api/palestras/buscar', methods=['POST'])
def api_palestras_buscar():
    """
    Body: { palestra, nome_ele, nome_ela }
    Regras:
      - Só permite casal que já foi encontrista OU encontreiros.
      - Retorna telefones/endereço mais recentes (encontreiros -> encontristas).
      - Retorna repeticoes = quantas vezes já deu essa 'palestra' (0..N).
      - Bloqueia se repeticoes >= 5.
    """
    data = request.get_json(silent=True) or {}
    palestra = (data.get('palestra') or '').strip()
    nome_ele = (data.get('nome_ele') or '').strip()
    nome_ela = (data.get('nome_ela') or '').strip()

    if not (palestra and nome_ele and nome_ela):
        return jsonify({"ok": False, "msg": "Parâmetros insuficientes."}), 400

    conn = mysql.connector.connect(**DB_CONFIG)
    cur = conn.cursor(dictionary=True)
    try:
        # 1) Verifica se é (encontrista OU encontreiros)
        cur.execute("""
            SELECT 1 FROM encontristas
             WHERE nome_usual_ele = %s AND nome_usual_ela = %s
             LIMIT 1
        """, (nome_ele, nome_ela))
        e1 = cur.fetchone() is not None

        if not e1:
            cur.execute("""
                SELECT 1 FROM encontreiros
                 WHERE nome_ele = %s AND nome_ela = %s
                 LIMIT 1
            """, (nome_ele, nome_ela))
            e2 = cur.fetchone() is not None
        else:
            e2 = True

        if not e2:
            return jsonify({"ok": False, "msg": "Casal precisa ser encontrista ou já ter trabalhado no ECC."}), 403

        # 2) Telefones/endereço mais recentes (encontreiros -> encontristas)
        telefones, endereco = '', ''
        cur.execute("""
            SELECT telefones, endereco
              FROM encontreiros
             WHERE nome_ele = %s AND nome_ela = %s
             ORDER BY ano DESC
             LIMIT 1
        """, (nome_ele, nome_ela))
        r = cur.fetchone()
        if r:
            telefones = (r.get('telefones') or '').strip()
            endereco = r.get('endereco') or ''
        else:
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

        # 3) Repetições desta palestra (coluna é 'palestra')
        cur.execute("""
            SELECT COUNT(*) AS n
              FROM palestras
             WHERE palestra = %s AND nome_ele = %s AND nome_ela = %s
        """, (palestra, nome_ele, nome_ela))
        n = (cur.fetchone() or {}).get("n", 0) or 0
        n = int(n)

        if n >= 5:
            return jsonify({"ok": False, "repeticoes": n,
                            "msg": "Limite de 5 repetições atingido para esta palestra."}), 409

        return jsonify({
            "ok": True,
            "telefones": telefones,
            "endereco": endereco,
            "repeticoes": n
        })
    finally:
        try:
            cur.close()
            conn.close()
        except Exception:
            pass


@app.route('/api/palestras/adicionar', methods=['POST'])
def api_palestras_adicionar():
    """
    Body (solo): { ano, palestra, nome_ele }      -> status 'Aberto'
    Body (casal): { ano, palestra, nome_ele, nome_ela, telefones?, endereco? } -> status 'Aberto'
    Revalida limite 5 antes de gravar.
    """
    data = request.get_json(silent=True) or {}
    ano = data.get('ano')
    palestra = (data.get('palestra') or '').strip()
    nome_ele = (data.get('nome_ele') or '').strip()
    nome_ela = (data.get('nome_ela') or '').strip()

    if not (ano and palestra and nome_ele):
        return jsonify({"ok": False, "msg": "Parâmetros insuficientes."}), 400

    # Solo (Penitência, Testem. Jovem, Ceia Eucarística) não exige nome_ela
    solo = palestra in PALESTRAS_SOLO
    if (not solo) and not nome_ela:
        return jsonify({"ok": False, "msg": "Informe Nome (Ela)."}), 400

    conn = mysql.connector.connect(**DB_CONFIG)
    cur = conn.cursor(dictionary=True)
    try:
        # bloqueio repetição >=5
        if not solo:
            cur.execute("""
                SELECT COUNT(*) AS n
                  FROM palestras
                 WHERE palestra = %s AND nome_ele = %s AND nome_ela = %s
            """, (palestra, nome_ele, nome_ela))
            n = (cur.fetchone() or {}).get("n", 0) or 0
            if int(n) >= 5:
                return jsonify({"ok": False, "msg": "Limite de 5 repetições atingido para esta palestra."}), 409

        cur2 = conn.cursor()
        if solo:
            cur2.execute("""
                INSERT INTO palestras (ano, palestra, nome_ele, status, observacao)
                VALUES (%s, %s, %s, 'Aberto', NULL)
            """, (int(ano), palestra, nome_ele))
        else:
            cur2.execute("""
                INSERT INTO palestras (ano, palestra, nome_ele, nome_ela, status, observacao)
                VALUES (%s, %s, %s, %s, 'Aberto', NULL)
            """, (int(ano), palestra, nome_ele, nome_ela))
        conn.commit()
        cur2.close()
        return jsonify({"ok": True})
    finally:
        try:
            cur.close()
            conn.close()
        except Exception:
            pass


@app.route('/api/palestras/encerrar', methods=['POST'])
def api_palestras_encerrar():
    """
    Fecha o ano de palestras: tudo que estiver 'Aberto' vira 'Concluido'.
    Body: { "ano": 2025 }
    """
    data = request.get_json(silent=True) or {}
    ano = data.get('ano')
    if not ano:
        return jsonify({"ok": False, "msg": "Ano obrigatório."}), 400

    conn = mysql.connector.connect(**DB_CONFIG)
    cur = conn.cursor()
    try:
        cur.execute("""
            UPDATE palestras
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

# --- Alias retrocompatível (/api/palestras/encerrar-ano) ---
# (deixe ESTA linha logo depois da função acima)
app.add_url_rule(
    '/api/palestras/encerrar-ano',
    endpoint='api_palestras_encerrar_ano',   # nome de endpoint antigo, se algum template usar url_for(...)
    view_func=api_palestras_encerrar,
    methods=['POST']
)

# >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
# NOVA ROTA ADICIONADA: marcar Recusou/Desistiu em PALESTRAS (com justificativa)
# Endpoint: api_palestras_marcar_status
# >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
@app.route('/api/palestras/marcar-status', methods=['POST'])
def api_palestras_marcar_status():
    """
    Marca um registro de palestra como Recusou/Desistiu com justificativa.

    Formato recomendado (por ID):
      { "id": 123, "novo_status": "Recusou"|"Desistiu", "observacao": "texto" }

    Alternativo (sem ID, usa chaves lógicas e pega o mais recente alterável):
      { "ano": 2025, "palestra": "Harmonia Conjugal", "nome_ele": "João", "nome_ela": "Maria",
        "novo_status": "Recusou"|"Desistiu", "observacao": "texto" }

    Só altera se status atual for NULL, 'Aberto' ou 'Aceito'.
    """
    data = request.get_json(silent=True) or {}
    _id = data.get('id')
    novo_status = (data.get('novo_status') or '').strip().title()
    observacao = (data.get('observacao') or '').strip()

    if novo_status not in ('Recusou', 'Desistiu'):
        return jsonify({"ok": False, "msg": "novo_status deve ser 'Recusou' ou 'Desistiu'."}), 400
    if not observacao:
        return jsonify({"ok": False, "msg": "Observação é obrigatória."}), 400

    conn = mysql.connector.connect(**DB_CONFIG)
    cur = conn.cursor()
    try:
        if _id:
            cur.execute("""
                UPDATE palestras
                   SET status = %s, observacao = %s
                 WHERE id = %s
                   AND (status IS NULL OR UPPER(status) IN ('ABERTO','ACEITO'))
                 LIMIT 1
            """, (novo_status, observacao, int(_id)))
            conn.commit()
            if cur.rowcount == 0:
                return jsonify({"ok": False, "msg": "Registro não encontrado ou não alterável."}), 404
            return jsonify({"ok": True})

        ano = data.get('ano')
        palestra = (data.get('palestra') or '').strip()
        if not (ano and palestra):
            return jsonify({"ok": False, "msg": "Informe id, ou ano e palestra."}), 400

        nome_ele = (data.get('nome_ele') or '').strip()
        nome_ela = (data.get('nome_ela') or '').strip()

        clauses = [
            "UPDATE palestras SET status=%s, observacao=%s",
            "WHERE ano=%s AND palestra=%s",
            "AND (status IS NULL OR UPPER(status) IN ('ABERTO','ACEITO'))"
        ]
        params = [novo_status, observacao, int(ano), palestra]

        if nome_ele:
            clauses.append("AND nome_ele=%s")
            params.append(nome_ele)
        if nome_ela:
            clauses.append("AND nome_ela=%s")
            params.append(nome_ela)

        clauses.append("ORDER BY id DESC LIMIT 1")
        sql = "\n".join(clauses)

        cur.execute(sql, tuple(params))
        conn.commit()
        if cur.rowcount == 0:
            return jsonify({"ok": False, "msg": "Registro não encontrado ou não alterável com os critérios informados."}), 404
        return jsonify({"ok": True})
    finally:
        try:
            cur.close(); conn.close()
        except Exception:
            pass
# <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

@app.route('/palestrantes')
def palestrantes():
    """Lista de palestras por ano com filtros e ordem de títulos padronizada."""
    nome_ele = (request.args.get('nome_ele') or '').strip()
    nome_ela = (request.args.get('nome_ela') or '').strip()
    ano_filtro = (request.args.get('ano') or '').strip()

    conn = db_conn()
    cur = conn.cursor(dictionary=True)

    sql = """
        SELECT id, ano, palestra, nome_ele, nome_ela
          FROM palestras
         WHERE 1=1
    """
    params = []
    if nome_ele:
        sql += " AND LOWER(nome_ele) LIKE LOWER(%s)"
        params.append(f"%{nome_ele}%")
    if nome_ela:
        sql += " AND LOWER(COALESCE(nome_ela,'')) LIKE LOWER(%s)"
        params.append(f"%{nome_ela}%")
    if ano_filtro:
        sql += " AND ano = %s"
        params.append(ano_filtro)

    # Ordena por ano desc; a exibição por palestra será ordenada em Python pela ordem oficial
    sql += " ORDER BY ano DESC, id ASC"

    cur.execute(sql, params)
    rows = cur.fetchall() or []
    cur.close(); conn.close()

    # Agrupa e ordena títulos segundo PALESTRAS_TITULOS; nomes em Title Case; None -> ''
    titulo_ordem = {t: i for i, t in enumerate(PALESTRAS_TITULOS)}
    por_ano = defaultdict(list)
    for r in rows:
        item = {
            "palestra": r.get("palestra") or "",
            "nome_ele": (r.get("nome_ele") or "").title(),
            "nome_ela": (r.get("nome_ela") or "").title() if r.get("nome_ela") else ""
        }
        por_ano[r["ano"]].append(item)

    # Ordena dentro do ano pela sequência oficial de títulos
    for ano in list(por_ano.keys()):
        por_ano[ano].sort(key=lambda x: titulo_ordem.get(x["palestra"], 9999))

    colunas = ["palestra", "nome_ele", "nome_ela"]  # sem status

    return render_template("palestrantes.html",
                           por_ano=por_ano,
                           colunas=colunas)

# ============================================
# RELATÓRIOS / IMPRESSÕES (ajustado para sua estrutura)
# ============================================
from flask import render_template, request, jsonify

def _q(cur, sql, params=None):
    cur.execute(sql, params or [])
    return cur.fetchall()

def _yes_coord_vals():
    # variações comuns para sinalizar coordenador
    return ('sim','s','coordenador','coordenadora','sim coordenador','sim - coordenador')

@app.route("/relatorios")
def relatorios():
    conn = db_conn(); cur = conn.cursor(dictionary=True)
    cur.execute("""
        SELECT DISTINCT ano
        FROM encontreiros
        WHERE ano IS NOT NULL
          AND ano NOT IN (2020, 2021)
        ORDER BY ano DESC
    """)
    anos = [r["ano"] for r in cur.fetchall()]
    cur.close(); conn.close()
    return render_template("relatorios.html", anos=anos)
@app.get("/api/trabalhos_por_ano")
def api_trabalhos_por_ano():
    conn = db_conn(); cur = conn.cursor(dictionary=True)
    cur.execute("""
        SELECT ano,
               COUNT(DISTINCT CONCAT('NM#', LOWER(TRIM(nome_ele)), '#', LOWER(TRIM(nome_ela)))) AS qtd
        FROM encontreiros
        WHERE ano IS NOT NULL
          AND ano NOT IN (2020, 2021)
          AND LOWER(TRIM(COALESCE(status,''))) NOT IN ('desistiu','recusou')
        GROUP BY ano
        ORDER BY ano
    """)
    rows = cur.fetchall()
    cur.close(); conn.close()
    return jsonify(rows)


@app.get("/api/ano_origem_dos_trabalhadores")
def api_ano_origem_dos_trabalhadores():
    ano = request.args.get("ano", type=int)
    if not ano:
        return jsonify({"error":"ano requerido"}), 400

    conn = db_conn(); cur = conn.cursor(dictionary=True)
    rows = _q(cur, """
        SELECT
          COALESCE(CAST(i.ano AS CHAR), 'Não informado') AS ano_encontro,
          COUNT(DISTINCT CONCAT('NM#', LOWER(TRIM(e.nome_ele)), '#', LOWER(TRIM(e.nome_ela)))) AS qtd
        FROM encontreiros e
        LEFT JOIN encontristas i
          ON LOWER(TRIM(e.nome_ele)) = LOWER(TRIM(i.nome_usual_ele))
         AND LOWER(TRIM(e.nome_ela)) = LOWER(TRIM(i.nome_usual_ela))
        WHERE e.ano = %s
          AND LOWER(TRIM(COALESCE(e.status,''))) NOT IN ('desistiu','recusou')
        GROUP BY ano_encontro
        ORDER BY
          CASE WHEN ano_encontro = 'Não informado' THEN 1 ELSE 0 END,
          ano_encontro
    """, [ano])
    cur.close(); conn.close()
    return jsonify({"ano_trabalho": ano, "dist": rows})
@app.get("/api/encontreiros_por_ano")
def api_encontreiros_por_ano():
    """
    Contagem de REGISTROS em 'encontreiros' por ano (não deduplica casal).
    Ignora status Desistiu/Recusou e exclui 2020/2021.
    """
    conn = db_conn(); cur = conn.cursor(dictionary=True)
    cur.execute("""
        SELECT ano, COUNT(*) AS qtd
        FROM encontreiros
        WHERE ano IS NOT NULL
          AND ano NOT IN (2020, 2021)
          AND LOWER(TRIM(COALESCE(status,''))) NOT IN ('desistiu','recusou')
        GROUP BY ano
        ORDER BY ano
    """)
    rows = cur.fetchall()
    cur.close(); conn.close()
    return jsonify(rows)


@app.route("/docs")
def docs_index():
    conn = db_conn(); cur = conn.cursor(dictionary=True)
    cur.execute("""
        SELECT DISTINCT ano
        FROM encontreiros
        WHERE ano IS NOT NULL
          AND ano NOT IN (2020, 2021)
        ORDER BY ano DESC
    """)
    anos = [r["ano"] for r in cur.fetchall()]

    cur.execute("""
        SELECT DISTINCT equipe
        FROM encontreiros
        WHERE equipe IS NOT NULL AND equipe <> ''
        ORDER BY equipe
    """)
    equipes = [r["equipe"] for r in cur.fetchall()]
    cur.close(); conn.close()
    return render_template("docs.html", anos=anos, equipes=equipes)


# --------------------------------------------
# 1) COORDENADORES (por ano) com endereço/telefones mais recentes
#    - Ignora Desistiu/Recusou
#    - Telefones/Endereço: usa ENCONTRISTAS se houver; senão, ENCONTREIROS.telefones/endereco
# --------------------------------------------
@app.get("/imprimir/coordenadores")
def imprimir_coordenadores():
    ano = request.args.get("ano", type=int)
    if not ano: return "Informe ?ano=YYYY", 400

    conn = db_conn(); cur = conn.cursor(dictionary=True)
    rows = _q(cur, """
        SELECT
          e.equipe,
          e.nome_ele AS ele,
          e.nome_ela AS ela,
          COALESCE(CONCAT_WS(' / ', i.telefone_ele, i.telefone_ela), e.telefones) AS telefones,
          COALESCE(i.endereco, e.endereco) AS endereco
        FROM encontreiros e
        LEFT JOIN encontristas i
          ON LOWER(TRIM(e.nome_ele)) = LOWER(TRIM(i.nome_usual_ele))
         AND LOWER(TRIM(e.nome_ela)) = LOWER(TRIM(i.nome_usual_ela))
        WHERE e.ano = %s
          AND LOWER(TRIM(COALESCE(e.status,''))) NOT IN ('desistiu','recusou')
          AND LOWER(TRIM(COALESCE(e.coordenador,''))) IN ('sim','s','coordenador','coordenadora','sim coordenador','sim - coordenador')
        ORDER BY e.equipe, ele, ela
    """, [ano])
    cur.close(); conn.close()

    return render_template("print_coordenadores.html", ano=ano, rows=rows)

# --------------------------------------------
# 2) INTEGRANTES por equipe (ano) — com destaque de coordenadores
#    - opcional ?equipe=
#    - Ignora Desistiu/Recusou
#    - Telefones/Endereço: mais recentes via ENCONTRISTAS, fallback ENCONTREIROS
# --------------------------------------------
@app.get("/imprimir/equipes")
def imprimir_equipes():
    ano = request.args.get("ano", type=int)
    equipe = request.args.get("equipe")
    if not ano: return "Informe ?ano=YYYY", 400

    params = [ano]
    where_equipe = ""
    if equipe:
        where_equipe = " AND e.equipe = %s "
        params.append(equipe)

    conn = db_conn(); cur = conn.cursor(dictionary=True)
    rows = _q(cur, f"""
        SELECT
          e.equipe,
          e.nome_ele AS ele,
          e.nome_ela AS ela,
          CASE
            WHEN LOWER(TRIM(COALESCE(e.coordenador,''))) IN ('sim','s','coordenador','coordenadora','sim coordenador','sim - coordenador')
            THEN 1 ELSE 0
          END AS is_coord,
          COALESCE(CONCAT_WS(' / ', i.telefone_ele, i.telefone_ela), e.telefones) AS telefones,
          COALESCE(i.endereco, e.endereco) AS endereco
        FROM encontreiros e
        LEFT JOIN encontristas i
          ON LOWER(TRIM(e.nome_ele)) = LOWER(TRIM(i.nome_usual_ele))
         AND LOWER(TRIM(e.nome_ela)) = LOWER(TRIM(i.nome_usual_ela))
        WHERE e.ano = %s
          {where_equipe}
          AND LOWER(TRIM(COALESCE(e.status,''))) NOT IN ('desistiu','recusou')
        ORDER BY e.equipe, is_coord DESC, ele, ela
    """.format(where_equipe=where_equipe), params)
    cur.close(); conn.close()

    return render_template("print_equipes.html", ano=ano, equipe=equipe, rows=rows)
# --------------------------------------------
# 3) VIGÍLIA VOLUNTÁRIA (N casais)
#    - ?ids=1,2,3 (lista específica) OU seleção automática
#    - Seleção automática exclui quem já está ESCALADO naquele ano (status != Desistiu/Recusou)
# --------------------------------------------
@app.get("/imprimir/vigilia")
def imprimir_vigilia():
    ano = request.args.get("ano", type=int)
    qtd = request.args.get("qtd", default=56, type=int)
    ids = request.args.get("ids")
    seed = request.args.get("seed", default="vigilia")
    if not ano: return "Informe ?ano=YYYY", 400

    conn = db_conn(); cur = conn.cursor(dictionary=True)

    if ids:
        lista_ids = [int(x) for x in ids.split(",") if x.strip().isdigit()]
        if not lista_ids:
            return "Parâmetro ids inválido.", 400
        placeholders = ",".join(["%s"]*len(lista_ids))
        rows = _q(cur, f"""
            SELECT id,
                   -- se quiser manter compatibilidade com o template:
                   nome_usual_ele AS nome_ele,
                   nome_usual_ela AS nome_ela,
                   nome_usual_ele, nome_usual_ela,
                   endereco, telefone_ele, telefone_ela
            FROM encontristas
            WHERE id IN ({placeholders})
            ORDER BY FIELD(id, {placeholders})
        """, lista_ids + lista_ids)
    else:
        rows = _q(cur, """
            SELECT i.id,
                   -- entregar campos com os nomes esperados pelo template
                   i.nome_usual_ele AS nome_ele,
                   i.nome_usual_ela AS nome_ela,
                   i.nome_usual_ele, i.nome_usual_ela,
                   i.endereco, i.telefone_ele, i.telefone_ela
            FROM encontristas i
            LEFT JOIN (
              SELECT DISTINCT
                LOWER(TRIM(nome_ele)) AS nme,
                LOWER(TRIM(nome_ela)) AS nma
              FROM encontreiros
              WHERE ano = %s
                AND LOWER(TRIM(COALESCE(status,''))) NOT IN ('desistiu','recusou')
            ) e
              ON LOWER(TRIM(i.nome_usual_ele)) = e.nme
             AND LOWER(TRIM(i.nome_usual_ela)) = e.nma
            WHERE e.nme IS NULL
            ORDER BY MD5(CONCAT_WS('#', i.id, %s))
            LIMIT %s
        """, [ano, seed, qtd])

    cur.close(); conn.close()
    return render_template("print_vigilia.html", ano=ano, qtd=qtd, seed=seed, rows=rows)


# =========================
# Autocomplete simples (se você já tem outro, pode manter o seu)
# =========================
@app.route('/autocomplete-nomes')
def autocomplete_nomes():
    q = (request.args.get('q') or '').strip()
    if len(q) < 3:
        return jsonify([])
    conn = db_conn()
    cur = conn.cursor()
    try:
        nomes = set()

        # encontristas (nomes usuais)
        cur.execute("""
            SELECT DISTINCT nome_usual_ele FROM encontristas
             WHERE nome_usual_ele LIKE %s
             LIMIT 30
        """, (f"%{q}%",))
        for (n,) in cur.fetchall(): 
            if n: nomes.add(n)

        cur.execute("""
            SELECT DISTINCT nome_usual_ela FROM encontristas
             WHERE nome_usual_ela LIKE %s
             LIMIT 30
        """, (f"%{q}%",))
        for (n,) in cur.fetchall():
            if n: nomes.add(n)

        # encontreiros (nomes “oficiais”)
        cur.execute("""
            SELECT DISTINCT nome_ele FROM encontreiros
             WHERE nome_ele LIKE %s
             LIMIT 30
        """, (f"%{q}%",))
        for (n,) in cur.fetchall():
            if n: nomes.add(n)

        cur.execute("""
            SELECT DISTINCT nome_ela FROM encontreiros
             WHERE nome_ela LIKE %s
             LIMIT 30
        """, (f"%{q}%",))
        for (n,) in cur.fetchall():
            if n: nomes.add(n)

        return jsonify(sorted(nomes))
    finally:
        try:
            cur.close(); conn.close()
        except Exception:
            pass

# =========================
# Rotas para ligação entre as tabelas encontristas e encontreiros
# =========================
def _norm(s: str) -> str:
    if s is None:
        return ""
    s = s.strip().lower()
    s = re.sub(r"[^\w\s]", " ", s, flags=re.UNICODE)
    s = re.sub(r"\s+", " ", s)
    return s

def _sim(a: str, b: str) -> float:
    return SequenceMatcher(None, _norm(a), _norm(b)).ratio()

def _get_db():
    return mysql.connector.connect(**DB_CONFIG)

@app.route("/admin/match-fuzzy")
def admin_match_fuzzy():
    # Segurança via token
    token = request.args.get("token", "")
    if token != os.environ.get("ADMIN_TOKEN", ""):
        return "Unauthorized", 401

    # Parâmetros de lote
    try:
        batch_size = int(request.args.get("size", "300"))
    except ValueError:
        batch_size = 300

    AUTO_THRESHOLD = 0.92
    SUGGEST_THRESHOLD = 0.80

    conn = _get_db()
    cur = conn.cursor(dictionary=True)

    # NÃO apaga pendências antigas em massa aqui.
    # Mantemos histórico e apenas acrescentamos novas sugestões quando houver.

    # Base de comparação (encontristas)
    cur.execute("SELECT id, nome_usual_ele, nome_usual_ela FROM encontristas")
    base = cur.fetchall()

    # Bucket simples por 1ª letra
    from collections import defaultdict
    def _norm(s: str) -> str:
        if s is None:
            return ""
        s = s.strip().lower()
        s = re.sub(r"[^\w\s]", " ", s, flags=re.UNICODE)
        s = re.sub(r"\s+", " ", s)
        return s

    def _sim(a: str, b: str) -> float:
        return SequenceMatcher(None, _norm(a), _norm(b)).ratio()

    bucket = defaultdict(list)
    for r in base:
        key = (_norm(r['nome_usual_ele'])[:1], _norm(r['nome_usual_ela'])[:1])
        bucket[key].append(r)

    # Pega um LOTE de pendentes (casal IS NULL)
    cur.execute("""
        SELECT id, nome_ele, nome_ela
        FROM encontreiros
        WHERE casal IS NULL
        ORDER BY id ASC
        LIMIT %s
    """, (batch_size,))
    pend = cur.fetchall()

    if not pend:
        cur.close()
        conn.close()
        return {"message": "Nada a processar. Já está zerado.", "processed": 0}, 200

    auto_count = 0
    pend_count = 0

    # Prepara insert de pendências (evita duplicar mesma sugestão)
    # Cria índice único opcional (faça uma vez só no MySQL):
    # ALTER TABLE pendencias_encontreiros ADD UNIQUE KEY uniq_sug (encontreiros_id, candidato_id);
    for row in pend:
        e_id = row['id']
        n_ele = row['nome_ele'] or ""
        n_ela = row['nome_ela'] or ""

        key = (_norm(n_ele)[:1], _norm(n_ela)[:1])
        candidates = bucket.get(key, base)

        scored = []
        for c in candidates:
            s_ele = _sim(n_ele, c['nome_usual_ele'])
            s_ela = _sim(n_ela, c['nome_usual_ela'])
            score = (s_ele + s_ela) / 2.0
            scored.append((score, s_ele, s_ela, c['id'], c['nome_usual_ele'], c['nome_usual_ela']))

        if not scored:
            continue

        scored.sort(key=lambda x: x[0], reverse=True)
        best = scored[0]
        best_score, best_ele, best_ela, best_id, best_nele, best_nela = best

        if best_ele >= AUTO_THRESHOLD and best_ela >= AUTO_THRESHOLD:
            try:
                cur.execute("UPDATE encontreiros SET casal=%s WHERE id=%s", (best_id, e_id))
                auto_count += 1
            except mysql_errors.Error as err:
                print(f"[fuzzy] erro ao atualizar encontreiros.id={e_id}: {err}")
        else:
            # guarda até 3 sugestões; ignora se já existir sugestão idêntica
            suggestions = [s for s in scored if s[0] >= SUGGEST_THRESHOLD][:3]
            for s in suggestions:
                score, s_ele, s_ela, s_id, s_nele, s_nela = s
                try:
                    cur.execute("""
                        INSERT INTO pendencias_encontreiros
                          (encontreiros_id, nome_ele, nome_ela, candidato_id,
                           candidato_nome_usual_ele, candidato_nome_usual_ela,
                           score_ele, score_ela, score_medio)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        ON DUPLICATE KEY UPDATE
                          score_ele=VALUES(score_ele),
                          score_ela=VALUES(score_ela),
                          score_medio=VALUES(score_medio),
                          nome_ele=VALUES(nome_ele),
                          nome_ela=VALUES(nome_ela),
                          candidato_nome_usual_ele=VALUES(candidato_nome_usual_ele),
                          candidato_nome_usual_ela=VALUES(candidato_nome_usual_ela)
                    """, (e_id, n_ele, n_ela, s_id, s_nele, s_nela,
                          round(s_ele,4), round(s_ela,4), round(score,4)))
                except mysql_errors.Error as err:
                    # Se a tabela não tem UNIQUE, ignore esse ON DUPLICATE e use INSERT simples
                    print(f"[pendencia] falha ao inserir sugestao e_id={e_id}, cand={s_id}: {err}")
            pend_count += 1

    conn.commit()

    # Quantos ainda faltam no total (para progresso)
    cur.execute("SELECT COUNT(*) AS faltando FROM encontreiros WHERE casal IS NULL")
    faltando = cur.fetchone()["faltando"]

    cur.close()
    conn.close()

    return {
        "processados_neste_lote": len(pend),
        "preenchimentos_automaticos_neste_lote": auto_count,
        "pendencias_neste_lote": pend_count,
        "restantes_no_total": faltando
    }, 200

@app.route("/admin/revisao")
def admin_revisao():
    if not _admin_ok():
        return "Unauthorized", 401

    # parâmetros
    try:
        page = int(request.args.get("page", "1"))
        per_page = int(request.args.get("per_page", "50"))
        min_score = float(request.args.get("min_score", "0.85"))
    except ValueError:
        page, per_page, min_score = 1, 50, 0.85
    page = max(1, page)
    per_page = max(10, min(per_page, 100))
    offset = (page - 1) * per_page
    token = request.args.get("token")

    conn = _get_db()
    cur = conn.cursor(dictionary=True)

    # total de grupos (encontreiros com pendências) acima do min_score
    cur.execute("""
        SELECT COUNT(*) AS total_groups FROM (
          SELECT p.encontreiros_id
          FROM pendencias_encontreiros p
          JOIN encontreiros e ON e.id = p.encontreiros_id
          WHERE e.casal IS NULL AND p.score_medio >= %s
          GROUP BY p.encontreiros_id
        ) t
    """, (min_score,))
    total_groups = cur.fetchone()["total_groups"]
    total_pages = max(1, (total_groups + per_page - 1) // per_page)

    # ids desta página, ordenados pelo melhor score desc
    cur.execute("""
        SELECT p.encontreiros_id, MAX(p.score_medio) AS best_score
        FROM pendencias_encontreiros p
        JOIN encontreiros e ON e.id = p.encontreiros_id
        WHERE e.casal IS NULL AND p.score_medio >= %s
        GROUP BY p.encontreiros_id
        ORDER BY best_score DESC, p.encontreiros_id ASC
        LIMIT %s OFFSET %s
    """, (min_score, per_page, offset))
    rows = cur.fetchall()
    ids = [r["encontreiros_id"] for r in rows]
    id2best = {r["encontreiros_id"]: r["best_score"] for r in rows}

    groups = []
    if ids:
        placeholders = ",".join(["%s"] * len(ids))
        # base (origem)
        cur.execute(f"""
            SELECT id, nome_ele, nome_ela, telefones, endereco
            FROM encontreiros
            WHERE id IN ({placeholders})
        """, ids)
        base = {r["id"]: r for r in cur.fetchall()}

        # candidatos desta página
        cur.execute(f"""
            SELECT *
            FROM pendencias_encontreiros
            WHERE encontreiros_id IN ({placeholders})
            ORDER BY encontreiros_id ASC, score_medio DESC, id ASC
        """, ids)
        cand = cur.fetchall()

        # agrupa
        from collections import defaultdict
        bucket = defaultdict(list)
        for c in cand:
            bucket[c["encontreiros_id"]].append(c)

        for eid in ids:
            groups.append({
                "best_score": id2best.get(eid, 0),
                "encontreiros": base.get(eid),
                "candidatos": bucket.get(eid, [])
            })

    cur.close(); conn.close()

    # feedback pós-POST (querystring)
    ok_count = request.args.get("ok", None)
    skipped_count = request.args.get("skipped", None)
    ok_count = int(ok_count) if ok_count is not None and ok_count.isdigit() else None
    skipped_count = int(skipped_count) if skipped_count is not None and skipped_count.isdigit() else None

    return render_template(
        "admin_revisao.html",
        token=token,
        page=page, per_page=per_page, min_score=min_score,
        total_groups=total_groups, total_pages=total_pages,
        groups=groups, ok_count=ok_count, skipped_count=skipped_count
    )
@app.route("/admin/revisao/confirmar", methods=["POST"])
def admin_revisao_confirmar():
    if not _admin_ok():
        return "Unauthorized", 401

    token = request.form.get("token", "")
    page = request.form.get("page", "1")
    per_page = request.form.get("per_page", "50")
    min_score = request.form.get("min_score", "0.85")

    conn = _get_db()
    cur = conn.cursor()
    ok_count = 0
    skipped = 0

    # Cada grupo vem como sel_<encontreiros_id> = <candidato_id ou vazio>
    for key, val in request.form.items():
        if not key.startswith("sel_"):
            continue
        try:
            eid = int(key.split("_", 1)[1])
        except:
            continue

        if not val:  # “Nenhum”
            skipped += 1
            continue

        try:
            cid = int(val)
        except:
            skipped += 1
            continue

        # Confirma: seta casal e limpa pendências daquele id
        try:
            cur.execute("UPDATE encontreiros SET casal=%s WHERE id=%s AND casal IS NULL", (cid, eid))
            if cur.rowcount > 0:
                cur.execute("DELETE FROM pendencias_encontreiros WHERE encontreiros_id=%s", (eid,))
                ok_count += 1
            else:
                # já estava preenchido por outro fluxo
                skipped += 1
        except Exception as err:
            print(f"[revisao] falha ao confirmar (eid={eid}, cid={cid}): {err}")
            skipped += 1

    conn.commit()
    cur.close(); conn.close()

    # redireciona de volta para a mesma página, com contagem
    return redirect(url_for(
        "admin_revisao",
        token=token, page=page, per_page=per_page, min_score=min_score,
        ok=ok_count, skipped=skipped
    ))

# ---------- CÍRCULOS (consulta) ----------
# ---------- CÍRCULOS (consulta em cards por ano) ----------
@app.route('/circulos')
def circulos_list():
    from collections import defaultdict

    # Helpers de cor (suporta alguns nomes PT e hex #RRGGBB/#RGB)
    def _hex_to_rgb(h):
        h = h.strip().lstrip('#')
        if len(h) == 3:
            h = ''.join([c*2 for c in h])
        if len(h) != 6:
            return None
        try:
            return tuple(int(h[i:i+2], 16) for i in (0,2,4))
        except ValueError:
            return None

    def _name_to_hex_pt(c):
        if not c: return None
        c = c.strip().lower()
        mapa = {
            'azul':'#2563eb','vermelho':'#ef4444','verde':'#22c55e','amarelo':'#eab308',
            'laranja':'#f59e0b','roxo':'#8b5cf6','rosa':'#ec4899','marrom':'#92400e',
            'cinza':'#6b7280','preto':'#111827','branco':'#ffffff'
        }
        # aceita alguns em EN tb
        mapa.update({
            'blue':'#2563eb','red':'#ef4444','green':'#22c55e','yellow':'#eab308',
            'orange':'#f59e0b','purple':'#8b5cf6','pink':'#ec4899','brown':'#92400e',
            'gray':'#6b7280','grey':'#6b7280','black':'#111827','white':'#ffffff'
        })
        return mapa.get(c)

    def _cor_to_rgb_triplet(c):
        if not c: return None
        c = c.strip()
        hx = _name_to_hex_pt(c) or (c if c.startswith('#') else None)
        rgb = _hex_to_rgb(hx) if hx else None
        if not rgb:  # não reconhecido
            return None
        return f"{rgb[0]},{rgb[1]},{rgb[2]}"

    conn = db_conn()
    cur = conn.cursor(dictionary=True)

    # Filtros simples
    ano = (request.args.get('ano') or '').strip()
    q   = (request.args.get('q') or '').strip()

    where = ["1=1"]
    params = []
    if ano:
        where.append("c.ano = %s")
        params.append(ano)
    if q:
        like = f"%{q}%"
        where.append("""(
              LOWER(c.nome_circulo)    LIKE LOWER(%s) OR
              LOWER(c.cor_circulo)     LIKE LOWER(%s) OR
              LOWER(c.coord_atual_ele) LIKE LOWER(%s) OR
              LOWER(c.coord_atual_ela) LIKE LOWER(%s) OR
              LOWER(c.coord_orig_ele)  LIKE LOWER(%s) OR
              LOWER(c.coord_orig_ela)  LIKE LOWER(%s)
        )""")
        params += [like, like, like, like, like, like]

    where_sql = " AND ".join(where)

    # Busca todos (sem paginação, pois agora é em cards por ano)
    cur.execute(f"""
        SELECT
           c.id, c.ano, c.cor_circulo, c.nome_circulo,
           c.coord_orig_ele, c.coord_orig_ela,
           c.coord_atual_ele, c.coord_atual_ela,
           c.integrantes_original, c.integrantes_atual,
           c.situacao, c.observacao, c.created_at
        FROM circulos c
        WHERE {where_sql}
        ORDER BY c.ano DESC, c.nome_circulo, c.coord_orig_ele
    """, params)
    rows = cur.fetchall() or []

    # Enriquecer com a cor pastel (rgba) para o fundo/borda do card do círculo
    for r in rows:
        trip = _cor_to_rgb_triplet(r.get('cor_circulo') or '')
        r['rgb_triplet'] = trip  # ex.: "37,99,235" para usar com rgba(var(--c),0.12)

    # Anos disponíveis para o <select>
    cur.execute("SELECT DISTINCT ano FROM circulos ORDER BY ano DESC")
    anos_combo = [a['ano'] for a in (cur.fetchall() or [])]

    cur.close(); conn.close()

    # Agrupar por ano -> lista de círculos
    agrupado = defaultdict(list)
    for r in rows:
        agrupado[r['ano']].append(r)
    anos_ordenados = sorted(agrupado.keys(), reverse=True)

    return render_template(
        'circulos.html',
        anos_combo=anos_combo,
        filtros={'ano': ano, 'q': q},
        anos_ordenados=anos_ordenados,
        agrupado=agrupado
    )
# ---------- CÍRCULOS • Detalhe ----------
@app.route('/circulos/<int:cid>')
def circulos_view(cid):
    # Helpers de cor (nomes comuns PT/EN ou #hex -> "R,G,B")
    def _hex_to_rgb(h):
        h = (h or '').strip().lstrip('#')
        if len(h) == 3:
            h = ''.join([c * 2 for c in h])
        if len(h) != 6:
            return None
        try:
            return tuple(int(h[i:i+2], 16) for i in (0, 2, 4))
        except ValueError:
            return None

    def _name_to_hex_pt(c):
        if not c: return None
        c = c.strip().lower()
        mapa = {
            'azul':'#2563eb','vermelho':'#ef4444','verde':'#22c55e','amarelo':'#eab308',
            'laranja':'#f59e0b','roxo':'#8b5cf6','rosa':'#ec4899','marrom':'#92400e',
            'cinza':'#6b7280','preto':'#111827','branco':'#ffffff',
            'blue':'#2563eb','red':'#ef4444','green':'#22c55e','yellow':'#eab308',
            'orange':'#f59e0b','purple':'#8b5cf6','pink':'#ec4899','brown':'#92400e',
            'gray':'#6b7280','grey':'#6b7280','black':'#111827','white':'#ffffff'
        }
        return mapa.get(c)

    def _to_triplet(c):
        if not c: return None
        c = c.strip()
        hx = _name_to_hex_pt(c) or (c if c.startswith('#') else None)
        rgb = _hex_to_rgb(hx) if hx else None
        if not rgb: return None
        return f"{rgb[0]},{rgb[1]},{rgb[2]}"

    # --- Busca o círculo ---
    conn = db_conn()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute("SELECT * FROM circulos WHERE id=%s", (cid,))
        r = cur.fetchone()
    finally:
        try:
            cur.close(); conn.close()
        except Exception:
            pass

    if not r:
        return "Registro não encontrado.", 404

    # --- Cor pastel para o card ---
    rgb_triplet = _to_triplet(r.get('cor_circulo'))

    # --- Listas de integrantes (IDs) ---
    raw_atual = (r.get('integrantes_atual') or '').replace(';', ',')
    raw_orig  = (r.get('integrantes_original') or '').replace(';', ',')

    integrantes_atual_list = [x.strip() for x in raw_atual.split(',') if x and x.strip()]
    integrantes_orig_list  = [x.strip() for x in raw_orig.split(',') if x and x.strip()]

    # Lista principal: prefere a "atual"; se vazia, cai para a "original"
    integrantes_list = integrantes_atual_list if integrantes_atual_list else integrantes_orig_list

    return render_template(
        'circulos_view.html',
        r=r,
        rgb_triplet=rgb_triplet,
        integrantes_list=integrantes_list,
        integrantes_atual_list=integrantes_atual_list,
        integrantes_orig_list=integrantes_orig_list
    )

# -----------------------------
# Helpers Circulos (internos)
# -----------------------------
def _csv_ids_unique(raw: str):
    raw = (raw or "").replace(";", ",")
    out = []
    seen = set()
    for part in raw.split(","):
        p = part.strip()
        if not p:
            continue
        if not p.isdigit():
            continue
        val = int(p)
        if val not in seen:
            seen.add(val)
            out.append(val)
    return out

def _ids_to_csv(ids):
    ids = [str(int(x)) for x in ids if str(x).isdigit()]
    return ",".join(ids)

def _resolve_encontristas(cur, id_list):
    """Recebe lista de IDs (encontristas.id) e devolve na mesma ordem:
       [{id, nome_ele, nome_ela, telefone_ele, telefone_ela, endereco, ano}]"""
    if not id_list:
        return []
    placeholders = ",".join(["%s"] * len(id_list))
    cur.execute(f"""
        SELECT id, nome_usual_ele, nome_usual_ela, telefone_ele, telefone_ela, endereco, ano
        FROM encontristas
        WHERE id IN ({placeholders})
    """, tuple(id_list))
    rows = cur.fetchall() or []
    by_id = {r["id"]: r for r in rows}
    out = []
    for i in id_list:
        r = by_id.get(i)
        if r:
            out.append({
                "id": r["id"],
                "nome_ele": r.get("nome_usual_ele") or "",
                "nome_ela": r.get("nome_usual_ela") or "",
                "telefone_ele": r.get("telefone_ele") or "",
                "telefone_ela": r.get("telefone_ela") or "",
                "endereco": r.get("endereco") or "",
                "ano": r.get("ano"),
            })
    return out

# -----------------------------
# API: buscar encontrista por nomes (para integrantes_atual)
# Regras:
#  - casa por nomes usuais (case-insensitive, ignora espaços extras)
#  - valida que o ANO do encontrista == ANO do círculo informado
# -----------------------------
@app.post("/api/circulos/buscar-encontrista")
def api_circulos_buscar_encontrista():
    data = request.get_json(silent=True) or {}
    ano_circulo = data.get("ano")
    ele = (data.get("ele") or "").strip()
    ela = (data.get("ela") or "").strip()

    if not (ano_circulo and ele and ela):
        return jsonify({"ok": False, "msg": "Informe ano, Ele e Ela."}), 400

    conn = db_conn(); cur = conn.cursor(dictionary=True)
    try:
        # busca exata (case-insensitive)
        cur.execute("""
            SELECT id, nome_usual_ele, nome_usual_ela, telefone_ele, telefone_ela, endereco, ano
            FROM encontristas
            WHERE LOWER(TRIM(nome_usual_ele)) = LOWER(TRIM(%s))
              AND LOWER(TRIM(nome_usual_ela)) = LOWER(TRIM(%s))
            ORDER BY ano DESC
            LIMIT 1
        """, (ele, ela))
        r = cur.fetchone()

        if not r:
            # fallback: prefixo (LIKE) para ajudar durante digitação
            cur.execute("""
                SELECT id, nome_usual_ele, nome_usual_ela, telefone_ele, telefone_ela, endereco, ano
                FROM encontristas
                WHERE nome_usual_ele LIKE %s
                  AND nome_usual_ela LIKE %s
                ORDER BY ano DESC
                LIMIT 3
            """, (f"{ele}%", f"{ela}%"))
            sugest = cur.fetchall() or []
            if not sugest:
                return jsonify({"ok": False, "msg": "Casal não encontrado."}), 404
            # Se houver mais de 1 sugestão, devolve lista para o cliente escolher
            multi = []
            for s in sugest:
                multi.append({
                    "id": s["id"],
                    "nome_ele": s["nome_usual_ele"],
                    "nome_ela": s["nome_usual_ela"],
                    "telefone_ele": s.get("telefone_ele") or "",
                    "telefone_ela": s.get("telefone_ela") or "",
                    "endereco": s.get("endereco") or "",
                    "ano": s.get("ano"),
                    "match_ano": int(s.get("ano") or 0) == int(ano_circulo),
                })
            return jsonify({"ok": True, "multiplo": True, "opcoes": multi})

        # valida ano
        match_ano = int(r.get("ano") or 0) == int(ano_circulo)
        return jsonify({
            "ok": True,
            "multiplo": False,
            "id": r["id"],
            "nome_ele": r["nome_usual_ele"],
            "nome_ela": r["nome_usual_ela"],
            "telefone_ele": r.get("telefone_ele") or "",
            "telefone_ela": r.get("telefone_ela") or "",
            "endereco": r.get("endereco") or "",
            "ano": r.get("ano"),
            "match_ano": match_ano
        })
    finally:
        try: cur.close(); conn.close()
        except Exception: pass

# -----------------------------
# API: obter integrantes resolvidos (nomes) do círculo
# -----------------------------
@app.get("/api/circulos/<int:cid>/integrantes")
def api_circulos_integrantes(cid):
    conn = db_conn(); cur = conn.cursor(dictionary=True)
    try:
        cur.execute("SELECT integrantes_atual, integrantes_original FROM circulos WHERE id=%s", (cid,))
        r = cur.fetchone()
        if not r:
            return jsonify({"ok": False, "msg": "Círculo não encontrado."}), 404

        ids_atual = _csv_ids_unique(r.get("integrantes_atual") or "")
        ids_orig  = _csv_ids_unique(r.get("integrantes_original") or "")

        atual = _resolve_encontristas(cur, ids_atual)
        orig  = _resolve_encontristas(cur, ids_orig)

        return jsonify({"ok": True, "atual": atual, "original": orig})
    finally:
        try: cur.close(); conn.close()
        except Exception: pass

# -----------------------------
# API: acrescentar 1 integrante no 'integrantes_atual'
# body: { "encontrista_id": 123 }
# -----------------------------
@app.post("/api/circulos/<int:cid>/integrantes/append")
def api_circulos_integrantes_append(cid):
    data = request.get_json(silent=True) or {}
    eid = data.get("encontrista_id")
    if not (eid and str(eid).isdigit()):
        return jsonify({"ok": False, "msg": "encontrista_id inválido."}), 400

    conn = db_conn(); cur = conn.cursor(dictionary=True)
    try:
        cur.execute("SELECT integrantes_atual FROM circulos WHERE id=%s", (cid,))
        r = cur.fetchone()
        if not r:
            return jsonify({"ok": False, "msg": "Círculo não encontrado."}), 404

        ids = _csv_ids_unique(r.get("integrantes_atual") or "")
        eid = int(eid)
        if eid not in ids:
            ids.append(eid)

        novo_csv = _ids_to_csv(ids)
        cur2 = conn.cursor()
        cur2.execute("UPDATE circulos SET integrantes_atual=%s WHERE id=%s", (novo_csv, cid))
        conn.commit()
        cur2.close()

        # devolve resolvido
        ids = _csv_ids_unique(novo_csv)
        atual = _resolve_encontristas(cur, ids)
        return jsonify({"ok": True, "atual": atual})
    finally:
        try: cur.close(); conn.close()
        except Exception: pass

# -----------------------------
# API: concluir integrantes -> copiar 'atual' para 'original'
# -----------------------------
@app.post("/api/circulos/<int:cid>/integrantes/concluir")
def api_circulos_integrantes_concluir(cid):
    conn = db_conn(); cur = conn.cursor(dictionary=True)
    try:
        cur.execute("SELECT integrantes_atual FROM circulos WHERE id=%s", (cid,))
        r = cur.fetchone()
        if not r:
            return jsonify({"ok": False, "msg": "Círculo não encontrado."}), 404
        atual = (r.get("integrantes_atual") or "").strip()
        cur2 = conn.cursor()
        cur2.execute("""
            UPDATE circulos
               SET integrantes_original=%s
             WHERE id=%s
        """, (atual, cid))
        conn.commit()
        cur2.close()
        return jsonify({"ok": True})
    finally:
        try: cur.close(); conn.close()
        except Exception: pass

# -----------------------------
# API: update genérico de campo (whitelist)
# body: { "field": "...", "value": "..." }
# Campos editáveis:
#   cor_circulo, nome_circulo, coord_atual_ele, coord_atual_ela, integrantes_atual,
#   situacao, observacao
# (NÃO permite: ano, coord_orig_ele, coord_orig_ela)
# -----------------------------
@app.post("/api/circulos/<int:cid>/update-field")
def api_circulos_update_field(cid):
    data = request.get_json(silent=True) or {}
    field = (data.get("field") or "").strip()
    value = data.get("value")

    ALLOWED = {
        "cor_circulo", "nome_circulo",
        "coord_atual_ele", "coord_atual_ela",
        "integrantes_atual",
        "situacao", "observacao"
    }
    if field not in ALLOWED:
        return jsonify({"ok": False, "msg": "Campo não permitido para edição."}), 400

    # normalização leve para CSV de integrantes se vier por aqui
    if field == "integrantes_atual":
        ids = _csv_ids_unique(str(value or ""))
        value = _ids_to_csv(ids)

    conn = db_conn(); cur = conn.cursor()
    try:
        sql = f"UPDATE circulos SET {field}=%s WHERE id=%s"
        cur.execute(sql, (value, cid))
        conn.commit()
        return jsonify({"ok": True})
    finally:
        try: cur.close(); conn.close()
        except Exception: pass

@app.get("/api/circulos/candidatos")
def api_circulos_candidatos():
    """
    Lista candidatos (ENCONTRISTAS) do ano informado, removendo quem já
    está em QUALQUER círculo daquele ano (em integrantes_atual OU original).
    Querystring: ?ano=YYYY
    """
    ano = request.args.get("ano", type=int)
    if not ano:
        return jsonify({"ok": False, "msg": "ano requerido"}), 400

    conn = db_conn(); cur = conn.cursor(dictionary=True)
    try:
        # 1) Coleta IDs já utilizados em círculos do ano
        cur.execute("""
            SELECT integrantes_atual, integrantes_original
              FROM circulos
             WHERE ano = %s
        """, (ano,))
        usados = set()
        rows = cur.fetchall() or []
        for r in rows:
            for col in ("integrantes_atual", "integrantes_original"):
                raw = (r.get(col) or "").replace(";", ",")
                for part in raw.split(","):
                    p = part.strip()
                    if p.isdigit():
                        usados.add(int(p))

        # 2) Busca encontristas daquele ano (excluindo já usados)
        cur.execute("""
            SELECT id, nome_usual_ele, nome_usual_ela, telefone_ele, telefone_ela, endereco, ano
              FROM encontristas
             WHERE ano = %s
             ORDER BY nome_usual_ele, nome_usual_ela
        """, (ano,))
        candidatos = []
        for r in cur.fetchall() or []:
            if int(r["id"]) in usados:
                continue
            candidatos.append({
                "id": r["id"],
                "nome_ele": r.get("nome_usual_ele") or "",
                "nome_ela": r.get("nome_usual_ela") or "",
                "telefone_ele": r.get("telefone_ele") or "",
                "telefone_ela": r.get("telefone_ela") or "",
                "endereco": r.get("endereco") or ""
            })

        return jsonify({"ok": True, "candidatos": candidatos})
    finally:
        try: cur.close(); conn.close()
        except Exception: pass

# =========================
# Main
# =========================
if __name__ == "__main__":
    app.run(debug=True)
