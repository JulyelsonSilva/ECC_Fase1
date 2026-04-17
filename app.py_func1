from flask import Flask, render_template, request, jsonify, redirect, url_for
import mysql.connector
from collections import defaultdict
import math
import re
import time
from difflib import SequenceMatcher

from mysql.connector import errors as mysql_errors

from config import (
    SECRET_KEY,
    ADMIN_TOKEN,
    DB_CONFIG,
    TEAM_MAP,
    TEAM_LIMITS,
    TEAM_CHOICES,
    PALESTRAS_TITULOS,
    PALESTRAS_SOLO,
)

from db import db_conn, safe_fetch_one, _get_db

from auth import _admin_ok

from utils import (
    _norm,
    _sim,
    _q,
    _yes_coord_vals,
    _hex_to_rgb_triplet,
    _color_to_rgb_triplet,
    _hex_to_rgb,
    _name_to_hex_pt,
    _to_triplet,
    _parse_id_list,
    _ids_to_str,
    _csv_ids_unique,
    _ids_to_csv,
    _parse_ids_csv,
)

from services.geocoding import (
    normalize_address,
    addr_hash,
    nominatim_geocode,
    save_cache,
    get_cache,
    split_address_components,
    viacep_busca_por_rua,
    geocode_br_smart,
)

from routes.encontristas import register_encontristas_routes
from routes.encontreiros import register_encontreiros_routes
from routes.circulos import register_circulos_routes
from routes.montagem import register_montagem_routes
from routes.implantacao import register_implantacao_routes
from routes.core import register_core_routes
from routes.admin import register_admin_routes

def _fetch_encontristas_by_ids(conn, ids):
    """Retorna na mesma ordem: [{id, nome_ele, nome_ela, telefone_ele, telefone_ela, endereco, ano}]"""
    if not ids:
        return []
    cur = conn.cursor(dictionary=True)
    try:
        placeholders = ",".join(["%s"]*len(ids))
        cur.execute(f"""
            SELECT id, nome_usual_ele, nome_usual_ela, telefone_ele, telefone_ela, endereco, ano
            FROM encontristas
            WHERE id IN ({placeholders})
        """, ids)
        rows = cur.fetchall() or []
    finally:
        cur.close()
    by_id = {r["id"]: r for r in rows}
    out = []
    for i in ids:
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

def _encontrista_name_by_id(conn, _id):
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute("SELECT nome_usual_ele, nome_usual_ela FROM encontristas WHERE id=%s", (_id,))
        r = cur.fetchone()
        if not r: return None, None
        return (r.get("nome_usual_ele") or ""), (r.get("nome_usual_ela") or "")
    finally:
        cur.close()


app = Flask(__name__)
app.config["SECRET_KEY"] = SECRET_KEY


def _team_label(value: str) -> str:
    """Normaliza chave/filtro curto para o rótulo salvo no banco."""
    v = (value or '').strip()
    if not v:
        return v
    vl = v.lower()
    for key, info in TEAM_MAP.items():
        if vl == key.lower() or vl == (info.get('filtro') or '').lower() or vl == (info.get('rotulo') or '').lower():
            return info['rotulo']
    return v

register_encontristas_routes(app, _encontrista_name_by_id)
register_encontreiros_routes(app, PALESTRAS_TITULOS, PALESTRAS_SOLO, DB_CONFIG, safe_fetch_one)
register_circulos_routes(app, _encontrista_name_by_id)
register_montagem_routes(app, TEAM_MAP, TEAM_LIMITS, _team_label)
register_implantacao_routes(app, TEAM_MAP, TEAM_LIMITS, TEAM_CHOICES, _team_label)
register_core_routes(app, TEAM_MAP, TEAM_LIMITS, _q)
register_admin_routes(app, _admin_ok, _get_db, _norm, _sim, SequenceMatcher)

# --- KPI: contagem de integrantes por equipe (exclui Coordenador; exclui Recusou/Desistiu) ---
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
        conn = db_conn()
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


# Compatibilidade com templates antigos que usam url_for('nova_palestra')
app.add_url_rule('/palestras/nova', endpoint='nova_palestra', view_func=palestras_nova)


@app.route('/api/palestras/validate', methods=['POST'], endpoint='api_palestras_validate')
def api_palestras_validate_compat():
    """Compatibilidade com templates antigos que enviam {ano, titulo, nome_ele, nome_ela}."""
    data = request.get_json(silent=True) or {}
    palestra = (data.get('titulo') or data.get('palestra') or '').strip()
    nome_ele = (data.get('nome_ele') or '').strip()
    nome_ela = (data.get('nome_ela') or '').strip()

    if not (palestra and nome_ele):
        return jsonify({"ok": False, "msg": "Parâmetros insuficientes."}), 400

    solo = palestra in PALESTRAS_SOLO
    if (not solo) and not nome_ela:
        return jsonify({"ok": False, "msg": "Informe Nome (Ela)."}), 400

    conn = db_conn()
    cur = conn.cursor(dictionary=True)
    try:
        eligible = False
        cur.execute(
            """
            SELECT 1 FROM encontristas
             WHERE nome_usual_ele = %s AND nome_usual_ela = %s
             LIMIT 1
            """,
            (nome_ele, nome_ela if not solo else '')
        )
        if cur.fetchone():
            eligible = True
        else:
            cur.execute(
                """
                SELECT 1 FROM encontreiros
                 WHERE nome_ele = %s AND nome_ela = %s
                 LIMIT 1
                """,
                (nome_ele, nome_ela if not solo else '')
            )
            eligible = cur.fetchone() is not None

        if solo:
            eligible = True

        telefones, endereco = '', ''
        if not solo:
            cur.execute(
                """
                SELECT telefones, endereco
                  FROM encontreiros
                 WHERE nome_ele = %s AND nome_ela = %s
                 ORDER BY ano DESC
                 LIMIT 1
                """,
                (nome_ele, nome_ela)
            )
            r = cur.fetchone()
            if r:
                telefones = (r.get('telefones') or '').strip()
                endereco = r.get('endereco') or ''
            else:
                cur.execute(
                    """
                    SELECT telefone_ele, telefone_ela, endereco
                      FROM encontristas
                     WHERE nome_usual_ele = %s AND nome_usual_ela = %s
                     ORDER BY ano DESC
                     LIMIT 1
                    """,
                    (nome_ele, nome_ela)
                )
                r2 = cur.fetchone()
                if r2:
                    tel_ele = (r2.get('telefone_ele') or '').strip()
                    tel_ela = (r2.get('telefone_ela') or '').strip()
                    telefones = " / ".join([t for t in [tel_ele, tel_ela] if t])
                    endereco = r2.get('endereco') or ''

        if solo:
            cur.execute(
                """
                SELECT COUNT(*) AS n
                  FROM palestras
                 WHERE palestra = %s AND nome_ele = %s
                """,
                (palestra, nome_ele)
            )
        else:
            cur.execute(
                """
                SELECT COUNT(*) AS n
                  FROM palestras
                 WHERE palestra = %s AND nome_ele = %s AND nome_ela = %s
                """,
                (palestra, nome_ele, nome_ela)
            )
        n = int(((cur.fetchone() or {}).get('n', 0) or 0))

        if not eligible:
            return jsonify({
                "ok": False,
                "eligible": False,
                "cap": 5,
                "repeticoes": n,
                "telefones": telefones,
                "endereco": endereco,
                "msg": "Casal precisa ser encontrista ou já ter trabalhado no ECC."
            }), 403

        return jsonify({
            "ok": True,
            "eligible": True,
            "cap": 5,
            "repeticoes": n,
            "telefones": telefones,
            "endereco": endereco
        })
    finally:
        try:
            cur.close()
            conn.close()
        except Exception:
            pass


@app.route('/api/palestras/save', methods=['POST'], endpoint='api_palestras_save')
def api_palestras_save_compat():
    """Compatibilidade com templates antigos; faz update do título/ano se já existir."""
    data = request.get_json(silent=True) or {}
    ano = data.get('ano')
    palestra = (data.get('titulo') or data.get('palestra') or '').strip()
    nome_ele = (data.get('nome_ele') or '').strip()
    nome_ela = (data.get('nome_ela') or '').strip()

    if not (ano and palestra and nome_ele):
        return jsonify({"ok": False, "msg": "Parâmetros insuficientes."}), 400

    solo = palestra in PALESTRAS_SOLO
    if (not solo) and not nome_ela:
        return jsonify({"ok": False, "msg": "Informe Nome (Ela)."}), 400

    conn = db_conn()
    cur = conn.cursor(dictionary=True)
    try:
        if solo:
            cur.execute(
                """
                SELECT COUNT(*) AS n
                  FROM palestras
                 WHERE palestra = %s AND nome_ele = %s
                """,
                (palestra, nome_ele)
            )
        else:
            cur.execute(
                """
                SELECT COUNT(*) AS n
                  FROM palestras
                 WHERE palestra = %s AND nome_ele = %s AND nome_ela = %s
                """,
                (palestra, nome_ele, nome_ela)
            )
        repeticoes = int(((cur.fetchone() or {}).get('n', 0) or 0))
        if repeticoes >= 5:
            return jsonify({"ok": False, "msg": "Limite de 5 repetições atingido para esta palestra."}), 409

        cur.execute(
            """
            SELECT id
              FROM palestras
             WHERE ano = %s AND palestra = %s
             ORDER BY id DESC
             LIMIT 1
            """,
            (int(ano), palestra)
        )
        existing = cur.fetchone()

        cur2 = conn.cursor()
        if existing:
            if solo:
                cur2.execute(
                    """
                    UPDATE palestras
                       SET nome_ele = %s,
                           nome_ela = '',
                           status = 'Aberto'
                     WHERE id = %s
                    """,
                    (nome_ele, existing['id'])
                )
            else:
                cur2.execute(
                    """
                    UPDATE palestras
                       SET nome_ele = %s,
                           nome_ela = %s,
                           status = 'Aberto'
                     WHERE id = %s
                    """,
                    (nome_ele, nome_ela, existing['id'])
                )
            action = 'update'
        else:
            if solo:
                cur2.execute(
                    """
                    INSERT INTO palestras (ano, palestra, nome_ele, nome_ela, status, observacao)
                    VALUES (%s, %s, %s, '', 'Aberto', NULL)
                    """,
                    (int(ano), palestra, nome_ele)
                )
            else:
                cur2.execute(
                    """
                    INSERT INTO palestras (ano, palestra, nome_ele, nome_ela, status, observacao)
                    VALUES (%s, %s, %s, %s, 'Aberto', NULL)
                    """,
                    (int(ano), palestra, nome_ele, nome_ela)
                )
            action = 'insert'
        conn.commit()
        cur2.close()
        return jsonify({"ok": True, "action": action})
    finally:
        try:
            cur.close()
            conn.close()
        except Exception:
            pass


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

    conn = db_conn()
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

    conn = db_conn()
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

    conn = db_conn()
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

    conn = db_conn()
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

@app.route("/__init_db__")
def __init_db__():
    stmts = [
        """
        CREATE TABLE IF NOT EXISTS encontristas (
          id INT NOT NULL AUTO_INCREMENT,
          ano INT NOT NULL,
          num_ecc VARCHAR(100) NULL,
          data_casamento DATE NULL,
          nome_completo_ele VARCHAR(250) NULL,
          nome_completo_ela VARCHAR(250) NULL,
          nome_usual_ele VARCHAR(120) NULL,
          nome_usual_ela VARCHAR(120) NULL,
          telefone_ele VARCHAR(40) NULL,
          telefone_ela VARCHAR(40) NULL,
          endereco VARCHAR(255) NULL,
          cor_circulo VARCHAR(100) NULL,
          casal_visitacao VARCHAR(255) NULL,
          ficha_num VARCHAR(100) NULL,
          aceitou VARCHAR(20) NULL,
          observacao VARCHAR(255) NULL,
          observacao_extra VARCHAR(255) NULL,
          PRIMARY KEY (id),
          INDEX idx_encontristas_ano (ano),
          INDEX idx_encontristas_nome (nome_usual_ele, nome_usual_ela)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """,
        """
        CREATE TABLE IF NOT EXISTS encontreiros (
          id INT NOT NULL AUTO_INCREMENT,
          ano INT NOT NULL,
          equipe VARCHAR(120) NOT NULL,
          casal VARCHAR(255) NULL,
          nome_ele VARCHAR(120) NOT NULL,
          nome_ela VARCHAR(120) NOT NULL,
          coordenador VARCHAR(10) NOT NULL DEFAULT 'Não',
          telefones VARCHAR(120) NULL,
          endereco VARCHAR(255) NULL,
          observacao TEXT NULL,
          status VARCHAR(40) NULL,
          PRIMARY KEY (id),
          INDEX idx_encontreiros_ano (ano),
          INDEX idx_encontreiros_equipe (equipe),
          INDEX idx_encontreiros_nome (nome_ele, nome_ela)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """,
        """
        CREATE TABLE IF NOT EXISTS implantacao (
          id INT NOT NULL AUTO_INCREMENT,
          ano INT NOT NULL,
          equipe VARCHAR(120) NOT NULL,
          nome_ele VARCHAR(120) NOT NULL,
          nome_ela VARCHAR(120) NOT NULL,
          coordenador VARCHAR(10) NOT NULL DEFAULT 'Não',
          telefones VARCHAR(120) NULL,
          endereco VARCHAR(255) NULL,
          observacao TEXT NULL,
          status VARCHAR(40) NULL,
          PRIMARY KEY (id),
          INDEX idx_implantacao_ano (ano),
          INDEX idx_implantacao_equipe (equipe),
          INDEX idx_implantacao_nome (nome_ele, nome_ela)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """,
        """
        CREATE TABLE IF NOT EXISTS palestras (
          id INT NOT NULL AUTO_INCREMENT,
          ano INT NOT NULL,
          palestra VARCHAR(255) NOT NULL,
          nome_ele VARCHAR(120) NOT NULL,
          nome_ela VARCHAR(120) NOT NULL,
          observacao TEXT NULL,
          status VARCHAR(40) NULL,
          PRIMARY KEY (id),
          INDEX idx_palestras_ano (ano),
          INDEX idx_palestras_palestra (palestra)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """,
        """
        CREATE TABLE IF NOT EXISTS circulos (
          id INT NOT NULL AUTO_INCREMENT,
          ano INT NOT NULL,
          nome_circulo VARCHAR(120) NULL,
          cor_circulo VARCHAR(60) NULL,
          integrantes_original TEXT NULL,
          integrantes_atual TEXT NULL,
          coord_atual_ele VARCHAR(120) NULL,
          coord_atual_ela VARCHAR(120) NULL,
          PRIMARY KEY (id),
          INDEX idx_circulos_ano (ano)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """,
        """
        CREATE TABLE IF NOT EXISTS pendencias_encontreiros (
          id INT NOT NULL AUTO_INCREMENT,
          encontreiros_id INT NOT NULL,
          nome_ele VARCHAR(120) NOT NULL,
          nome_ela VARCHAR(120) NOT NULL,
          candidato_id INT NULL,
          candidato_nome_usual_ele VARCHAR(120) NULL,
          candidato_nome_usual_ela VARCHAR(120) NULL,
          score_ele DECIMAL(10,6) NULL,
          score_ela DECIMAL(10,6) NULL,
          score_medio DECIMAL(10,6) NULL,
          PRIMARY KEY (id),
          INDEX idx_pend_encontreiros_id (encontreiros_id),
          INDEX idx_pend_candidato_id (candidato_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """,
        """
        CREATE TABLE IF NOT EXISTS geocoding_cache (
          id INT NOT NULL AUTO_INCREMENT,
          endereco_hash VARCHAR(64) NOT NULL,
          query VARCHAR(255) NULL,
          status VARCHAR(40) NULL,
          provider VARCHAR(40) NULL,
          lat DECIMAL(10,7) NULL,
          lng DECIMAL(10,7) NULL,
          formatted_address VARCHAR(255) NULL,
          updated_at DATETIME NULL,
          PRIMARY KEY (id),
          UNIQUE KEY uq_geocoding_cache_hash (endereco_hash),
          INDEX idx_geocoding_cache_status (status)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """,
        """
        CREATE TABLE IF NOT EXISTS encontristas_geo (
          id INT NOT NULL AUTO_INCREMENT,
          encontrista_id INT NOT NULL,
          endereco_original VARCHAR(255) NULL,
          endereco_normalizado VARCHAR(255) NULL,
          endereco_hash VARCHAR(64) NULL,
          formatted_address VARCHAR(255) NULL,
          geo_lat DECIMAL(10,7) NULL,
          geo_lng DECIMAL(10,7) NULL,
          geocode_status VARCHAR(40) NULL,
          geocode_source VARCHAR(40) NULL,
          geocode_updated_at DATETIME NULL,
          PRIMARY KEY (id),
          UNIQUE KEY uq_encontristas_geo_encontrista (encontrista_id),
          INDEX idx_encontristas_geo_hash (endereco_hash),
          CONSTRAINT fk_encontristas_geo_encontrista
            FOREIGN KEY (encontrista_id) REFERENCES encontristas(id)
            ON DELETE CASCADE
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """,
    ]

    try:
        conn = db_conn()
        cur = conn.cursor()
        for s in stmts:
            cur.execute(s)
        conn.commit()
        return "OK: tabelas criadas"
    except Exception as e:
        return f"ERRO ao criar tabelas: {type(e).__name__}: {e}", 500
    finally:
        try:
            cur.close()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass

# =========================
# Main
# =========================
if __name__ == "__main__":
    app.run(debug=True)
