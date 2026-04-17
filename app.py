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
from routes.palestras import register_palestras_routes
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
register_palestras_routes(app, PALESTRAS_TITULOS, PALESTRAS_SOLO)
register_core_routes(app, TEAM_MAP, TEAM_LIMITS, _q)
register_admin_routes(app, _admin_ok, _get_db, _norm, _sim, SequenceMatcher)

# --- KPI: contagem de integrantes por equipe (exclui Coordenador; exclui Recusou/Desistiu) ---
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
