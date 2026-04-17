from difflib import SequenceMatcher

from flask import Flask

from config import (
    SECRET_KEY,
    DB_CONFIG,
    TEAM_MAP,
    TEAM_LIMITS,
    TEAM_CHOICES,
    PALESTRAS_TITULOS,
    PALESTRAS_SOLO,
)

from db import safe_fetch_one, _get_db
from auth import _admin_ok
from utils import _norm, _sim, _q

from routes.encontristas import register_encontristas_routes
from routes.encontreiros import register_encontreiros_routes
from routes.circulos import register_circulos_routes
from routes.montagem import register_montagem_routes
from routes.implantacao import register_implantacao_routes
from routes.palestras import register_palestras_routes
from routes.core import register_core_routes
from routes.admin import register_admin_routes


def _encontrista_name_by_id(conn, _id):
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute("SELECT nome_usual_ele, nome_usual_ela FROM encontristas WHERE id=%s", (_id,))
        r = cur.fetchone()
        if not r:
            return None, None
        return (r.get("nome_usual_ele") or ""), (r.get("nome_usual_ela") or "")
    finally:
        cur.close()


def _team_label(value: str) -> str:
    """Normaliza chave/filtro curto para o rótulo salvo no banco."""
    v = (value or '').strip()
    if not v:
        return v

    vl = v.lower()
    for key, info in TEAM_MAP.items():
        if (
            vl == key.lower()
            or vl == (info.get('filtro') or '').lower()
            or vl == (info.get('rotulo') or '').lower()
        ):
            return info['rotulo']
    return v


app = Flask(__name__)
app.config["SECRET_KEY"] = SECRET_KEY

register_encontristas_routes(app, _encontrista_name_by_id)
register_encontreiros_routes(app, PALESTRAS_TITULOS, PALESTRAS_SOLO, DB_CONFIG, safe_fetch_one)
register_circulos_routes(app, _encontrista_name_by_id)
register_montagem_routes(app, TEAM_MAP, TEAM_LIMITS, _team_label)
register_implantacao_routes(app, TEAM_MAP, TEAM_LIMITS, TEAM_CHOICES, _team_label)
register_palestras_routes(app, PALESTRAS_TITULOS, PALESTRAS_SOLO)
register_core_routes(app, TEAM_MAP, TEAM_LIMITS, _q)
register_admin_routes(app, _admin_ok, _get_db, _norm, _sim, SequenceMatcher)


if __name__ == "__main__":
    app.run(debug=True)
