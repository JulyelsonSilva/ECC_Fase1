from flask import Flask

from config import (
    SECRET_KEY,
    DB_CONFIG,
    TEAM_MAP,
    TEAM_LIMITS,
    PALESTRAS_TITULOS,
    PALESTRAS_SOLO,
)

from db import safe_fetch_one
from auth import _admin_ok
from utils import _norm, _sim, _q, _team_label
from services.shared_service import encontrista_name_by_id

from routes.encontristas import register_encontristas_routes
from routes.encontreiros import register_encontreiros_routes
from routes.circulos import register_circulos_routes
from routes.montagem import register_montagem_routes
from routes.palestras import register_palestras_routes
from routes.core import register_core_routes
from routes.admin import register_admin_routes
from routes.vinculos import register_vinculos_routes


app = Flask(__name__)
app.config["SECRET_KEY"] = SECRET_KEY

register_encontristas_routes(app, encontrista_name_by_id)
register_encontreiros_routes(app, PALESTRAS_TITULOS, PALESTRAS_SOLO, DB_CONFIG, safe_fetch_one)
register_circulos_routes(app, encontrista_name_by_id)
register_montagem_routes(app, TEAM_MAP, TEAM_LIMITS, _team_label)
register_palestras_routes(app, PALESTRAS_TITULOS, PALESTRAS_SOLO)
register_core_routes(app, TEAM_MAP, TEAM_LIMITS, _q)
register_admin_routes(app, _admin_ok)
register_vinculos_routes(app, _admin_ok, _norm, _sim)


if __name__ == "__main__":
    app.run(debug=True)