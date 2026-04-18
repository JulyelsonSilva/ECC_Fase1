from flask import jsonify
from services.schema_service import ensure_database_schema


def register_admin_routes(app, _admin_ok):

    @app.route("/__init_db__")
    def init_db_route():
        if not _admin_ok():
            return jsonify({"ok": False, "msg": "Acesso negado"}), 403

        try:
            resultado = ensure_database_schema()
            return jsonify(resultado)
        except Exception as e:
            return jsonify({"ok": False, "msg": str(e)}), 500
