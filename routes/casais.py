from flask import request, jsonify

from services.casais_service import montar_resposta_busca_casal
from utils import paroquia_id_atual


def register_casais_routes(app):

    @app.route("/api/casais/buscar")
    def api_casais_buscar():
        paroquia_id = paroquia_id_atual()

        if not paroquia_id:
            return jsonify({
                "ok": False,
                "modo": "erro",
                "msg": "Paróquia não selecionada.",
                "casal": None,
                "casais": [],
            }), 400

        nome_ele = (request.args.get("nome_ele") or "").strip()
        nome_ela = (request.args.get("nome_ela") or "").strip()

        limite = request.args.get("limite", default=50, type=int)

        resposta = montar_resposta_busca_casal(
            paroquia_id=paroquia_id,
            nome_ele=nome_ele,
            nome_ela=nome_ela,
            limite=limite
        )

        return jsonify(resposta)
