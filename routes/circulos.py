from flask import render_template, request, jsonify

from services.circulos_service import (
    listar_circulos,
    buscar_circulo_por_id,
    buscar_encontrista_para_circulo,
    buscar_integrantes_circulo,
    append_integrante_circulo,
    concluir_integrantes_circulo,
    atualizar_campo_circulo,
    listar_candidatos_circulo,
    pesquisar_circulos,
    listar_circulos_transferencia,
    transferir_casal_circulo,
    add_integrante_circulo,
    remove_integrante_circulo,
    copiar_atual_para_original,
    definir_coord_circulo,
)
from utils import _parse_id_list


def register_circulos_routes(app, _encontrista_name_by_id):
    @app.route("/circulos")
    def circulos_list():
        ano = (request.args.get('ano') or '').strip()
        q = (request.args.get('q') or '').strip()

        dados = listar_circulos(ano=ano, q=q)

        return render_template(
            'circulos.html',
            anos_combo=dados["anos_combo"],
            filtros=dados["filtros"],
            anos_ordenados=dados["anos_ordenados"],
            agrupado=dados["agrupado"],
            anos=dados["anos"],
            por_ano=dados["por_ano"]
        )

    @app.route('/circulos/<int:cid>')
    def circulos_view(cid):
        dados = buscar_circulo_por_id(cid)
        if not dados:
            return "Registro não encontrado.", 404

        return render_template(
            'circulos_view.html',
            r=dados["r"],
            rgb_triplet=dados["rgb_triplet"],
            integrantes_list=dados["integrantes_list"],
            integrantes_atual_list=dados["integrantes_atual_list"],
            integrantes_orig_list=dados["integrantes_orig_list"]
        )

    @app.post("/api/circulos/buscar-encontrista")
    def api_circulos_buscar_encontrista():
        data = request.get_json(silent=True) or {}
        ano_circulo = data.get("ano")
        ele = (data.get("ele") or "").strip()
        ela = (data.get("ela") or "").strip()

        if not (ano_circulo and ele and ela):
            return jsonify({"ok": False, "msg": "Informe ano, Ele e Ela."}), 400

        resultado = buscar_encontrista_para_circulo(ano_circulo, ele, ela)
        if not resultado["ok"]:
            return jsonify({"ok": False, "msg": resultado["msg"]}), resultado["status_code"]

        return jsonify(resultado)

    @app.get("/api/circulos/<int:cid>/integrantes")
    def api_circulos_integrantes(cid):
        resultado = buscar_integrantes_circulo(cid)
        if not resultado["ok"]:
            return jsonify({"ok": False, "msg": resultado["msg"]}), resultado["status_code"]

        return jsonify(resultado)

    @app.post("/api/circulos/<int:cid>/integrantes/append")
    def api_circulos_integrantes_append(cid):
        data = request.get_json(silent=True) or {}
        eid = data.get("encontrista_id")
        if not (eid and str(eid).isdigit()):
            return jsonify({"ok": False, "msg": "encontrista_id inválido."}), 400

        resultado = append_integrante_circulo(cid, eid)
        if not resultado["ok"]:
            return jsonify({"ok": False, "msg": resultado["msg"]}), resultado["status_code"]

        return jsonify(resultado)

    @app.post("/api/circulos/<int:cid>/integrantes/concluir")
    def api_circulos_integrantes_concluir(cid):
        resultado = concluir_integrantes_circulo(cid)
        if not resultado["ok"]:
            return jsonify({"ok": False, "msg": resultado["msg"]}), resultado["status_code"]

        return jsonify(resultado)

    @app.post("/api/circulos/<int:cid>/update-field")
    def api_circulos_update_field(cid):
        data = request.get_json(silent=True) or {}
        field = (data.get("field") or "").strip()
        value = data.get("value")

        resultado = atualizar_campo_circulo(cid, field, value)
        if not resultado["ok"]:
            return jsonify({"ok": False, "msg": resultado["msg"]}), resultado["status_code"]

        return jsonify(resultado)

    @app.get("/api/circulos/candidatos")
    def api_circulos_candidatos():
        ano = request.args.get("ano", type=int)
        if not ano:
            return jsonify({"ok": False, "msg": "ano requerido"}), 400

        resultado = listar_candidatos_circulo(ano)
        return jsonify(resultado)

    @app.route("/pesquisa-circulos")
    def pesquisa_circulos():
        dados = pesquisar_circulos()
        return render_template("pesquisa_circulos.html", anos=dados["anos"], por_ano=dados["por_ano"])

    @app.route("/circulos/transferir")
    def circulos_transferir():
        dados = listar_circulos_transferencia()
        return render_template("transferir_circulos.html", anos=dados["anos"], por_ano=dados["por_ano"])

    @app.route("/api/circulos/transferir", methods=["POST"])
    def api_circulos_transferir():
        data = request.get_json(silent=True) or {}
        from_id = data.get("from_id")
        to_id = data.get("to_id")
        pid = data.get("encontrista_id")
        if not (from_id and to_id and pid):
            return jsonify({"ok": False, "msg": "Parâmetros insuficientes."}), 400

        resultado = transferir_casal_circulo(from_id, to_id, pid, _encontrista_name_by_id)
        if not resultado["ok"]:
            return jsonify({"ok": False, "msg": resultado["msg"]}), resultado["status_code"]

        return jsonify(resultado)

    @app.route("/api/circulos/<int:cid>/update", methods=["POST"])
    def api_circulos_update_alias(cid):
        data = request.get_json(silent=True) or {}
        field = (data.get("field") or "").strip()
        value = data.get("value")
        resultado = atualizar_campo_circulo(cid, field, value)
        if not resultado["ok"]:
            return jsonify({"ok": False, "msg": resultado["msg"]}), resultado["status_code"]
        return jsonify(resultado)

    @app.route("/api/circulos/<int:cid>/integrantes/add", methods=["POST"])
    def api_circulos_add_integrante(cid):
        data = request.get_json(silent=True) or {}
        pid = data.get("encontrista_id")
        if not pid:
            return jsonify({"ok": False, "msg": "ID do encontrista obrigatório."}), 400

        resultado = add_integrante_circulo(cid, pid)
        if not resultado["ok"]:
            return jsonify({"ok": False, "msg": resultado["msg"]}), resultado["status_code"]

        return jsonify(resultado)

    @app.route("/api/circulos/<int:cid>/integrantes/remove", methods=["POST"])
    def api_circulos_remove_integrante(cid):
        data = request.get_json(silent=True) or {}
        pid = data.get("encontrista_id")
        if not pid:
            return jsonify({"ok": False, "msg": "ID do encontrista obrigatório."}), 400

        resultado = remove_integrante_circulo(cid, pid, _encontrista_name_by_id)
        if not resultado["ok"]:
            return jsonify({"ok": False, "msg": resultado["msg"]}), resultado["status_code"]

        return jsonify(resultado)

    @app.route("/api/circulos/<int:cid>/integrantes/copy-atual-para-original", methods=["POST"])
    def api_circulos_copy_atual_para_original(cid):
        resultado = copiar_atual_para_original(cid)
        return jsonify(resultado)

    @app.route("/api/circulos/<int:cid>/definir-coord", methods=["POST"])
    def api_circulos_definir_coord(cid):
        data = request.get_json(silent=True) or {}
        pid = data.get("encontrista_id")

        resultado = definir_coord_circulo(cid, pid, _encontrista_name_by_id)
        if not resultado["ok"]:
            return jsonify({"ok": False, "msg": resultado["msg"]}), resultado["status_code"]

        return jsonify(resultado)
