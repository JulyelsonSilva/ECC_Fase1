from flask import render_template, request, jsonify, redirect, url_for, session

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
    sincronizar_circulos_por_encontreiros,
)


def register_circulos_routes(app, _encontrista_name_by_id):

    def paroquia_id_atual():
        return session.get("paroquia_id")

    def exigir_paroquia():
        if not paroquia_id_atual():
            return redirect(url_for("selecionar_paroquia"))
        return None

    @app.route("/circulos")
    def circulos_list():
        bloqueio = exigir_paroquia()
        if bloqueio:
            return bloqueio

        paroquia_id = paroquia_id_atual()
        ano = (request.args.get("ano") or "").strip()
        q = (request.args.get("q") or "").strip()

        sincronizar_circulos_por_encontreiros(paroquia_id)

        dados = listar_circulos(paroquia_id=paroquia_id, ano=ano, q=q)

        return render_template(
            "circulos.html",
            anos_combo=dados["anos_combo"],
            filtros=dados["filtros"],
            anos_ordenados=dados["anos_ordenados"],
            agrupado=dados["agrupado"],
            anos=dados["anos"],
            por_ano=dados["por_ano"]
        )

    @app.route("/circulos/<int:cid>")
    def circulos_view(cid):
        bloqueio = exigir_paroquia()
        if bloqueio:
            return bloqueio

        paroquia_id = paroquia_id_atual()
        dados = buscar_circulo_por_id(cid, paroquia_id)

        if not dados:
            return "Registro não encontrado.", 404

        return render_template(
            "circulos_view.html",
            r=dados["r"],
            rgb_triplet=dados["rgb_triplet"],
            integrantes_list=dados["integrantes_list"],
            integrantes_atual_list=dados["integrantes_atual_list"],
            integrantes_orig_list=dados["integrantes_orig_list"]
        )

    @app.post("/api/circulos/buscar-encontrista")
    def api_circulos_buscar_encontrista():
        paroquia_id = paroquia_id_atual()
        if not paroquia_id:
            return jsonify({"ok": False, "msg": "Paróquia não selecionada."}), 400

        data = request.get_json(silent=True) or {}
        ano_circulo = data.get("ano")
        ele = (data.get("ele") or "").strip()
        ela = (data.get("ela") or "").strip()

        if not (ano_circulo and ele and ela):
            return jsonify({"ok": False, "msg": "Informe ano, Ele e Ela."}), 400

        resultado = buscar_encontrista_para_circulo(paroquia_id, ano_circulo, ele, ela)

        if not resultado["ok"]:
            return jsonify({"ok": False, "msg": resultado["msg"]}), resultado["status_code"]

        return jsonify(resultado)

    @app.get("/api/circulos/<int:cid>/integrantes")
    def api_circulos_integrantes(cid):
        paroquia_id = paroquia_id_atual()
        if not paroquia_id:
            return jsonify({"ok": False, "msg": "Paróquia não selecionada."}), 400

        resultado = buscar_integrantes_circulo(cid, paroquia_id)

        if not resultado["ok"]:
            return jsonify({"ok": False, "msg": resultado["msg"]}), resultado["status_code"]

        return jsonify(resultado)

    @app.post("/api/circulos/<int:cid>/integrantes/append")
    def api_circulos_integrantes_append(cid):
        paroquia_id = paroquia_id_atual()
        if not paroquia_id:
            return jsonify({"ok": False, "msg": "Paróquia não selecionada."}), 400

        data = request.get_json(silent=True) or {}
        eid = data.get("encontrista_id")

        if not (eid and str(eid).isdigit()):
            return jsonify({"ok": False, "msg": "encontrista_id inválido."}), 400

        resultado = append_integrante_circulo(cid, int(eid), paroquia_id)

        if not resultado["ok"]:
            return jsonify({"ok": False, "msg": resultado["msg"]}), resultado["status_code"]

        return jsonify(resultado)

    @app.post("/api/circulos/<int:cid>/integrantes/concluir")
    def api_circulos_integrantes_concluir(cid):
        paroquia_id = paroquia_id_atual()
        if not paroquia_id:
            return jsonify({"ok": False, "msg": "Paróquia não selecionada."}), 400

        resultado = concluir_integrantes_circulo(cid, paroquia_id)

        if not resultado["ok"]:
            return jsonify({"ok": False, "msg": resultado["msg"]}), resultado["status_code"]

        return jsonify(resultado)

    @app.post("/api/circulos/<int:cid>/update-field")
    def api_circulos_update_field(cid):
        paroquia_id = paroquia_id_atual()
        if not paroquia_id:
            return jsonify({"ok": False, "msg": "Paróquia não selecionada."}), 400

        data = request.get_json(silent=True) or {}
        field = (data.get("field") or "").strip()
        value = data.get("value")

        resultado = atualizar_campo_circulo(cid, paroquia_id, field, value)

        if not resultado["ok"]:
            return jsonify({"ok": False, "msg": resultado["msg"]}), resultado["status_code"]

        return jsonify(resultado)

    @app.get("/api/circulos/candidatos")
    def api_circulos_candidatos():
        paroquia_id = paroquia_id_atual()
        if not paroquia_id:
            return jsonify({"ok": False, "msg": "Paróquia não selecionada."}), 400

        ano = request.args.get("ano", type=int)

        if not ano:
            return jsonify({"ok": False, "msg": "ano requerido"}), 400

        resultado = listar_candidatos_circulo(paroquia_id, ano)
        return jsonify(resultado)

    @app.route("/pesquisa-circulos")
    def pesquisa_circulos():
        bloqueio = exigir_paroquia()
        if bloqueio:
            return bloqueio

        paroquia_id = paroquia_id_atual()

        sincronizar_circulos_por_encontreiros(paroquia_id)

        dados = pesquisar_circulos(paroquia_id)

        return render_template(
            "pesquisa_circulos.html",
            anos=dados["anos"],
            por_ano=dados["por_ano"]
        )

    @app.route("/circulos/transferir")
    def circulos_transferir():
        bloqueio = exigir_paroquia()
        if bloqueio:
            return bloqueio

        paroquia_id = paroquia_id_atual()

        sincronizar_circulos_por_encontreiros(paroquia_id)

        dados = listar_circulos_transferencia(paroquia_id)

        return render_template(
            "transferir_circulos.html",
            anos=dados["anos"],
            por_ano=dados["por_ano"]
        )

    @app.route("/api/circulos/transferir", methods=["POST"])
    def api_circulos_transferir():
        paroquia_id = paroquia_id_atual()
        if not paroquia_id:
            return jsonify({"ok": False, "msg": "Paróquia não selecionada."}), 400

        data = request.get_json(silent=True) or {}
        from_id = data.get("from_id")
        to_id = data.get("to_id")
        pid = data.get("encontrista_id")

        if not (from_id and to_id and pid):
            return jsonify({"ok": False, "msg": "Parâmetros insuficientes."}), 400

        resultado = transferir_casal_circulo(
            int(from_id),
            int(to_id),
            int(pid),
            paroquia_id,
            _encontrista_name_by_id
        )

        if not resultado["ok"]:
            return jsonify({"ok": False, "msg": resultado["msg"]}), resultado["status_code"]

        return jsonify(resultado)

    @app.route("/api/circulos/<int:cid>/update", methods=["POST"])
    def api_circulos_update_alias(cid):
        paroquia_id = paroquia_id_atual()
        if not paroquia_id:
            return jsonify({"ok": False, "msg": "Paróquia não selecionada."}), 400

        data = request.get_json(silent=True) or {}
        field = (data.get("field") or "").strip()
        value = data.get("value")

        resultado = atualizar_campo_circulo(cid, paroquia_id, field, value)

        if not resultado["ok"]:
            return jsonify({"ok": False, "msg": resultado["msg"]}), resultado["status_code"]

        return jsonify(resultado)

    @app.route("/api/circulos/<int:cid>/integrantes/add", methods=["POST"])
    def api_circulos_add_integrante(cid):
        paroquia_id = paroquia_id_atual()
        if not paroquia_id:
            return jsonify({"ok": False, "msg": "Paróquia não selecionada."}), 400

        data = request.get_json(silent=True) or {}
        pid = data.get("encontrista_id")

        if not pid:
            return jsonify({"ok": False, "msg": "ID do encontrista obrigatório."}), 400

        resultado = add_integrante_circulo(cid, int(pid), paroquia_id)

        if not resultado["ok"]:
            return jsonify({"ok": False, "msg": resultado["msg"]}), resultado["status_code"]

        return jsonify(resultado)

    @app.route("/api/circulos/<int:cid>/integrantes/remove", methods=["POST"])
    def api_circulos_remove_integrante(cid):
        paroquia_id = paroquia_id_atual()
        if not paroquia_id:
            return jsonify({"ok": False, "msg": "Paróquia não selecionada."}), 400

        data = request.get_json(silent=True) or {}
        pid = data.get("encontrista_id")

        if not pid:
            return jsonify({"ok": False, "msg": "ID do encontrista obrigatório."}), 400

        resultado = remove_integrante_circulo(
            cid,
            int(pid),
            paroquia_id,
            _encontrista_name_by_id
        )

        if not resultado["ok"]:
            return jsonify({"ok": False, "msg": resultado["msg"]}), resultado["status_code"]

        return jsonify(resultado)

    @app.route("/api/circulos/<int:cid>/integrantes/copy-atual-para-original", methods=["POST"])
    def api_circulos_copy_atual_para_original(cid):
        paroquia_id = paroquia_id_atual()
        if not paroquia_id:
            return jsonify({"ok": False, "msg": "Paróquia não selecionada."}), 400

        resultado = copiar_atual_para_original(cid, paroquia_id)
        return jsonify(resultado)

    @app.route("/api/circulos/<int:cid>/definir-coord", methods=["POST"])
    def api_circulos_definir_coord(cid):
        paroquia_id = paroquia_id_atual()
        if not paroquia_id:
            return jsonify({"ok": False, "msg": "Paróquia não selecionada."}), 400

        data = request.get_json(silent=True) or {}
        pid = data.get("encontrista_id")

        resultado = definir_coord_circulo(
            cid,
            int(pid) if pid else None,
            paroquia_id,
            _encontrista_name_by_id
        )

        if not resultado["ok"]:
            return jsonify({"ok": False, "msg": resultado["msg"]}), resultado["status_code"]

        return jsonify(resultado)
