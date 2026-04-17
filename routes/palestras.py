from flask import render_template, request, jsonify

from services.palestras_service import (
    listar_anos_palestras,
    carregar_palestras_do_ano,
    obter_dados_casal_palestra,
    contar_repeticoes_palestra,
    salvar_palestra_ano,
    adicionar_palestra,
    encerrar_palestras_ano,
    marcar_status_palestra_por_id,
    marcar_status_palestra_por_criterios,
)


def register_palestras_routes(
    app,
    PALESTRAS_TITULOS,
    PALESTRAS_SOLO,
):
    @app.route('/palestras')
    def palestras_painel():
        dados = listar_anos_palestras()
        return render_template(
            'palestras_painel.html',
            anos_aberto=dados["anos_aberto"],
            anos_concluidos=dados["anos_concluidos"]
        )

    @app.route('/palestras/nova')
    def palestras_nova():
        ano_preselecionado = request.args.get('ano', type=int)
        existentes = {}
        tem_abertos = False

        if ano_preselecionado:
            dados = carregar_palestras_do_ano(ano_preselecionado)
            existentes = dados["existentes"]
            tem_abertos = dados["tem_abertos"]

        return render_template(
            'nova_palestras.html',
            ano_preselecionado=ano_preselecionado,
            titulos=PALESTRAS_TITULOS,
            solo_titulos=list(PALESTRAS_SOLO),
            existentes=existentes,
            tem_abertos=tem_abertos
        )

    app.add_url_rule('/palestras/nova', endpoint='nova_palestra', view_func=palestras_nova)

    @app.route('/api/palestras/validate', methods=['POST'], endpoint='api_palestras_validate')
    def api_palestras_validate_compat():
        data = request.get_json(silent=True) or {}
        palestra = (data.get('titulo') or data.get('palestra') or '').strip()
        nome_ele = (data.get('nome_ele') or '').strip()
        nome_ela = (data.get('nome_ela') or '').strip()

        if not (palestra and nome_ele):
            return jsonify({"ok": False, "msg": "Parâmetros insuficientes."}), 400

        solo = palestra in PALESTRAS_SOLO
        if (not solo) and not nome_ela:
            return jsonify({"ok": False, "msg": "Informe Nome (Ela)."}), 400

        dados = obter_dados_casal_palestra(nome_ele, nome_ela if not solo else "", solo=solo)
        repeticoes = contar_repeticoes_palestra(
            palestra,
            nome_ele,
            nome_ela if not solo else "",
            solo=solo
        )

        if not dados["eligible"]:
            return jsonify({
                "ok": False,
                "eligible": False,
                "cap": 5,
                "repeticoes": repeticoes,
                "telefones": dados["telefones"],
                "endereco": dados["endereco"],
                "msg": "Casal precisa ser encontrista ou já ter trabalhado no ECC."
            }), 403

        return jsonify({
            "ok": True,
            "eligible": True,
            "cap": 5,
            "repeticoes": repeticoes,
            "telefones": dados["telefones"],
            "endereco": dados["endereco"]
        })

    @app.route('/api/palestras/save', methods=['POST'], endpoint='api_palestras_save')
    def api_palestras_save_compat():
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

        repeticoes = contar_repeticoes_palestra(
            palestra,
            nome_ele,
            nome_ela if not solo else "",
            solo=solo
        )
        if repeticoes >= 5:
            return jsonify({"ok": False, "msg": "Limite de 5 repetições atingido para esta palestra."}), 409

        action = salvar_palestra_ano(
            ano,
            palestra,
            nome_ele,
            nome_ela if not solo else "",
            solo=solo
        )
        return jsonify({"ok": True, "action": action})

    @app.route('/api/palestras/buscar', methods=['POST'])
    def api_palestras_buscar():
        data = request.get_json(silent=True) or {}
        palestra = (data.get('palestra') or '').strip()
        nome_ele = (data.get('nome_ele') or '').strip()
        nome_ela = (data.get('nome_ela') or '').strip()

        if not (palestra and nome_ele and nome_ela):
            return jsonify({"ok": False, "msg": "Parâmetros insuficientes."}), 400

        dados = obter_dados_casal_palestra(nome_ele, nome_ela, solo=False)
        if not dados["eligible"]:
            return jsonify({"ok": False, "msg": "Casal precisa ser encontrista ou já ter trabalhado no ECC."}), 403

        repeticoes = contar_repeticoes_palestra(palestra, nome_ele, nome_ela, solo=False)
        if repeticoes >= 5:
            return jsonify({
                "ok": False,
                "repeticoes": repeticoes,
                "msg": "Limite de 5 repetições atingido para esta palestra."
            }), 409

        return jsonify({
            "ok": True,
            "telefones": dados["telefones"],
            "endereco": dados["endereco"],
            "repeticoes": repeticoes
        })

    @app.route('/api/palestras/adicionar', methods=['POST'])
    def api_palestras_adicionar():
        data = request.get_json(silent=True) or {}
        ano = data.get('ano')
        palestra = (data.get('palestra') or '').strip()
        nome_ele = (data.get('nome_ele') or '').strip()
        nome_ela = (data.get('nome_ela') or '').strip()

        if not (ano and palestra and nome_ele):
            return jsonify({"ok": False, "msg": "Parâmetros insuficientes."}), 400

        solo = palestra in PALESTRAS_SOLO
        if (not solo) and not nome_ela:
            return jsonify({"ok": False, "msg": "Informe Nome (Ela)."}), 400

        if not solo:
            repeticoes = contar_repeticoes_palestra(palestra, nome_ele, nome_ela, solo=False)
            if repeticoes >= 5:
                return jsonify({"ok": False, "msg": "Limite de 5 repetições atingido para esta palestra."}), 409

        adicionar_palestra(
            ano,
            palestra,
            nome_ele,
            nome_ela if not solo else "",
            solo=solo
        )
        return jsonify({"ok": True})

    @app.route('/api/palestras/encerrar', methods=['POST'])
    def api_palestras_encerrar():
        data = request.get_json(silent=True) or {}
        ano = data.get('ano')
        if not ano:
            return jsonify({"ok": False, "msg": "Ano obrigatório."}), 400

        alterados = encerrar_palestras_ano(ano)
        return jsonify({"ok": True, "alterados": alterados})

    app.add_url_rule(
        '/api/palestras/encerrar-ano',
        endpoint='api_palestras_encerrar_ano',
        view_func=api_palestras_encerrar,
        methods=['POST']
    )

    @app.route('/api/palestras/marcar-status', methods=['POST'])
    def api_palestras_marcar_status():
        data = request.get_json(silent=True) or {}
        _id = data.get('id')
        novo_status = (data.get('novo_status') or '').strip().title()
        observacao = (data.get('observacao') or '').strip()

        if novo_status not in ('Recusou', 'Desistiu'):
            return jsonify({"ok": False, "msg": "novo_status deve ser 'Recusou' ou 'Desistiu'."}), 400
        if not observacao:
            return jsonify({"ok": False, "msg": "Observação é obrigatória."}), 400

        if _id:
            alterados = marcar_status_palestra_por_id(_id, novo_status, observacao)
            if alterados == 0:
                return jsonify({"ok": False, "msg": "Registro não encontrado ou não alterável."}), 404
            return jsonify({"ok": True})

        ano = data.get('ano')
        palestra = (data.get('palestra') or '').strip()
        if not (ano and palestra):
            return jsonify({"ok": False, "msg": "Informe id, ou ano e palestra."}), 400

        nome_ele = (data.get('nome_ele') or '').strip()
        nome_ela = (data.get('nome_ela') or '').strip()

        alterados = marcar_status_palestra_por_criterios(
            ano,
            palestra,
            novo_status,
            observacao,
            nome_ele=nome_ele,
            nome_ela=nome_ela
        )
        if alterados == 0:
            return jsonify({"ok": False, "msg": "Registro não encontrado ou não alterável com os critérios informados."}), 404

        return jsonify({"ok": True})
