from flask import render_template, request, jsonify, redirect, url_for, session

from services.montagem_service import (
    listar_montagem_por_ano,
    carregar_dados_iniciais_montagem,
    buscar_casal_para_montagem,
    adicionar_dirigente_montagem,
    buscar_cg_montagem,
    adicionar_cg_montagem,
    contar_equipes_montagem,
    carregar_equipe_montagem,
    check_casal_equipe,
    add_membro_equipe,
    marcar_status_dirigente,
    marcar_status_membro,
    validar_requisitos_montagem_ano,
    concluir_montagem_ano,
    buscar_dados_organograma,
    buscar_relatorio_montagem,
)


def register_montagem_routes(
    app,
    TEAM_MAP,
    TEAM_LIMITS,
    _team_label,
):
    def paroquia_id_atual():
        return session.get("paroquia_id")

    def exigir_paroquia():
        if not paroquia_id_atual():
            return redirect(url_for("selecionar_paroquia"))
        return None

    # =========================
    # MONTAGEM (Aberto x Concluído)
    # =========================
    @app.route('/montagem')
    def montagem():
        bloqueio = exigir_paroquia()
        if bloqueio:
            return bloqueio

        paroquia_id = paroquia_id_atual()

        dados = listar_montagem_por_ano(paroquia_id)
        return render_template(
            'montagem.html',
            anos_aberto=dados["anos_aberto"],
            anos_concluidos=dados["anos_concluidos"]
        )

    # =========================
    # Nova Montagem + APIs Dirigentes/CG
    # =========================
    @app.route('/montagem/nova')
    def nova_montagem():
        bloqueio = exigir_paroquia()
        if bloqueio:
            return bloqueio

        paroquia_id = paroquia_id_atual()
        ano_preselecionado = request.args.get('ano', type=int)

        initial_data = carregar_dados_iniciais_montagem(
            ano_preselecionado,
            TEAM_MAP,
            paroquia_id
        )

        return render_template(
            'nova_montagem.html',
            ano_preselecionado=ano_preselecionado,
            initial_data=initial_data,
            team_map=TEAM_MAP
        )

    @app.route('/api/buscar-casal', methods=['POST'])
    def api_buscar_casal():
        paroquia_id = paroquia_id_atual()
        if not paroquia_id:
            return jsonify({"ok": False, "msg": "Paróquia não selecionada."}), 400

        data = request.get_json(silent=True) or {}
        nome_ele = (data.get('nome_ele') or '').strip()
        nome_ela = (data.get('nome_ela') or '').strip()

        if not nome_ele or not nome_ela:
            return jsonify({"ok": False, "msg": "Informe nome_ele e nome_ela."}), 400

        resultado = buscar_casal_para_montagem(
            nome_ele,
            nome_ela,
            paroquia_id
        )

        if not resultado["ok"]:
            return jsonify(resultado), resultado.get("status_code", 404)

        return jsonify(resultado)

    @app.route('/api/adicionar-dirigente', methods=['POST'])
    def api_adicionar_dirigente():
        paroquia_id = paroquia_id_atual()
        if not paroquia_id:
            return jsonify({"ok": False, "msg": "Paróquia não selecionada."}), 400

        data = request.get_json(silent=True) or {}

        ano = (str(data.get('ano') or '')).strip()
        equipe = _team_label((data.get('equipe') or '').strip())
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

        adicionar_dirigente_montagem(
            int(ano),
            equipe,
            nome_ele,
            nome_ela,
            telefones,
            endereco,
            paroquia_id
        )

        return jsonify({"ok": True})

    @app.route('/api/buscar-cg', methods=['POST'])
    def api_buscar_cg():
        paroquia_id = paroquia_id_atual()
        if not paroquia_id:
            return jsonify({"ok": False, "msg": "Paróquia não selecionada."}), 400

        data = request.get_json(silent=True) or {}

        nome_ele = (data.get('nome_ele') or '').strip()
        nome_ela = (data.get('nome_ela') or '').strip()

        if not nome_ele or not nome_ela:
            return jsonify({"ok": False, "msg": "Informe nome_ele e nome_ela."}), 400

        resultado = buscar_cg_montagem(
            nome_ele,
            nome_ela,
            paroquia_id
        )

        if not resultado["ok"]:
            return jsonify({"ok": False, "msg": resultado["msg"]}), resultado["status_code"]

        return jsonify(resultado)

    @app.route('/api/adicionar-cg', methods=['POST'])
    def api_adicionar_cg():
        paroquia_id = paroquia_id_atual()
        if not paroquia_id:
            return jsonify({"ok": False, "msg": "Paróquia não selecionada."}), 400

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

        resultado = adicionar_cg_montagem(
            int(ano),
            nome_ele,
            nome_ela,
            telefones,
            endereco,
            paroquia_id
        )

        if not resultado["ok"]:
            return jsonify({"ok": False, "msg": resultado["msg"]}), resultado["status_code"]

        return jsonify({"ok": True})

    @app.route('/api/equipe-counts')
    def api_equipe_counts():
        paroquia_id = paroquia_id_atual()
        if not paroquia_id:
            return jsonify({"ok": False, "msg": "Paróquia não selecionada.", "counts": {}}), 400

        ano = request.args.get('ano', type=int)

        if not ano:
            return jsonify({"ok": False, "msg": "Ano obrigatório.", "counts": {}}), 400

        counts = contar_equipes_montagem(
            ano,
            TEAM_MAP,
            paroquia_id
        )

        return jsonify({"ok": True, "counts": counts})

    # =========================
    # Montagem de Equipe (integrantes) + APIs auxiliares
    # =========================
    @app.route('/equipe-montagem')
    def equipe_montagem():
        bloqueio = exigir_paroquia()
        if bloqueio:
            return bloqueio

        paroquia_id = paroquia_id_atual()

        ano = request.args.get('ano', type=int)
        equipe_filtro = (request.args.get('equipe') or '').strip()

        dados = carregar_equipe_montagem(
            ano,
            equipe_filtro,
            TEAM_MAP,
            TEAM_LIMITS,
            paroquia_id
        )

        if dados["modo"] == "sala":
            return render_template(
                'equipe_montagem_sala.html',
                ano=dados["ano"],
                limites=dados["limites"],
                sala_slots=dados["sala_slots"],
                sugestoes_prev_ano=dados["sugestoes_prev_ano"],
                pref_recepcao=dados["pref_recepcao"]
            )

        return render_template(
            'equipe_montagem.html',
            ano=dados["ano"],
            equipe=dados["equipe"],
            equipe_final=dados["equipe_final"],
            limites=dados["limites"],
            membros_existentes=dados["membros_existentes"],
            sugestoes_prev_ano=dados["sugestoes_prev_ano"]
        )

    @app.route('/api/check-casal-equipe', methods=['POST'])
    def api_check_casal_equipe():
        paroquia_id = paroquia_id_atual()
        if not paroquia_id:
            return jsonify({"ok": False, "msg": "Paróquia não selecionada."}), 400

        data = request.get_json(silent=True) or {}

        ano = data.get('ano')
        equipe_final = _team_label((data.get('equipe_final') or '').strip())
        nome_ele = (data.get('nome_ele') or '').strip()
        nome_ela = (data.get('nome_ela') or '').strip()

        if not (ano and equipe_final and nome_ele and nome_ela):
            return jsonify({"ok": False, "msg": "Parâmetros insuficientes."}), 400

        dados = check_casal_equipe(
            ano,
            equipe_final,
            nome_ele,
            nome_ela,
            paroquia_id
        )

        return jsonify({
            "ok": True,
            "ja_coordenador": dados["ja_coordenador"],
            "trabalhou_antes": dados["trabalhou_antes"],
            "ja_no_ano": dados["ja_no_ano"],
            "telefones": dados["telefones"],
            "endereco": dados["endereco"]
        })

    @app.route('/api/add-membro-equipe', methods=['POST'])
    def api_add_membro_equipe():
        paroquia_id = paroquia_id_atual()
        if not paroquia_id:
            return jsonify({"ok": False, "msg": "Paróquia não selecionada."}), 400

        data = request.get_json(silent=True) or {}

        ano = data.get('ano')
        equipe_final = _team_label((data.get('equipe_final') or '').strip())
        nome_ele = (data.get('nome_ele') or '').strip()
        nome_ela = (data.get('nome_ela') or '').strip()
        telefones = (data.get('telefones') or '').strip()
        endereco = (data.get('endereco') or '').strip()
        confirmar_repeticao = bool(data.get('confirmar_repeticao'))

        if not (ano and equipe_final and nome_ele and nome_ela):
            return jsonify({"ok": False, "msg": "Parâmetros insuficientes."}), 400

        resultado = add_membro_equipe(
            ano,
            equipe_final,
            nome_ele,
            nome_ela,
            telefones,
            endereco,
            confirmar_repeticao,
            paroquia_id
        )

        if not resultado["ok"]:
            payload = {"ok": False, "msg": resultado["msg"]}

            if resultado.get("needs_confirm"):
                payload["needs_confirm"] = True

            return jsonify(payload), resultado["status_code"]

        return jsonify({"ok": True, "id": resultado["id"]})

    @app.route('/api/marcar-status-dirigente', methods=['POST'])
    def api_marcar_status_dirigente():
        paroquia_id = paroquia_id_atual()
        if not paroquia_id:
            return jsonify({"ok": False, "msg": "Paróquia não selecionada."}), 400

        data = request.get_json(silent=True) or {}

        ano = data.get('ano')
        equipe = _team_label((data.get('equipe') or '').strip())
        novo_status = (data.get('novo_status') or '').strip()
        observacao = (data.get('observacao') or '').strip()

        if not (ano and equipe and novo_status in ('Recusou', 'Desistiu') and observacao):
            return jsonify({"ok": False, "msg": "Parâmetros inválidos. Observação é obrigatória."}), 400

        alterados = marcar_status_dirigente(
            ano,
            equipe,
            novo_status,
            observacao,
            paroquia_id
        )

        if alterados == 0:
            return jsonify({"ok": False, "msg": "Nenhum registro ABERTO encontrado para alterar."}), 404

        return jsonify({"ok": True})

    @app.route('/api/marcar-status-membro', methods=['POST'])
    def api_marcar_status_membro():
        paroquia_id = paroquia_id_atual()
        if not paroquia_id:
            return jsonify({"ok": False, "msg": "Paróquia não selecionada."}), 400

        data = request.get_json(silent=True) or {}

        _id = data.get('id')
        novo_status = (data.get('novo_status') or '').strip()
        observacao = (data.get('observacao') or '').strip()

        if not (_id and novo_status in ('Recusou', 'Desistiu') and observacao):
            return jsonify({"ok": False, "msg": "Parâmetros inválidos. Observação é obrigatória."}), 400

        alterados = marcar_status_membro(
            _id,
            novo_status,
            observacao,
            paroquia_id
        )

        if alterados == 0:
            return jsonify({"ok": False, "msg": "Registro não encontrado ou não alterável."}), 404

        return jsonify({"ok": True})

    @app.route('/api/validar-montagem-ano', methods=['POST'])
    def api_validar_montagem_ano():
        paroquia_id = paroquia_id_atual()
        if not paroquia_id:
            return jsonify({"ok": False, "msg": "Paróquia não selecionada.", "pendencias": []}), 400

        data = request.get_json(silent=True) or {}
        ano = data.get('ano')

        if not ano:
            return jsonify({"ok": False, "msg": "Ano obrigatório.", "pendencias": []}), 400

        resultado = validar_requisitos_montagem_ano(
            ano,
            TEAM_MAP,
            TEAM_LIMITS,
            paroquia_id
        )

        status_code = 200 if resultado["ok"] else 409
        return jsonify(resultado), status_code

    @app.route('/api/concluir-montagem-ano', methods=['POST'])
    def api_concluir_montagem_ano():
        paroquia_id = paroquia_id_atual()
        if not paroquia_id:
            return jsonify({"ok": False, "msg": "Paróquia não selecionada.", "pendencias": []}), 400

        data = request.get_json(silent=True) or {}
        ano = data.get('ano')

        if not ano:
            return jsonify({"ok": False, "msg": "Ano obrigatório.", "pendencias": []}), 400

        resultado = concluir_montagem_ano(
            ano,
            TEAM_MAP,
            TEAM_LIMITS,
            paroquia_id
        )

        status_code = 200 if resultado["ok"] else 409
        return jsonify(resultado), status_code

    # =========================
    # Organograma
    # =========================
    @app.route('/organograma')
    def organograma():
        bloqueio = exigir_paroquia()
        if bloqueio:
            return bloqueio

        return render_template('organograma.html')

    @app.route('/dados-organograma')
    def dados_organograma():
        paroquia_id = paroquia_id_atual()
        if not paroquia_id:
            return jsonify([])

        ano = request.args.get("ano", type=int)

        if not ano:
            return jsonify([])

        dados = buscar_dados_organograma(
            ano,
            paroquia_id
        )

        return jsonify(dados)

    @app.route("/imprimir/relatorio-montagem")
    def imprimir_relatorio_montagem():
        bloqueio = exigir_paroquia()
        if bloqueio:
            return bloqueio

        paroquia_id = paroquia_id_atual()
        ano = request.args.get("ano", type=int)

        try:
            rows = buscar_relatorio_montagem(
                ano,
                paroquia_id
            )

            return render_template(
                "relatorio_montagem.html",
                ano=ano,
                rows=rows
            )

        except Exception as e:
            return f"Erro ao gerar relatório de montagem: {e}", 500