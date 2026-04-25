from flask import render_template, request, jsonify

from services.implantacao_service import (
    listar_implantacao_por_ano,
    buscar_sugestoes_prev_ano_implantacao,
    contar_implantacao_por_equipe,
    checar_casal_implantacao,
    adicionar_membro_implantacao,
    marcar_status_implantacao,
    concluir_implantacao_ano,
)


def register_implantacao_routes(
    app,
    TEAM_MAP,
    TEAM_LIMITS,
    TEAM_CHOICES,
    _team_label,
):
    @app.route('/implantacao')
    def implantacao_painel():
        dados = listar_implantacao_por_ano()
        return render_template(
            'implantacao.html',
            anos_aberto=dados["anos_aberto"],
            anos_concluidos=dados["anos_concluidos"]
        )

    @app.route('/implantacao/nova')
    def implantacao_nova():
        ano_pre = request.args.get('ano', type=int)

        sugestoes_prev_ano = buscar_sugestoes_prev_ano_implantacao(ano_pre)

        return render_template(
            'implantacao.html',
            ano_preselecionado=ano_pre,
            team_map=TEAM_MAP,
            team_limits=TEAM_LIMITS,
            team_choices=TEAM_CHOICES,
            sugestoes_prev_ano=sugestoes_prev_ano
        )

    @app.route('/api/implantacao/equipe-counts')
    def api_implantacao_equipe_counts():
        ano = request.args.get('ano', type=int)
        if not ano:
            return jsonify({"ok": False, "msg": "Ano obrigatório.", "counts": {}}), 400

        counts = contar_implantacao_por_equipe(ano, TEAM_MAP)
        return jsonify({"ok": True, "counts": counts})

    @app.route('/api/implantacao/check-casal', methods=['POST'])
    def api_implantacao_check_casal():
        data = request.get_json(silent=True) or {}
        ano = data.get('ano')
        equipe = _team_label((data.get('equipe') or '').strip())
        nome_ele = (data.get('nome_ele') or '').strip()
        nome_ela = (data.get('nome_ela') or '').strip()

        if not (ano and equipe and nome_ele and nome_ela):
            return jsonify({"ok": False, "msg": "Parâmetros insuficientes."}), 400

        dados = checar_casal_implantacao(ano, equipe, nome_ele, nome_ela)

        return jsonify({
            "ok": True,
            "ja_no_ano": dados["ja_no_ano"],
            "trabalhou_antes": dados["trabalhou_antes"],
            "ja_coordenador": dados["ja_coordenador"],
            "telefones": dados["telefones"],
            "endereco": dados["endereco"]
        })

    @app.route('/api/implantacao/add-membro', methods=['POST'])
    def api_implantacao_add_membro():
        data = request.get_json(silent=True) or {}
        ano = data.get('ano')
        equipe = _team_label((data.get('equipe') or '').strip())
        nome_ele = (data.get('nome_ele') or '').strip()
        nome_ela = (data.get('nome_ela') or '').strip()
        telefones = (data.get('telefones') or '').strip()
        endereco = (data.get('endereco') or '').strip()
        coord_sim = str(data.get('coordenador') or 'Não').strip()
        confirmar_repeticao = bool(data.get('confirmar_repeticao'))

        if not (ano and equipe and nome_ele and nome_ela):
            return jsonify({"ok": False, "msg": "Parâmetros insuficientes."}), 400

        if coord_sim.lower() in ('true', '1', 'sim', 's'):
            coord_val = 'Sim'
        else:
            coord_val = 'Não'

        resultado = adicionar_membro_implantacao(
            ano=ano,
            equipe=equipe,
            nome_ele=nome_ele,
            nome_ela=nome_ela,
            telefones=telefones,
            endereco=endereco,
            coord_val=coord_val,
            confirmar_repeticao=confirmar_repeticao
        )

        if not resultado["ok"]:
            status_code = resultado.get("status_code", 400)
            payload = {"ok": False, "msg": resultado["msg"]}
            if resultado.get("needs_confirm"):
                payload["needs_confirm"] = True
            return jsonify(payload), status_code

        return jsonify({"ok": True, "id": resultado["id"]})

    @app.route('/api/implantacao/marcar-status', methods=['POST'])
    def api_implantacao_marcar_status():
        data = request.get_json(silent=True) or {}
        _id = data.get('id')
        novo_status = (data.get('novo_status') or '').strip().title()
        observacao = (data.get('observacao') or '').strip()

        if not (_id and novo_status in ('Recusou', 'Desistiu') and observacao):
            return jsonify({"ok": False, "msg": "Parâmetros inválidos. Observação é obrigatória."}), 400

        alterados = marcar_status_implantacao(_id, novo_status, observacao)
        if alterados == 0:
            return jsonify({"ok": False, "msg": "Registro não encontrado ou não alterável."}), 404

        return jsonify({"ok": True})

    @app.route('/api/implantacao/concluir-ano', methods=['POST'])
    def api_implantacao_concluir_ano():
        data = request.get_json(silent=True) or {}
        ano = data.get('ano')
        if not ano:
            return jsonify({"ok": False, "msg": "Ano obrigatório."}), 400

        alterados = concluir_implantacao_ano(ano)
        return jsonify({"ok": True, "alterados": alterados})
