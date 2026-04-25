from flask import render_template, request, jsonify, redirect, url_for, flash

from services.vinculos_service import (
    processar_match_fuzzy,
    carregar_revisao_pendencias,
    confirmar_revisao_vinculos,
    autocomplete_nomes_encontristas,
    listar_encontreiros_sem_casal_manual,
    listar_encontristas_para_vinculo_manual,
    vincular_encontreiros_em_lote,
)


def register_vinculos_routes(app, _admin_ok, _norm, _sim):
    @app.route("/autocomplete-nomes")
    def autocomplete_nomes():
        return jsonify(autocomplete_nomes_encontristas(request.args.get("q")))

    @app.route("/admin/vinculos/processar")
    def admin_vinculos_processar():
        if not _admin_ok():
            return "Unauthorized", 401

        try:
            batch_size = int(request.args.get("size", "300"))
        except ValueError:
            batch_size = 300

        try:
            auto_threshold = float(request.args.get("auto_threshold", "0.92"))
            suggest_threshold = float(request.args.get("suggest_threshold", "0.80"))
        except ValueError:
            auto_threshold, suggest_threshold = 0.92, 0.80

        resultado = processar_match_fuzzy(
            _norm=_norm,
            _sim=_sim,
            batch_size=batch_size,
            auto_threshold=auto_threshold,
            suggest_threshold=suggest_threshold,
        )
        return jsonify(resultado), 200

    @app.route("/admin/vinculos/revisao")
    def admin_vinculos_revisao():
        if not _admin_ok():
            return "Unauthorized", 401

        try:
            page = int(request.args.get("page", "1"))
            per_page = int(request.args.get("per_page", "50"))
            min_score = float(request.args.get("min_score", "0.85"))
        except ValueError:
            page, per_page, min_score = 1, 50, 0.85

        token = request.args.get("token")
        ok_count = request.args.get("ok", None)
        skipped_count = request.args.get("skipped", None)

        ok_count = int(ok_count) if ok_count is not None and ok_count.isdigit() else None
        skipped_count = int(skipped_count) if skipped_count is not None and skipped_count.isdigit() else None

        dados = carregar_revisao_pendencias(
            min_score=min_score,
            page=page,
            per_page=per_page
        )

        return render_template(
            "vinculos_revisao.html",
            token=token,
            ok_count=ok_count,
            skipped_count=skipped_count,
            **dados
        )

    @app.route("/admin/vinculos/revisao/confirmar", methods=["POST"])
    def admin_vinculos_revisao_confirmar():
        if not _admin_ok():
            return "Unauthorized", 401

        token = request.form.get("token", "")
        page = request.form.get("page", "1")
        per_page = request.form.get("per_page", "50")
        min_score = request.form.get("min_score", "0.85")

        resultado = confirmar_revisao_vinculos(request.form)

        return redirect(url_for(
            "admin_vinculos_revisao",
            token=token,
            page=page,
            per_page=per_page,
            min_score=min_score,
            ok=resultado["ok_count"],
            skipped=resultado["skipped"]
        ))

    @app.route("/admin/vinculos/manual")
    def admin_vinculos_manual():
        if not _admin_ok():
            return "Unauthorized", 401

        filtros = {
            "e_nome_ele": request.args.get("e_nome_ele", ""),
            "e_nome_ela": request.args.get("e_nome_ela", ""),
            "e_ano": request.args.get("e_ano", ""),
            "e_endereco": request.args.get("e_endereco", ""),
            "c_nome_completo_ele": request.args.get("c_nome_completo_ele", ""),
            "c_nome_completo_ela": request.args.get("c_nome_completo_ela", ""),
            "c_nome_usual_ele": request.args.get("c_nome_usual_ele", ""),
            "c_nome_usual_ela": request.args.get("c_nome_usual_ela", ""),
            "c_ano": request.args.get("c_ano", ""),
            "c_endereco": request.args.get("c_endereco", ""),
            "token": request.args.get("token", ""),
        }

        encontreiros = listar_encontreiros_sem_casal_manual(filtros)
        encontristas = listar_encontristas_para_vinculo_manual(filtros)

        return render_template(
            "vinculos_manual.html",
            filtros=filtros,
            encontreiros=encontreiros,
            encontristas=encontristas
        )

    @app.route("/admin/vinculos/manual/vincular", methods=["POST"])
    def admin_vinculos_manual_vincular():
        if not _admin_ok():
            return "Unauthorized", 401

        token = request.form.get("token", "")

        filtros = {
            "e_nome_ele": request.form.get("e_nome_ele", ""),
            "e_nome_ela": request.form.get("e_nome_ela", ""),
            "e_ano": request.form.get("e_ano", ""),
            "e_endereco": request.form.get("e_endereco", ""),
            "c_nome_completo_ele": request.form.get("c_nome_completo_ele", ""),
            "c_nome_completo_ela": request.form.get("c_nome_completo_ela", ""),
            "c_nome_usual_ele": request.form.get("c_nome_usual_ele", ""),
            "c_nome_usual_ela": request.form.get("c_nome_usual_ela", ""),
            "c_ano": request.form.get("c_ano", ""),
            "c_endereco": request.form.get("c_endereco", ""),
        }

        encontreiros_ids = request.form.getlist("encontreiros_ids")
        casal_id = request.form.get("casal_id")

        resultado = vincular_encontreiros_em_lote(encontreiros_ids, casal_id)

        if resultado["ok"]:
            flash(resultado["msg"], "success")
        else:
            flash(resultado["msg"], "danger")

        return redirect(url_for(
            "admin_vinculos_manual",
            token=token,
            e_nome_ele=filtros["e_nome_ele"],
            e_nome_ela=filtros["e_nome_ela"],
            e_ano=filtros["e_ano"],
            e_endereco=filtros["e_endereco"],
            c_nome_completo_ele=filtros["c_nome_completo_ele"],
            c_nome_completo_ela=filtros["c_nome_completo_ela"],
            c_nome_usual_ele=filtros["c_nome_usual_ele"],
            c_nome_usual_ela=filtros["c_nome_usual_ela"],
            c_ano=filtros["c_ano"],
            c_endereco=filtros["c_endereco"],
        ))
