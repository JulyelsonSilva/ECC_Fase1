from flask import render_template, request, jsonify, redirect, url_for

from services.vinculos_service import (
    processar_match_fuzzy,
    carregar_revisao_pendencias,
    confirmar_revisao_vinculos,
    autocomplete_nomes_encontristas,
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
            batch_size = int(request.args.get("size", "1000"))
        except ValueError:
            batch_size = 1000

        resultado = processar_match_fuzzy(
            _norm=_norm,
            _sim=_sim,
            batch_size=batch_size,
            auto_threshold=0.92,
            suggest_threshold=0.80,
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
