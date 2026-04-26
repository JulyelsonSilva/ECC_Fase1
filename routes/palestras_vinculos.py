from flask import render_template, request, redirect, url_for, flash

from services.palestras_vinculos_service import (
    listar_palestras_sem_casal_manual,
    listar_encontristas_para_vinculo_palestras,
    vincular_palestras_em_lote,
)


def register_palestras_vinculos_routes(app, _admin_ok):

    @app.route("/admin/palestras/vinculos/manual")
    def admin_palestras_vinculos_manual():
        if not _admin_ok():
            return "Unauthorized", 401

        filtros = {
            "p_nome_ele": request.args.get("p_nome_ele", ""),
            "p_nome_ela": request.args.get("p_nome_ela", ""),
            "p_ano": request.args.get("p_ano", ""),
            "p_palestra": request.args.get("p_palestra", ""),
            "c_nome_completo_ele": request.args.get("c_nome_completo_ele", ""),
            "c_nome_completo_ela": request.args.get("c_nome_completo_ela", ""),
            "c_nome_usual_ele": request.args.get("c_nome_usual_ele", ""),
            "c_nome_usual_ela": request.args.get("c_nome_usual_ela", ""),
            "c_ano": request.args.get("c_ano", ""),
            "c_endereco": request.args.get("c_endereco", ""),
            "token": request.args.get("token", ""),
        }

        palestras = listar_palestras_sem_casal_manual(filtros)
        encontristas = listar_encontristas_para_vinculo_palestras(filtros)

        return render_template(
            "palestras_vinculos_manual.html",
            filtros=filtros,
            palestras=palestras,
            encontristas=encontristas
        )

    @app.route("/admin/palestras/vinculos/manual/vincular", methods=["POST"])
    def admin_palestras_vinculos_manual_vincular():
        if not _admin_ok():
            return "Unauthorized", 401

        token = request.form.get("token", "")

        filtros = {
            "p_nome_ele": request.form.get("p_nome_ele", ""),
            "p_nome_ela": request.form.get("p_nome_ela", ""),
            "p_ano": request.form.get("p_ano", ""),
            "p_palestra": request.form.get("p_palestra", ""),
            "c_nome_completo_ele": request.form.get("c_nome_completo_ele", ""),
            "c_nome_completo_ela": request.form.get("c_nome_completo_ela", ""),
            "c_nome_usual_ele": request.form.get("c_nome_usual_ele", ""),
            "c_nome_usual_ela": request.form.get("c_nome_usual_ela", ""),
            "c_ano": request.form.get("c_ano", ""),
            "c_endereco": request.form.get("c_endereco", ""),
        }

        palestras_ids = request.form.getlist("palestras_ids")
        casal_id = request.form.get("casal_id")

        resultado = vincular_palestras_em_lote(palestras_ids, casal_id)

        if resultado["ok"]:
            flash(resultado["msg"], "success")
        else:
            flash(resultado["msg"], "danger")

        return redirect(url_for(
            "admin_palestras_vinculos_manual",
            token=token,
            p_nome_ele=filtros["p_nome_ele"],
            p_nome_ela=filtros["p_nome_ela"],
            p_ano=filtros["p_ano"],
            p_palestra=filtros["p_palestra"],
            c_nome_completo_ele=filtros["c_nome_completo_ele"],
            c_nome_completo_ela=filtros["c_nome_completo_ela"],
            c_nome_usual_ele=filtros["c_nome_usual_ele"],
            c_nome_usual_ela=filtros["c_nome_usual_ela"],
            c_ano=filtros["c_ano"],
            c_endereco=filtros["c_endereco"],
        ))