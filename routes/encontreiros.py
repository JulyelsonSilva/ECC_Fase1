
from flask import render_template, request, redirect, url_for
from utils import paroquia_id_atual, exigir_paroquia

from services.encontreiros_service import (
    listar_encontreiros,
    montar_visao_equipes,
    buscar_visao_casal,
    buscar_relatorio_casais,
)


def register_encontreiros_routes(
    app,
    PALESTRAS_TITULOS,
    PALESTRAS_SOLO,
    DB_CONFIG,
    safe_fetch_one
):
    @app.route('/encontreiros')
    def encontreiros():
        bloqueio = exigir_paroquia()
        if bloqueio:
            return bloqueio

        paroquia_id = paroquia_id_atual()

        nome_ele = (request.args.get('nome_ele', '') or '').strip()
        nome_ela = (request.args.get('nome_ela', '') or '').strip()
        ano_filtro = (request.args.get('ano', '') or '').strip()

        dados = listar_encontreiros(
            paroquia_id=paroquia_id,
            nome_ele=nome_ele,
            nome_ela=nome_ela,
            ano_filtro=ano_filtro
        )

        return render_template(
            'encontreiros.html',
            por_ano=dados["por_ano"],
            colunas_visiveis=dados["colunas_visiveis"]
        )

    @app.route('/visao-equipes')
    def visao_equipes():
        bloqueio = exigir_paroquia()
        if bloqueio:
            return bloqueio

        paroquia_id = paroquia_id_atual()

        equipe = request.args.get('equipe', '')
        target = request.args.get('target', '')
        ano_montagem = request.args.get('ano_montagem', '')

        dados = montar_visao_equipes(
            equipe=equipe,
            paroquia_id=paroquia_id
        )

        return render_template(
            'visao_equipes.html',
            equipe_selecionada=equipe,
            tabela=dados["tabela"],
            colunas=dados["colunas"],
            target=target,
            ano_montagem=ano_montagem
        )

    @app.route('/visao-equipes/select')
    def visao_equipes_select():
        bloqueio = exigir_paroquia()
        if bloqueio:
            return bloqueio

        ano_montagem = request.args.get('ano_montagem', type=int) or request.args.get('ano', type=int)
        target = (request.args.get('target') or request.args.get('ret_target') or '').strip()

        ele = (
            request.args.get('ele')
            or request.args.get('selecionar_ele')
            or request.args.get('nome_ele')
            or ''
        ).strip()

        ela = (
            request.args.get('ela')
            or request.args.get('selecionar_ela')
            or request.args.get('nome_ela')
            or ''
        ).strip()

        if not (ano_montagem and target and ele and ela):
            return redirect(url_for(
                'visao_equipes',
                target=target,
                ano_montagem=ano_montagem
            ))

        return redirect(url_for(
            'nova_montagem',
            ano=ano_montagem,
            target=target,
            selecionar_ele=ele,
            selecionar_ela=ela
        ))

    @app.route('/visao-casal')
    def visao_casal():
        bloqueio = exigir_paroquia()
        if bloqueio:
            return bloqueio

        paroquia_id = paroquia_id_atual()

        nome_ele = (request.args.get("nome_ele") or "").strip()
        nome_ela = (request.args.get("nome_ela") or "").strip()
        casal_id = request.args.get("casal_id", type=int)

        dados = buscar_visao_casal(
            paroquia_id=paroquia_id,
            nome_ele=nome_ele,
            nome_ela=nome_ela,
            casal_id=casal_id,
            PALESTRAS_TITULOS=PALESTRAS_TITULOS,
            PALESTRAS_SOLO=PALESTRAS_SOLO
        )

        return render_template(
            "visao_casal.html",
            nome_ele=nome_ele,
            nome_ela=nome_ela,
            casal_id=casal_id,
            candidatos=dados["candidatos"],
            dados_encontrista=dados["dados_encontrista"],
            dados_encontreiros=dados["dados_encontreiros"],
            dados_palestras=dados["dados_palestras"],
            anos=dados["anos"],
            por_ano_trabalhos=dados["por_ano_trabalhos"],
            por_ano_palestras=dados["por_ano_palestras"],
            erro=dados["erro"]
        )

    @app.route('/relatorio-casais', methods=['GET', 'POST'])
    def relatorio_casais():
        bloqueio = exigir_paroquia()
        if bloqueio:
            return bloqueio

        paroquia_id = paroquia_id_atual()

        titulo = (
            request.form.get("titulo") or "Relatório de Casais"
        ) if request.method == 'POST' else "Relatório de Casais"

        entrada = (
            request.form.get("lista_nomes", "") or ""
        ) if request.method == 'POST' else ""

        resultados = []

        if request.method == 'POST' and entrada.strip():
            dados = buscar_relatorio_casais(
                paroquia_id=paroquia_id,
                entrada=entrada,
                titulo=titulo
            )
            resultados = dados["resultados"]
            titulo = dados["titulo"]
            entrada = dados["entrada"]

        return render_template(
            "relatorio_casais.html",
            resultados=resultados,
            titulo=titulo,
            entrada=entrada
        )
