from flask import render_template, request, jsonify, redirect, url_for
import math
import time

from db import db_conn
from utils import _parse_id_list, paroquia_id_atual, exigir_paroquia, json_sem_paroquia
from services.geocoding import normalize_address, addr_hash, geocode_br_smart

from services.encontristas_service import (
    listar_encontristas_paroquia,
    buscar_encontrista_por_id_paroquia,
    atualizar_encontrista_paroquia,
    contar_encontristas_por_ano_paroquia,
    buscar_encontrista_por_nomes_e_ano_paroquia,
)
def register_encontristas_routes(app, _encontrista_name_by_id):


    # =========================
    # ENCONTRISTAS
    # =========================
    @app.route('/encontristas')
    def encontristas():
        bloqueio = exigir_paroquia()
        if bloqueio:
            return bloqueio

        paroquia_id = paroquia_id_atual()

        nome_ele = request.args.get('nome_usual_ele', '')
        nome_ela = request.args.get('nome_usual_ela', '')
        ano = request.args.get('ano', '')
        pagina = int(request.args.get('pagina', 1))
        por_pagina = 50

        resultado = listar_encontristas_paroquia(
            paroquia_id=paroquia_id,
            nome_ele=nome_ele,
            nome_ela=nome_ela,
            ano=ano,
            pagina=pagina,
            por_pagina=por_pagina
        )

        total = resultado["total"]
        total_paginas = max(1, math.ceil(total / por_pagina))
        dados = resultado["dados"]

        updated = request.args.get('updated')
        notfound = request.args.get('notfound')

        return render_template(
            'encontristas.html',
            dados=dados,
            pagina=pagina,
            total_paginas=total_paginas,
            updated=updated,
            notfound=notfound
        )

    @app.route('/encontristas/<int:encontrista_id>/editar', methods=['GET', 'POST'])
    def editar_encontrista(encontrista_id):
        bloqueio = exigir_paroquia()
        if bloqueio:
            return bloqueio

        paroquia_id = paroquia_id_atual()

        if request.method == 'POST':
            nome_completo_ele = request.form.get('nome_completo_ele', '').strip()
            nome_completo_ela = request.form.get('nome_completo_ela', '').strip()
            nome_usual_ele = request.form.get('nome_usual_ele', '').strip()
            nome_usual_ela = request.form.get('nome_usual_ela', '').strip()
            telefone_ele = request.form.get('telefone_ele', '').strip()
            telefone_ela = request.form.get('telefone_ela', '').strip()
            endereco = request.form.get('endereco', '').strip()
            num_ecc = request.form.get('num_ecc', '').strip()
            ano_raw = request.form.get('ano', '').strip()
            data_casamento = request.form.get('data_casamento', '').strip() or None
            data_1_etapa = request.form.get('data_1_etapa', '').strip() or None
            data_2_etapa = request.form.get('data_2_etapa', '').strip() or None
            data_3_etapa = request.form.get('data_3_etapa', '').strip() or None
            casal_visitacao = request.form.get('casal_visitacao', '').strip()
            ficha_num = request.form.get('ficha_num', '').strip()
            aceitou = request.form.get('aceitou', '').strip()
            observacao = request.form.get('observacao', '').strip()
            observacao_extra = request.form.get('observacao_extra', '').strip()

            try:
                ano = int(ano_raw) if ano_raw else None
            except ValueError:
                ano = None

            payload = {
                "nome_completo_ele": nome_completo_ele,
                "nome_completo_ela": nome_completo_ela,
                "nome_usual_ele": nome_usual_ele,
                "nome_usual_ela": nome_usual_ela,
                "telefone_ele": telefone_ele,
                "telefone_ela": telefone_ela,
                "endereco": endereco,
                "num_ecc": num_ecc,
                "ano": ano,
                "data_casamento": data_casamento,
                "data_1_etapa": data_1_etapa,
                "data_2_etapa": data_2_etapa,
                "data_3_etapa": data_3_etapa,
                "casal_visitacao": casal_visitacao,
                "ficha_num": ficha_num,
                "aceitou": aceitou,
                "observacao": observacao,
                "observacao_extra": observacao_extra,
            }

            atualizar_encontrista_paroquia(encontrista_id, paroquia_id, payload)
            return redirect(url_for('encontristas') + '?updated=1')

        registro = buscar_encontrista_por_id_paroquia(encontrista_id, paroquia_id)

        if not registro:
            return redirect(url_for('encontristas') + '?notfound=1')

        return render_template('editar_encontrista.html', r=registro)

    # Encontristas por ano
    @app.route("/api/encontristas/ano/<int:ano>")
    def api_encontristas_por_ano(ano):
        paroquia_id = paroquia_id_atual()
        if not paroquia_id:
            return json_sem_paroquia()

        livres = request.args.get("livres", "").strip() in ("1", "true", "True")

        conn = db_conn()
        cur = conn.cursor(dictionary=True)
        try:
            usados = set()
            if livres:
                cur.execute("""
                    SELECT integrantes_atual
                    FROM circulos
                    WHERE ano = %s
                      AND paroquia_id = %s
                """, (ano, paroquia_id))

                for r in cur.fetchall() or []:
                    usados.update(_parse_id_list(r.get("integrantes_atual")))

            cur.execute("""
                SELECT id, nome_usual_ele, nome_usual_ela, telefone_ele, telefone_ela, endereco
                FROM encontristas
                WHERE ano = %s
                  AND paroquia_id = %s
                ORDER BY nome_usual_ele, nome_usual_ela
            """, (ano, paroquia_id))

            lista = []
            for r in cur.fetchall() or []:
                if livres and r["id"] in usados:
                    continue

                lista.append({
                    "id": r["id"],
                    "nome_usual_ele": r["nome_usual_ele"],
                    "nome_usual_ela": r["nome_usual_ela"],
                    "telefone_ele": r.get("telefone_ele") or "",
                    "telefone_ela": r.get("telefone_ela") or "",
                    "endereco": r.get("endereco") or "",
                })

            return jsonify({"ok": True, "lista": lista})
        finally:
            cur.close()
            conn.close()

    @app.route("/api/encontristas_por_ano", methods=["GET"])
    def api_encontristas_por_ano_count():
        paroquia_id = paroquia_id_atual()
        if not paroquia_id:
            return jsonify({"ok": False, "error": "Paróquia não selecionada."}), 400

        ano_min = request.args.get("ano_min", type=int)
        ano_max = request.args.get("ano_max", type=int)

        try:
            out = contar_encontristas_por_ano_paroquia(
                paroquia_id=paroquia_id,
                ano_min=ano_min,
                ano_max=ano_max
            )
            return jsonify(out), 200
        except Exception as e:
            return jsonify({"ok": False, "error": str(e)}), 500

    @app.route("/api/encontristas/busca")
    def api_encontrista_busca():
        paroquia_id = paroquia_id_atual()
        if not paroquia_id:
            return json_sem_paroquia()

        ele = (request.args.get("ele") or "").strip()
        ela = (request.args.get("ela") or "").strip()
        ano = request.args.get("ano", type=int)

        if not (ele and ela and ano):
            return jsonify({"ok": False, "msg": "Parâmetros insuficientes."}), 400

        r = buscar_encontrista_por_nomes_e_ano_paroquia(paroquia_id, ele, ela, ano)

        if not r:
            return jsonify({"ok": False, "msg": "Casal não encontrado para este ano."}), 404

        tels = " / ".join([
            t for t in [
                (r.get("telefone_ele") or "").strip(),
                (r.get("telefone_ela") or "").strip()
            ] if t
        ])

        return jsonify({
            "ok": True,
            "id": r["id"],
            "nome_ele": r["nome_usual_ele"],
            "nome_ela": r["nome_usual_ela"],
            "telefones": tels,
            "endereco": r.get("endereco") or ""
        })

    # =========================
    # AUDITORIA / MAPA
    # =========================
    @app.route("/relatorios/auditoria-enderecos")
    def auditoria_enderecos():
        bloqueio = exigir_paroquia()
        if bloqueio:
            return bloqueio

        paroquia_id = paroquia_id_atual()

        resumo = []
        faltantes = []

        conn = db_conn()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute("""
              SELECT g.geocode_status AS status, COUNT(*) AS qtd
                FROM encontristas_geo g
                JOIN encontristas e ON e.id = g.encontrista_id
               WHERE e.paroquia_id = %s
               GROUP BY g.geocode_status
            """, (paroquia_id,))
            resumo = cur.fetchall() or []

            cur.execute("""
              SELECT e.id, e.nome_usual_ele, e.nome_usual_ela, e.endereco,
                     g.endereco_normalizado, g.geocode_status, g.formatted_address
                FROM encontristas e
                JOIN encontristas_geo g ON g.encontrista_id = e.id
               WHERE e.paroquia_id = %s
                 AND (
                       g.geocode_status IS NULL
                    OR g.geocode_status IN ('pending','error')
                    OR g.endereco_normalizado IS NULL
                    OR (g.geocode_status='not_found' AND (g.endereco_normalizado IS NULL OR g.endereco_normalizado=''))
                 )
               ORDER BY e.id DESC
               LIMIT 200
            """, (paroquia_id,))
            faltantes = cur.fetchall() or []
        finally:
            try:
                cur.close()
                conn.close()
            except Exception:
                pass

        return render_template(
            "auditoria_enderecos.html",
            resumo=resumo,
            faltantes=faltantes
        )

    @app.route("/admin/normalizar-geocodificar", methods=["POST"])
    def normalizar_geocodificar():
        paroquia_id = paroquia_id_atual()
        if not paroquia_id:
            return json_sem_paroquia()

        try:
            lote = max(1, int(request.args.get("lote", 50)))
        except ValueError:
            lote = 50

        conn_sel = db_conn()
        cur_sel = conn_sel.cursor(dictionary=True)
        try:
            cur_sel.execute(f"""
                SELECT e.id AS encontrista_id,
                       COALESCE(e.endereco, '') AS endereco_original,
                       g.endereco_normalizado,
                       g.geocode_status
                  FROM encontristas e
                  JOIN encontristas_geo g ON g.encontrista_id = e.id
                 WHERE e.paroquia_id = %s
                   AND (
                         g.geocode_status IS NULL
                      OR g.geocode_status IN ('pending','error')
                      OR g.endereco_normalizado IS NULL
                      OR (g.geocode_status='not_found' AND (g.endereco_normalizado IS NULL OR g.endereco_normalizado=''))
                   )
                 ORDER BY
                       CASE
                         WHEN g.geocode_status IS NULL THEN 0
                         WHEN g.geocode_status IN ('pending','error') THEN 1
                         WHEN g.endereco_normalizado IS NULL THEN 2
                         WHEN g.geocode_status='not_found' THEN 3
                         ELSE 9
                       END,
                       e.id ASC
                 LIMIT {lote}
            """, (paroquia_id,))
            rows = cur_sel.fetchall() or []
        finally:
            try:
                cur_sel.close()
                conn_sel.close()
            except Exception:
                pass

        if not rows:
            return jsonify({"ok": True, "processados": 0, "mensagem": "Nada a processar."})

        processados = ok = partial = not_found = erros = 0

        for r in rows:
            try:
                raw = (r.get("endereco_original") or "").strip()

                lat, lng, display, status = geocode_br_smart(raw)

                conn_up = db_conn()
                try:
                    try:
                        conn_up.autocommit = True
                    except Exception:
                        pass

                    try:
                        conn_up.ping(reconnect=True, attempts=2, delay=0.2)
                    except Exception:
                        pass

                    cur_up = conn_up.cursor()
                    endereco_normalizado = normalize_address(raw)

                    cur_up.execute("""
                      INSERT INTO encontristas_geo
                        (encontrista_id, endereco_original, endereco_normalizado, endereco_hash,
                         formatted_address, geo_lat, geo_lng, geocode_status, geocode_source, geocode_updated_at)
                      VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
                      ON DUPLICATE KEY UPDATE
                        endereco_original=VALUES(endereco_original),
                        endereco_normalizado=VALUES(endereco_normalizado),
                        endereco_hash=VALUES(endereco_hash),
                        formatted_address=VALUES(formatted_address),
                        geo_lat=VALUES(geo_lat),
                        geo_lng=VALUES(geo_lng),
                        geocode_status=VALUES(geocode_status),
                        geocode_source=VALUES(geocode_source),
                        geocode_updated_at=VALUES(geocode_updated_at)
                    """, (
                        r["encontrista_id"],
                        raw or None,
                        endereco_normalizado or None,
                        addr_hash(endereco_normalizado) if endereco_normalizado else None,
                        display,
                        lat,
                        lng,
                        status,
                        "smart"
                    ))
                    cur_up.close()
                finally:
                    try:
                        conn_up.close()
                    except Exception:
                        pass

                processados += 1

                if status == "ok":
                    ok += 1
                elif status == "partial":
                    partial += 1
                elif status == "not_found":
                    not_found += 1

                time.sleep(0.3)

            except Exception:
                erros += 1
                continue

        return jsonify({
            "ok": True,
            "processados": processados,
            "ok_count": ok,
            "partial": partial,
            "not_found": not_found,
            "erros": erros,
            "mensagem": "Lote concluído."
        })

    @app.route("/api/encontristas/geo")
    def api_encontristas_geo():
        paroquia_id = paroquia_id_atual()
        if not paroquia_id:
            return jsonify({"type": "FeatureCollection", "features": []})

        features = []

        conn = db_conn()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute("""
              SELECT e.id, e.nome_usual_ele, e.nome_usual_ela,
                     e.ano,
                     g.formatted_address, g.geo_lat, g.geo_lng, g.geocode_status
                FROM encontristas e
                JOIN encontristas_geo g ON g.encontrista_id = e.id
               WHERE e.paroquia_id = %s
                 AND g.geo_lat IS NOT NULL
                 AND g.geo_lng IS NOT NULL
                 AND g.geocode_status IN ('ok','partial')
            """, (paroquia_id,))

            for r in cur.fetchall() or []:
                features.append({
                    "type": "Feature",
                    "geometry": {
                        "type": "Point",
                        "coordinates": [float(r["geo_lng"]), float(r["geo_lat"])]
                    },
                    "properties": {
                        "id": r["id"],
                        "nome": f'{r.get("nome_usual_ele") or ""} e {r.get("nome_usual_ela") or ""}',
                        "ano": r.get("ano"),
                        "address": r.get("formatted_address") or "",
                        "status": r.get("geocode_status") or "ok"
                    }
                })
        finally:
            try:
                cur.close()
                conn.close()
            except Exception:
                pass

        return jsonify({"type": "FeatureCollection", "features": features})

    @app.route("/relatorios/mapa-encontristas")
    def relatorio_mapa_encontristas():
        bloqueio = exigir_paroquia()
        if bloqueio:
            return bloqueio

        return render_template("relatorio_mapa_encontristas.html")
