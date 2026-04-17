from collections import defaultdict
from flask import render_template, request, jsonify, redirect, url_for

from db import db_conn
from services.schema_service import ensure_database_schema


def register_admin_routes(
    app,
    _admin_ok,
    _get_db,
    _norm,
    _sim,
    SequenceMatcher,
):
    @app.route("/autocomplete-nomes")
    def autocomplete_nomes():
        q = (request.args.get("q") or "").strip()
        if len(q) < 2:
            return jsonify([])

        conn = db_conn()
        cur = conn.cursor(dictionary=True)
        try:
            like = f"%{q}%"
            cur.execute("""
                SELECT DISTINCT nome_usual_ele, nome_usual_ela
                FROM encontristas
                WHERE nome_usual_ele LIKE %s
                   OR nome_usual_ela LIKE %s
                ORDER BY nome_usual_ele, nome_usual_ela
                LIMIT 20
            """, (like, like))
            rows = cur.fetchall() or []

            out = []
            for r in rows:
                ele = (r.get("nome_usual_ele") or "").strip()
                ela = (r.get("nome_usual_ela") or "").strip()
                if ele or ela:
                    out.append({
                        "nome_ele": ele,
                        "nome_ela": ela,
                        "label": f"{ele} e {ela}".strip()
                    })
            return jsonify(out)
        finally:
            try:
                cur.close()
                conn.close()
            except Exception:
                pass

    @app.route("/admin/match-fuzzy")
    def admin_match_fuzzy():
        if not _admin_ok():
            return "Unauthorized", 401

        try:
            batch_size = int(request.args.get("size", "300"))
        except ValueError:
            batch_size = 300

        auto_threshold = 0.92
        suggest_threshold = 0.80

        conn = _get_db()
        cur = conn.cursor(dictionary=True)

        try:
            cur.execute("SELECT id, nome_usual_ele, nome_usual_ela FROM encontristas")
            base = cur.fetchall() or []

            bucket = defaultdict(list)
            for r in base:
                key = (_norm(r['nome_usual_ele'])[:1], _norm(r['nome_usual_ela'])[:1])
                bucket[key].append(r)

            cur.execute("""
                SELECT id, nome_ele, nome_ela
                FROM encontreiros
                WHERE casal IS NULL
                ORDER BY id ASC
                LIMIT %s
            """, (batch_size,))
            pend = cur.fetchall() or []

            if not pend:
                return {
                    "message": "Nada a processar. Já está zerado.",
                    "processed": 0
                }, 200

            auto_count = 0
            pend_count = 0

            for row in pend:
                e_id = row['id']
                n_ele = row['nome_ele'] or ""
                n_ela = row['nome_ela'] or ""

                key = (_norm(n_ele)[:1], _norm(n_ela)[:1])
                candidates = bucket.get(key, base)

                scored = []
                for c in candidates:
                    s_ele = _sim(n_ele, c['nome_usual_ele'])
                    s_ela = _sim(n_ela, c['nome_usual_ela'])
                    score = (s_ele + s_ela) / 2.0
                    scored.append((score, s_ele, s_ela, c['id'], c['nome_usual_ele'], c['nome_usual_ela']))

                if not scored:
                    continue

                scored.sort(key=lambda x: x[0], reverse=True)
                best = scored[0]
                best_score, best_ele, best_ela, best_id, best_nele, best_nela = best

                if best_ele >= auto_threshold and best_ela >= auto_threshold:
                    try:
                        cur.execute("UPDATE encontreiros SET casal=%s WHERE id=%s", (best_id, e_id))
                        auto_count += 1
                    except Exception as err:
                        print(f"[fuzzy] erro ao atualizar encontreiros.id={e_id}: {err}")
                else:
                    suggestions = [s for s in scored if s[0] >= suggest_threshold][:3]
                    for s in suggestions:
                        score, s_ele, s_ela, s_id, s_nele, s_nela = s
                        try:
                            cur.execute("""
                                INSERT INTO pendencias_encontreiros
                                  (encontreiros_id, nome_ele, nome_ela, candidato_id,
                                   candidato_nome_usual_ele, candidato_nome_usual_ela,
                                   score_ele, score_ela, score_medio, status)
                                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,'PENDENTE')
                                ON DUPLICATE KEY UPDATE
                                  score_ele=VALUES(score_ele),
                                  score_ela=VALUES(score_ela),
                                  score_medio=VALUES(score_medio),
                                  nome_ele=VALUES(nome_ele),
                                  nome_ela=VALUES(nome_ela),
                                  candidato_nome_usual_ele=VALUES(candidato_nome_usual_ele),
                                  candidato_nome_usual_ela=VALUES(candidato_nome_usual_ela),
                                  status='PENDENTE'
                            """, (
                                e_id, n_ele, n_ela, s_id, s_nele, s_nela,
                                round(s_ele, 4), round(s_ela, 4), round(score, 4)
                            ))
                        except Exception as err:
                            print(f"[pendencia] falha ao inserir sugestao e_id={e_id}, cand={s_id}: {err}")
                    pend_count += 1

            conn.commit()

            cur.execute("SELECT COUNT(*) AS faltando FROM encontreiros WHERE casal IS NULL")
            faltando = cur.fetchone()["faltando"]

            return {
                "processados_neste_lote": len(pend),
                "preenchimentos_automaticos_neste_lote": auto_count,
                "pendencias_neste_lote": pend_count,
                "restantes_no_total": faltando
            }, 200
        finally:
            try:
                cur.close()
                conn.close()
            except Exception:
                pass

    @app.route("/admin/revisao")
    def admin_revisao():
        if not _admin_ok():
            return "Unauthorized", 401

        try:
            page = int(request.args.get("page", "1"))
            per_page = int(request.args.get("per_page", "50"))
            min_score = float(request.args.get("min_score", "0.85"))
        except ValueError:
            page, per_page, min_score = 1, 50, 0.85

        page = max(1, page)
        per_page = max(10, min(per_page, 100))
        offset = (page - 1) * per_page
        token = request.args.get("token")

        conn = _get_db()
        cur = conn.cursor(dictionary=True)

        try:
            cur.execute("""
                SELECT COUNT(*) AS total_groups FROM (
                  SELECT p.encontreiros_id
                  FROM pendencias_encontreiros p
                  JOIN encontreiros e ON e.id = p.encontreiros_id
                  WHERE e.casal IS NULL
                    AND COALESCE(p.status, 'PENDENTE') = 'PENDENTE'
                    AND p.score_medio >= %s
                  GROUP BY p.encontreiros_id
                ) t
            """, (min_score,))
            total_groups = cur.fetchone()["total_groups"]
            total_pages = max(1, (total_groups + per_page - 1) // per_page)

            cur.execute("""
                SELECT p.encontreiros_id, MAX(p.score_medio) AS best_score
                FROM pendencias_encontreiros p
                JOIN encontreiros e ON e.id = p.encontreiros_id
                WHERE e.casal IS NULL
                  AND COALESCE(p.status, 'PENDENTE') = 'PENDENTE'
                  AND p.score_medio >= %s
                GROUP BY p.encontreiros_id
                ORDER BY best_score DESC, p.encontreiros_id ASC
                LIMIT %s OFFSET %s
            """, (min_score, per_page, offset))
            rows = cur.fetchall() or []
            ids = [r["encontreiros_id"] for r in rows]
            id2best = {r["encontreiros_id"]: r["best_score"] for r in rows}

            groups = []
            if ids:
                placeholders = ",".join(["%s"] * len(ids))

                cur.execute(f"""
                    SELECT id, nome_ele, nome_ela, telefones, endereco
                    FROM encontreiros
                    WHERE id IN ({placeholders})
                """, ids)
                base = {r["id"]: r for r in (cur.fetchall() or [])}

                cur.execute(f"""
                    SELECT *
                    FROM pendencias_encontreiros
                    WHERE encontreiros_id IN ({placeholders})
                      AND COALESCE(status, 'PENDENTE') = 'PENDENTE'
                    ORDER BY encontreiros_id ASC, score_medio DESC, id ASC
                """, ids)
                cand = cur.fetchall() or []

                bucket = defaultdict(list)
                for c in cand:
                    bucket[c["encontreiros_id"]].append(c)

                for eid in ids:
                    groups.append({
                        "best_score": id2best.get(eid, 0),
                        "encontreiros": base.get(eid),
                        "candidatos": bucket.get(eid, [])
                    })

            ok_count = request.args.get("ok", None)
            skipped_count = request.args.get("skipped", None)
            ok_count = int(ok_count) if ok_count is not None and ok_count.isdigit() else None
            skipped_count = int(skipped_count) if skipped_count is not None and skipped_count.isdigit() else None

            return render_template(
                "admin_revisao.html",
                token=token,
                page=page,
                per_page=per_page,
                min_score=min_score,
                total_groups=total_groups,
                total_pages=total_pages,
                groups=groups,
                ok_count=ok_count,
                skipped_count=skipped_count
            )
        finally:
            try:
                cur.close()
                conn.close()
            except Exception:
                pass

    @app.route("/admin/revisao/confirmar", methods=["POST"])
    def admin_revisao_confirmar():
        if not _admin_ok():
            return "Unauthorized", 401

        token = request.form.get("token", "")
        page = request.form.get("page", "1")
        per_page = request.form.get("per_page", "50")
        min_score = request.form.get("min_score", "0.85")

        conn = _get_db()
        cur = conn.cursor()
        ok_count = 0
        skipped = 0

        try:
            for key, val in request.form.items():
                if not key.startswith("sel_"):
                    continue

                try:
                    eid = int(key.split("_", 1)[1])
                except Exception:
                    continue

                if not val:
                    skipped += 1
                    continue

                try:
                    cid = int(val)
                except Exception:
                    skipped += 1
                    continue

                try:
                    cur.execute(
                        "UPDATE encontreiros SET casal=%s WHERE id=%s AND casal IS NULL",
                        (cid, eid)
                    )
                    if cur.rowcount > 0:
                        cur.execute("DELETE FROM pendencias_encontreiros WHERE encontreiros_id=%s", (eid,))
                        ok_count += 1
                    else:
                        skipped += 1
                except Exception as err:
                    print(f"[revisao] falha ao confirmar (eid={eid}, cid={cid}): {err}")
                    skipped += 1

            conn.commit()
        finally:
            try:
                cur.close()
                conn.close()
            except Exception:
                pass

        return redirect(url_for(
            "admin_revisao",
            token=token,
            page=page,
            per_page=per_page,
            min_score=min_score,
            ok=ok_count,
            skipped=skipped
        ))

    @app.route("/__init_db__")
    def init_db_route():
        if not _admin_ok():
            return jsonify({"ok": False, "msg": "Acesso negado"}), 403

        try:
            resultado = ensure_database_schema()
            return jsonify(resultado)
        except Exception as e:
            return jsonify({"ok": False, "msg": str(e)}), 500
