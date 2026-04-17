from flask import render_template, request, jsonify, redirect, url_for

from db import db_conn


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
            return "Acesso negado", 403

        db = _get_db()
        cur = db.cursor(dictionary=True)

        try:
            # pendentes: encontreiros sem vínculo com encontristas
            cur.execute("""
                SELECT id, ano, equipe, nome_ele, nome_ela, casal
                FROM encontreiros
                WHERE (casal IS NULL OR casal = 0)
                ORDER BY ano DESC, id DESC
            """)
            pendentes = cur.fetchall() or []

            # base de encontristas
            cur.execute("""
                SELECT id, nome_usual_ele, nome_usual_ela, ano
                FROM encontristas
            """)
            base = cur.fetchall() or []

            sugestoes = []
            stats = {
                "total_encontreiros": len(pendentes),
                "vinculados": 0,
                "faltando": 0,
                "faixas": {
                    "97+": 0,
                    "95-97": 0,
                    "93-95": 0,
                    "90-93": 0,
                    "<90": 0,
                }
            }

            base_norm = []
            for b in base:
                ele_b = _norm(b.get("nome_usual_ele"))
                ela_b = _norm(b.get("nome_usual_ela"))
                casal_b = f"{ele_b} {ela_b}".strip()
                base_norm.append((b, ele_b, ela_b, casal_b))

            for e in pendentes:
                nome_ele = _norm(e.get("nome_ele"))
                nome_ela = _norm(e.get("nome_ela"))
                casal_e = f"{nome_ele} {nome_ela}".strip()

                melhor = None
                melhor_score = 0

                for b, ele_b, ela_b, casal_b in base_norm:
                    s1 = SequenceMatcher(None, nome_ele, ele_b).ratio()
                    s2 = SequenceMatcher(None, nome_ela, ela_b).ratio()
                    s3 = SequenceMatcher(None, casal_e, casal_b).ratio()
                    score = max((s1 + s2) / 2, s3)

                    if score > melhor_score:
                        melhor_score = score
                        melhor = b

                pct = round(melhor_score * 100, 2)

                if pct >= 97:
                    stats["faixas"]["97+"] += 1
                elif pct >= 95:
                    stats["faixas"]["95-97"] += 1
                elif pct >= 93:
                    stats["faixas"]["93-95"] += 1
                elif pct >= 90:
                    stats["faixas"]["90-93"] += 1
                else:
                    stats["faixas"]["<90"] += 1

                if melhor:
                    sugestoes.append({
                        "encontreiro_id": e["id"],
                        "ano": e.get("ano"),
                        "equipe": e.get("equipe") or "",
                        "nome_ele": e.get("nome_ele") or "",
                        "nome_ela": e.get("nome_ela") or "",
                        "match_id": melhor["id"],
                        "match_nome_ele": melhor.get("nome_usual_ele") or "",
                        "match_nome_ela": melhor.get("nome_usual_ela") or "",
                        "match_ano": melhor.get("ano"),
                        "score": pct
                    })

            stats["faltando"] = len(sugestoes)

            sugestoes.sort(key=lambda x: x["score"], reverse=True)

            return render_template(
                "admin_match_fuzzy.html",
                sugestoes=sugestoes,
                stats=stats
            )
        finally:
            try:
                cur.close()
                db.close()
            except Exception:
                pass

    @app.route("/admin/revisao")
    def admin_revisao():
        if not _admin_ok():
            return "Acesso negado", 403

        db = _get_db()
        cur = db.cursor(dictionary=True)

        try:
            cur.execute("""
                SELECT 
                    p.id,
                    p.encontreiro_id,
                    p.nome_ele_encontreiro,
                    p.nome_ela_encontreiro,
                    p.encontrista_id_sugerido,
                    p.nome_ele_encontrista,
                    p.nome_ela_encontrista,
                    p.score,
                    e.ano,
                    e.equipe
                FROM pendencias_encontreiros p
                LEFT JOIN encontreiros e ON e.id = p.encontreiro_id
                WHERE COALESCE(p.status, 'PENDENTE') = 'PENDENTE'
                ORDER BY p.score DESC, p.id ASC
            """)
            pendencias = cur.fetchall() or []

            return render_template("admin_revisao.html", pendencias=pendencias)
        finally:
            try:
                cur.close()
                db.close()
            except Exception:
                pass

    @app.route("/admin/revisao/confirmar", methods=["POST"])
    def admin_revisao_confirmar():
        if not _admin_ok():
            return "Acesso negado", 403

        pendencia_id = request.form.get("pendencia_id", type=int)
        encontreiro_id = request.form.get("encontreiro_id", type=int)
        encontrista_id = request.form.get("encontrista_id", type=int)
        acao = (request.form.get("acao") or "").strip().lower()

        if not pendencia_id:
            return redirect(url_for("admin_revisao", token=request.args.get("token") or request.form.get("token")))

        db = _get_db()
        cur = db.cursor()

        try:
            if acao == "confirmar" and encontreiro_id and encontrista_id:
                cur.execute("""
                    UPDATE encontreiros
                    SET casal = %s
                    WHERE id = %s
                """, (encontrista_id, encontreiro_id))

                cur.execute("""
                    UPDATE pendencias_encontreiros
                    SET status = 'CONFIRMADO'
                    WHERE id = %s
                """, (pendencia_id,))

            elif acao == "ignorar":
                cur.execute("""
                    UPDATE pendencias_encontreiros
                    SET status = 'IGNORADO'
                    WHERE id = %s
                """, (pendencia_id,))
            else:
                db.rollback()
                return redirect(url_for("admin_revisao", token=request.args.get("token") or request.form.get("token")))

            db.commit()
            return redirect(url_for("admin_revisao", token=request.args.get("token") or request.form.get("token")))
        finally:
            try:
                cur.close()
                db.close()
            except Exception:
                pass

    @app.route("/__init_db__")
    def init_db_route():
        if not _admin_ok():
            return jsonify({"ok": False, "msg": "Acesso negado"}), 403

        db = _get_db()
        cur = db.cursor()
        try:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS pendencias_encontreiros (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    encontreiro_id INT NOT NULL,
                    nome_ele_encontreiro VARCHAR(255),
                    nome_ela_encontreiro VARCHAR(255),
                    encontrista_id_sugerido INT,
                    nome_ele_encontrista VARCHAR(255),
                    nome_ela_encontrista VARCHAR(255),
                    score DECIMAL(5,2),
                    status VARCHAR(30) DEFAULT 'PENDENTE',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            db.commit()
            return jsonify({"ok": True})
        finally:
            try:
                cur.close()
                db.close()
            except Exception:
                pass
