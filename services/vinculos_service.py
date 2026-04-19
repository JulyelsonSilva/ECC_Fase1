from collections import defaultdict

from db import db_conn


def processar_match_fuzzy(_norm, _sim, batch_size=300, auto_threshold=0.92, suggest_threshold=0.80):
    conn = db_conn()
    cur = conn.cursor(dictionary=True)

    try:
        cur.execute("SELECT id, nome_usual_ele, nome_usual_ela FROM encontristas")
        base = cur.fetchall() or []

        bucket = defaultdict(list)
        for r in base:
            key = (_norm(r["nome_usual_ele"])[:1], _norm(r["nome_usual_ela"])[:1])
            bucket[key].append(r)

        cur.execute("""
            SELECT id, nome_ele, nome_ela
            FROM encontreiros
            WHERE casal_id IS NULL
            ORDER BY id ASC
            LIMIT %s
        """, (batch_size,))
        pend = cur.fetchall() or []

        if not pend:
            return {
                "message": "Nada a processar. Já está zerado.",
                "processed": 0
            }

        auto_count = 0
        pend_count = 0

        for row in pend:
            e_id = row["id"]
            n_ele = row.get("nome_ele") or ""
            n_ela = row.get("nome_ela") or ""

            key = (_norm(n_ele)[:1], _norm(n_ela)[:1])
            candidates = bucket.get(key, base)

            scored = []
            for c in candidates:
                s_ele = _sim(n_ele, c["nome_usual_ele"])
                s_ela = _sim(n_ela, c["nome_usual_ela"])
                score = (s_ele + s_ela) / 2.0
                scored.append((
                    score,
                    s_ele,
                    s_ela,
                    c["id"],
                    c["nome_usual_ele"],
                    c["nome_usual_ela"]
                ))

            if not scored:
                continue

            scored.sort(key=lambda x: x[0], reverse=True)
            best = scored[0]
            _, best_ele, best_ela, best_id, _, _ = best

            if best_ele >= auto_threshold and best_ela >= auto_threshold:
                try:
                    cur.execute(
                        "UPDATE encontreiros SET casal_id=%s WHERE id=%s AND casal_id IS NULL",
                        (best_id, e_id)
                    )
                    if cur.rowcount > 0:
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
                            e_id,
                            n_ele,
                            n_ela,
                            s_id,
                            s_nele,
                            s_nela,
                            round(s_ele, 4),
                            round(s_ela, 4),
                            round(score, 4)
                        ))
                    except Exception as err:
                        print(f"[pendencia] falha ao inserir sugestao e_id={e_id}, cand={s_id}: {err}")
                pend_count += 1

        conn.commit()

        cur.execute("SELECT COUNT(*) AS faltando FROM encontreiros WHERE casal_id IS NULL")
        faltando = cur.fetchone()["faltando"]

        return {
            "processados_neste_lote": len(pend),
            "preenchimentos_automaticos_neste_lote": auto_count,
            "pendencias_neste_lote": pend_count,
            "restantes_no_total": faltando
        }
    finally:
        try:
            cur.close()
            conn.close()
        except Exception:
            pass


def carregar_revisao_pendencias(min_score=0.85, page=1, per_page=50):
    conn = db_conn()
    cur = conn.cursor(dictionary=True)

    try:
        page = max(1, int(page))
        per_page = max(10, min(int(per_page), 100))
        offset = (page - 1) * per_page

        cur.execute("""
            SELECT COUNT(*) AS total_groups FROM (
              SELECT p.encontreiros_id
              FROM pendencias_encontreiros p
              JOIN encontreiros e ON e.id = p.encontreiros_id
              WHERE e.casal_id IS NULL
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
            WHERE e.casal_id IS NULL
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

        return {
            "page": page,
            "per_page": per_page,
            "min_score": min_score,
            "total_groups": total_groups,
            "total_pages": total_pages,
            "groups": groups
        }
    finally:
        try:
            cur.close()
            conn.close()
        except Exception:
            pass


def confirmar_revisao_vinculos(form_data):
    conn = db_conn()
    cur = conn.cursor()
    ok_count = 0
    skipped = 0

    try:
        for key, val in form_data.items():
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
                    "UPDATE encontreiros SET casal_id=%s WHERE id=%s AND casal_id IS NULL",
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
        return {"ok_count": ok_count, "skipped": skipped}
    finally:
        try:
            cur.close()
            conn.close()
        except Exception:
            pass


def autocomplete_nomes_encontristas(q):
    q = (q or "").strip()
    if len(q) < 2:
        return []

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
        return out
    finally:
        try:
            cur.close()
            conn.close()
        except Exception:
            pass

def listar_encontreiros_sem_casal_manual(filtros):
    conn = db_conn()
    cur = conn.cursor(dictionary=True)

    try:
        where = ["e.casal_id IS NULL"]
        params = []

        nome_ele = (filtros.get("e_nome_ele") or "").strip()
        nome_ela = (filtros.get("e_nome_ela") or "").strip()
        ano = (filtros.get("e_ano") or "").strip()
        endereco = (filtros.get("e_endereco") or "").strip()

        if nome_ele:
            where.append("e.nome_ele LIKE %s")
            params.append(f"%{nome_ele}%")

        if nome_ela:
            where.append("e.nome_ela LIKE %s")
            params.append(f"%{nome_ela}%")

        if ano:
            where.append("e.ano = %s")
            params.append(ano)

        if endereco:
            where.append("e.endereco LIKE %s")
            params.append(f"%{endereco}%")

        sql = f"""
            SELECT
                e.id,
                e.ano,
                e.equipe,
                e.nome_ele,
                e.nome_ela,
                e.telefones,
                e.endereco
            FROM encontreiros e
            WHERE {' AND '.join(where)}
            ORDER BY
                e.ano DESC,
                e.nome_ele ASC,
                e.nome_ela ASC
            LIMIT 1000
        """

        cur.execute(sql, params)
        return cur.fetchall() or []
    finally:
        try:
            cur.close()
            conn.close()
        except Exception:
            pass


def listar_encontristas_para_vinculo_manual(filtros):
    conn = db_conn()
    cur = conn.cursor(dictionary=True)

    try:
        where = ["1=1"]
        params = []

        nome_completo = (filtros.get("c_nome_completo") or "").strip()
        nome_usual = (filtros.get("c_nome_usual") or "").strip()
        ano = (filtros.get("c_ano") or "").strip()
        endereco = (filtros.get("c_endereco") or "").strip()

        if nome_completo:
            where.append("""
                (
                    c.nome_completo_ele LIKE %s
                    OR c.nome_completo_ela LIKE %s
                )
            """)
            params.extend([f"%{nome_completo}%", f"%{nome_completo}%"])

        if nome_usual:
            where.append("""
                (
                    c.nome_usual_ele LIKE %s
                    OR c.nome_usual_ela LIKE %s
                )
            """)
            params.extend([f"%{nome_usual}%", f"%{nome_usual}%"])

        if ano:
            where.append("c.ano = %s")
            params.append(ano)

        if endereco:
            where.append("c.endereco LIKE %s")
            params.append(f"%{endereco}%")

        sql = f"""
            SELECT
                c.id,
                c.ano,
                c.nome_completo_ele,
                c.nome_completo_ela,
                c.nome_usual_ele,
                c.nome_usual_ela,
                c.telefone_ele,
                c.telefone_ela,
                c.endereco
            FROM encontristas c
            WHERE {' AND '.join(where)}
            ORDER BY
                c.ano DESC,
                c.nome_usual_ele ASC,
                c.nome_usual_ela ASC
            LIMIT 300
        """

        cur.execute(sql, params)
        return cur.fetchall() or []
    finally:
        try:
            cur.close()
            conn.close()
        except Exception:
            pass


def vincular_encontreiros_em_lote(encontreiros_ids, casal_id):
    conn = db_conn()
    cur = conn.cursor()

    try:
        ids = []
        for item in encontreiros_ids:
            try:
                ids.append(int(item))
            except Exception:
                pass

        try:
            casal_id = int(casal_id)
        except Exception:
            return {"ok": False, "msg": "Casal selecionado inválido."}

        if not ids:
            return {"ok": False, "msg": "Selecione pelo menos um registro em Encontreiros."}

        placeholders = ",".join(["%s"] * len(ids))
        sql = f"""
            UPDATE encontreiros
            SET casal_id = %s
            WHERE id IN ({placeholders})
              AND casal_id IS NULL
        """

        cur.execute(sql, [casal_id] + ids)
        afetados = cur.rowcount

        if ids:
            placeholders_del = ",".join(["%s"] * len(ids))
            cur.execute(f"""
                DELETE FROM pendencias_encontreiros
                WHERE encontreiros_id IN ({placeholders_del})
            """, ids)

        conn.commit()

        return {
            "ok": True,
            "msg": f"{afetados} registro(s) vinculado(s) com sucesso."
        }
    finally:
        try:
            cur.close()
            conn.close()
        except Exception:
            pass
