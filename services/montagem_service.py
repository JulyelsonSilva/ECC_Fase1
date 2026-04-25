from db import db_conn


def _telefones(row):
    tel_ele = (row.get("telefone_ele") or "").strip()
    tel_ela = (row.get("telefone_ela") or "").strip()
    return " / ".join([t for t in [tel_ele, tel_ela] if t])


def _buscar_encontrista_por_nomes(cur, nome_ele, nome_ela, paroquia_id):
    nome_ele = (nome_ele or "").strip()
    nome_ela = (nome_ela or "").strip()

    cur.execute("""
        SELECT id, ano, nome_usual_ele, nome_usual_ela, telefone_ele, telefone_ela, endereco
          FROM encontristas
         WHERE paroquia_id = %s
           AND UPPER(TRIM(nome_usual_ele)) = UPPER(TRIM(%s))
           AND UPPER(TRIM(nome_usual_ela)) = UPPER(TRIM(%s))
         ORDER BY ano DESC, id DESC
         LIMIT 1
    """, (paroquia_id, nome_ele, nome_ela))
    return cur.fetchone()


def _buscar_casal_id_por_nomes(cur, nome_ele, nome_ela, paroquia_id):
    r = _buscar_encontrista_por_nomes(cur, nome_ele, nome_ela, paroquia_id)
    return r["id"] if r else None


def listar_montagem_por_ano(paroquia_id):
    conn = db_conn()
    cursor = conn.cursor(dictionary=True)
    try:
        cursor.execute("""
            SELECT 
                ano,
                SUM(CASE WHEN TRIM(LOWER(status)) = 'concluido' THEN 1 ELSE 0 END) AS qtd_concluido,
                COUNT(*) AS total
            FROM encontreiros
            WHERE paroquia_id = %s
            GROUP BY ano
            ORDER BY ano DESC
        """, (paroquia_id,))
        rows = cursor.fetchall() or []

        anos_concluidos, anos_aberto = [], []
        for r in rows:
            item = {
                "ano": r["ano"],
                "qtd_concluido": int(r["qtd_concluido"] or 0),
                "total": int(r["total"] or 0),
            }
            if item["total"] > 0 and item["qtd_concluido"] == item["total"]:
                anos_concluidos.append(item)
            else:
                anos_aberto.append(item)

        return {
            "anos_aberto": anos_aberto,
            "anos_concluidos": anos_concluidos,
        }
    finally:
        try:
            cursor.close()
            conn.close()
        except Exception:
            pass


def carregar_dados_iniciais_montagem(ano_preselecionado, TEAM_MAP, paroquia_id):
    initial_data = {
        "dirigentes": {},
        "cg": None,
        "coord_teams": {}
    }

    if not ano_preselecionado:
        return initial_data

    equipes_dir = [
        "Equipe Dirigente - MONTAGEM",
        "Equipe Dirigente -FICHAS",
        "Equipe Dirigente - FINANÇAS",
        "Equipe Dirigente - PALESTRA",
        "Equipe Dirigente - PÓS ENCONTRO",
    ]

    conn = db_conn()
    cur = conn.cursor(dictionary=True)
    try:
        for equipe in equipes_dir:
            cur.execute("""
                SELECT
                    i.nome_usual_ele AS nome_ele,
                    i.nome_usual_ela AS nome_ela,
                    CONCAT_WS(' / ',
                        NULLIF(TRIM(i.telefone_ele), ''),
                        NULLIF(TRIM(i.telefone_ela), '')
                    ) AS telefones,
                    i.endereco AS endereco
                  FROM encontreiros e
                  JOIN encontristas i ON i.id = e.casal_id
                 WHERE e.ano = %s
                   AND e.paroquia_id = %s
                   AND i.paroquia_id = %s
                   AND e.equipe = %s
                   AND (e.status IS NULL OR UPPER(e.status) NOT IN ('RECUSOU','DESISTIU'))
                 ORDER BY e.id ASC
                 LIMIT 1
            """, (ano_preselecionado, paroquia_id, paroquia_id, equipe))
            r = cur.fetchone()
            if r:
                initial_data["dirigentes"][equipe] = {
                    "nome_ele": r.get("nome_ele") or "",
                    "nome_ela": r.get("nome_ela") or "",
                    "telefones": r.get("telefones") or "",
                    "endereco": r.get("endereco") or ""
                }

        cur.execute("""
            SELECT
                i.nome_usual_ele AS nome_ele,
                i.nome_usual_ela AS nome_ela,
                CONCAT_WS(' / ',
                    NULLIF(TRIM(i.telefone_ele), ''),
                    NULLIF(TRIM(i.telefone_ela), '')
                ) AS telefones,
                i.endereco AS endereco
              FROM encontreiros e
              JOIN encontristas i ON i.id = e.casal_id
             WHERE e.ano = %s
               AND e.paroquia_id = %s
               AND i.paroquia_id = %s
               AND UPPER(e.equipe) = 'CASAL COORDENADOR GERAL'
               AND UPPER(e.status) = 'ABERTO'
             ORDER BY e.id DESC
             LIMIT 1
        """, (ano_preselecionado, paroquia_id, paroquia_id))
        r_cg = cur.fetchone()
        if r_cg:
            initial_data["cg"] = {
                "nome_ele": r_cg.get("nome_ele") or "",
                "nome_ela": r_cg.get("nome_ela") or "",
                "telefones": r_cg.get("telefones") or "",
                "endereco": r_cg.get("endereco") or "",
            }

        for key, info in TEAM_MAP.items():
            rotulo = info["rotulo"]
            filtro = info["filtro"]
            cur.execute("""
                SELECT
                    i.nome_usual_ele AS nome_ele,
                    i.nome_usual_ela AS nome_ela,
                    CONCAT_WS(' / ',
                        NULLIF(TRIM(i.telefone_ele), ''),
                        NULLIF(TRIM(i.telefone_ela), '')
                    ) AS telefones,
                    i.endereco AS endereco
                  FROM encontreiros e
                  JOIN encontristas i ON i.id = e.casal_id
                 WHERE e.ano = %s
                   AND e.paroquia_id = %s
                   AND i.paroquia_id = %s
                   AND e.equipe IN (%s, %s)
                   AND UPPER(e.coordenador) = 'SIM'
                   AND UPPER(e.status) = 'ABERTO'
                 ORDER BY e.id DESC
                 LIMIT 1
            """, (ano_preselecionado, paroquia_id, paroquia_id, rotulo, filtro))
            r_team = cur.fetchone()
            if r_team:
                initial_data["coord_teams"][key] = {
                    "nome_ele": r_team.get("nome_ele") or "",
                    "nome_ela": r_team.get("nome_ela") or "",
                    "telefones": r_team.get("telefones") or "",
                    "endereco": r_team.get("endereco") or "",
                }

        return initial_data
    finally:
        try:
            cur.close()
            conn.close()
        except Exception:
            pass


def buscar_casal_para_montagem(nome_ele, nome_ela, paroquia_id):
    conn = db_conn()
    cur = conn.cursor(dictionary=True)
    try:
        encontrista = _buscar_encontrista_por_nomes(cur, nome_ele, nome_ela, paroquia_id)
        if not encontrista:
            return {"ok": False, "status_code": 404, "msg": "Casal não participou do ECC."}

        cur.execute("""
            SELECT ano
              FROM encontreiros
             WHERE casal_id = %s
               AND paroquia_id = %s
             ORDER BY ano DESC, id DESC
             LIMIT 1
        """, (encontrista["id"], paroquia_id))
        r = cur.fetchone()

        return {
            "ok": True,
            "origem": "encontreiros" if r else "encontristas",
            "casal_id": encontrista["id"],
            "ano": r["ano"] if r else encontrista["ano"],
            "telefones": _telefones(encontrista),
            "endereco": encontrista.get("endereco") or ""
        }
    finally:
        try:
            cur.close()
            conn.close()
        except Exception:
            pass


def adicionar_dirigente_montagem(ano, equipe, nome_ele, nome_ela, telefones=None, endereco=None, paroquia_id=None):
    conn = db_conn()
    cur = conn.cursor(dictionary=True)
    try:
        casal_id = _buscar_casal_id_por_nomes(cur, nome_ele, nome_ela, paroquia_id)
        if not casal_id:
            return {"ok": False, "status_code": 404, "msg": "Casal não participou do ECC."}

        cur.execute("""
            INSERT INTO encontreiros
                (paroquia_id, ano, equipe, casal_id, coordenador, status)
            VALUES
                (%s, %s, %s, %s, 'Sim', 'Aberto')
        """, (paroquia_id, int(ano), equipe, casal_id))
        conn.commit()
        return {"ok": True, "id": cur.lastrowid}
    finally:
        try:
            cur.close()
            conn.close()
        except Exception:
            pass


def buscar_cg_montagem(nome_ele, nome_ela, paroquia_id):
    conn = db_conn()
    cur = conn.cursor(dictionary=True)
    try:
        encontrista = _buscar_encontrista_por_nomes(cur, nome_ele, nome_ela, paroquia_id)
        if not encontrista:
            return {"ok": False, "status_code": 404, "msg": "Casal não participou do ECC."}

        casal_id = encontrista["id"]

        cur.execute("""
            SELECT ano
              FROM encontreiros
             WHERE casal_id = %s
               AND paroquia_id = %s
             ORDER BY ano DESC, id DESC
             LIMIT 1
        """, (casal_id, paroquia_id))
        r = cur.fetchone()
        if not r:
            return {"ok": False, "status_code": 404, "msg": "Casal nunca trabalhou no ECC."}

        cur.execute("""
            SELECT 1
              FROM encontreiros
             WHERE casal_id = %s
               AND paroquia_id = %s
               AND UPPER(equipe) = 'CASAL COORDENADOR GERAL'
             LIMIT 1
        """, (casal_id, paroquia_id))
        if cur.fetchone():
            return {"ok": False, "status_code": 409, "msg": "Casal já foi Coordenador Geral."}

        return {
            "ok": True,
            "casal_id": casal_id,
            "ano_ref": r["ano"],
            "telefones": _telefones(encontrista),
            "endereco": encontrista.get("endereco") or ""
        }
    finally:
        try:
            cur.close()
            conn.close()
        except Exception:
            pass


def adicionar_cg_montagem(ano, nome_ele, nome_ela, telefones=None, endereco=None, paroquia_id=None):
    conn = db_conn()
    cur = conn.cursor(dictionary=True)
    try:
        casal_id = _buscar_casal_id_por_nomes(cur, nome_ele, nome_ela, paroquia_id)
        if not casal_id:
            return {"ok": False, "status_code": 404, "msg": "Casal não participou do ECC."}

        cur.execute("""
            SELECT 1
              FROM encontreiros
             WHERE casal_id = %s
               AND paroquia_id = %s
             LIMIT 1
        """, (casal_id, paroquia_id))
        if not cur.fetchone():
            return {"ok": False, "status_code": 404, "msg": "Casal nunca trabalhou no ECC."}

        cur.execute("""
            SELECT 1
              FROM encontreiros
             WHERE casal_id = %s
               AND paroquia_id = %s
               AND UPPER(equipe) = 'CASAL COORDENADOR GERAL'
             LIMIT 1
        """, (casal_id, paroquia_id))
        if cur.fetchone():
            return {"ok": False, "status_code": 409, "msg": "Casal já foi Coordenador Geral."}

        cur.execute("""
            INSERT INTO encontreiros
                (paroquia_id, ano, equipe, casal_id, coordenador, status)
            VALUES
                (%s, %s, 'Casal Coordenador Geral', %s, 'Sim', 'Aberto')
        """, (paroquia_id, int(ano), casal_id))
        conn.commit()

        return {"ok": True, "id": cur.lastrowid}
    finally:
        try:
            cur.close()
            conn.close()
        except Exception:
            pass


def contar_equipes_montagem(ano, TEAM_MAP, paroquia_id):
    conn = db_conn()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute("""
            SELECT equipe, coordenador
            FROM encontreiros
            WHERE ano = %s
              AND paroquia_id = %s
              AND (status IS NULL OR UPPER(TRIM(status)) NOT IN ('RECUSOU','DESISTIU'))
        """, (ano, paroquia_id))
        rows = cur.fetchall() or []

        rotulo_to_filtro = {info["rotulo"]: info["filtro"] for info in TEAM_MAP.values()}
        counts = {info["filtro"]: 0 for info in TEAM_MAP.values()}

        for r in rows:
            equipe_txt = (r.get("equipe") or "").strip()
            is_coord = (r.get("coordenador") or "").strip().upper() == "SIM"

            if is_coord:
                continue

            eq_upper = equipe_txt.upper()

            if "SALA" in eq_upper:
                counts["Sala"] = counts.get("Sala", 0) + 1
                continue

            filtro = rotulo_to_filtro.get(equipe_txt)
            if filtro:
                counts[filtro] = counts.get(filtro, 0) + 1

        return counts
    finally:
        try:
            cur.close()
            conn.close()
        except Exception:
            pass


def buscar_sugestoes_prev_ano_montagem(ano, paroquia_id):
    sugestoes_prev_ano = []
    if not ano:
        return sugestoes_prev_ano

    conn = db_conn()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute("""
            SELECT
                e.id,
                e.nome_usual_ele,
                e.nome_usual_ela,
                e.telefone_ele,
                e.telefone_ela,
                e.endereco
              FROM encontristas e
             WHERE e.paroquia_id = %s
               AND e.ano = %s
               AND NOT EXISTS (
                    SELECT 1
                      FROM encontreiros w
                     WHERE w.ano = %s
                       AND w.paroquia_id = %s
                       AND w.casal_id = e.id
                       AND (w.status IS NULL OR UPPER(TRIM(w.status)) NOT IN ('RECUSOU','DESISTIU'))
               )
             ORDER BY e.nome_usual_ele, e.nome_usual_ela
        """, (paroquia_id, ano - 1, ano, paroquia_id))

        for r in cur.fetchall() or []:
            sugestoes_prev_ano.append({
                "casal_id": r.get("id"),
                "nome_ele": r.get("nome_usual_ele") or "",
                "nome_ela": r.get("nome_usual_ela") or "",
                "telefones": _telefones(r),
                "endereco": r.get("endereco") or ""
            })

        return sugestoes_prev_ano
    finally:
        try:
            cur.close()
            conn.close()
        except Exception:
            pass


def carregar_equipe_montagem(ano, equipe_filtro, TEAM_MAP, TEAM_LIMITS, paroquia_id):
    equipe_final = None
    for _key, info in TEAM_MAP.items():
        if info["filtro"].lower() == equipe_filtro.lower():
            equipe_final = info["rotulo"]
            break

    if not equipe_final:
        equipe_final = equipe_filtro or "Equipe"

    if equipe_filtro.lower() == "sala":
        SALA_DB = {
            "Canto": "Equipe de Sala - Canto",
            "Som e Projeção": "Equipe de Sala - Som e Projeção",
            "Boa Vontade": "Equipe de Sala - Boa Vontade",
            "Recepção Palestrantes": "Equipe de Sala - Recepção Palestrantes",
        }

        sala_order = [
            ("Canto 1", "Canto"),
            ("Canto 2", "Canto"),
            ("Som e Projeção 1", "Som e Projeção"),
            ("Som e Projeção 2", "Som e Projeção"),
            ("Boa Vontade", "Boa Vontade"),
            ("Recepção Palestrantes", "Recepção Palestrantes"),
        ]

        conn = db_conn()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute("""
                SELECT
                    e.id,
                    e.ano,
                    e.equipe,
                    e.casal_id,
                    i.nome_usual_ele AS nome_ele,
                    i.nome_usual_ela AS nome_ela,
                    CONCAT_WS(' / ',
                        NULLIF(TRIM(i.telefone_ele), ''),
                        NULLIF(TRIM(i.telefone_ela), '')
                    ) AS telefones,
                    i.endereco AS endereco,
                    e.status
                  FROM encontreiros e
                  JOIN encontristas i ON i.id = e.casal_id
                 WHERE e.ano = %s
                   AND e.paroquia_id = %s
                   AND i.paroquia_id = %s
                   AND e.equipe IN (%s, %s, %s, %s)
                   AND (e.coordenador IS NULL OR UPPER(TRIM(e.coordenador)) <> 'SIM')
                   AND (e.status IS NULL OR UPPER(TRIM(e.status)) NOT IN ('RECUSOU','DESISTIU'))
                 ORDER BY e.id ASC
            """, (
                ano,
                paroquia_id,
                paroquia_id,
                SALA_DB["Canto"],
                SALA_DB["Som e Projeção"],
                SALA_DB["Boa Vontade"],
                SALA_DB["Recepção Palestrantes"],
            ))
            rows = cur.fetchall() or []

            buckets = {k: [] for k in SALA_DB.keys()}
            for r in rows:
                eq = r.get("equipe") or ""
                for k, dbname in SALA_DB.items():
                    if eq == dbname:
                        buckets[k].append(r)
                        break

            cur.execute("""
                SELECT
                    i.nome_usual_ele AS nome_ele,
                    i.nome_usual_ela AS nome_ela,
                    CONCAT_WS(' / ',
                        NULLIF(TRIM(i.telefone_ele), ''),
                        NULLIF(TRIM(i.telefone_ela), '')
                    ) AS telefones,
                    i.endereco AS endereco
                  FROM encontreiros e
                  JOIN encontristas i ON i.id = e.casal_id
                 WHERE e.ano = %s
                   AND e.paroquia_id = %s
                   AND i.paroquia_id = %s
                   AND UPPER(e.equipe) = 'EQUIPE DIRIGENTE - PALESTRA'
                   AND (e.status IS NULL OR UPPER(TRIM(e.status)) NOT IN ('RECUSOU','DESISTIU'))
                 ORDER BY e.id DESC
                 LIMIT 1
            """, (ano, paroquia_id, paroquia_id))
            pref_recepcao = cur.fetchone() or {}
        finally:
            try:
                cur.close()
                conn.close()
            except Exception:
                pass

        sala_slots = []
        use_index = {
            "Canto": 0,
            "Som e Projeção": 0,
            "Boa Vontade": 0,
            "Recepção Palestrantes": 0
        }

        for label, kind in sala_order:
            lst = buckets.get(kind, [])
            idx = use_index[kind]
            existing = lst[idx] if idx < len(lst) else None
            use_index[kind] = idx + 1
            sala_slots.append({
                "label": label,
                "kind": kind,
                "equipe_db": SALA_DB[kind],
                "existing": existing or None,
            })

        limites = {"min": 6, "max": 6}
        sugestoes_prev_ano = buscar_sugestoes_prev_ano_montagem(ano, paroquia_id)

        return {
            "modo": "sala",
            "ano": ano,
            "limites": limites,
            "sala_slots": sala_slots,
            "sugestoes_prev_ano": sugestoes_prev_ano,
            "pref_recepcao": pref_recepcao,
        }

    limites_cfg = TEAM_LIMITS.get(equipe_filtro, TEAM_LIMITS.get(equipe_final, {}))
    limites = {
        "min": int(limites_cfg.get("min", 0)),
        "max": int(limites_cfg.get("max", 8)),
    }

    conn = db_conn()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute("""
            SELECT
                e.id,
                e.casal_id,
                i.nome_usual_ele AS nome_ele,
                i.nome_usual_ela AS nome_ela,
                CONCAT_WS(' / ',
                    NULLIF(TRIM(i.telefone_ele), ''),
                    NULLIF(TRIM(i.telefone_ela), '')
                ) AS telefones,
                i.endereco AS endereco,
                e.status
              FROM encontreiros e
              JOIN encontristas i ON i.id = e.casal_id
             WHERE e.ano = %s
               AND e.paroquia_id = %s
               AND i.paroquia_id = %s
               AND e.equipe = %s
               AND (e.coordenador IS NULL OR UPPER(TRIM(e.coordenador)) <> 'SIM')
               AND (e.status IS NULL OR UPPER(TRIM(e.status)) NOT IN ('RECUSOU','DESISTIU'))
             ORDER BY e.id ASC
        """, (ano, paroquia_id, paroquia_id, equipe_final))
        membros_existentes = cur.fetchall() or []
    finally:
        try:
            cur.close()
            conn.close()
        except Exception:
            pass

    sugestoes_prev_ano = buscar_sugestoes_prev_ano_montagem(ano, paroquia_id)

    return {
        "modo": "normal",
        "ano": ano,
        "equipe": equipe_filtro,
        "equipe_final": equipe_final,
        "limites": limites,
        "membros_existentes": membros_existentes,
        "sugestoes_prev_ano": sugestoes_prev_ano,
    }


def equipe_eh_dirigente(equipe: str) -> bool:
    e = (equipe or "").strip().upper()
    return e.startswith("EQUIPE DIRIGENTE") or "EQUIPE DIRIGENTE" in e


def casal_ja_no_ano(ano: int, nome_ele: str, nome_ela: str, equipe_final: str = "", paroquia_id=None) -> bool:
    equipe_final = (equipe_final or "").strip()

    conn = db_conn()
    cur = conn.cursor(dictionary=True)
    try:
        casal_id = _buscar_casal_id_por_nomes(cur, nome_ele, nome_ela, paroquia_id)
        if not casal_id:
            return False

        if equipe_eh_dirigente(equipe_final):
            cur.execute("""
                SELECT 1
                  FROM encontreiros
                 WHERE ano = %s
                   AND paroquia_id = %s
                   AND casal_id = %s
                   AND (status IS NULL OR UPPER(status) NOT IN ('RECUSOU','DESISTIU'))
                   AND UPPER(equipe) LIKE 'EQUIPE DIRIGENTE%%'
                 LIMIT 1
            """, (ano, paroquia_id, casal_id))
            return cur.fetchone() is not None

        cur.execute("""
            SELECT 1
              FROM encontreiros
             WHERE ano = %s
               AND paroquia_id = %s
               AND casal_id = %s
               AND (status IS NULL OR UPPER(status) NOT IN ('RECUSOU','DESISTIU'))
               AND UPPER(equipe) NOT LIKE 'EQUIPE DIRIGENTE%%'
             LIMIT 1
        """, (ano, paroquia_id, casal_id))
        return cur.fetchone() is not None
    finally:
        try:
            cur.close()
            conn.close()
        except Exception:
            pass


def check_casal_equipe(ano, equipe_final, nome_ele, nome_ela, paroquia_id):
    conn = db_conn()
    cur = conn.cursor(dictionary=True)
    try:
        encontrista = _buscar_encontrista_por_nomes(cur, nome_ele, nome_ela, paroquia_id)

        if not encontrista:
            return {
                "ja_coordenador": False,
                "trabalhou_antes": False,
                "ja_no_ano": False,
                "telefones": "",
                "endereco": "",
            }

        casal_id = encontrista["id"]

        cur.execute("""
            SELECT 1
              FROM encontreiros
             WHERE casal_id = %s
               AND paroquia_id = %s
               AND equipe = %s
               AND UPPER(coordenador) = 'SIM'
             LIMIT 1
        """, (casal_id, paroquia_id, equipe_final))
        ja_coord = cur.fetchone() is not None

        cur.execute("""
            SELECT 1
              FROM encontreiros
             WHERE casal_id = %s
               AND paroquia_id = %s
               AND equipe = %s
             LIMIT 1
        """, (casal_id, paroquia_id, equipe_final))
        trabalhou_antes = cur.fetchone() is not None

        ja_no_ano = casal_ja_no_ano(int(ano), nome_ele, nome_ela, equipe_final, paroquia_id)

        return {
            "ja_coordenador": ja_coord,
            "trabalhou_antes": trabalhou_antes,
            "ja_no_ano": ja_no_ano,
            "telefones": _telefones(encontrista),
            "endereco": encontrista.get("endereco") or "",
        }
    finally:
        try:
            cur.close()
            conn.close()
        except Exception:
            pass


def add_membro_equipe(
    ano,
    equipe_final,
    nome_ele,
    nome_ela,
    telefones=None,
    endereco=None,
    confirmar_repeticao=False,
    paroquia_id=None
):
    conn = db_conn()
    cur = conn.cursor(dictionary=True)
    try:
        casal_id = _buscar_casal_id_por_nomes(cur, nome_ele, nome_ela, paroquia_id)
        if not casal_id:
            return {"ok": False, "status_code": 404, "msg": "Casal não participou do ECC."}

        cur.execute("""
            SELECT 1
              FROM encontreiros
             WHERE casal_id = %s
               AND paroquia_id = %s
               AND equipe = %s
               AND UPPER(coordenador) = 'SIM'
             LIMIT 1
        """, (casal_id, paroquia_id, equipe_final))
        if cur.fetchone():
            return {"ok": False, "status_code": 409, "msg": "Casal já foi coordenador desta equipe."}

        if casal_ja_no_ano(int(ano), nome_ele, nome_ela, equipe_final, paroquia_id):
            return {"ok": False, "status_code": 409, "msg": "Casal já está montado neste ano."}

        cur.execute("""
            SELECT 1
              FROM encontreiros
             WHERE casal_id = %s
               AND paroquia_id = %s
               AND equipe = %s
             LIMIT 1
        """, (casal_id, paroquia_id, equipe_final))
        if cur.fetchone() and not confirmar_repeticao:
            return {
                "ok": False,
                "status_code": 200,
                "needs_confirm": True,
                "msg": "Casal já trabalhou na equipe. Confirmar para montar novamente?"
            }

        cur.execute("""
            INSERT INTO encontreiros
                (paroquia_id, ano, equipe, casal_id, coordenador, status)
            VALUES
                (%s, %s, %s, %s, 'Não', 'Aberto')
        """, (paroquia_id, int(ano), equipe_final, casal_id))
        conn.commit()

        return {"ok": True, "id": cur.lastrowid}
    finally:
        try:
            cur.close()
            conn.close()
        except Exception:
            pass


def marcar_status_dirigente(ano, equipe, novo_status, observacao, paroquia_id):
    conn = db_conn()
    cur = conn.cursor()
    try:
        cur.execute("""
            UPDATE encontreiros
               SET status = %s,
                   observacao = %s
             WHERE ano = %s
               AND paroquia_id = %s
               AND equipe = %s
               AND UPPER(status) = 'ABERTO'
             ORDER BY id DESC
             LIMIT 1
        """, (novo_status, observacao, int(ano), paroquia_id, equipe))
        conn.commit()
        return cur.rowcount
    finally:
        try:
            cur.close()
            conn.close()
        except Exception:
            pass


def marcar_status_membro(_id, novo_status, observacao, paroquia_id):
    conn = db_conn()
    cur = conn.cursor()
    try:
        cur.execute("""
            UPDATE encontreiros
               SET status = %s,
                   observacao = %s
             WHERE id = %s
               AND paroquia_id = %s
               AND (status IS NULL OR UPPER(status) IN ('ABERTO','ACEITO'))
             LIMIT 1
        """, (novo_status, observacao, int(_id), paroquia_id))
        conn.commit()
        return cur.rowcount
    finally:
        try:
            cur.close()
            conn.close()
        except Exception:
            pass


def buscar_dados_organograma(ano, paroquia_id):
    conn = db_conn()
    cursor = conn.cursor(dictionary=True)
    try:
        cursor.execute("""
            SELECT
                e.equipe,
                i.nome_usual_ele AS nome_ele,
                i.nome_usual_ela AS nome_ela,
                e.coordenador
              FROM encontreiros e
              JOIN encontristas i ON i.id = e.casal_id
             WHERE e.ano = %s
               AND e.paroquia_id = %s
               AND i.paroquia_id = %s
               AND (e.status IS NULL OR UPPER(TRIM(e.status)) IN ('ABERTO','CONCLUIDO'))
        """, (ano, paroquia_id, paroquia_id))
        return cursor.fetchall() or []
    finally:
        try:
            cursor.close()
            conn.close()
        except Exception:
            pass


def buscar_relatorio_montagem(ano, paroquia_id):
    conn = db_conn()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute("""
            SELECT 
                e.ano,
                e.equipe,
                COALESCE(i.nome_usual_ele, '') AS nome_ele,
                COALESCE(i.nome_usual_ela, '') AS nome_ela,
                COALESCE(
                    CONCAT_WS(' / ',
                        NULLIF(TRIM(i.telefone_ele), ''),
                        NULLIF(TRIM(i.telefone_ela), '')
                    ),
                    ''
                ) AS telefones,
                COALESCE(i.endereco, '') AS endereco,
                COALESCE(e.coordenador, '') AS coordenador,
                COALESCE(e.status, '') AS status,
                COALESCE(e.observacao, '') AS observacao,
                COALESCE(i.ano, NULL) AS ano_encontro
            FROM encontreiros e
            JOIN encontristas i ON i.id = e.casal_id
            WHERE e.paroquia_id = %s
              AND i.paroquia_id = %s
              AND (%s IS NULL OR e.ano = %s)
            ORDER BY e.equipe,
                     (COALESCE(e.coordenador,'')='Sim') DESC,
                     i.nome_usual_ele,
                     i.nome_usual_ela
        """, (paroquia_id, paroquia_id, ano, ano))
        return cur.fetchall() or []
    finally:
        try:
            cur.close()
            conn.close()
        except Exception:
            pass


def validar_requisitos_montagem_ano(ano, TEAM_MAP, TEAM_LIMITS, paroquia_id):
    ano = int(ano)
    pendencias = []

    equipes_dir = [
        "Equipe Dirigente - MONTAGEM",
        "Equipe Dirigente -FICHAS",
        "Equipe Dirigente - FINANÇAS",
        "Equipe Dirigente - PALESTRA",
        "Equipe Dirigente - PÓS ENCONTRO",
    ]

    conn = db_conn()
    cur = conn.cursor(dictionary=True)
    try:
        for equipe in equipes_dir:
            cur.execute("""
                SELECT COUNT(*) AS total
                  FROM encontreiros
                 WHERE ano = %s
                   AND paroquia_id = %s
                   AND equipe = %s
                   AND (status IS NULL OR UPPER(TRIM(status)) NOT IN ('RECUSOU','DESISTIU'))
            """, (ano, paroquia_id, equipe))
            total = int((cur.fetchone() or {}).get("total") or 0)
            if total < 1:
                pendencias.append(f"Dirigente não definido: {equipe}")

        cur.execute("""
            SELECT COUNT(*) AS total
              FROM encontreiros
             WHERE ano = %s
               AND paroquia_id = %s
               AND UPPER(TRIM(equipe)) = 'CASAL COORDENADOR GERAL'
               AND (status IS NULL OR UPPER(TRIM(status)) NOT IN ('RECUSOU','DESISTIU'))
        """, (ano, paroquia_id))
        total_cg = int((cur.fetchone() or {}).get("total") or 0)
        if total_cg < 1:
            pendencias.append("Casal Coordenador Geral não definido")

        for key, info in TEAM_MAP.items():
            rotulo = info["rotulo"]
            filtro = info["filtro"]

            cur.execute("""
                SELECT COUNT(*) AS total
                  FROM encontreiros
                 WHERE ano = %s
                   AND paroquia_id = %s
                   AND equipe IN (%s, %s)
                   AND UPPER(TRIM(coordenador)) = 'SIM'
                   AND (status IS NULL OR UPPER(TRIM(status)) NOT IN ('RECUSOU','DESISTIU'))
            """, (ano, paroquia_id, rotulo, filtro))
            total_coord = int((cur.fetchone() or {}).get("total") or 0)
            if total_coord < 1:
                pendencias.append(f"Coordenador não definido: {filtro}")

            limites_cfg = TEAM_LIMITS.get(filtro, TEAM_LIMITS.get(rotulo, {}))
            minimo = int(limites_cfg.get("min", 0) or 0)

            if filtro.lower() == "sala":
                equipes_sala = (
                    "Equipe de Sala - Canto",
                    "Equipe de Sala - Som e Projeção",
                    "Equipe de Sala - Boa Vontade",
                    "Equipe de Sala - Recepção Palestrantes",
                )
                cur.execute("""
                    SELECT COUNT(*) AS total
                      FROM encontreiros
                     WHERE ano = %s
                       AND paroquia_id = %s
                       AND equipe IN (%s, %s, %s, %s)
                       AND (coordenador IS NULL OR UPPER(TRIM(coordenador)) <> 'SIM')
                       AND (status IS NULL OR UPPER(TRIM(status)) NOT IN ('RECUSOU','DESISTIU'))
                """, (ano, paroquia_id, *equipes_sala))
            else:
                cur.execute("""
                    SELECT COUNT(*) AS total
                      FROM encontreiros
                     WHERE ano = %s
                       AND paroquia_id = %s
                       AND equipe = %s
                       AND (coordenador IS NULL OR UPPER(TRIM(coordenador)) <> 'SIM')
                       AND (status IS NULL OR UPPER(TRIM(status)) NOT IN ('RECUSOU','DESISTIU'))
                """, (ano, paroquia_id, rotulo))

            total_integrantes = int((cur.fetchone() or {}).get("total") or 0)
            if total_integrantes < minimo:
                pendencias.append(f"{filtro}: mínimo {minimo}, atual {total_integrantes}")

        return {
            "ok": len(pendencias) == 0,
            "pendencias": pendencias,
        }
    finally:
        try:
            cur.close()
            conn.close()
        except Exception:
            pass


def concluir_montagem_ano(ano, TEAM_MAP=None, TEAM_LIMITS=None, paroquia_id=None):
    if TEAM_MAP is not None and TEAM_LIMITS is not None:
        validacao = validar_requisitos_montagem_ano(ano, TEAM_MAP, TEAM_LIMITS, paroquia_id)
        if not validacao["ok"]:
            return validacao

    conn = db_conn()
    cur = conn.cursor()
    try:
        cur.execute("""
            UPDATE encontreiros
               SET status = 'Concluido'
             WHERE ano = %s
               AND paroquia_id = %s
               AND UPPER(status) = 'ABERTO'
        """, (int(ano), paroquia_id))
        conn.commit()
        return {
            "ok": True,
            "alterados": cur.rowcount,
            "msg": "Montagem concluída com sucesso."
        }
    finally:
        try:
            cur.close()
            conn.close()
        except Exception:
            pass