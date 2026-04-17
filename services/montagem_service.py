from db import db_conn


def listar_montagem_por_ano():
    conn = db_conn()
    cursor = conn.cursor(dictionary=True)
    try:
        cursor.execute("""
            SELECT 
                ano,
                SUM(CASE WHEN TRIM(LOWER(status)) = 'concluido' THEN 1 ELSE 0 END) AS qtd_concluido,
                COUNT(*) AS total
            FROM encontreiros
            GROUP BY ano
            ORDER BY ano DESC
        """)
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


def carregar_dados_iniciais_montagem(ano_preselecionado, TEAM_MAP):
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
                SELECT nome_ele, nome_ela, telefones, endereco
                  FROM encontreiros
                 WHERE ano = %s AND equipe = %s
                   AND (status IS NULL OR UPPER(status) NOT IN ('RECUSOU','DESISTIU'))
                 ORDER BY id ASC
                 LIMIT 1
            """, (ano_preselecionado, equipe))
            r = cur.fetchone()
            if r:
                initial_data["dirigentes"][equipe] = {
                    "nome_ele": r.get("nome_ele") or "",
                    "nome_ela": r.get("nome_ela") or "",
                    "telefones": r.get("telefones") or "",
                    "endereco": r.get("endereco") or ""
                }

        cur.execute("""
            SELECT nome_ele, nome_ela, telefones, endereco
              FROM encontreiros
             WHERE ano = %s
               AND UPPER(equipe) = 'CASAL COORDENADOR GERAL'
               AND UPPER(status) = 'ABERTO'
             ORDER BY id DESC
             LIMIT 1
        """, (ano_preselecionado,))
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
                SELECT nome_ele, nome_ela, telefones, endereco
                  FROM encontreiros
                 WHERE ano = %s
                   AND equipe IN (%s, %s)
                   AND UPPER(coordenador) = 'SIM'
                   AND UPPER(status) = 'ABERTO'
                 ORDER BY id DESC
                 LIMIT 1
            """, (ano_preselecionado, rotulo, filtro))
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


def buscar_casal_para_montagem(nome_ele, nome_ela):
    conn = db_conn()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute("""
            SELECT ano, telefones, endereco
              FROM encontreiros
             WHERE nome_ele = %s AND nome_ela = %s
             ORDER BY ano DESC
             LIMIT 1
        """, (nome_ele, nome_ela))
        r = cur.fetchone()
        if r:
            return {
                "ok": True,
                "origem": "encontreiros",
                "ano": r["ano"],
                "telefones": r.get("telefones") or "",
                "endereco": r.get("endereco") or ""
            }

        cur.execute("""
            SELECT telefone_ele, telefone_ela, endereco, ano
              FROM encontristas
             WHERE nome_usual_ele = %s AND nome_usual_ela = %s
             ORDER BY ano DESC
             LIMIT 1
        """, (nome_ele, nome_ela))
        r2 = cur.fetchone()
        if r2:
            tel_ele = (r2.get('telefone_ele') or '').strip()
            tel_ela = (r2.get('telefone_ela') or '').strip()
            tels = " / ".join([t for t in [tel_ele, tel_ela] if t])
            return {
                "ok": True,
                "origem": "encontristas",
                "ano": r2.get("ano"),
                "telefones": tels or "",
                "endereco": r2.get("endereco") or ""
            }

        return {"ok": False, "msg": "Casal não participou do ECC."}
    finally:
        try:
            cur.close()
            conn.close()
        except Exception:
            pass


def adicionar_dirigente_montagem(ano, equipe, nome_ele, nome_ela, telefones, endereco):
    conn = db_conn()
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO encontreiros
                (ano, equipe, nome_ele, nome_ela, telefones, endereco, coordenador, status)
            VALUES
                (%s,  %s,     %s,       %s,       %s,         %s,       'Sim',      'Aberto')
        """, (int(ano), equipe, nome_ele, nome_ela, telefones, endereco))
        conn.commit()
        return True
    finally:
        try:
            cur.close()
            conn.close()
        except Exception:
            pass


def buscar_cg_montagem(nome_ele, nome_ela):
    conn = db_conn()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute("""
            SELECT ano, telefones, endereco
              FROM encontreiros
             WHERE nome_ele = %s AND nome_ela = %s
             ORDER BY ano DESC
             LIMIT 1
        """, (nome_ele, nome_ela))
        r = cur.fetchone()
        if not r:
            return {"ok": False, "status_code": 404, "msg": "Casal nunca trabalhou no ECC."}

        cur.execute("""
            SELECT 1
              FROM encontreiros
             WHERE nome_ele = %s AND nome_ela = %s
               AND UPPER(equipe) = 'CASAL COORDENADOR GERAL'
             LIMIT 1
        """, (nome_ele, nome_ela))
        r2 = cur.fetchone()
        if r2:
            return {"ok": False, "status_code": 409, "msg": "Casal já foi Coordenador Geral."}

        return {
            "ok": True,
            "ano_ref": r["ano"],
            "telefones": r.get("telefones") or "",
            "endereco": r.get("endereco") or ""
        }
    finally:
        try:
            cur.close()
            conn.close()
        except Exception:
            pass


def adicionar_cg_montagem(ano, nome_ele, nome_ela, telefones, endereco):
    conn = db_conn()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute("""
            SELECT 1 FROM encontreiros
             WHERE nome_ele = %s AND nome_ela = %s
             LIMIT 1
        """, (nome_ele, nome_ela))
        if not cur.fetchone():
            return {"ok": False, "status_code": 404, "msg": "Casal nunca trabalhou no ECC."}

        cur.execute("""
            SELECT 1 FROM encontreiros
             WHERE nome_ele = %s AND nome_ela = %s
               AND UPPER(equipe) = 'CASAL COORDENADOR GERAL'
             LIMIT 1
        """, (nome_ele, nome_ela))
        if cur.fetchone():
            return {"ok": False, "status_code": 409, "msg": "Casal já foi Coordenador Geral."}

        cur2 = conn.cursor()
        try:
            cur2.execute("""
                INSERT INTO encontreiros
                    (ano, equipe, nome_ele, nome_ela, telefones, endereco, coordenador, status)
                VALUES
                    (%s,  'Casal Coordenador Geral', %s, %s, %s, %s, 'Sim', 'Aberto')
            """, (int(ano), nome_ele, nome_ela, telefones, endereco))
            conn.commit()
        finally:
            cur2.close()

        return {"ok": True}
    finally:
        try:
            cur.close()
            conn.close()
        except Exception:
            pass


def contar_equipes_montagem(ano, TEAM_MAP):
    conn = db_conn()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute("""
            SELECT equipe, coordenador
            FROM encontreiros
            WHERE ano = %s
              AND (status IS NULL OR UPPER(TRIM(status)) NOT IN ('RECUSOU','DESISTIU'))
        """, (ano,))
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


def buscar_sugestoes_prev_ano_montagem(ano):
    sugestoes_prev_ano = []
    if not ano:
        return sugestoes_prev_ano

    conn = db_conn()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute("""
            SELECT e.nome_usual_ele, e.nome_usual_ela, e.telefone_ele, e.telefone_ela, e.endereco
              FROM encontristas e
             WHERE e.ano = %s
               AND NOT EXISTS (
                    SELECT 1
                      FROM encontreiros w
                     WHERE w.ano = %s
                       AND LOWER(TRIM(w.nome_ele)) = LOWER(TRIM(e.nome_usual_ele))
                       AND LOWER(TRIM(w.nome_ela)) = LOWER(TRIM(e.nome_usual_ela))
               )
             ORDER BY e.nome_usual_ele, e.nome_usual_ela
        """, (ano - 1, ano))
        for r in cur.fetchall() or []:
            tel_ele = (r.get('telefone_ele') or '').strip()
            tel_ela = (r.get('telefone_ela') or '').strip()
            tels = " / ".join([t for t in [tel_ele, tel_ela] if t])
            sugestoes_prev_ano.append({
                "nome_ele": r.get('nome_usual_ele') or '',
                "nome_ela": r.get('nome_usual_ela') or '',
                "telefones": tels,
                "endereco": r.get('endereco') or ''
            })
        return sugestoes_prev_ano
    finally:
        try:
            cur.close()
            conn.close()
        except Exception:
            pass


def carregar_equipe_montagem(ano, equipe_filtro, TEAM_MAP, TEAM_LIMITS):
    equipe_final = None
    for _key, info in TEAM_MAP.items():
        if info['filtro'].lower() == equipe_filtro.lower():
            equipe_final = info['rotulo']
            break
    if not equipe_final:
        equipe_final = equipe_filtro or 'Equipe'

    if equipe_filtro.lower() == 'sala':
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
                SELECT id, ano, equipe, nome_ele, nome_ela, telefones, endereco, status
                  FROM encontreiros
                 WHERE ano = %s
                   AND equipe IN (%s, %s, %s, %s)
                   AND (coordenador IS NULL OR UPPER(TRIM(coordenador)) <> 'SIM')
                   AND (status IS NULL OR UPPER(TRIM(status)) NOT IN ('RECUSOU','DESISTIU'))
                 ORDER BY id ASC
            """, (
                ano,
                SALA_DB["Canto"],
                SALA_DB["Som e Projeção"],
                SALA_DB["Boa Vontade"],
                SALA_DB["Recepção Palestrantes"],
            ))
            rows = cur.fetchall() or []

            buckets = {k: [] for k in SALA_DB.keys()}
            for r in rows:
                eq = (r.get("equipe") or "")
                for k, dbname in SALA_DB.items():
                    if eq == dbname:
                        buckets[k].append(r)
                        break

            cur.execute("""
                SELECT nome_ele, nome_ela, telefones, endereco
                  FROM encontreiros
                 WHERE ano = %s
                   AND UPPER(equipe) = 'EQUIPE DIRIGENTE - PALESTRA'
                   AND (status IS NULL OR UPPER(TRIM(status)) NOT IN ('RECUSOU','DESISTIU'))
                 ORDER BY id DESC
                 LIMIT 1
            """, (ano,))
            pref_recepcao = cur.fetchone() or {}
        finally:
            try:
                cur.close()
                conn.close()
            except Exception:
                pass

        sala_slots = []
        use_index = {"Canto": 0, "Som e Projeção": 0, "Boa Vontade": 0, "Recepção Palestrantes": 0}
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
        sugestoes_prev_ano = buscar_sugestoes_prev_ano_montagem(ano)

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
        "min": int(limites_cfg.get('min', 0)),
        "max": int(limites_cfg.get('max', 8)),
    }

    conn = db_conn()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute("""
            SELECT id, nome_ele, nome_ela, telefones, endereco, status
              FROM encontreiros
             WHERE ano = %s
               AND equipe = %s
               AND (coordenador IS NULL OR UPPER(TRIM(coordenador)) <> 'SIM')
               AND (status IS NULL OR UPPER(TRIM(status)) NOT IN ('RECUSOU','DESISTIU'))
             ORDER BY id ASC
        """, (ano, equipe_final))
        membros_existentes = cur.fetchall() or []
    finally:
        try:
            cur.close()
            conn.close()
        except Exception:
            pass

    sugestoes_prev_ano = buscar_sugestoes_prev_ano_montagem(ano)

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


def casal_ja_no_ano(ano: int, nome_ele: str, nome_ela: str, equipe_final: str = "") -> bool:
    equipe_final = (equipe_final or "").strip()

    conn = db_conn()
    cur = conn.cursor()
    try:
        if equipe_eh_dirigente(equipe_final):
            cur.execute("""
                SELECT 1
                  FROM encontreiros
                 WHERE ano = %s
                   AND nome_ele = %s AND nome_ela = %s
                   AND (status IS NULL OR UPPER(status) NOT IN ('RECUSOU','DESISTIU'))
                   AND UPPER(equipe) LIKE 'EQUIPE DIRIGENTE%%'
                 LIMIT 1
            """, (ano, nome_ele, nome_ela))
            return cur.fetchone() is not None

        cur.execute("""
            SELECT 1
              FROM encontreiros
             WHERE ano = %s
               AND nome_ele = %s AND nome_ela = %s
               AND (status IS NULL OR UPPER(status) NOT IN ('RECUSOU','DESISTIU'))
               AND UPPER(equipe) NOT LIKE 'EQUIPE DIRIGENTE%%'
             LIMIT 1
        """, (ano, nome_ele, nome_ela))
        return cur.fetchone() is not None
    finally:
        try:
            cur.close()
            conn.close()
        except Exception:
            pass


def check_casal_equipe(ano, equipe_final, nome_ele, nome_ela):
    conn = db_conn()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute("""
            SELECT 1 FROM encontreiros
             WHERE nome_ele = %s AND nome_ela = %s
               AND equipe = %s
               AND UPPER(coordenador) = 'SIM'
             LIMIT 1
        """, (nome_ele, nome_ela, equipe_final))
        ja_coord = cur.fetchone() is not None

        cur.execute("""
            SELECT 1 FROM encontreiros
             WHERE nome_ele = %s AND nome_ela = %s
               AND equipe = %s
             LIMIT 1
        """, (nome_ele, nome_ela, equipe_final))
        trabalhou_antes = cur.fetchone() is not None

        ja_no_ano = casal_ja_no_ano(int(ano), nome_ele, nome_ela, equipe_final)

        cur.execute("""
            SELECT telefones, endereco
              FROM encontreiros
             WHERE nome_ele = %s AND nome_ela = %s
             ORDER BY ano DESC
             LIMIT 1
        """, (nome_ele, nome_ela))
        r = cur.fetchone()
        telefones = (r.get('telefones') if r else '') or ''
        endereco = (r.get('endereco') if r else '') or ''

        if not r:
            cur.execute("""
                SELECT telefone_ele, telefone_ela, endereco
                  FROM encontristas
                 WHERE nome_usual_ele = %s AND nome_usual_ela = %s
                 ORDER BY ano DESC
                 LIMIT 1
            """, (nome_ele, nome_ela))
            r2 = cur.fetchone()
            if r2:
                tel_ele = (r2.get('telefone_ele') or '').strip()
                tel_ela = (r2.get('telefone_ela') or '').strip()
                telefones = " / ".join([t for t in [tel_ele, tel_ela] if t])
                endereco = r2.get('endereco') or ''

        return {
            "ja_coordenador": ja_coord,
            "trabalhou_antes": trabalhou_antes,
            "ja_no_ano": ja_no_ano,
            "telefones": telefones,
            "endereco": endereco,
        }
    finally:
        try:
            cur.close()
            conn.close()
        except Exception:
            pass


def add_membro_equipe(ano, equipe_final, nome_ele, nome_ela, telefones, endereco, confirmar_repeticao=False):
    conn = db_conn()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute("""
            SELECT 1 FROM encontreiros
             WHERE nome_ele = %s AND nome_ela = %s
               AND equipe = %s
               AND UPPER(coordenador) = 'SIM'
             LIMIT 1
        """, (nome_ele, nome_ela, equipe_final))
        if cur.fetchone():
            return {"ok": False, "status_code": 409, "msg": "Casal já foi coordenador desta equipe."}

        if casal_ja_no_ano(int(ano), nome_ele, nome_ela, equipe_final):
            return {"ok": False, "status_code": 409, "msg": "Casal já está montado neste ano."}

        cur.execute("""
            SELECT 1 FROM encontreiros
             WHERE nome_ele = %s AND nome_ela = %s
               AND equipe = %s
             LIMIT 1
        """, (nome_ele, nome_ela, equipe_final))
        if cur.fetchone() and not confirmar_repeticao:
            return {
                "ok": False,
                "status_code": 200,
                "needs_confirm": True,
                "msg": "Casal já trabalhou na equipe. Confirmar para montar novamente?"
            }

        cur2 = conn.cursor()
        try:
            cur2.execute("""
                INSERT INTO encontreiros
                    (ano, equipe, nome_ele, nome_ela, telefones, endereco, coordenador, status)
                VALUES
                    (%s,  %s,     %s,       %s,       %s,         %s,       'Não',      'Aberto')
            """, (int(ano), equipe_final, nome_ele, nome_ela, telefones, endereco))
            conn.commit()
            new_id = cur2.lastrowid
        finally:
            cur2.close()

        return {"ok": True, "id": new_id}
    finally:
        try:
            cur.close()
            conn.close()
        except Exception:
            pass


def marcar_status_dirigente(ano, equipe, novo_status, observacao):
    conn = db_conn()
    cur = conn.cursor()
    try:
        cur.execute("""
            UPDATE encontreiros
               SET status = %s, observacao = %s
             WHERE ano = %s
               AND equipe = %s
               AND UPPER(status) = 'ABERTO'
             ORDER BY id DESC
             LIMIT 1
        """, (novo_status, observacao, int(ano), equipe))
        conn.commit()
        return cur.rowcount
    finally:
        try:
            cur.close()
            conn.close()
        except Exception:
            pass


def marcar_status_membro(_id, novo_status, observacao):
    conn = db_conn()
    cur = conn.cursor()
    try:
        cur.execute("""
            UPDATE encontreiros
               SET status = %s, observacao = %s
             WHERE id = %s
               AND (status IS NULL OR UPPER(status) IN ('ABERTO','ACEITO'))
             LIMIT 1
        """, (novo_status, observacao, int(_id)))
        conn.commit()
        return cur.rowcount
    finally:
        try:
            cur.close()
            conn.close()
        except Exception:
            pass


def concluir_montagem_ano(ano):
    conn = db_conn()
    cur = conn.cursor()
    try:
        cur.execute("""
            UPDATE encontreiros
               SET status = 'Concluido'
             WHERE ano = %s
               AND UPPER(status) = 'ABERTO'
        """, (int(ano),))
        conn.commit()
        return cur.rowcount
    finally:
        try:
            cur.close()
            conn.close()
        except Exception:
            pass


def buscar_dados_organograma(ano):
    conn = db_conn()
    cursor = conn.cursor(dictionary=True)
    try:
        cursor.execute("""
            SELECT equipe, nome_ele, nome_ela, coordenador
              FROM encontreiros
             WHERE ano = %s
               AND (status IS NULL OR UPPER(TRIM(status)) IN ('ABERTO','CONCLUIDO'))
        """, (ano,))
        return cursor.fetchall() or []
    finally:
        try:
            cursor.close()
            conn.close()
        except Exception:
            pass


def buscar_relatorio_montagem(ano):
    conn = db_conn()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute("""
            SELECT 
                e.ano,
                e.equipe,
                COALESCE(e.nome_ele, '')        AS nome_ele,
                COALESCE(e.nome_ela, '')        AS nome_ela,
                COALESCE(e.telefones, '')       AS telefones,
                COALESCE(e.endereco, '')        AS endereco,
                COALESCE(e.coordenador, '')     AS coordenador,
                COALESCE(e.status, '')          AS status,
                COALESCE(e.observacao, '')      AS observacao,
                COALESCE(i.ano, NULL)           AS ano_encontro
            FROM encontreiros e
            LEFT JOIN encontristas i
              ON UPPER(TRIM(i.nome_usual_ele)) = UPPER(TRIM(e.nome_ele))
             AND UPPER(TRIM(i.nome_usual_ela)) = UPPER(TRIM(e.nome_ela))
            WHERE (%s IS NULL OR e.ano = %s)
            ORDER BY e.equipe, (COALESCE(e.coordenador,'')='Sim') DESC, e.nome_ele, e.nome_ela
        """, (ano, ano))
        return cur.fetchall() or []
    finally:
        try:
            cur.close()
            conn.close()
        except Exception:
            pass
