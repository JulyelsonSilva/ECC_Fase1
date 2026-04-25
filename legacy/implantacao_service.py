from db import db_conn


def listar_implantacao_por_ano():
    conn = db_conn()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute("""
            SELECT 
                ano,
                SUM(CASE WHEN UPPER(TRIM(status))='CONCLUIDO' THEN 1 ELSE 0 END) AS qtd_concluido,
                COUNT(*) AS total
            FROM implantacao
            GROUP BY ano
            ORDER BY ano DESC
        """)
        rows = cur.fetchall() or []

        anos_concluidos, anos_aberto = [], []
        for r in rows:
            item = {
                "ano": r["ano"],
                "qtd_concluido": int(r["qtd_concluido"] or 0),
                "total": int(r["total"] or 0)
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
            cur.close()
            conn.close()
        except Exception:
            pass


def buscar_sugestoes_prev_ano_implantacao(ano_pre):
    if not ano_pre:
        return []

    conn = db_conn()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute("""
            SELECT e.nome_usual_ele, e.nome_usual_ela, e.telefone_ele, e.telefone_ela, e.endereco
              FROM encontristas e
             WHERE e.ano = %s
               AND NOT EXISTS (
                     SELECT 1 FROM implantacao i
                      WHERE i.ano = %s
                        AND i.nome_ele = e.nome_usual_ele
                        AND i.nome_ela = e.nome_usual_ela
                        AND (i.status IS NULL OR UPPER(TRIM(i.status)) NOT IN ('RECUSOU','DESISTIU'))
               )
             ORDER BY e.nome_usual_ele, e.nome_usual_ela
        """, (ano_pre - 1, ano_pre))

        sugestoes_prev_ano = []
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


def contar_implantacao_por_equipe(ano, TEAM_MAP):
    conn = db_conn()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute("""
            SELECT equipe, coordenador
              FROM implantacao
             WHERE ano = %s
               AND (status IS NULL OR UPPER(TRIM(status)) NOT IN ('RECUSOU','DESISTIU'))
        """, (ano,))
        rows = cur.fetchall() or []

        rotulo_to_filtro = {info["rotulo"]: info["filtro"] for info in TEAM_MAP.values()}
        counts = {info["filtro"]: 0 for info in TEAM_MAP.values()}

        for r in rows:
            eq = (r.get("equipe") or "").strip()
            is_coord = (r.get("coordenador") or "").strip().upper() == "SIM"
            if is_coord:
                continue
            filtro = rotulo_to_filtro.get(eq)
            if filtro:
                counts[filtro] = counts.get(filtro, 0) + 1

        return counts
    finally:
        try:
            cur.close()
            conn.close()
        except Exception:
            pass


def checar_casal_implantacao(ano, equipe, nome_ele, nome_ela):
    conn = db_conn()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute("""
            SELECT 1 FROM implantacao
             WHERE ano = %s AND nome_ele = %s AND nome_ela = %s
               AND (status IS NULL OR UPPER(status) NOT IN ('RECUSOU','DESISTIU'))
             LIMIT 1
        """, (int(ano), nome_ele, nome_ela))
        ja_no_ano = cur.fetchone() is not None

        cur.execute("""
            SELECT 1 FROM implantacao
             WHERE nome_ele = %s AND nome_ela = %s AND equipe = %s
             LIMIT 1
        """, (nome_ele, nome_ela, equipe))
        trabalhou_antes = cur.fetchone() is not None

        cur.execute("""
            SELECT 1 FROM implantacao
             WHERE nome_ele = %s AND nome_ela = %s AND equipe = %s
               AND UPPER(coordenador)='SIM'
             LIMIT 1
        """, (nome_ele, nome_ela, equipe))
        ja_coord = cur.fetchone() is not None

        telefones, endereco = '', ''

        cur.execute("""
            SELECT telefones, endereco
              FROM implantacao
             WHERE nome_ele = %s AND nome_ela = %s
             ORDER BY ano DESC, id DESC
             LIMIT 1
        """, (nome_ele, nome_ela))
        r = cur.fetchone()
        if r:
            telefones = (r.get('telefones') or '').strip()
            endereco = r.get('endereco') or ''
        else:
            cur.execute("""
                SELECT telefones, endereco
                  FROM encontreiros
                 WHERE nome_ele = %s AND nome_ela = %s
                 ORDER BY ano DESC, id DESC
                 LIMIT 1
            """, (nome_ele, nome_ela))
            r2 = cur.fetchone()
            if r2:
                telefones = (r2.get('telefones') or '').strip()
                endereco = r2.get('endereco') or ''
            else:
                cur.execute("""
                    SELECT telefone_ele, telefone_ela, endereco
                      FROM encontristas
                     WHERE nome_usual_ele = %s AND nome_usual_ela = %s
                     ORDER BY ano DESC
                     LIMIT 1
                """, (nome_ele, nome_ela))
                r3 = cur.fetchone()
                if r3:
                    tel_ele = (r3.get('telefone_ele') or '').strip()
                    tel_ela = (r3.get('telefone_ela') or '').strip()
                    telefones = " / ".join([t for t in [tel_ele, tel_ela] if t])
                    endereco = r3.get('endereco') or ''

        return {
            "ja_no_ano": ja_no_ano,
            "trabalhou_antes": trabalhou_antes,
            "ja_coordenador": ja_coord,
            "telefones": telefones,
            "endereco": endereco,
        }
    finally:
        try:
            cur.close()
            conn.close()
        except Exception:
            pass


def adicionar_membro_implantacao(ano, equipe, nome_ele, nome_ela, telefones, endereco, coord_val, confirmar_repeticao=False):
    conn = db_conn()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute("""
            SELECT 1 FROM implantacao
             WHERE ano = %s AND nome_ele = %s AND nome_ela = %s
               AND (status IS NULL OR UPPER(status) IN ('ABERTO','ACEITO','CONCLUIDO'))
             LIMIT 1
        """, (int(ano), nome_ele, nome_ela))
        if cur.fetchone():
            return {"ok": False, "status_code": 409, "msg": "Casal já está lançado neste ano (Implantação)."}

        cur.execute("""
            SELECT 1 FROM implantacao
             WHERE nome_ele = %s AND nome_ela = %s AND equipe = %s
             LIMIT 1
        """, (nome_ele, nome_ela, equipe))
        if cur.fetchone() and not confirmar_repeticao:
            return {
                "ok": False,
                "status_code": 200,
                "needs_confirm": True,
                "msg": "Casal já trabalhou nesta equipe (Implantação). Confirmar para lançar novamente?"
            }

        cur2 = conn.cursor()
        try:
            cur2.execute("""
                INSERT INTO implantacao
                    (ano, equipe, nome_ele, nome_ela, telefones, endereco, coordenador, status)
                VALUES
                    (%s,  %s,     %s,       %s,       %s,         %s,       %s,           'Aberto')
            """, (int(ano), equipe, nome_ele, nome_ela, telefones, endereco, coord_val))
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


def marcar_status_implantacao(_id, novo_status, observacao):
    conn = db_conn()
    cur = conn.cursor()
    try:
        cur.execute("""
            UPDATE implantacao
               SET status=%s, observacao=%s
             WHERE id=%s
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


def concluir_implantacao_ano(ano):
    conn = db_conn()
    cur = conn.cursor()
    try:
        cur.execute("""
            UPDATE implantacao
               SET status='Concluido'
             WHERE ano=%s
               AND UPPER(status)='ABERTO'
        """, (int(ano),))
        conn.commit()
        return cur.rowcount
    finally:
        try:
            cur.close()
            conn.close()
        except Exception:
            pass
