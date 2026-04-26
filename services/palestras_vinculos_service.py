from db import db_conn


def listar_palestras_sem_casal_manual(filtros):
    conn = db_conn()
    cur = conn.cursor(dictionary=True)

    try:
        where = ["p.casal_id IS NULL"]
        params = []

        nome_ele = (filtros.get("p_nome_ele") or "").strip()
        nome_ela = (filtros.get("p_nome_ela") or "").strip()
        ano = (filtros.get("p_ano") or "").strip()
        palestra = (filtros.get("p_palestra") or "").strip()

        if nome_ele:
            where.append("p.nome_ele LIKE %s")
            params.append(f"%{nome_ele}%")

        if nome_ela:
            where.append("p.nome_ela LIKE %s")
            params.append(f"%{nome_ela}%")

        if ano:
            where.append("p.ano = %s")
            params.append(ano)

        if palestra:
            where.append("p.palestra LIKE %s")
            params.append(f"%{palestra}%")

        sql = f"""
            SELECT
                p.id,
                p.ano,
                p.palestra,
                p.nome_ele,
                p.nome_ela,
                p.status
            FROM palestras p
            WHERE {' AND '.join(where)}
            ORDER BY
                p.ano DESC,
                p.palestra ASC,
                p.nome_ele ASC,
                p.nome_ela ASC
            LIMIT 1000
        """

        cur.execute(sql, params)
        return cur.fetchall() or []

    finally:
        cur.close()
        conn.close()


def listar_encontristas_para_vinculo_palestras(filtros):
    conn = db_conn()
    cur = conn.cursor(dictionary=True)

    try:
        where = ["1=1"]
        params = []

        nome_completo_ele = (filtros.get("c_nome_completo_ele") or "").strip()
        nome_completo_ela = (filtros.get("c_nome_completo_ela") or "").strip()
        nome_usual_ele = (filtros.get("c_nome_usual_ele") or "").strip()
        nome_usual_ela = (filtros.get("c_nome_usual_ela") or "").strip()
        ano = (filtros.get("c_ano") or "").strip()
        endereco = (filtros.get("c_endereco") or "").strip()

        if nome_completo_ele:
            where.append("c.nome_completo_ele LIKE %s")
            params.append(f"%{nome_completo_ele}%")

        if nome_completo_ela:
            where.append("c.nome_completo_ela LIKE %s")
            params.append(f"%{nome_completo_ela}%")

        if nome_usual_ele:
            where.append("c.nome_usual_ele LIKE %s")
            params.append(f"%{nome_usual_ele}%")

        if nome_usual_ela:
            where.append("c.nome_usual_ela LIKE %s")
            params.append(f"%{nome_usual_ela}%")

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
                c.num_ecc,
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
        cur.close()
        conn.close()


def vincular_palestras_em_lote(palestras_ids, casal_id):
    conn = db_conn()
    cur = conn.cursor()

    try:
        ids = []

        for item in palestras_ids:
            try:
                ids.append(int(item))
            except Exception:
                pass

        try:
            casal_id = int(casal_id)
        except Exception:
            return {"ok": False, "msg": "Casal selecionado inválido."}

        if not ids:
            return {"ok": False, "msg": "Selecione pelo menos uma palestra."}

        placeholders = ",".join(["%s"] * len(ids))

        sql = f"""
            UPDATE palestras
            SET casal_id = %s
            WHERE id IN ({placeholders})
              AND casal_id IS NULL
        """

        cur.execute(sql, [casal_id] + ids)
        afetados = cur.rowcount

        conn.commit()

        return {
            "ok": True,
            "msg": f"{afetados} palestra(s) vinculada(s) com sucesso."
        }

    finally:
        cur.close()
        conn.close()