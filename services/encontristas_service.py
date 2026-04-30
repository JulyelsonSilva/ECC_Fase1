from db import db_conn


CAMPOS_ENCONTRISTA = """
    id,
    ano,
    num_ecc,
    data_casamento,
    data_1_etapa,
    data_2_etapa,
    data_3_etapa,
    nome_completo_ele,
    nome_completo_ela,
    nome_usual_ele,
    nome_usual_ela,
    apelidos,
    telefone_ele,
    telefone_ela,
    endereco,
    casal_visitacao,
    ficha_num,
    aceitou,
    observacao,
    observacao_extra
"""


def listar_encontristas_paroquia(paroquia_id, nome_ele="", nome_ela="", ano="", pagina=1, por_pagina=50):
    conn = db_conn()
    cursor = conn.cursor(dictionary=True)

    try:
        where = ["paroquia_id = %s"]
        params = [paroquia_id]

        if nome_ele:
            where.append("nome_usual_ele LIKE %s")
            params.append(f"%{nome_ele}%")

        if nome_ela:
            where.append("nome_usual_ela LIKE %s")
            params.append(f"%{nome_ela}%")

        if ano:
            where.append("ano = %s")
            params.append(ano)

        where_sql = " AND ".join(where)

        count_query = f"""
            SELECT COUNT(*) AS total
            FROM encontristas
            WHERE {where_sql}
        """
        cursor.execute(count_query, params)
        total_row = cursor.fetchone() or {}
        total = total_row.get("total", 0) or 0

        try:
            pagina = max(int(pagina or 1), 1)
        except (TypeError, ValueError):
            pagina = 1

        try:
            por_pagina = max(int(por_pagina or 50), 1)
        except (TypeError, ValueError):
            por_pagina = 50

        offset = (pagina - 1) * por_pagina

        query = f"""
            SELECT
                {CAMPOS_ENCONTRISTA}
            FROM encontristas
            WHERE {where_sql}
            ORDER BY ano DESC, id DESC
            LIMIT %s OFFSET %s
        """
        cursor.execute(query, params + [por_pagina, offset])
        dados = cursor.fetchall() or []

        return {
            "dados": dados,
            "total": total,
            "pagina": pagina,
            "por_pagina": por_pagina,
        }
    finally:
        try:
            cursor.close()
            conn.close()
        except Exception:
            pass


def buscar_encontrista_por_id_paroquia(encontrista_id, paroquia_id):
    conn = db_conn()
    cursor = conn.cursor(dictionary=True)

    try:
        cursor.execute(f"""
            SELECT
                {CAMPOS_ENCONTRISTA}
            FROM encontristas
            WHERE id = %s
              AND paroquia_id = %s
        """, (encontrista_id, paroquia_id))
        return cursor.fetchone()
    finally:
        try:
            cursor.close()
            conn.close()
        except Exception:
            pass


def atualizar_encontrista_paroquia(encontrista_id, paroquia_id, payload):
    conn = db_conn()
    cursor = conn.cursor()

    try:
        sql = """
            UPDATE encontristas SET
                nome_completo_ele = %s,
                nome_completo_ela = %s,
                nome_usual_ele = %s,
                nome_usual_ela = %s,
                telefone_ele = %s,
                telefone_ela = %s,
                endereco = %s,
                num_ecc = %s,
                ano = %s,
                data_casamento = %s,
                data_1_etapa = %s,
                data_2_etapa = %s,
                data_3_etapa = %s,
                casal_visitacao = %s,
                ficha_num = %s,
                aceitou = %s,
                observacao = %s,
                observacao_extra = %s
            WHERE id = %s
              AND paroquia_id = %s
        """
        cursor.execute(sql, (
            payload["nome_completo_ele"],
            payload["nome_completo_ela"],
            payload["nome_usual_ele"],
            payload["nome_usual_ela"],
            payload["telefone_ele"],
            payload["telefone_ela"],
            payload["endereco"],
            payload["num_ecc"],
            payload["ano"],
            payload["data_casamento"],
            payload["data_1_etapa"],
            payload["data_2_etapa"],
            payload["data_3_etapa"],
            payload["casal_visitacao"],
            payload["ficha_num"],
            payload["aceitou"],
            payload["observacao"],
            payload["observacao_extra"],
            encontrista_id,
            paroquia_id
        ))
        conn.commit()
        return True
    finally:
        try:
            cursor.close()
            conn.close()
        except Exception:
            pass


def contar_encontristas_por_ano_paroquia(paroquia_id, ano_min=None, ano_max=None):
    conn = db_conn()
    cur = conn.cursor(dictionary=True)

    try:
        sql = """
            SELECT ano, COUNT(*) AS qtd
              FROM encontristas
             WHERE paroquia_id = %s
               AND ano IS NOT NULL
        """
        params = [paroquia_id]

        if ano_min is not None:
            sql += " AND ano >= %s"
            params.append(ano_min)

        if ano_max is not None:
            sql += " AND ano <= %s"
            params.append(ano_max)

        sql += " GROUP BY ano ORDER BY ano ASC"

        cur.execute(sql, params)
        rows = cur.fetchall() or []

        out = []
        for r in rows:
            a = r.get("ano")
            q = r.get("qtd") or 0
            if a is not None:
                out.append({
                    "ano": int(a),
                    "qtd": int(q)
                })

        return out
    finally:
        try:
            cur.close()
            conn.close()
        except Exception:
            pass


def buscar_encontrista_por_nomes_e_ano_paroquia(paroquia_id, ele, ela, ano):
    conn = db_conn()
    cur = conn.cursor(dictionary=True)

    try:
        cur.execute("""
            SELECT id, nome_usual_ele, nome_usual_ela, telefone_ele, telefone_ela, endereco
            FROM encontristas
            WHERE paroquia_id = %s
              AND ano = %s
              AND LOWER(TRIM(nome_usual_ele)) = LOWER(TRIM(%s))
              AND LOWER(TRIM(nome_usual_ela)) = LOWER(TRIM(%s))
            LIMIT 1
        """, (paroquia_id, ano, ele, ela))
        return cur.fetchone()
    finally:
        try:
            cur.close()
            conn.close()
        except Exception:
            pass


# Compatibilidade temporária com nomes antigos, caso algum ponto legado importe estas funções.
def listar_encontristas(nome_ele="", nome_ela="", ano="", pagina=1, por_pagina=50, paroquia_id=None):
    if paroquia_id is None:
        raise ValueError("paroquia_id é obrigatório para listar encontristas.")
    return listar_encontristas_paroquia(paroquia_id, nome_ele, nome_ela, ano, pagina, por_pagina)


def buscar_encontrista_por_id(encontrista_id, paroquia_id=None):
    if paroquia_id is None:
        raise ValueError("paroquia_id é obrigatório para buscar encontrista por id.")
    return buscar_encontrista_por_id_paroquia(encontrista_id, paroquia_id)


def atualizar_encontrista(encontrista_id, payload, paroquia_id=None):
    if paroquia_id is None:
        raise ValueError("paroquia_id é obrigatório para atualizar encontrista.")
    return atualizar_encontrista_paroquia(encontrista_id, paroquia_id, payload)


def contar_encontristas_por_ano(ano_min=None, ano_max=None, paroquia_id=None):
    if paroquia_id is None:
        raise ValueError("paroquia_id é obrigatório para contar encontristas por ano.")
    return contar_encontristas_por_ano_paroquia(paroquia_id, ano_min, ano_max)


def buscar_encontrista_por_nomes_e_ano(ele, ela, ano, paroquia_id=None):
    if paroquia_id is None:
        raise ValueError("paroquia_id é obrigatório para buscar encontrista por nomes e ano.")
    return buscar_encontrista_por_nomes_e_ano_paroquia(paroquia_id, ele, ela, ano)
