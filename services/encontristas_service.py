from db import db_conn


def listar_encontristas(nome_ele="", nome_ela="", ano="", pagina=1, por_pagina=50):
    conn = db_conn()
    cursor = conn.cursor(dictionary=True)

    try:
        query = "SELECT * FROM encontristas WHERE 1=1"
        params = []

        if nome_ele:
            query += " AND nome_usual_ele LIKE %s"
            params.append(f"%{nome_ele}%")

        if nome_ela:
            query += " AND nome_usual_ela LIKE %s"
            params.append(f"%{nome_ela}%")

        if ano:
            query += " AND ano = %s"
            params.append(ano)

        cursor.execute(query, params)
        todos = cursor.fetchall() or []

        inicio = (pagina - 1) * por_pagina
        fim = pagina * por_pagina
        dados = todos[inicio:fim]

        return {
            "dados": dados,
            "total": len(todos),
            "pagina": pagina,
            "por_pagina": por_pagina,
        }
    finally:
        try:
            cursor.close()
            conn.close()
        except Exception:
            pass


def buscar_encontrista_por_id(encontrista_id):
    conn = db_conn()
    cursor = conn.cursor(dictionary=True)

    try:
        cursor.execute("SELECT * FROM encontristas WHERE id = %s", (encontrista_id,))
        return cursor.fetchone()
    finally:
        try:
            cursor.close()
            conn.close()
        except Exception:
            pass


def atualizar_encontrista(encontrista_id, payload):
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
                cor_circulo = %s,
                casal_visitacao = %s,
                ficha_num = %s,
                aceitou = %s,
                observacao = %s,
                observacao_extra = %s
            WHERE id = %s
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
            payload["cor_circulo"],
            payload["casal_visitacao"],
            payload["ficha_num"],
            payload["aceitou"],
            payload["observacao"],
            payload["observacao_extra"],
            encontrista_id
        ))
        conn.commit()
        return True
    finally:
        try:
            cursor.close()
            conn.close()
        except Exception:
            pass
