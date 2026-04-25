from db import db_conn


def _telefones_encontrista(row):
    tel_ele = (row.get("telefone_ele") or "").strip()
    tel_ela = (row.get("telefone_ela") or "").strip()
    return " / ".join([t for t in [tel_ele, tel_ela] if t])


def _buscar_encontrista_por_nomes(cur, paroquia_id, nome_ele, nome_ela):
    nome_ele = (nome_ele or "").strip()
    nome_ela = (nome_ela or "").strip()

    cur.execute("""
        SELECT
            id,
            ano,
            nome_usual_ele,
            nome_usual_ela,
            telefone_ele,
            telefone_ela,
            endereco,
            apelidos
        FROM encontristas
        WHERE paroquia_id = %s
          AND (
                UPPER(TRIM(nome_usual_ele)) = UPPER(TRIM(%s))
                OR JSON_SEARCH(apelidos, 'one', %s, NULL, '$.ele[*]') IS NOT NULL
          )
          AND (
                UPPER(TRIM(nome_usual_ela)) = UPPER(TRIM(%s))
                OR JSON_SEARCH(apelidos, 'one', %s, NULL, '$.ela[*]') IS NOT NULL
          )
        ORDER BY ano DESC, id DESC
        LIMIT 1
    """, (paroquia_id, nome_ele, nome_ele, nome_ela, nome_ela))

    return cur.fetchone()


def listar_anos_palestras(paroquia_id):
    conn = db_conn()
    cur = conn.cursor(dictionary=True)

    try:
        cur.execute("""
            SELECT
                ano,
                SUM(CASE WHEN UPPER(TRIM(status)) = 'CONCLUIDO' THEN 1 ELSE 0 END) AS qtd_concluido,
                COUNT(*) AS total,
                SUM(CASE WHEN UPPER(TRIM(status)) = 'ABERTO' THEN 1 ELSE 0 END) AS qtd_aberto
            FROM palestras
            WHERE paroquia_id = %s
            GROUP BY ano
            ORDER BY ano DESC
        """, (paroquia_id,))

        rows = cur.fetchall() or []

        anos_concluidos = []
        anos_aberto = []

        for r in rows:
            item = {
                "ano": r["ano"],
                "qtd_concluido": int(r["qtd_concluido"] or 0),
                "total": int(r["total"] or 0),
                "qtd_aberto": int(r["qtd_aberto"] or 0),
            }

            if item["total"] > 0 and item["qtd_aberto"] == 0:
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


def carregar_palestras_do_ano(ano, paroquia_id):
    conn = db_conn()
    cur = conn.cursor(dictionary=True)

    try:
        cur.execute("""
            SELECT
                id,
                palestra,
                nome_ele,
                nome_ela,
                status
            FROM palestras
            WHERE ano = %s
              AND paroquia_id = %s
            ORDER BY id DESC
        """, (ano, paroquia_id))

        rows = cur.fetchall() or []

        existentes = {}
        tem_abertos = False

        for r in rows:
            titulo = r.get("palestra") or ""

            if titulo and titulo not in existentes:
                existentes[titulo] = {
                    "id": r.get("id"),
                    "nome_ele": r.get("nome_ele"),
                    "nome_ela": r.get("nome_ela"),
                    "status": r.get("status"),
                }

            st = (r.get("status") or "").strip().lower()
            if st == "aberto":
                tem_abertos = True

        return {
            "existentes": existentes,
            "tem_abertos": tem_abertos,
        }

    finally:
        try:
            cur.close()
            conn.close()
        except Exception:
            pass


def obter_dados_casal_palestra(paroquia_id, nome_ele, nome_ela, solo=False):
    conn = db_conn()
    cur = conn.cursor(dictionary=True)

    try:
        if solo:
            return {
                "telefones": "",
                "endereco": "",
                "eligible": True,
            }

        encontrista = _buscar_encontrista_por_nomes(cur, paroquia_id, nome_ele, nome_ela)

        if not encontrista:
            return {
                "telefones": "",
                "endereco": "",
                "eligible": False,
            }

        return {
            "telefones": _telefones_encontrista(encontrista),
            "endereco": encontrista.get("endereco") or "",
            "eligible": True,
        }

    finally:
        try:
            cur.close()
            conn.close()
        except Exception:
            pass


def contar_repeticoes_palestra(paroquia_id, palestra, nome_ele, nome_ela="", solo=False):
    conn = db_conn()
    cur = conn.cursor(dictionary=True)

    try:
        if solo:
            cur.execute("""
                SELECT COUNT(*) AS n
                FROM palestras
                WHERE paroquia_id = %s
                  AND palestra = %s
                  AND UPPER(TRIM(nome_ele)) = UPPER(TRIM(%s))
            """, (paroquia_id, palestra, nome_ele))
        else:
            cur.execute("""
                SELECT COUNT(*) AS n
                FROM palestras
                WHERE paroquia_id = %s
                  AND palestra = %s
                  AND UPPER(TRIM(nome_ele)) = UPPER(TRIM(%s))
                  AND UPPER(TRIM(nome_ela)) = UPPER(TRIM(%s))
            """, (paroquia_id, palestra, nome_ele, nome_ela))

        n = int(((cur.fetchone() or {}).get("n", 0) or 0))
        return n

    finally:
        try:
            cur.close()
            conn.close()
        except Exception:
            pass


def salvar_palestra_ano(paroquia_id, ano, palestra, nome_ele, nome_ela="", solo=False):
    conn = db_conn()
    cur = conn.cursor(dictionary=True)

    try:
        if not solo:
            encontrista = _buscar_encontrista_por_nomes(cur, paroquia_id, nome_ele, nome_ela)
            if not encontrista:
                return "not_found"

            nome_ele = encontrista.get("nome_usual_ele") or nome_ele
            nome_ela = encontrista.get("nome_usual_ela") or nome_ela

        cur.execute("""
            SELECT id
            FROM palestras
            WHERE ano = %s
              AND palestra = %s
              AND paroquia_id = %s
            ORDER BY id DESC
            LIMIT 1
        """, (int(ano), palestra, paroquia_id))

        existing = cur.fetchone()
        cur2 = conn.cursor()

        try:
            if existing:
                if solo:
                    cur2.execute("""
                        UPDATE palestras
                        SET nome_ele = %s,
                            nome_ela = '',
                            status = 'Aberto'
                        WHERE id = %s
                          AND paroquia_id = %s
                    """, (nome_ele, existing["id"], paroquia_id))
                else:
                    cur2.execute("""
                        UPDATE palestras
                        SET nome_ele = %s,
                            nome_ela = %s,
                            status = 'Aberto'
                        WHERE id = %s
                          AND paroquia_id = %s
                    """, (nome_ele, nome_ela, existing["id"], paroquia_id))

                action = "update"

            else:
                if solo:
                    cur2.execute("""
                        INSERT INTO palestras
                            (paroquia_id, ano, palestra, nome_ele, nome_ela, status)
                        VALUES
                            (%s, %s, %s, %s, '', 'Aberto')
                    """, (paroquia_id, int(ano), palestra, nome_ele))
                else:
                    cur2.execute("""
                        INSERT INTO palestras
                            (paroquia_id, ano, palestra, nome_ele, nome_ela, status)
                        VALUES
                            (%s, %s, %s, %s, %s, 'Aberto')
                    """, (paroquia_id, int(ano), palestra, nome_ele, nome_ela))

                action = "insert"

            conn.commit()
            return action

        finally:
            cur2.close()

    finally:
        try:
            cur.close()
            conn.close()
        except Exception:
            pass


def adicionar_palestra(paroquia_id, ano, palestra, nome_ele, nome_ela="", solo=False):
    conn = db_conn()
    cur = conn.cursor(dictionary=True)

    try:
        if not solo:
            encontrista = _buscar_encontrista_por_nomes(cur, paroquia_id, nome_ele, nome_ela)
            if not encontrista:
                return False

            nome_ele = encontrista.get("nome_usual_ele") or nome_ele
            nome_ela = encontrista.get("nome_usual_ela") or nome_ela

        cur2 = conn.cursor()

        try:
            if solo:
                cur2.execute("""
                    INSERT INTO palestras
                        (paroquia_id, ano, palestra, nome_ele, nome_ela, status, observacao)
                    VALUES
                        (%s, %s, %s, %s, '', 'Aberto', NULL)
                """, (paroquia_id, int(ano), palestra, nome_ele))
            else:
                cur2.execute("""
                    INSERT INTO palestras
                        (paroquia_id, ano, palestra, nome_ele, nome_ela, status, observacao)
                    VALUES
                        (%s, %s, %s, %s, %s, 'Aberto', NULL)
                """, (paroquia_id, int(ano), palestra, nome_ele, nome_ela))

            conn.commit()
            return True

        finally:
            cur2.close()

    finally:
        try:
            cur.close()
            conn.close()
        except Exception:
            pass


def encerrar_palestras_ano(paroquia_id, ano):
    conn = db_conn()
    cur = conn.cursor()

    try:
        cur.execute("""
            UPDATE palestras
            SET status = 'Concluido'
            WHERE ano = %s
              AND paroquia_id = %s
              AND UPPER(status) = 'ABERTO'
        """, (int(ano), paroquia_id))

        conn.commit()
        return cur.rowcount

    finally:
        try:
            cur.close()
            conn.close()
        except Exception:
            pass


def marcar_status_palestra_por_id(paroquia_id, _id, novo_status, observacao):
    conn = db_conn()
    cur = conn.cursor()

    try:
        cur.execute("""
            UPDATE palestras
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


def marcar_status_palestra_por_criterios(
    paroquia_id,
    ano,
    palestra,
    novo_status,
    observacao,
    nome_ele="",
    nome_ela=""
):
    conn = db_conn()
    cur = conn.cursor()

    try:
        clauses = [
            "UPDATE palestras SET status = %s, observacao = %s",
            "WHERE ano = %s AND palestra = %s AND paroquia_id = %s",
            "AND (status IS NULL OR UPPER(status) IN ('ABERTO','ACEITO'))"
        ]

        params = [novo_status, observacao, int(ano), palestra, paroquia_id]

        if nome_ele:
            clauses.append("AND UPPER(TRIM(nome_ele)) = UPPER(TRIM(%s))")
            params.append(nome_ele)

        if nome_ela:
            clauses.append("AND UPPER(TRIM(nome_ela)) = UPPER(TRIM(%s))")
            params.append(nome_ela)

        clauses.append("ORDER BY id DESC LIMIT 1")

        sql = "\n".join(clauses)

        cur.execute(sql, tuple(params))
        conn.commit()
        return cur.rowcount

    finally:
        try:
            cur.close()
            conn.close()
        except Exception:
            pass