from db import db_conn


def listar_anos_palestras():
    conn = db_conn()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute("""
            SELECT ano,
                   SUM(CASE WHEN UPPER(TRIM(status))='CONCLUIDO' THEN 1 ELSE 0 END) AS qtd_concluido,
                   COUNT(*) AS total,
                   SUM(CASE WHEN UPPER(TRIM(status))='ABERTO' THEN 1 ELSE 0 END) AS qtd_aberto
              FROM palestras
             GROUP BY ano
             ORDER BY ano DESC
        """)
        rows = cur.fetchall() or []

        anos_concluidos, anos_aberto = [], []
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


def carregar_palestras_do_ano(ano):
    conn = db_conn()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute("""
            SELECT id, palestra, nome_ele, nome_ela, status
              FROM palestras
             WHERE ano = %s
             ORDER BY id DESC
        """, (ano,))
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


def obter_dados_casal_palestra(nome_ele, nome_ela, solo=False):
    conn = db_conn()
    cur = conn.cursor(dictionary=True)
    try:
        if solo:
            return {
                "telefones": "",
                "endereco": "",
                "eligible": True,
            }

        eligible = False

        cur.execute("""
            SELECT 1 FROM encontristas
             WHERE nome_usual_ele = %s AND nome_usual_ela = %s
             LIMIT 1
        """, (nome_ele, nome_ela))
        if cur.fetchone():
            eligible = True
        else:
            cur.execute("""
                SELECT 1 FROM encontreiros
                 WHERE nome_ele = %s AND nome_ela = %s
                 LIMIT 1
            """, (nome_ele, nome_ela))
            eligible = cur.fetchone() is not None

        telefones = ""
        endereco = ""

        cur.execute("""
            SELECT telefones, endereco
              FROM encontreiros
             WHERE nome_ele = %s AND nome_ela = %s
             ORDER BY ano DESC
             LIMIT 1
        """, (nome_ele, nome_ela))
        r = cur.fetchone()
        if r:
            telefones = (r.get("telefones") or "").strip()
            endereco = r.get("endereco") or ""
        else:
            cur.execute("""
                SELECT telefone_ele, telefone_ela, endereco
                  FROM encontristas
                 WHERE nome_usual_ele = %s AND nome_usual_ela = %s
                 ORDER BY ano DESC
                 LIMIT 1
            """, (nome_ele, nome_ela))
            r2 = cur.fetchone()
            if r2:
                tel_ele = (r2.get("telefone_ele") or "").strip()
                tel_ela = (r2.get("telefone_ela") or "").strip()
                telefones = " / ".join([t for t in [tel_ele, tel_ela] if t])
                endereco = r2.get("endereco") or ""

        return {
            "telefones": telefones,
            "endereco": endereco,
            "eligible": eligible,
        }
    finally:
        try:
            cur.close()
            conn.close()
        except Exception:
            pass


def contar_repeticoes_palestra(palestra, nome_ele, nome_ela="", solo=False):
    conn = db_conn()
    cur = conn.cursor(dictionary=True)
    try:
        if solo:
            cur.execute("""
                SELECT COUNT(*) AS n
                  FROM palestras
                 WHERE palestra = %s AND nome_ele = %s
            """, (palestra, nome_ele))
        else:
            cur.execute("""
                SELECT COUNT(*) AS n
                  FROM palestras
                 WHERE palestra = %s AND nome_ele = %s AND nome_ela = %s
            """, (palestra, nome_ele, nome_ela))

        n = int(((cur.fetchone() or {}).get("n", 0) or 0))
        return n
    finally:
        try:
            cur.close()
            conn.close()
        except Exception:
            pass


def salvar_palestra_ano(ano, palestra, nome_ele, nome_ela="", solo=False):
    conn = db_conn()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute("""
            SELECT id
              FROM palestras
             WHERE ano = %s AND palestra = %s
             ORDER BY id DESC
             LIMIT 1
        """, (int(ano), palestra))
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
                    """, (nome_ele, existing["id"]))
                else:
                    cur2.execute("""
                        UPDATE palestras
                           SET nome_ele = %s,
                               nome_ela = %s,
                               status = 'Aberto'
                         WHERE id = %s
                    """, (nome_ele, nome_ela, existing["id"]))
                action = "update"
            else:
                if solo:
                    cur2.execute("""
                        INSERT INTO palestras (ano, palestra, nome_ele, nome_ela, status)
                        VALUES (%s, %s, %s, '', 'Aberto')
                    """, (int(ano), palestra, nome_ele))
                else:
                    cur2.execute("""
                        INSERT INTO palestras (ano, palestra, nome_ele, nome_ela, status)
                        VALUES (%s, %s, %s, %s, 'Aberto')
                    """, (int(ano), palestra, nome_ele, nome_ela))
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


def adicionar_palestra(ano, palestra, nome_ele, nome_ela="", solo=False):
    conn = db_conn()
    cur = conn.cursor()
    try:
        if solo:
            cur.execute("""
                INSERT INTO palestras (ano, palestra, nome_ele, status, observacao)
                VALUES (%s, %s, %s, 'Aberto', NULL)
            """, (int(ano), palestra, nome_ele))
        else:
            cur.execute("""
                INSERT INTO palestras (ano, palestra, nome_ele, nome_ela, status, observacao)
                VALUES (%s, %s, %s, %s, 'Aberto', NULL)
            """, (int(ano), palestra, nome_ele, nome_ela))

        conn.commit()
        return True
    finally:
        try:
            cur.close()
            conn.close()
        except Exception:
            pass


def encerrar_palestras_ano(ano):
    conn = db_conn()
    cur = conn.cursor()
    try:
        cur.execute("""
            UPDATE palestras
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


def marcar_status_palestra_por_id(_id, novo_status, observacao):
    conn = db_conn()
    cur = conn.cursor()
    try:
        cur.execute("""
            UPDATE palestras
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


def marcar_status_palestra_por_criterios(ano, palestra, novo_status, observacao, nome_ele="", nome_ela=""):
    conn = db_conn()
    cur = conn.cursor()
    try:
        clauses = [
            "UPDATE palestras SET status=%s, observacao=%s",
            "WHERE ano=%s AND palestra=%s",
            "AND (status IS NULL OR UPPER(status) IN ('ABERTO','ACEITO'))"
        ]
        params = [novo_status, observacao, int(ano), palestra]

        if nome_ele:
            clauses.append("AND nome_ele=%s")
            params.append(nome_ele)

        if nome_ela:
            clauses.append("AND nome_ela=%s")
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
