def encontrista_name_by_id(conn, _id, paroquia_id=None):
    cur = conn.cursor(dictionary=True)
    try:
        if paroquia_id:
            cur.execute("""
                SELECT nome_usual_ele, nome_usual_ela
                FROM encontristas
                WHERE id = %s
                  AND paroquia_id = %s
            """, (_id, paroquia_id))
        else:
            cur.execute("""
                SELECT nome_usual_ele, nome_usual_ela
                FROM encontristas
                WHERE id = %s
            """, (_id,))

        r = cur.fetchone()
        if not r:
            return None, None

        return (
            r.get("nome_usual_ele") or "",
            r.get("nome_usual_ela") or ""
        )
    finally:
        cur.close()