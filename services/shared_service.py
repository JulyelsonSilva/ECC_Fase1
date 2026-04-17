def encontrista_name_by_id(conn, _id):
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute("SELECT nome_usual_ele, nome_usual_ela FROM encontristas WHERE id=%s", (_id,))
        r = cur.fetchone()
        if not r:
            return None, None
        return (r.get("nome_usual_ele") or ""), (r.get("nome_usual_ela") or "")
    finally:
        cur.close()
