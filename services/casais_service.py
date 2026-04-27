import json

from db import db_conn


def _telefones_encontrista(row):
    tel_ele = (row.get("telefone_ele") or "").strip()
    tel_ela = (row.get("telefone_ela") or "").strip()
    return " / ".join([t for t in [tel_ele, tel_ela] if t])


def _json_apelidos(valor):
    if not valor:
        return {"ele": [], "ela": []}

    if isinstance(valor, dict):
        return {
            "ele": valor.get("ele") or [],
            "ela": valor.get("ela") or [],
        }

    try:
        data = json.loads(valor)
        return {
            "ele": data.get("ele") or [],
            "ela": data.get("ela") or [],
        }
    except Exception:
        return {"ele": [], "ela": []}


def _apelidos_texto(valor, lado):
    data = _json_apelidos(valor)
    nomes = data.get(lado) or []
    nomes = [str(n).strip() for n in nomes if str(n).strip()]
    return ", ".join(nomes)


def buscar_casais(paroquia_id, nome_ele="", nome_ela="", limite=50):
    nome_ele = (nome_ele or "").strip()
    nome_ela = (nome_ela or "").strip()

    if not paroquia_id:
        return []

    if not nome_ele and not nome_ela:
        return []

    conn = db_conn()
    cursor = conn.cursor(dictionary=True)

    try:
        where = []
        params = [paroquia_id, paroquia_id]

        if nome_ele:
            like_ele = f"%{nome_ele}%"
            where.append("""
                (
                    i.nome_usual_ele LIKE %s
                    OR i.nome_completo_ele LIKE %s
                    OR JSON_SEARCH(i.apelidos, 'one', %s, NULL, '$.ele[*]') IS NOT NULL
                )
            """)
            params.extend([like_ele, like_ele, like_ele])

        if nome_ela:
            like_ela = f"%{nome_ela}%"
            where.append("""
                (
                    i.nome_usual_ela LIKE %s
                    OR i.nome_completo_ela LIKE %s
                    OR JSON_SEARCH(i.apelidos, 'one', %s, NULL, '$.ela[*]') IS NOT NULL
                )
            """)
            params.extend([like_ela, like_ela, like_ela])

        sql = f"""
            SELECT
                i.id,
                i.ano,
                i.num_ecc,
                i.nome_completo_ele,
                i.nome_completo_ela,
                i.nome_usual_ele,
                i.nome_usual_ela,
                i.apelidos,
                i.telefone_ele,
                i.telefone_ela,
                i.endereco,
                (
                    SELECT COUNT(*)
                    FROM encontreiros e
                    WHERE e.casal_id = i.id
                      AND e.paroquia_id = %s
                ) AS qtd_trabalhos
            FROM encontristas i
            WHERE i.paroquia_id = %s
              AND {' AND '.join(where)}
            ORDER BY
                i.ano DESC,
                i.nome_usual_ele ASC,
                i.nome_usual_ela ASC
            LIMIT %s
        """

        params.append(int(limite))
        cursor.execute(sql, params)

        casais = []

        for r in cursor.fetchall() or []:
            casais.append({
                "id": r["id"],
                "ano": r.get("ano"),
                "num_ecc": r.get("num_ecc") or "",
                "nome_completo_ele": r.get("nome_completo_ele") or "",
                "nome_completo_ela": r.get("nome_completo_ela") or "",
                "nome_usual_ele": r.get("nome_usual_ele") or "",
                "nome_usual_ela": r.get("nome_usual_ela") or "",
                "apelidos_ele": _apelidos_texto(r.get("apelidos"), "ele"),
                "apelidos_ela": _apelidos_texto(r.get("apelidos"), "ela"),
                "telefone_ele": r.get("telefone_ele") or "",
                "telefone_ela": r.get("telefone_ela") or "",
                "telefones": _telefones_encontrista(r),
                "endereco": r.get("endereco") or "",
                "qtd_trabalhos": int(r.get("qtd_trabalhos") or 0),
            })

        return casais

    finally:
        cursor.close()
        conn.close()


def montar_resposta_busca_casal(paroquia_id, nome_ele="", nome_ela="", limite=50):
    casais = buscar_casais(
        paroquia_id=paroquia_id,
        nome_ele=nome_ele,
        nome_ela=nome_ela,
        limite=limite
    )

    if not casais:
        return {
            "ok": False,
            "modo": "vazio",
            "msg": "Nenhum casal encontrado.",
            "casal": None,
            "casais": [],
        }

    if len(casais) == 1:
        return {
            "ok": True,
            "modo": "unico",
            "msg": "",
            "casal": casais[0],
            "casais": casais,
        }

    return {
        "ok": True,
        "modo": "multiplo",
        "msg": "",
        "casal": None,
        "casais": casais,
    }
