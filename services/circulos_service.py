from collections import defaultdict

from db import db_conn
from utils import (
    _color_to_rgb_triplet,
    _hex_to_rgb_triplet,
    _parse_id_list,
    _ids_to_str,
    _csv_ids_unique,
    _ids_to_csv,
)


def resolve_encontristas(id_list, paroquia_id):
    if not id_list:
        return []

    conn = db_conn()
    cur = conn.cursor(dictionary=True)
    try:
        placeholders = ",".join(["%s"] * len(id_list))
        params = [paroquia_id] + list(id_list)

        cur.execute(f"""
            SELECT id, nome_usual_ele, nome_usual_ela, telefone_ele, telefone_ela, endereco, ano
            FROM encontristas
            WHERE paroquia_id = %s
              AND id IN ({placeholders})
        """, params)

        rows = cur.fetchall() or []
        by_id = {r["id"]: r for r in rows}

        out = []
        for i in id_list:
            r = by_id.get(i)
            if r:
                out.append({
                    "id": r["id"],
                    "nome_ele": r.get("nome_usual_ele") or "",
                    "nome_ela": r.get("nome_usual_ela") or "",
                    "telefone_ele": r.get("telefone_ele") or "",
                    "telefone_ela": r.get("telefone_ela") or "",
                    "endereco": r.get("endereco") or "",
                    "ano": r.get("ano"),
                })
        return out
    finally:
        try:
            cur.close()
            conn.close()
        except Exception:
            pass


def parse_ids_csv(raw: str):
    if not raw:
        return []
    raw = raw.replace(";", ",")
    out = []
    for p in raw.split(","):
        p = p.strip()
        if p.isdigit():
            out.append(int(p))
    return out




def sincronizar_circulos_por_encontreiros(paroquia_id):
    conn = db_conn()
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO circulos (
                paroquia_id,
                ano,
                nome_circulo,
                cor_circulo,
                integrantes_original,
                integrantes_atual,
                coord_orig_casal_id,
                coord_atual_casal_id
                observacao
                situacao
            )
            SELECT
                e.paroquia_id,
                e.ano,
                '',
                '',
                '',
                '',
                e.casal_id,
                e.casal_id,
                '',
                ''
            FROM encontreiros e
            WHERE e.paroquia_id = %s
              AND LOWER(TRIM(e.equipe)) IN ('equipe de círculos', 'equipe de circulos')
              AND LOWER(TRIM(COALESCE(e.coordenador, ''))) NOT IN ('sim', 's')
              AND e.casal_id IS NOT NULL
              AND NOT EXISTS (
                  SELECT 1
                  FROM circulos c
                  WHERE c.paroquia_id = e.paroquia_id
                    AND c.ano = e.ano
                    AND c.coord_orig_casal_id = e.casal_id
              )
        """, (paroquia_id,))
        conn.commit()
        return {"ok": True, "inseridos": cur.rowcount}
    finally:
        try:
            cur.close()
            conn.close()
        except Exception:
            pass


def listar_circulos(paroquia_id, ano="", q=""):
    conn = db_conn()
    cur = conn.cursor(dictionary=True)
    try:
        where = ["c.paroquia_id = %s"]
        params = [paroquia_id]

        if ano:
            where.append("c.ano = %s")
            params.append(ano)

        if q:
            like = f"%{q}%"
            where.append("""
                (
                    LOWER(c.nome_circulo)    LIKE LOWER(%s) OR
                    LOWER(c.cor_circulo)     LIKE LOWER(%s) OR
                    LOWER(COALESCE(ea.nome_usual_ele, c.coord_atual_ele, '')) LIKE LOWER(%s) OR
                    LOWER(COALESCE(ea.nome_usual_ela, c.coord_atual_ela, '')) LIKE LOWER(%s) OR
                    LOWER(COALESCE(eo.nome_usual_ele, c.coord_orig_ele, '')) LIKE LOWER(%s) OR
                    LOWER(COALESCE(eo.nome_usual_ela, c.coord_orig_ela, '')) LIKE LOWER(%s)
                )
            """)
            params += [like, like, like, like, like, like]

        where_sql = " AND ".join(where)

        cur.execute(f"""
            SELECT
                c.id, c.ano, c.cor_circulo, c.nome_circulo,
                c.coord_orig_casal_id, c.coord_atual_casal_id,
                COALESCE(eo.nome_usual_ele, c.coord_orig_ele) AS coord_orig_ele,
                COALESCE(eo.nome_usual_ela, c.coord_orig_ela) AS coord_orig_ela,
                COALESCE(ea.nome_usual_ele, c.coord_atual_ele) AS coord_atual_ele,
                COALESCE(ea.nome_usual_ela, c.coord_atual_ela) AS coord_atual_ela,
                c.integrantes_original, c.integrantes_atual,
                c.situacao, c.observacao, c.created_at
            FROM circulos c
            LEFT JOIN encontristas eo
                   ON eo.id = c.coord_orig_casal_id
                  AND eo.paroquia_id = c.paroquia_id
            LEFT JOIN encontristas ea
                   ON ea.id = c.coord_atual_casal_id
                  AND ea.paroquia_id = c.paroquia_id
            WHERE {where_sql}
            ORDER BY c.ano DESC, c.nome_circulo, coord_orig_ele
        """, params)

        rows = cur.fetchall() or []

        cur.execute("""
            SELECT DISTINCT ano
            FROM circulos
            WHERE paroquia_id = %s
            ORDER BY ano DESC
        """, (paroquia_id,))
        anos_combo = [a["ano"] for a in (cur.fetchall() or [])]

    finally:
        try:
            cur.close()
            conn.close()
        except Exception:
            pass

    for r in rows:
        r["rgb_triplet"] = _color_to_rgb_triplet(r.get("cor_circulo") or "")

    agrupado = defaultdict(list)
    for r in rows:
        agrupado[r["ano"]].append(r)

    anos_ordenados = sorted(agrupado.keys(), reverse=True)

    por_ano = defaultdict(list)
    for r in rows:
        ano_item = r["ano"]
        nome = (r.get("nome_circulo") or "").strip() or "— Sem nome —"
        trip = r.get("rgb_triplet")

        ca_ele = (r.get("coord_atual_ele") or "").strip()
        ca_ela = (r.get("coord_atual_ela") or "").strip()
        co_ele = (r.get("coord_orig_ele") or "").strip()
        co_ela = (r.get("coord_orig_ela") or "").strip()

        if ca_ele and ca_ela:
            coord = f"{ca_ele} & {ca_ela}"
            hint = ""
        elif co_ele or co_ela:
            coord = f"{co_ele} & {co_ela}"
            hint = " (Original)"
        else:
            coord = "— (sem coordenadores)"
            hint = ""

        por_ano[ano_item].append({
            "id": r["id"],
            "nome": nome,
            "rgb": trip,
            "coord": coord,
            "coord_hint": hint
        })

    anos = sorted(por_ano.keys(), reverse=True)

    return {
        "anos_combo": anos_combo,
        "filtros": {"ano": ano, "q": q},
        "anos_ordenados": anos_ordenados,
        "agrupado": agrupado,
        "anos": anos,
        "por_ano": por_ano
    }


def buscar_circulo_por_id(cid, paroquia_id):
    def _hex_to_rgb(h):
        h = (h or "").strip().lstrip("#")
        if len(h) == 3:
            h = "".join([c * 2 for c in h])
        if len(h) != 6:
            return None
        try:
            return tuple(int(h[i:i + 2], 16) for i in (0, 2, 4))
        except ValueError:
            return None

    def _name_to_hex_pt(c):
        if not c:
            return None
        c = c.strip().lower()
        mapa = {
            "azul": "#2563eb",
            "vermelho": "#ef4444",
            "verde": "#22c55e",
            "amarelo": "#eab308",
            "laranja": "#f59e0b",
            "roxo": "#8b5cf6",
            "rosa": "#ec4899",
            "marrom": "#92400e",
            "cinza": "#6b7280",
            "preto": "#111827",
            "branco": "#ffffff",
            "blue": "#2563eb",
            "red": "#ef4444",
            "green": "#22c55e",
            "yellow": "#eab308",
            "orange": "#f59e0b",
            "purple": "#8b5cf6",
            "pink": "#ec4899",
            "brown": "#92400e",
            "gray": "#6b7280",
            "grey": "#6b7280",
            "black": "#111827",
            "white": "#ffffff"
        }
        return mapa.get(c)

    def _to_triplet(c):
        if not c:
            return None
        c = c.strip()
        hx = _name_to_hex_pt(c) or (c if c.startswith("#") else None)
        rgb = _hex_to_rgb(hx) if hx else None
        if not rgb:
            return None
        return f"{rgb[0]},{rgb[1]},{rgb[2]}"

    conn = db_conn()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute("""
            SELECT
                c.*,
                COALESCE(eo.nome_usual_ele, c.coord_orig_ele) AS coord_orig_ele_resolvido,
                COALESCE(eo.nome_usual_ela, c.coord_orig_ela) AS coord_orig_ela_resolvido,
                COALESCE(ea.nome_usual_ele, c.coord_atual_ele) AS coord_atual_ele_resolvido,
                COALESCE(ea.nome_usual_ela, c.coord_atual_ela) AS coord_atual_ela_resolvido
            FROM circulos c
            LEFT JOIN encontristas eo
                   ON eo.id = c.coord_orig_casal_id
                  AND eo.paroquia_id = c.paroquia_id
            LEFT JOIN encontristas ea
                   ON ea.id = c.coord_atual_casal_id
                  AND ea.paroquia_id = c.paroquia_id
            WHERE c.id = %s
              AND c.paroquia_id = %s
        """, (cid, paroquia_id))
        r = cur.fetchone()
    finally:
        try:
            cur.close()
            conn.close()
        except Exception:
            pass

    if not r:
        return None

    r["coord_orig_ele"] = r.get("coord_orig_ele_resolvido") or ""
    r["coord_orig_ela"] = r.get("coord_orig_ela_resolvido") or ""
    r["coord_atual_ele"] = r.get("coord_atual_ele_resolvido") or ""
    r["coord_atual_ela"] = r.get("coord_atual_ela_resolvido") or ""

    rgb_triplet = _to_triplet(r.get("cor_circulo"))

    raw_atual = (r.get("integrantes_atual") or "").replace(";", ",")
    raw_orig = (r.get("integrantes_original") or "").replace(";", ",")

    integrantes_atual_list = [x.strip() for x in raw_atual.split(",") if x and x.strip()]
    integrantes_orig_list = [x.strip() for x in raw_orig.split(",") if x and x.strip()]
    integrantes_list = integrantes_atual_list if integrantes_atual_list else integrantes_orig_list

    return {
        "r": r,
        "rgb_triplet": rgb_triplet,
        "integrantes_list": integrantes_list,
        "integrantes_atual_list": integrantes_atual_list,
        "integrantes_orig_list": integrantes_orig_list,
    }


def buscar_encontrista_para_circulo(paroquia_id, ano_circulo, ele, ela):
    conn = db_conn()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute("""
            SELECT id, nome_usual_ele, nome_usual_ela, telefone_ele, telefone_ela, endereco, ano
            FROM encontristas
            WHERE paroquia_id = %s
              AND LOWER(TRIM(nome_usual_ele)) = LOWER(TRIM(%s))
              AND LOWER(TRIM(nome_usual_ela)) = LOWER(TRIM(%s))
            ORDER BY ano DESC
            LIMIT 1
        """, (paroquia_id, ele, ela))
        r = cur.fetchone()

        if not r:
            cur.execute("""
                SELECT id, nome_usual_ele, nome_usual_ela, telefone_ele, telefone_ela, endereco, ano
                FROM encontristas
                WHERE paroquia_id = %s
                  AND nome_usual_ele LIKE %s
                  AND nome_usual_ela LIKE %s
                ORDER BY ano DESC
                LIMIT 3
            """, (paroquia_id, f"{ele}%", f"{ela}%"))
            sugest = cur.fetchall() or []

            if not sugest:
                return {"ok": False, "status_code": 404, "msg": "Casal não encontrado."}

            multi = []
            for s in sugest:
                multi.append({
                    "id": s["id"],
                    "nome_ele": s["nome_usual_ele"],
                    "nome_ela": s["nome_usual_ela"],
                    "telefone_ele": s.get("telefone_ele") or "",
                    "telefone_ela": s.get("telefone_ela") or "",
                    "endereco": s.get("endereco") or "",
                    "ano": s.get("ano"),
                    "match_ano": int(s.get("ano") or 0) == int(ano_circulo),
                })
            return {"ok": True, "multiplo": True, "opcoes": multi}

        match_ano = int(r.get("ano") or 0) == int(ano_circulo)

        return {
            "ok": True,
            "multiplo": False,
            "id": r["id"],
            "nome_ele": r["nome_usual_ele"],
            "nome_ela": r["nome_usual_ela"],
            "telefone_ele": r.get("telefone_ele") or "",
            "telefone_ela": r.get("telefone_ela") or "",
            "endereco": r.get("endereco") or "",
            "ano": r.get("ano"),
            "match_ano": match_ano
        }
    finally:
        try:
            cur.close()
            conn.close()
        except Exception:
            pass


def buscar_integrantes_circulo(cid, paroquia_id):
    conn = db_conn()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute("""
            SELECT integrantes_atual, integrantes_original
            FROM circulos
            WHERE id = %s
              AND paroquia_id = %s
        """, (cid, paroquia_id))
        r = cur.fetchone()

        if not r:
            return {"ok": False, "status_code": 404, "msg": "Círculo não encontrado."}

        ids_atual = _csv_ids_unique(r.get("integrantes_atual") or "")
        ids_orig = _csv_ids_unique(r.get("integrantes_original") or "")

        atual = resolve_encontristas(ids_atual, paroquia_id)
        orig = resolve_encontristas(ids_orig, paroquia_id)

        return {"ok": True, "atual": atual, "original": orig}
    finally:
        try:
            cur.close()
            conn.close()
        except Exception:
            pass


def append_integrante_circulo(cid, eid, paroquia_id):
    conn = db_conn()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute("""
            SELECT ano, integrantes_atual
            FROM circulos
            WHERE id = %s
              AND paroquia_id = %s
        """, (cid, paroquia_id))
        r = cur.fetchone()

        if not r:
            return {"ok": False, "status_code": 404, "msg": "Círculo não encontrado."}

        cur.execute("""
            SELECT id, ano
            FROM encontristas
            WHERE id = %s
              AND paroquia_id = %s
        """, (eid, paroquia_id))
        enc = cur.fetchone()

        if not enc:
            return {"ok": False, "status_code": 404, "msg": "Casal não encontrado nesta paróquia."}

        if int(enc.get("ano") or 0) != int(r.get("ano") or 0):
            return {"ok": False, "status_code": 409, "msg": "Casal não pertence ao mesmo ano do círculo."}

        ids = _csv_ids_unique(r.get("integrantes_atual") or "")
        eid = int(eid)

        if eid not in ids:
            ids.append(eid)

        novo_csv = _ids_to_csv(ids)

        cur.execute("""
            UPDATE circulos
            SET integrantes_atual = %s
            WHERE id = %s
              AND paroquia_id = %s
        """, (novo_csv, cid, paroquia_id))
        conn.commit()

        atual = resolve_encontristas(_csv_ids_unique(novo_csv), paroquia_id)
        return {"ok": True, "atual": atual}
    finally:
        try:
            cur.close()
            conn.close()
        except Exception:
            pass


def concluir_integrantes_circulo(cid, paroquia_id):
    conn = db_conn()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute("""
            SELECT integrantes_atual
            FROM circulos
            WHERE id = %s
              AND paroquia_id = %s
        """, (cid, paroquia_id))
        r = cur.fetchone()

        if not r:
            return {"ok": False, "status_code": 404, "msg": "Círculo não encontrado."}

        atual = (r.get("integrantes_atual") or "").strip()

        cur.execute("""
            UPDATE circulos
            SET integrantes_original = %s
            WHERE id = %s
              AND paroquia_id = %s
        """, (atual, cid, paroquia_id))
        conn.commit()

        return {"ok": True}
    finally:
        try:
            cur.close()
            conn.close()
        except Exception:
            pass


def atualizar_campo_circulo(cid, paroquia_id, field, value):
    allowed = {
        "cor_circulo",
        "nome_circulo",
        "coord_atual_ele",
        "coord_atual_ela",
        "integrantes_atual",
        "situacao",
        "observacao"
    }

    if field not in allowed:
        return {"ok": False, "status_code": 400, "msg": "Campo não permitido para edição."}

    if field == "integrantes_atual":
        ids = _csv_ids_unique(str(value or ""))
        value = _ids_to_csv(ids)

    conn = db_conn()
    cur = conn.cursor()
    try:
        sql = f"""
            UPDATE circulos
            SET {field} = %s
            WHERE id = %s
              AND paroquia_id = %s
        """
        cur.execute(sql, (value, cid, paroquia_id))
        conn.commit()

        if cur.rowcount == 0:
            return {"ok": False, "status_code": 404, "msg": "Círculo não encontrado."}

        return {"ok": True}
    finally:
        try:
            cur.close()
            conn.close()
        except Exception:
            pass


def listar_candidatos_circulo(paroquia_id, ano):
    conn = db_conn()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute("""
            SELECT integrantes_atual, integrantes_original
            FROM circulos
            WHERE ano = %s
              AND paroquia_id = %s
        """, (ano, paroquia_id))

        usados = set()
        rows = cur.fetchall() or []

        for r in rows:
            for col in ("integrantes_atual", "integrantes_original"):
                raw = (r.get(col) or "").replace(";", ",")
                for part in raw.split(","):
                    p = part.strip()
                    if p.isdigit():
                        usados.add(int(p))

        cur.execute("""
            SELECT id, nome_usual_ele, nome_usual_ela, telefone_ele, telefone_ela, endereco, ano
            FROM encontristas
            WHERE ano = %s
              AND paroquia_id = %s
            ORDER BY nome_usual_ele, nome_usual_ela
        """, (ano, paroquia_id))

        candidatos = []
        for r in cur.fetchall() or []:
            if int(r["id"]) in usados:
                continue

            candidatos.append({
                "id": r["id"],
                "nome_ele": r.get("nome_usual_ele") or "",
                "nome_ela": r.get("nome_usual_ela") or "",
                "telefone_ele": r.get("telefone_ele") or "",
                "telefone_ela": r.get("telefone_ela") or "",
                "endereco": r.get("endereco") or ""
            })

        return {"ok": True, "candidatos": candidatos}
    finally:
        try:
            cur.close()
            conn.close()
        except Exception:
            pass


def pesquisar_circulos(paroquia_id):
    conn = db_conn()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute("""
            SELECT
                c.id, c.ano, c.cor_circulo, c.nome_circulo,
                COALESCE(eo.nome_usual_ele, c.coord_orig_ele) AS coord_orig_ele,
                COALESCE(eo.nome_usual_ela, c.coord_orig_ela) AS coord_orig_ela,
                COALESCE(ea.nome_usual_ele, c.coord_atual_ele) AS coord_atual_ele,
                COALESCE(ea.nome_usual_ela, c.coord_atual_ela) AS coord_atual_ela,
                c.integrantes_atual, c.integrantes_original
            FROM circulos c
            LEFT JOIN encontristas eo
                   ON eo.id = c.coord_orig_casal_id
                  AND eo.paroquia_id = c.paroquia_id
            LEFT JOIN encontristas ea
                   ON ea.id = c.coord_atual_casal_id
                  AND ea.paroquia_id = c.paroquia_id
            WHERE c.paroquia_id = %s
            ORDER BY c.ano DESC, c.id ASC
        """, (paroquia_id,))
        rows = cur.fetchall() or []

        all_ids = set()
        for r in rows:
            all_ids.update(parse_ids_csv(r.get("integrantes_atual") or ""))
            all_ids.update(parse_ids_csv(r.get("integrantes_original") or ""))

        id2nome = {}
        if all_ids:
            placeholders = ",".join(["%s"] * len(all_ids))
            params = [paroquia_id] + list(all_ids)

            cur.execute(f"""
                SELECT id, nome_usual_ele, nome_usual_ela
                FROM encontristas
                WHERE paroquia_id = %s
                  AND id IN ({placeholders})
            """, params)

            for e in cur.fetchall() or []:
                id2nome[int(e["id"])] = (
                    f"{(e.get('nome_usual_ele') or '').strip()} "
                    f"& {(e.get('nome_usual_ela') or '').strip()}"
                )

        por_ano = defaultdict(list)

        for r in rows:
            ids_atual = parse_ids_csv(r.get("integrantes_atual") or "")
            nomes_atual = [id2nome.get(i, f"ID {i}") for i in ids_atual]

            ca_ele = (r.get("coord_atual_ele") or "").strip()
            ca_ela = (r.get("coord_atual_ela") or "").strip()

            if ca_ele and ca_ela:
                coord = f"{ca_ele} & {ca_ela}"
                coord_hint = ""
            else:
                co_ele = (r.get("coord_orig_ele") or "").strip()
                co_ela = (r.get("coord_orig_ela") or "").strip()
                coord = f"{co_ele} & {co_ela}" if (co_ele or co_ela) else "—"
                coord_hint = " (Original)"

            triplet = _hex_to_rgb_triplet(r.get("cor_circulo") or "")

            por_ano[r["ano"]].append({
                "id": r["id"],
                "nome": (r.get("nome_circulo") or "").strip() or "— Sem nome —",
                "rgb": triplet,
                "cor_text": r.get("cor_circulo") or "",
                "coord": coord,
                "coord_hint": coord_hint,
                "integrantes": nomes_atual
            })

        for ano in por_ano:
            por_ano[ano].sort(key=lambda x: x["nome"].lower())

        anos = sorted(por_ano.keys(), reverse=True)

        return {"anos": anos, "por_ano": por_ano}
    finally:
        try:
            cur.close()
            conn.close()
        except Exception:
            pass


def listar_circulos_transferencia(paroquia_id):
    conn = db_conn()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute("""
            SELECT id, ano, nome_circulo, cor_circulo
            FROM circulos
            WHERE paroquia_id = %s
            ORDER BY ano DESC, id ASC
        """, (paroquia_id,))
        rows = cur.fetchall() or []
    finally:
        try:
            cur.close()
            conn.close()
        except Exception:
            pass

    por_ano = defaultdict(list)
    anos = []

    for r in rows:
        ano = r["ano"]
        if ano not in anos:
            anos.append(ano)

        por_ano[ano].append({
            "id": r["id"],
            "nome": r.get("nome_circulo") or "— Sem nome —",
            "rgb": _color_to_rgb_triplet(r.get("cor_circulo") or "")
        })

    return {"anos": anos, "por_ano": por_ano}


def transferir_casal_circulo(from_id, to_id, pid, paroquia_id, _encontrista_name_by_id):
    conn = db_conn()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute("""
            SELECT id, ano, integrantes_atual, coord_atual_casal_id
            FROM circulos
            WHERE id = %s
              AND paroquia_id = %s
        """, (from_id, paroquia_id))
        src = cur.fetchone()

        cur.execute("""
            SELECT id, ano, integrantes_atual
            FROM circulos
            WHERE id = %s
              AND paroquia_id = %s
        """, (to_id, paroquia_id))
        dst = cur.fetchone()

        if not src or not dst:
            return {"ok": False, "status_code": 404, "msg": "Círculo origem/destino não encontrado."}

        if int(src.get("ano") or 0) != int(dst.get("ano") or 0):
            return {"ok": False, "status_code": 409, "msg": "Os círculos precisam ser do mesmo ano."}

        cur.execute("""
            SELECT id
            FROM encontristas
            WHERE id = %s
              AND paroquia_id = %s
        """, (pid, paroquia_id))
        if not cur.fetchone():
            return {"ok": False, "status_code": 404, "msg": "Casal não encontrado nesta paróquia."}

        src_ids = _parse_id_list(src.get("integrantes_atual"))

        if pid not in src_ids:
            return {"ok": False, "status_code": 404, "msg": "Casal não está no círculo de origem."}

        dst_ids = _parse_id_list(dst.get("integrantes_atual"))

        if pid in dst_ids:
            return {"ok": False, "status_code": 409, "msg": "Casal já está no círculo de destino."}

        src_ids = [i for i in src_ids if i != pid]
        dst_ids.append(int(pid))

        cleared_coord = int(src.get("coord_atual_casal_id") or 0) == int(pid)

        cur.execute(
            """
            UPDATE circulos
            SET integrantes_atual = %s
                {clear}
            WHERE id = %s
              AND paroquia_id = %s
            """.format(
                clear=", coord_atual_casal_id = NULL" if cleared_coord else ""
            ),
            (_ids_to_str(src_ids), from_id, paroquia_id)
        )

        cur.execute("""
            UPDATE circulos
            SET integrantes_atual = %s
            WHERE id = %s
              AND paroquia_id = %s
        """, (_ids_to_str(dst_ids), to_id, paroquia_id))

        conn.commit()

        return {"ok": True, "cleared_coord": cleared_coord}
    finally:
        try:
            cur.close()
            conn.close()
        except Exception:
            pass


def add_integrante_circulo(cid, pid, paroquia_id):
    conn = db_conn()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute("""
            SELECT ano, integrantes_atual
            FROM circulos
            WHERE id = %s
              AND paroquia_id = %s
        """, (cid, paroquia_id))
        circ = cur.fetchone()

        if not circ:
            return {"ok": False, "status_code": 404, "msg": "Círculo não encontrado."}

        cur.execute("""
            SELECT ano
            FROM encontristas
            WHERE id = %s
              AND paroquia_id = %s
        """, (pid, paroquia_id))
        r = cur.fetchone()

        if not r:
            return {"ok": False, "status_code": 404, "msg": "Casal não encontrado nesta paróquia."}

        if int(r["ano"] or 0) != int(circ["ano"] or 0):
            return {"ok": False, "status_code": 409, "msg": "Casal não pertence ao mesmo ano do círculo."}

        cur.execute("""
            SELECT id, integrantes_atual
            FROM circulos
            WHERE ano = %s
              AND paroquia_id = %s
        """, (circ["ano"], paroquia_id))

        for row in cur.fetchall() or []:
            ids = _parse_id_list(row.get("integrantes_atual"))
            if pid in ids:
                return {"ok": False, "status_code": 409, "msg": "Casal já está em outro círculo deste ano."}

        ids = _parse_id_list(circ.get("integrantes_atual"))

        if pid in ids:
            return {"ok": True}

        ids.append(int(pid))

        cur.execute("""
            UPDATE circulos
            SET integrantes_atual = %s
            WHERE id = %s
              AND paroquia_id = %s
        """, (_ids_to_str(ids), cid, paroquia_id))
        conn.commit()

        return {"ok": True}
    finally:
        try:
            cur.close()
            conn.close()
        except Exception:
            pass


def remove_integrante_circulo(cid, pid, paroquia_id, _encontrista_name_by_id):
    conn = db_conn()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute("""
            SELECT integrantes_atual, coord_atual_casal_id
            FROM circulos
            WHERE id = %s
              AND paroquia_id = %s
        """, (cid, paroquia_id))
        circ = cur.fetchone()

        if not circ:
            return {"ok": False, "status_code": 404, "msg": "Círculo não encontrado."}

        ids = _parse_id_list(circ.get("integrantes_atual"))

        if pid not in ids:
            return {"ok": True, "cleared_coord": False}

        ids = [i for i in ids if i != pid]

        cleared_coord = int(circ.get("coord_atual_casal_id") or 0) == int(pid)

        if cleared_coord:
            cur.execute("""
                UPDATE circulos
                SET integrantes_atual = %s,
                    coord_atual_casal_id = NULL
                WHERE id = %s
                  AND paroquia_id = %s
            """, (_ids_to_str(ids), cid, paroquia_id))
        else:
            cur.execute("""
                UPDATE circulos
                SET integrantes_atual = %s
                WHERE id = %s
                  AND paroquia_id = %s
            """, (_ids_to_str(ids), cid, paroquia_id))

        conn.commit()

        return {"ok": True, "cleared_coord": cleared_coord}
    finally:
        try:
            cur.close()
            conn.close()
        except Exception:
            pass


def copiar_atual_para_original(cid, paroquia_id):
    conn = db_conn()
    cur = conn.cursor()
    try:
        cur.execute("""
            UPDATE circulos
            SET integrantes_original = COALESCE(integrantes_atual, '')
            WHERE id = %s
              AND paroquia_id = %s
        """, (cid, paroquia_id))
        conn.commit()

        if cur.rowcount == 0:
            return {"ok": False, "status_code": 404, "msg": "Círculo não encontrado."}

        return {"ok": True}
    finally:
        try:
            cur.close()
            conn.close()
        except Exception:
            pass


def definir_coord_circulo(cid, pid, paroquia_id, _encontrista_name_by_id):
    conn = db_conn()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute("""
            SELECT integrantes_atual
            FROM circulos
            WHERE id = %s
              AND paroquia_id = %s
        """, (cid, paroquia_id))
        r = cur.fetchone()

        if not r:
            return {"ok": False, "status_code": 404, "msg": "Círculo não encontrado."}

        if not pid:
            cur.execute("""
                UPDATE circulos
                SET coord_atual_casal_id = NULL
                WHERE id = %s
                  AND paroquia_id = %s
            """, (cid, paroquia_id))
            conn.commit()
            return {"ok": True}

        cur.execute("""
            SELECT id
            FROM encontristas
            WHERE id = %s
              AND paroquia_id = %s
        """, (pid, paroquia_id))

        if not cur.fetchone():
            return {"ok": False, "status_code": 404, "msg": "Casal não encontrado nesta paróquia."}

        ids = _parse_id_list(r.get("integrantes_atual"))

        if pid not in ids:
            return {"ok": False, "status_code": 409, "msg": "Casal não está na lista de integrantes atuais."}

        cur.execute("""
            UPDATE circulos
            SET coord_atual_casal_id = %s
            WHERE id = %s
              AND paroquia_id = %s
        """, (pid, cid, paroquia_id))
        conn.commit()

        return {"ok": True}
    finally:
        try:
            cur.close()
            conn.close()
        except Exception:
            pass
