from collections import defaultdict
import re
import mysql.connector
from mysql.connector import errors as mysql_errors

from db import db_conn


def _telefones_encontrista(row):
    tel_ele = (row.get("telefone_ele") or "").strip()
    tel_ela = (row.get("telefone_ela") or "").strip()
    return " / ".join([t for t in [tel_ele, tel_ela] if t])


def listar_encontreiros(nome_ele="", nome_ela="", ano_filtro=""):
    conn = db_conn()
    cursor = conn.cursor(dictionary=True)

    try:
        query = """
            SELECT
                e.id,
                e.ano,
                e.equipe,
                e.casal_id,
                i.nome_usual_ele AS nome_ele,
                i.nome_usual_ela AS nome_ela,
                CONCAT_WS(' / ',
                    NULLIF(TRIM(i.telefone_ele), ''),
                    NULLIF(TRIM(i.telefone_ela), '')
                ) AS telefones,
                i.endereco AS endereco,
                e.coordenador,
                e.status,
                e.observacao
            FROM encontreiros e
            JOIN encontristas i ON i.id = e.casal_id
            WHERE (e.status IS NULL OR UPPER(TRIM(e.status)) IN ('ABERTO','CONCLUIDO'))
        """
        params = []

        if nome_ele:
            query += " AND i.nome_usual_ele LIKE %s"
            params.append(f"%{nome_ele}%")

        if nome_ela:
            query += " AND i.nome_usual_ela LIKE %s"
            params.append(f"%{nome_ela}%")

        if ano_filtro:
            query += " AND e.ano = %s"
            params.append(ano_filtro)

        query += " ORDER BY e.ano DESC, e.equipe ASC, e.id ASC"

        cursor.execute(query, params)
        todos = cursor.fetchall() or []

        por_ano = defaultdict(list)
        for row in todos:
            por_ano[row["ano"]].append(row)

        return {
            "por_ano": por_ano,
            "colunas_visiveis": [
                "equipe",
                "nome_ele",
                "nome_ela",
                "telefones",
                "endereco",
                "coordenador"
            ]
        }

    finally:
        cursor.close()
        conn.close()


def montar_visao_equipes(equipe):
    tabela = {}
    colunas = []

    if not equipe:
        return {"tabela": tabela, "colunas": colunas}

    conn = db_conn()
    cursor = conn.cursor(dictionary=True)

    try:
        base_select = """
            SELECT
                e.id,
                e.ano,
                e.equipe,
                e.casal_id,
                i.nome_usual_ele AS nome_ele,
                i.nome_usual_ela AS nome_ela,
                e.coordenador,
                e.status,
                e.observacao
            FROM encontreiros e
            JOIN encontristas i ON i.id = e.casal_id
            WHERE 1=1
        """

        if equipe == "Dirigentes":
            colunas = ["Montagem", "Fichas", "Palestra", "Finanças", "Pós Encontro"]
            dados = {col: {} for col in colunas}

            for pasta in colunas:
                cursor.execute(
                    base_select + """
                    AND e.equipe LIKE '%DIRIGENTE%'
                    AND UPPER(e.equipe) LIKE %s
                    ORDER BY e.ano ASC, e.id ASC
                    """,
                    (f"%{pasta.upper()}%",)
                )

                for row in cursor.fetchall() or []:
                    ano = row["ano"]
                    nome_base = f"{row['nome_ele']} e {row['nome_ela']}"

                    nome = (
                        f"*{nome_base}"
                        if (row.get("coordenador") or "").strip().lower() == "sim"
                        else nome_base
                    )

                    dados[pasta].setdefault(ano, nome)

            anos = sorted({ano for pasta_data in dados.values() for ano in pasta_data})

            for ano in anos:
                tabela[ano] = [dados[col].get(ano, "") for col in colunas]

        else:
            cursor.execute(
                base_select + """
                AND e.equipe LIKE %s
                ORDER BY e.ano ASC, e.equipe ASC, e.id ASC
                """,
                (f"%{equipe}%",)
            )

            all_rows = cursor.fetchall() or []

            coordenadores_globais = set(
                f"{row['nome_ele']} e {row['nome_ela']}"
                for row in all_rows
                if (row.get("coordenador") or "").strip().lower() == "sim"
            )

            if equipe == "Sala":
                colunas = [
                    "Coordenador",
                    "Boa Vontade",
                    "Canto 1",
                    "Canto 2",
                    "Som e Projeção 1",
                    "Som e Projeção 2",
                    "Recepção de Palestras"
                ]

                dados_ano = defaultdict(lambda: {col: "" for col in colunas})

                for row in all_rows:
                    ano = row["ano"]
                    equipe_txt = (row["equipe"] or "").upper()
                    nome_chave = f"{row['nome_ele']} e {row['nome_ela']}"
                    coordenador = (row.get("coordenador") or "").strip().lower() == "sim"

                    if coordenador and all(
                        x not in equipe_txt
                        for x in ["BOA VONTADE", "CANTO", "SOM E PROJEÇÃO", "RECEPÇÃO"]
                    ):
                        dados_ano[ano]["Coordenador"] = f"*{nome_chave}"

                    elif "BOA VONTADE" in equipe_txt:
                        dados_ano[ano]["Boa Vontade"] = nome_chave

                    elif "CANTO" in equipe_txt:
                        if not dados_ano[ano]["Canto 1"]:
                            dados_ano[ano]["Canto 1"] = nome_chave
                        elif not dados_ano[ano]["Canto 2"]:
                            dados_ano[ano]["Canto 2"] = nome_chave

                    elif "SOM E PROJEÇÃO" in equipe_txt:
                        if not dados_ano[ano]["Som e Projeção 1"]:
                            dados_ano[ano]["Som e Projeção 1"] = nome_chave
                        elif not dados_ano[ano]["Som e Projeção 2"]:
                            dados_ano[ano]["Som e Projeção 2"] = nome_chave

                    elif "RECEPÇÃO" in equipe_txt:
                        dados_ano[ano]["Recepção de Palestras"] = nome_chave

                for ano, linha_dict in dados_ano.items():
                    linha = []

                    for col in colunas:
                        nome = linha_dict[col]

                        if nome.startswith("*"):
                            linha.append(nome)
                        elif nome in coordenadores_globais:
                            linha.append(f"~{nome}")
                        else:
                            linha.append(nome)

                    tabela[ano] = linha

            else:
                colunas = ["Coordenador"] + [f"Integrante {i}" for i in range(1, 10)]
                por_ano = defaultdict(list)

                for row in all_rows:
                    ano = row["ano"]
                    nome_chave = f"{row['nome_ele']} e {row['nome_ela']}"

                    if (row.get("coordenador") or "").strip().lower() == "sim":
                        por_ano[ano].insert(0, f"*{nome_chave}")
                    else:
                        por_ano[ano].append(nome_chave)

                for ano, nomes in por_ano.items():
                    linha = []

                    for nome in nomes:
                        if nome.startswith("*"):
                            linha.append(nome)
                        elif nome in coordenadores_globais:
                            linha.append(f"~{nome}")
                        else:
                            linha.append(nome)

                    while len(linha) < len(colunas):
                        linha.append("")

                    tabela[ano] = linha

        return {"tabela": tabela, "colunas": colunas}

    finally:
        cursor.close()
        conn.close()


def buscar_visao_casal(nome_ele, nome_ela, PALESTRAS_TITULOS, PALESTRAS_SOLO):
    dados_encontrista = {}
    dados_encontreiros = []
    dados_palestras = []
    erro = None

    if not nome_ele or not nome_ela:
        return {
            "dados_encontrista": None,
            "dados_encontreiros": [],
            "dados_palestras": [],
            "anos": [],
            "por_ano_trabalhos": defaultdict(list),
            "por_ano_palestras": defaultdict(list),
            "erro": "Informe ambos os nomes para realizar a busca.",
        }

    conn = db_conn()
    cursor = conn.cursor(dictionary=True)

    try:
        cursor.execute("""
            SELECT id, ano, endereco, telefone_ele, telefone_ela
            FROM encontristas
            WHERE nome_usual_ele = %s
              AND nome_usual_ela = %s
            ORDER BY ano DESC, id DESC
            LIMIT 1
        """, (nome_ele, nome_ela))

        encontrista = cursor.fetchone()
        casal_id = None

        if encontrista:
            casal_id = encontrista["id"]
            dados_encontrista = {
                "ano_encontro": encontrista["ano"],
                "endereco": encontrista.get("endereco") or "",
                "telefones": _telefones_encontrista(encontrista)
            }

        if casal_id:
            cursor.execute("""
                SELECT
                    e.ano,
                    e.equipe,
                    e.coordenador,
                    i.endereco AS endereco,
                    CONCAT_WS(' / ',
                        NULLIF(TRIM(i.telefone_ele), ''),
                        NULLIF(TRIM(i.telefone_ela), '')
                    ) AS telefones,
                    e.status,
                    e.observacao
                FROM encontreiros e
                JOIN encontristas i ON i.id = e.casal_id
                WHERE e.casal_id = %s
                ORDER BY e.ano DESC, e.equipe ASC, e.id ASC
            """, (casal_id,))

            dados_encontreiros = cursor.fetchall() or []

        format_titles = tuple(t for t in PALESTRAS_TITULOS if t not in PALESTRAS_SOLO)

        if format_titles:
            in_clause = ", ".join(["%s"] * len(format_titles))

            sql = f"""
                SELECT ano, palestra
                FROM palestras
                WHERE LOWER(nome_ele) = LOWER(%s)
                  AND LOWER(COALESCE(nome_ela, '')) = LOWER(%s)
                  AND palestra IN ({in_clause})
                ORDER BY ano DESC
            """

            params = [nome_ele, nome_ela] + list(format_titles)
            cursor.execute(sql, params)
            dados_palestras = cursor.fetchall() or []

        if not encontrista and not dados_encontreiros and not dados_palestras:
            erro = "Casal não encontrado."

    finally:
        cursor.close()
        conn.close()

    por_ano_trabalhos = defaultdict(list)

    for row in dados_encontreiros:
        por_ano_trabalhos[row["ano"]].append({
            "equipe": row.get("equipe") or "",
            "coordenador": row.get("coordenador") or "",
            "status": row.get("status") or "",
            "observacao": row.get("observacao") or ""
        })

    por_ano_palestras = defaultdict(list)

    for row in dados_palestras:
        por_ano_palestras[row["ano"]].append({
            "palestra": row.get("palestra") or ""
        })

    anos = sorted(
        set(por_ano_trabalhos.keys()) | set(por_ano_palestras.keys()),
        reverse=True
    )

    return {
        "dados_encontrista": dados_encontrista if dados_encontrista else None,
        "dados_encontreiros": dados_encontreiros,
        "dados_palestras": dados_palestras,
        "anos": anos,
        "por_ano_trabalhos": por_ano_trabalhos,
        "por_ano_palestras": por_ano_palestras,
        "erro": erro,
    }


def buscar_relatorio_casais(entrada, titulo, DB_CONFIG, safe_fetch_one):
    def split_casal(line):
        raw = (line or "").strip()

        if not raw:
            return None, None

        if ";" in raw:
            a, b = raw.split(";", 1)
            return a.strip(), b.strip()

        if re.search(r"\s+e\s+", raw, flags=re.I):
            a, b = re.split(r"\s+e\s+", raw, maxsplit=1, flags=re.I)
            return a.strip(), b.strip()

        if " " in raw:
            a, b = raw.split(" ", 1)
            return a.strip(), b.strip()

        return None, None

    resultados_ok = []
    resultados_fail = []

    linhas = [linha.strip() for linha in (entrada or "").splitlines() if linha.strip()]

    if not linhas:
        return {
            "resultados": [],
            "titulo": titulo,
            "entrada": entrada
        }

    try:
        conn = mysql.connector.connect(
            host=DB_CONFIG["host"],
            user=DB_CONFIG["user"],
            password=DB_CONFIG["password"],
            database=DB_CONFIG["database"],
            connection_timeout=5
        )
        cur = conn.cursor(dictionary=True)

    except mysql_errors.Error:
        for linha in linhas:
            resultados_fail.append({
                "nome": linha,
                "endereco": "Erro de conexão com o banco",
                "telefones": "— / —"
            })

        return {
            "resultados": resultados_fail,
            "titulo": titulo,
            "entrada": entrada
        }

    try:
        def consulta_prefix_like(a, b):
            a = (a or "").strip()
            b = (b or "").strip()

            if not a or not b:
                return None

            a_pref = f"{a}%"
            b_pref = f"{b}%"

            base = safe_fetch_one(
                cur,
                """
                SELECT
                    i.endereco,
                    i.telefone_ele,
                    i.telefone_ela
                FROM encontristas i
                WHERE (i.nome_usual_ele LIKE %s AND i.nome_usual_ela LIKE %s)
                   OR (i.nome_usual_ele LIKE %s AND i.nome_usual_ela LIKE %s)
                ORDER BY i.ano DESC, i.id DESC
                LIMIT 1
                """,
                (a_pref, b_pref, b_pref, a_pref)
            )

            if base is None:
                base = safe_fetch_one(
                    cur,
                    """
                    SELECT
                        i.endereco,
                        i.telefone_ele,
                        i.telefone_ela
                    FROM encontreiros e
                    JOIN encontristas i ON i.id = e.casal_id
                    WHERE (i.nome_usual_ele LIKE %s AND i.nome_usual_ela LIKE %s)
                       OR (i.nome_usual_ele LIKE %s AND i.nome_usual_ela LIKE %s)
                    ORDER BY e.ano DESC, e.id DESC
                    LIMIT 1
                    """,
                    (a_pref, b_pref, b_pref, a_pref)
                )

            if base:
                telefones = f"{base.get('telefone_ele') or '—'} / {base.get('telefone_ela') or '—'}"

                return {
                    "endereco": base.get("endereco") or "—",
                    "telefones": telefones
                }

            return None

        for linha in linhas:
            ele, ela = split_casal(linha)

            if not ele or not ela:
                resultados_fail.append({
                    "nome": linha,
                    "endereco": "Formato não reconhecido",
                    "telefones": "— / —"
                })
                continue

            dados = consulta_prefix_like(ele, ela)

            if dados:
                resultados_ok.append({
                    "nome": f"{ele} e {ela}",
                    **dados
                })
            else:
                resultados_fail.append({
                    "nome": f"{ele} e {ela}",
                    "endereco": "Não encontrado",
                    "telefones": "— / —"
                })

    finally:
        cur.close()
        conn.close()

    return {
        "resultados": resultados_ok + resultados_fail,
        "titulo": titulo,
        "entrada": entrada
    }
