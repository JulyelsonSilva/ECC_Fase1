from db import get_connection


# =========================
# VALIDAR REQUISITOS DA MONTAGEM
# =========================
def validar_requisitos_montagem_ano(ano, TEAM_MAP, TEAM_LIMITS):
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)

    pendencias = []

    # 1. Verificar dirigentes
    cursor.execute("""
        SELECT COUNT(*) as total
        FROM encontreiros
        WHERE ano = %s
        AND equipe LIKE 'Equipe Dirigente%%'
        AND status = 'Aberto'
    """, (ano,))
    dirigentes = cursor.fetchone()["total"]

    if dirigentes < 5:
        pendencias.append(f"Dirigentes incompletos ({dirigentes}/5)")

    # 2. Coordenador Geral
    cursor.execute("""
        SELECT COUNT(*) as total
        FROM encontreiros
        WHERE ano = %s
        AND equipe = 'Casal Coordenador Geral'
        AND status = 'Aberto'
    """, (ano,))
    cg = cursor.fetchone()["total"]

    if cg < 1:
        pendencias.append("Coordenador Geral não definido")

    # 3. Coordenadores de equipe
    cursor.execute("""
        SELECT equipe, COUNT(*) as total
        FROM encontreiros
        WHERE ano = %s
        AND coordenador = 'Sim'
        AND status = 'Aberto'
        GROUP BY equipe
    """, (ano,))

    coordenadores = {row["equipe"]: row["total"] for row in cursor.fetchall()}

    for equipe in TEAM_MAP.values():
        if equipe not in coordenadores:
            pendencias.append(f"Sem coordenador: {equipe}")

    # 4. Mínimos das equipes
    for key, equipe in TEAM_MAP.items():
        limites = TEAM_LIMITS.get(key)
        if not limites:
            continue

        cursor.execute("""
            SELECT COUNT(*) as total
            FROM encontreiros
            WHERE ano = %s
            AND equipe = %s
            AND status = 'Aberto'
        """, (ano, equipe))

        total = cursor.fetchone()["total"]

        if total < limites["min"]:
            pendencias.append(f"{equipe}: mínimo {limites['min']} (atual {total})")

    cursor.close()
    conn.close()

    return {
        "ok": len(pendencias) == 0,
        "pendencias": pendencias
    }


# =========================
# CONCLUIR MONTAGEM
# =========================
def concluir_montagem_ano(ano, TEAM_MAP, TEAM_LIMITS):
    validacao = validar_requisitos_montagem_ano(ano, TEAM_MAP, TEAM_LIMITS)

    if not validacao["ok"]:
        return validacao

    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        UPDATE encontreiros
        SET status = 'Concluido'
        WHERE ano = %s
        AND status = 'Aberto'
    """, (ano,))

    conn.commit()

    cursor.close()
    conn.close()

    return {
        "ok": True,
        "msg": "Montagem concluída com sucesso"
    }