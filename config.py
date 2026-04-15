import os

# =========================
# Configurações gerais
# =========================
SECRET_KEY = os.getenv("SECRET_KEY", "dev-secret-change-me")
ADMIN_TOKEN = os.getenv("ADMIN_TOKEN", "")

# =========================
# Configurações para MAPAS
# =========================
DEFAULT_CITY = os.getenv("DEFAULT_CITY", "Maceió")
DEFAULT_STATE = os.getenv("DEFAULT_STATE", "AL")
DEFAULT_COUNTRY = os.getenv("DEFAULT_COUNTRY", "Brasil")
NOMINATIM_EMAIL = os.getenv("NOMINATIM_EMAIL", "julyelson@gmail.com")

# =========================
# Config do Banco
# =========================
DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME"),
    "port": int(os.getenv("DB_PORT") or "3306"),
}

# =========================
# Constantes de Equipes
# =========================
TEAM_MAP = {
    "sala": {
        "rotulo": "Equipe de Sala - Coordenador/Apresentador",
        "filtro": "Sala",
    },
    "circulos": {
        "rotulo": "Equipe de Círculos",
        "filtro": "Circulos",
    },
    "cafe": {
        "rotulo": "Equipe Café e Minimercado",
        "filtro": "Café e Minimercado",
    },
    "compras": {
        "rotulo": "Equipe Compras",
        "filtro": "Compras",
    },
    "acolhida": {
        "rotulo": "Equipe Acolhida",
        "filtro": "Acolhida",
    },
    "ordem": {
        "rotulo": "Equipe Ordem e Limpeza",
        "filtro": "Ordem e Limpeza",
    },
    "liturgia": {
        "rotulo": "Equipe Liturgia e Vigilia",
        "filtro": "Liturgia e Vigilia",
    },
    "secretaria": {
        "rotulo": "Equipe Secretaria",
        "filtro": "Secretaria",
    },
    "cozinha": {
        "rotulo": "Equipe Cozinha",
        "filtro": "Cozinha",
    },
    "visitacao": {
        "rotulo": "Equipe Visitação",
        "filtro": "Visitação",
    },
}

TEAM_LIMITS = {
    "Sala": {"min": 4, "max": 6},
    "Circulos": {"min": 5, "max": 5},
    "Café e Minimercado": {"min": 3, "max": 7},
    "Compras": {"min": 0, "max": 1},
    "Acolhida": {"min": 4, "max": 6},
    "Ordem e Limpeza": {"min": 3, "max": 7},
    "Liturgia e Vigilia": {"min": 2, "max": 6},
    "Secretaria": {"min": 3, "max": 5},
    "Cozinha": {"min": 7, "max": 9},
    "Visitação": {"min": 6, "max": 10},
}

TEAM_CHOICES = [info["rotulo"] for info in TEAM_MAP.values()]

# =========================
# Constantes de Palestras
# =========================
PALESTRAS_TITULOS = [
    "Plano de Deus",
    "Testem.Plano de Deus",
    "Harmonia Conjugal",
    "Diálogo c/ filhos",
    "Penitência",
    "Testem. Jovem",
    "Ceia Eucarística",
    "N.SrªVida da Família",
    "Testem. Ceia Eucarística",
    "Fé Revezes da Vida",
    "Sentido da Vida",
    "Oração",
    "Corresponsabilidade",
    "Vivência do Sacramento do Matrimônio",
    "O casal Cristão no Mundo de Hoje",
]

PALESTRAS_SOLO = {
    "Penitência",
    "Testem. Jovem",
    "Ceia Eucarística",
}
