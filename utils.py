import re
from difflib import SequenceMatcher

from config import TEAM_MAP


# =========================
# Helpers genéricos de texto
# =========================
def _norm(s: str) -> str:
    if s is None:
        return ""
    s = s.strip().lower()
    s = re.sub(r"[^\w\s]", " ", s, flags=re.UNICODE)
    s = re.sub(r"\s+", " ", s)
    return s


def _sim(a: str, b: str) -> float:
    return SequenceMatcher(None, _norm(a), _norm(b)).ratio()


# =========================
# Helpers de consulta / relatórios
# =========================
def _q(cur, sql, params=None):
    cur.execute(sql, params or [])
    return cur.fetchall()


def _yes_coord_vals():
    return (
        'sim',
        's',
        'coordenador',
        'coordenadora',
        'sim coordenador',
        'sim - coordenador',
    )


# =========================
# Helpers de equipes
# =========================
def _team_label(value: str) -> str:
    """Normaliza chave/filtro curto para o rótulo salvo no banco."""
    v = (value or '').strip()
    if not v:
        return v

    vl = v.lower()
    for key, info in TEAM_MAP.items():
        if (
            vl == key.lower()
            or vl == (info.get('filtro') or '').lower()
            or vl == (info.get('rotulo') or '').lower()
        ):
            return info['rotulo']
    return v


# =========================
# Helpers de cores
# =========================
_COLOR_MAP = {
    "azul": "#2563eb",
    "vermelho": "#ef4444",
    "amarelo": "#f59e0b",
    "verde": "#10b981",
    "roxo": "#8b5cf6",
    "violeta": "#7c3aed",
    "laranja": "#fb923c",
    "rosa": "#ec4899",
    "turquesa": "#14b8a6",
    "ciano": "#06b6d4",
    "magenta": "#d946ef",
    "marrom": "#92400e",
    "preto": "#111827",
    "branco": "#ffffff",
    "cinza": "#9ca3af",
    "dourado": "#d4af37",
    "prata": "#c0c0c0",
}

_HEX_RE = re.compile(r"^#?([0-9a-fA-F]{3}|[0-9a-fA-F]{6})$")


def _hex_to_rgb_triplet(hexstr: str):
    s = (hexstr or '').strip().lstrip("#")
    if len(s) == 3:
        s = "".join(c * 2 for c in s)
    try:
        r = int(s[0:2], 16)
        g = int(s[2:4], 16)
        b = int(s[4:6], 16)
        return f"{r}, {g}, {b}"
    except Exception:
        return None


def _color_to_rgb_triplet(color: str):
    """Aceita nome PT-BR (inclui substrings), ou #hex 3/6 dígitos."""
    if not color:
        return None
    s = color.strip().lower()
    if _HEX_RE.match(s):
        return _hex_to_rgb_triplet(s)
    if s in _COLOR_MAP:
        return _hex_to_rgb_triplet(_COLOR_MAP[s])
    for k, v in _COLOR_MAP.items():
        if k in s:
            return _hex_to_rgb_triplet(v)
    return None


def _hex_to_rgb(h):
    h = (h or '').strip().lstrip('#')
    if len(h) == 3:
        h = ''.join([c * 2 for c in h])
    if len(h) != 6:
        return None
    try:
        return tuple(int(h[i:i+2], 16) for i in (0, 2, 4))
    except ValueError:
        return None


def _name_to_hex_pt(c):
    if not c:
        return None
    c = c.strip().lower()
    mapa = {
        'azul': '#2563eb',
        'vermelho': '#ef4444',
        'verde': '#22c55e',
        'amarelo': '#eab308',
        'laranja': '#f59e0b',
        'roxo': '#8b5cf6',
        'rosa': '#ec4899',
        'marrom': '#92400e',
        'cinza': '#6b7280',
        'preto': '#111827',
        'branco': '#ffffff',
        'blue': '#2563eb',
        'red': '#ef4444',
        'green': '#22c55e',
        'yellow': '#eab308',
        'orange': '#f59e0b',
        'purple': '#8b5cf6',
        'pink': '#ec4899',
        'brown': '#92400e',
        'gray': '#6b7280',
        'grey': '#6b7280',
        'black': '#111827',
        'white': '#ffffff',
    }
    return mapa.get(c)


def _to_triplet(c):
    if not c:
        return None
    c = c.strip()
    hx = _name_to_hex_pt(c) or (c if c.startswith('#') else None)
    rgb = _hex_to_rgb(hx) if hx else None
    if not rgb:
        return None
    return f"{rgb[0]},{rgb[1]},{rgb[2]}"


# =========================
# Helpers para listas de IDs
# =========================
def _parse_id_list(raw):
    """Converte '1,2 3;4' -> [1,2,3,4] (únicos, preservando ordem)."""
    if not raw:
        return []
    parts = re.split(r"[,\s;|]+", str(raw))
    out, seen = [], set()
    for p in parts:
        if p.isdigit():
            i = int(p)
            if i not in seen:
                seen.add(i)
                out.append(i)
    return out


def _ids_to_str(ids):
    return ",".join(str(int(x)) for x in ids)


def _csv_ids_unique(raw: str):
    raw = (raw or "").replace(";", ",")
    out = []
    seen = set()
    for part in raw.split(","):
        p = part.strip()
        if not p:
            continue
        if not p.isdigit():
            continue
        val = int(p)
        if val not in seen:
            seen.add(val)
            out.append(val)
    return out


def _ids_to_csv(ids):
    ids = [str(int(x)) for x in ids if str(x).isdigit()]
    return ",".join(ids)


def _parse_ids_csv(raw: str):
    if not raw:
        return []
    raw = raw.replace(";", ",")
    out = []
    for p in raw.split(","):
        p = p.strip()
        if p.isdigit():
            out.append(int(p))
    return out
