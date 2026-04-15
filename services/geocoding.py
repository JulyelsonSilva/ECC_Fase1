import hashlib
import re

import requests

from config import (
    DEFAULT_CITY,
    DEFAULT_COUNTRY,
    DEFAULT_STATE,
    NOMINATIM_EMAIL,
)


# =========================
# Helpers para MAPAS
# =========================
def normalize_address(raw: str) -> str:
    if not raw:
        return ""
    s = raw.strip()
    s = re.sub(r"\s+", " ", s)
    s = s.replace(" ,", ",").replace(", ,", ",")
    s = s.replace(" Apt®", " Apt").replace("Apt®", "Apt")
    s = s.replace(" nº", " n.º").replace(" No ", " n.º ")
    s = s.replace("Av:", "Av.").replace("Av :", "Av.").replace("Av ", "Av. ")
    s = s.replace("Rua:", "Rua").replace("R:", "Rua ")
    s = s.replace("Jatiuca", "Jatiúca")

    lower = s.lower()
    needs_city = DEFAULT_CITY.lower() not in lower
    needs_state = (f"- {DEFAULT_STATE.lower()}" not in lower) and (f", {DEFAULT_STATE.lower()}" not in lower)
    needs_country = DEFAULT_COUNTRY.lower() not in lower

    tail = []
    if needs_city:
        tail.append(DEFAULT_CITY)
    if needs_state:
        tail.append(DEFAULT_STATE)
    if needs_country:
        tail.append(DEFAULT_COUNTRY)

    if tail:
        if not s.endswith(","):
            if not s.endswith(" "):
                s += " "
            s += ", "
        s += " - ".join(tail) if len(tail) == 2 else ", ".join(tail)

    s = re.sub(r"\s*,\s*,", ", ", s)
    s = re.sub(r"\s*-\s*-", " - ", s)
    s = re.sub(r"\s*,\s*-\s*", ", ", s)
    return s.strip(" ," )


def addr_hash(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()


def nominatim_geocode(query: str):
    url = "https://nominatim.openstreetmap.org/search"
    params = {
        "q": query,
        "format": "jsonv2",
        "addressdetails": 0,
        "limit": 1,
        "countrycodes": "br",
    }
    headers = {"User-Agent": f"ECCDivino/1.0 ({NOMINATIM_EMAIL})"}
    try:
        r = requests.get(url, params=params, headers=headers, timeout=10)
        r.raise_for_status()
        data = r.json()
        if not data:
            return None, None, None, "not_found"
        item = data[0]
        return float(item["lat"]), float(item["lon"]), item.get("display_name"), "ok"
    except requests.RequestException:
        return None, None, None, "error"


def save_cache(conn, h, query, lat, lng, display, status):
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO geocoding_cache (endereco_hash, query, formatted_address, lat, lng, status, provider, updated_at)
        VALUES (%s,%s,%s,%s,%s,%s,'nominatim',NOW())
        ON DUPLICATE KEY UPDATE
          query=VALUES(query), formatted_address=VALUES(formatted_address),
          lat=VALUES(lat), lng=VALUES(lng), status=VALUES(status), updated_at=NOW()
        """,
        (h, query, display, lat, lng, status),
    )
    conn.commit()
    cur.close()


def get_cache(conn, h):
    cur = conn.cursor(dictionary=True)
    cur.execute("SELECT * FROM geocoding_cache WHERE endereco_hash=%s", (h,))
    row = cur.fetchone()
    cur.close()
    return row


# =========================
# Helpers adicionais p/ normalização & geocodificação "esperta"
# =========================
BAIRRO_FIX = {
    "jatiuca": "Jatiúca",
    "jatiúca": "Jatiúca",
    "ponta verde": "Ponta Verde",
    "pajuçara": "Pajuçara",
    "pajucara": "Pajuçara",
    "poço": "Poço",
    "poco": "Poço",
    "pitanguinha": "Pitanguinha",
}

LOGR_FIX = {
    r"^(av|av\.|avenida)\b": "Av.",
    r"^(rua|r\.?)\b": "Rua",
    r"^(trav|travessa)\b": "Travessa",
    r"^(al|alameda)\b": "Alameda",
}

CEP_RE = re.compile(r"\b\d{5}-?\d{3}\b")
NUM_RE = re.compile(r"\b(\d{1,5})(?:\s*[^\w\s]\s*[\w\-\/]+)?\b")


def _apply_map_start(s: str, fmap: dict):
    for pat, rep in fmap.items():
        s = re.sub(pat, rep, s, flags=re.I)
    return s


def split_address_components(raw: str, default_city=DEFAULT_CITY, default_state=DEFAULT_STATE):
    if not raw:
        return {
            "logradouro": "",
            "numero": "",
            "bairro": "",
            "cep": "",
            "cidade": default_city,
            "uf": default_state,
        }

    s = re.sub(r"\s+", " ", raw.strip())

    cep = CEP_RE.search(s)
    cep = cep.group(0) if cep else ""
    if cep:
        s = s.replace(cep, "").strip(",; ")

    partes = [p.strip() for p in re.split(r"[,\-•–;|]", s) if p.strip()]
    logradouro = partes[0] if partes else ""
    bairro = ""

    if len(partes) >= 2:
        poss = partes[-1]
        low = poss.lower()
        if "maceió" not in low and "al" != low and "brasil" not in low:
            bairro = poss

    logradouro = _apply_map_start(logradouro, LOGR_FIX).strip()

    numero = ""
    m = NUM_RE.search(logradouro)
    if m:
        numero = m.group(1)
        logradouro = re.sub(r"\b" + re.escape(numero) + r"\b", "", logradouro).replace(" ,", ",").strip(" ,")

    if bairro:
        b_low = bairro.lower()
        if b_low in BAIRRO_FIX:
            bairro = BAIRRO_FIX[b_low]

    return {
        "logradouro": logradouro.strip(),
        "numero": numero.strip(),
        "bairro": bairro.strip(),
        "cep": cep.replace("-", ""),
        "cidade": default_city,
        "uf": default_state,
    }


def viacep_busca_por_rua(cidade: str, uf: str, logradouro: str):
    if not (cidade and uf and logradouro):
        return []
    try:
        url = f"https://viacep.com.br/ws/{uf}/{cidade}/{logradouro}/json/"
        r = requests.get(url, timeout=8)
        r.raise_for_status()
        data = r.json()
        if isinstance(data, dict) and data.get("erro"):
            return []
        if isinstance(data, list):
            return data
        return []
    except requests.RequestException:
        return []


def geocode_br_smart(raw: str):
    norm = normalize_address(raw or "")
    if not norm:
        return None, None, None, "not_found"

    lat, lng, display, status = nominatim_geocode(norm)
    if status == "ok":
        return lat, lng, display, "ok"

    c = split_address_components(norm)

    comp_full = ", ".join(
        [
            p
            for p in [
                f"{c['logradouro']} {c['numero']}".strip(),
                c["bairro"] or None,
                f"{c['cidade']} - {c['uf']}",
                "Brasil",
            ]
            if p
        ]
    )
    lat, lng, display, status = nominatim_geocode(comp_full)
    if status == "ok":
        return lat, lng, display, "ok"

    comp_sem_num = ", ".join(
        [
            p
            for p in [
                c["logradouro"],
                c["bairro"] or None,
                f"{c['cidade']} - {c['uf']}",
                "Brasil",
            ]
            if p
        ]
    )
    lat, lng, display, status = nominatim_geocode(comp_sem_num)
    if status == "ok":
        return lat, lng, display, "ok"

    viacep_hits = viacep_busca_por_rua(c["cidade"], c["uf"], c["logradouro"])
    if viacep_hits:
        hit = viacep_hits[0]
        cep = (hit.get("cep") or "").replace("-", "")
        if cep:
            lat, lng, display, status = nominatim_geocode(f"{cep}, Brasil")
            if status == "ok":
                return lat, lng, display, "ok"

        alt_q = ", ".join(
            [
                p
                for p in [
                    hit.get("logradouro") or c["logradouro"],
                    (hit.get("bairro") or c["bairro"]) or None,
                    f"{hit.get('localidade') or c['cidade']} - {hit.get('uf') or c['uf']}",
                    "Brasil",
                ]
                if p
            ]
        )
        lat, lng, display, status = nominatim_geocode(alt_q)
        if status == "ok":
            return lat, lng, display, "ok"

    if c["bairro"]:
        q_bairro = f"{c['bairro']}, {c['cidade']} - {c['uf']}, Brasil"
        lat, lng, display, status = nominatim_geocode(q_bairro)
        if status == "ok":
            return lat, lng, display, f"{display} (centro do bairro)", "partial"

    lat, lng, display, status = nominatim_geocode(f"{c['cidade']} - {c['uf']}, Brasil")
    if status == "ok":
        return lat, lng, display, f"{display} (centro da cidade)", "partial"

    return None, None, None, "not_found"
