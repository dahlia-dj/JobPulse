"""
Fonctions de parsing et de normalisation de la couche Silver (Python pur,
sans dépendance à Spark : testables seules, appelées depuis des UDF Spark
dans transform.py).

Chaque source Bronze a son propre format pour les mêmes informations
(lieu, contrat, salaire). Ce module les ramène à un vocabulaire commun :

  - contrat : CDI, CDD, FREELANCE, INTERIM, STAGE, ALTERNANCE, AUTRE, INCONNU
  - temps de travail : FULL_TIME, PART_TIME, INCONNU
  - salaire : montant min/max + période (YEAR, MONTH, DAY, HOUR), puis un
    équivalent annuel quand il a un sens (pas pour un tarif journalier
    freelance)
  - lieu : ville, code département, région (13 régions + DOM)
"""
from __future__ import annotations

import html
import json
import re
import unicodedata

# --------------------------------------------------------------------------
# Texte
# --------------------------------------------------------------------------
_TAG_RE = re.compile(r"<[^>]+>")
_EMAIL_RE = re.compile(r"[\w.+-]+@[\w-]+(?:\.[\w-]+)+")
# Numéros français : 0X XX XX XX XX, +33 X ..., 0033 X ... (séparateurs : espace, point, tiret)
_PHONE_RE = re.compile(r"(?<!\d)(?:\+33|0033|0)\s?[1-9](?:[\s.\-]?\d{2}){4}(?!\d)")
_SPACES_RE = re.compile(r"[ \t ]+")
_BLANK_LINES_RE = re.compile(r"\n\s*\n+")


def unaccent(text: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFKD", text) if not unicodedata.combining(c))


def clean_description(raw: str | None) -> str:
    """Texte brut exploitable par spaCy : sans balises HTML résiduelles,
    entités décodées, espaces normalisés, et sans données personnelles
    (e-mails et numéros de téléphone de recruteurs, exigence de la
    section 6 du cahier des charges)."""
    if not raw:
        return ""
    text = html.unescape(str(raw))
    text = _TAG_RE.sub(" ", text)
    text = _EMAIL_RE.sub(" ", text)
    text = _PHONE_RE.sub(" ", text)
    text = text.replace("\r", "\n")
    text = _SPACES_RE.sub(" ", text)
    text = re.sub(r" ?\n ?", "\n", text)
    text = _BLANK_LINES_RE.sub("\n", text)
    return text.strip()


_CONTRACT_WORDS = r"(?:cdi|cdd|stage|stagiaire|alternance|apprentissage|freelance|free-lance|interim|intérim|cdi\s*/\s*cdd)"
# Référence d'annonce en tête : « 441 - », « 103886/ », « #5521 : »
_TITLE_REF_PREFIX = re.compile(r"^\s*#?\d{2,}\s*[-–—/:|.]\s*")
# Type de contrat en tête : « Cdi - », « Stage : », « Alternance – »
_TITLE_CONTRACT_PREFIX = re.compile(r"^\s*" + _CONTRACT_WORDS + r"\s*[-–—:|]\s*", re.IGNORECASE)
# Type de contrat en queue : « (CDI) », « [Stage] », « - Freelance »
_TITLE_CONTRACT_SUFFIX = re.compile(
    r"\s*(?:[\(\[]\s*" + _CONTRACT_WORDS + r"\s*[\)\]]|[-–—|]\s*" + _CONTRACT_WORDS + r")\s*$", re.IGNORECASE)
# « - CDI (H/F) » en queue : on retire le contrat mais on garde « (H/F) »
_TITLE_CONTRACT_BEFORE_GENDER = re.compile(
    r"\s*[-–—|]\s*" + _CONTRACT_WORDS + r"\s*(\(?\s*[HhFf]\s*/\s*[HhFf]\s*\)?)\s*(?:[\(\[]\s*" + _CONTRACT_WORDS + r"\s*[\)\]])?\s*$", re.IGNORECASE)
_TITLE_EDGE_LEFT = re.compile(r"^[\s&+\-–—|/:,.;*•·]+")
_TITLE_EDGE_RIGHT = re.compile(r"[\s\-–—|/:,&;*•·]+$")
_GENDER_TOKEN = re.compile(r"\b([FHfh])/([FHfh])\b")


def clean_title(raw: str | None) -> str | None:
    """Intitulé de poste débarrassé de ce qui n'en fait pas partie : numéro de
    référence en tête, type de contrat en tête ou en queue (il est déjà dans
    contract_type), symboles orphelins. « H/F » est conservé. Un titre entièrement
    en minuscules est mis en casse de titre (« data scientist f/h » -> « Data Scientist F/H »)."""
    if not raw or raw.strip().upper() == "N/A":
        return None
    title = _SPACES_RE.sub(" ", html.unescape(raw)).strip()
    for _ in range(3):  # plusieurs couches possibles : « Stage : Stage - ... », « 12 - Cdi - ... »
        before = title
        title = _TITLE_REF_PREFIX.sub("", title)
        title = re.sub(r"^\s*(?:cdi|cdd)\s+(?=\S)", "", title, flags=re.IGNORECASE)  # « CDI Data Engineer »
        m = _TITLE_CONTRACT_PREFIX.match(title)
        if m:
            word = re.match(r"\s*([\w/ -]+?)\s*[-–—:|]", m.group(0)).group(1)
            title = title[m.end():]
            title = re.sub(r"^\s*" + re.escape(word) + r"\b\s*[-–—:|]?\s*", "", title, flags=re.IGNORECASE)  # « Alternance : Alternance … »
        title = _TITLE_CONTRACT_BEFORE_GENDER.sub(lambda m: " " + m.group(1).strip(), title)
        title = _TITLE_CONTRACT_SUFFIX.sub("", title)
        title = _TITLE_EDGE_LEFT.sub("", title)
        title = _TITLE_EDGE_RIGHT.sub("", title)
        if title == before:
            break
    if title and title == title.lower():
        title = _GENDER_TOKEN.sub(lambda m: f"{m.group(1).upper()}/{m.group(2).upper()}", title.title())
    return title or None


def norm_text(raw: str | None) -> str:
    """Clé de comparaison : minuscules, sans accents ni ponctuation."""
    if not raw:
        return ""
    text = unaccent(str(raw)).lower()
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return text.strip()


# « H/F », « F/H », « (M/F/D) »... n'identifient pas le poste
_GENDER_RE = re.compile(r"\b(?:h f|f h|h f x|f h x|m f d|m w d|h f d)\b")
_LEGAL_FORMS = {"sas", "sasu", "sarl", "eurl", "sa", "se", "snc", "sca", "inc", "ltd", "gmbh", "llc"}


def norm_title(raw: str | None) -> str:
    text = norm_text(raw)
    text = _GENDER_RE.sub(" ", text)
    return re.sub(r"\s+", " ", text).strip()


def norm_company(raw: str | None) -> str:
    tokens = [t for t in norm_text(raw).split() if t not in _LEGAL_FORMS]
    return " ".join(tokens)


# --------------------------------------------------------------------------
# Lieu
# --------------------------------------------------------------------------
REGIONS = {
    "Auvergne-Rhône-Alpes": ["01", "03", "07", "15", "26", "38", "42", "43", "63", "69", "73", "74"],
    "Bourgogne-Franche-Comté": ["21", "25", "39", "58", "70", "71", "89", "90"],
    "Bretagne": ["22", "29", "35", "56"],
    "Centre-Val de Loire": ["18", "28", "36", "37", "41", "45"],
    "Corse": ["2A", "2B", "20"],
    "Grand Est": ["08", "10", "51", "52", "54", "55", "57", "67", "68", "88"],
    "Hauts-de-France": ["02", "59", "60", "62", "80"],
    "Île-de-France": ["75", "77", "78", "91", "92", "93", "94", "95"],
    "Normandie": ["14", "27", "50", "61", "76"],
    "Nouvelle-Aquitaine": ["16", "17", "19", "23", "24", "33", "40", "47", "64", "79", "86", "87"],
    "Occitanie": ["09", "11", "12", "30", "31", "32", "34", "46", "48", "65", "66", "81", "82"],
    "Pays de la Loire": ["44", "49", "53", "72", "85"],
    "Provence-Alpes-Côte d'Azur": ["04", "05", "06", "13", "83", "84"],
    "Guadeloupe": ["971"],
    "Martinique": ["972"],
    "Guyane": ["973"],
    "La Réunion": ["974"],
    "Mayotte": ["976"],
}
DEPT_TO_REGION = {dept: region for region, depts in REGIONS.items() for dept in depts}
_REGION_BY_KEY = {norm_text(region): region for region in REGIONS}
_REGION_BY_KEY.update({
    "ile de france": "Île-de-France",
    "provence alpes cote d azur": "Provence-Alpes-Côte d'Azur",
    "paca": "Provence-Alpes-Côte d'Azur",
    "reunion": "La Réunion",
})
_NOT_A_CITY = {"france", "fr", "teletravail", "remote", "n a", ""}
_SMALL_WORDS = {"de", "du", "des", "la", "le", "les", "sur", "sous", "en", "et", "aux", "au", "d", "l"}
_ARRONDISSEMENT_RE = re.compile(r"[\s-]+\d{1,2}\s*(?:er|e|ème|eme)?(?:[\s-]+arrondissement)?$", re.I)
_ARRONDISSEMENT_ONLY_RE = re.compile(r"^\d{1,2}\s*(?:er|e|ème|eme)?[\s-]*arrondissement$", re.I)


def canonical_region(raw: str | None) -> str | None:
    return _REGION_BY_KEY.get(norm_text(raw)) if raw else None


def _title_case_city(city: str) -> str:
    """« PARIS » -> « Paris », « LA CIOTAT » -> « La Ciotat », « SAINT-DENIS » -> « Saint-Denis »."""
    if not (city.isupper() or city.islower()):
        return city  # déjà casé par la source
    out, first = [], True
    for chunk in re.split(r"(\s+|-|')", city.lower()):
        if chunk in (" ", "-", "'") or not chunk.strip():
            out.append(chunk)
            continue
        out.append(chunk if (chunk in _SMALL_WORDS and not first) else chunk.capitalize())
        first = False
    return "".join(out)


def _clean_city(city: str | None) -> str | None:
    if not city:
        return None
    city = _ARRONDISSEMENT_RE.sub("", city.strip())
    if norm_text(city) in _NOT_A_CITY or canonical_region(city):
        return None
    return _title_case_city(city)


def dept_from_postal(postal: str | None, commune: str | None = None) -> str | None:
    for code in (postal, commune):
        if code and re.fullmatch(r"\d{5}", code.strip()):
            code = code.strip()
            if code.startswith("97"):
                return code[:3]
            if code.startswith("20"):
                return "2A" if int(code[:3]) < 202 else "2B"
            return code[:2]
    return None


def parse_location_text(raw: str | None) -> dict:
    """HelloWork / Free-Work : « Paris, Île-de-France » ou « Lyon, Auvergne-Rhône-Alpes, FR »."""
    result = {"city": None, "department_code": None, "region": None}
    if not raw or raw.strip().upper() == "N/A":
        return result
    parts = [p.strip() for p in raw.split(",") if p.strip()]
    for part in parts:
        region = canonical_region(part)
        if region and not result["region"]:
            result["region"] = region
    for part in parts:
        city = _clean_city(part)
        if city:
            result["city"] = city
            break
    return result


def parse_location_ft(libelle: str | None, postal: str | None, commune: str | None) -> dict:
    """France Travail : « 75 - PARIS 09 », « 69 - Rhône », « France », avec code postal / INSEE."""
    result = {"city": None, "department_code": None, "region": None}
    libelle = (libelle or "").strip()
    match = re.match(r"^(\d{2,3}|2A|2B)\s*-\s*(.+)$", libelle)
    if match:
        result["department_code"] = match.group(1)
        result["city"] = _clean_city(match.group(2))
    else:
        result["region"] = canonical_region(libelle)
        result["city"] = _clean_city(libelle)
    if not result["department_code"]:
        result["department_code"] = dept_from_postal(postal, commune)
    if result["department_code"] and not result["region"]:
        result["region"] = DEPT_TO_REGION.get(result["department_code"])
    return result


def parse_location_adzuna(area: list | None, display_name: str | None) -> dict:
    """Adzuna : area = ["France", "Ile-de-France", "Hauts-de-Seine", "Nanterre", "Courbevoie"]."""
    result = {"city": None, "department_code": None, "region": None}
    area = [a for a in (area or []) if a]
    if len(area) >= 2:
        result["region"] = canonical_region(area[1])
    dept_name = norm_text(area[2]) if len(area) >= 3 else None
    if len(area) >= 4:
        # area[2] = département, area[3:] = communes -> la plus précise
        for part in reversed(area[3:]):
            if _ARRONDISSEMENT_ONLY_RE.match(part.strip()):
                continue  # « 8ème Arrondissement » seul : on remonte à la commune
            result["city"] = _clean_city(part)
            if result["city"]:
                break
    if not result["city"] and display_name:
        # 3 niveaux ou moins : le dernier élément est un département, pas une ville
        candidate = _clean_city(display_name.split(",")[0])
        if candidate and _ARRONDISSEMENT_ONLY_RE.match(candidate.strip()):
            candidate = None
        if candidate and (len(area) >= 4 or norm_text(candidate) != dept_name):
            result["city"] = candidate
    if not result["city"] and len(area) >= 4 and dept_name == "paris":
        result["city"] = "Paris"
    return result


# --------------------------------------------------------------------------
# Contrat
# --------------------------------------------------------------------------
def _tokens(raw) -> set[str]:
    """Valeurs de contrat de HelloWork / Free-Work : liste, ou texte
    « FULL_TIME » / « ["INTERN","FULL_TIME"] » selon ce que Spark a inféré."""
    if raw is None:
        return set()
    if isinstance(raw, (list, tuple)):
        return {str(t).strip().upper() for t in raw}
    text = str(raw).strip()
    if text.startswith("["):
        try:
            return {str(t).strip().upper() for t in json.loads(text)}
        except json.JSONDecodeError:
            pass
    return {t.upper() for t in re.findall(r"[A-Za-z_]+", text)}


def map_contract_schemaorg(raw) -> tuple[str, str]:
    """HelloWork / Free-Work (types schema.org). FULL_TIME seul n'indique pas
    un CDI : le contrat reste INCONNU, seul le temps de travail est renseigné.
    TEMPORARY est lu comme CDD (hypothèse, à re-vérifier si besoin)."""
    tokens = _tokens(raw)
    if "INTERN" in tokens:
        contract = "STAGE"
    elif "APPRENTICE" in tokens:
        contract = "ALTERNANCE"
    elif "CONTRACTOR" in tokens:
        contract = "FREELANCE"
    elif "TEMPORARY" in tokens:
        contract = "CDD"
    else:
        contract = "INCONNU"
    work_time = "PART_TIME" if "PART_TIME" in tokens else "FULL_TIME" if "FULL_TIME" in tokens else "INCONNU"
    return contract, work_time


_FT_CONTRACTS = {"CDI": "CDI", "CDD": "CDD", "SAI": "CDD", "LIB": "FREELANCE", "MIS": "INTERIM"}


def map_contract_ft(type_contrat: str | None, nature: str | None, alternance, duree: str | None) -> tuple[str, str]:
    nature_key = norm_text(nature)
    if alternance is True or "apprentissage" in nature_key or "professionnalisation" in nature_key:
        contract = "ALTERNANCE"
    else:
        contract = _FT_CONTRACTS.get((type_contrat or "").strip().upper(), "AUTRE" if type_contrat else "INCONNU")
    duree_key = norm_text(duree)
    work_time = "PART_TIME" if "temps partiel" in duree_key else "FULL_TIME" if "temps plein" in duree_key else "INCONNU"
    return contract, work_time


def map_contract_adzuna(contract_type: str | None, contract_time: str | None) -> tuple[str, str]:
    """Adzuna : « permanent » = CDI ; « contract » est trop vague (CDD ou
    freelance) donc classé AUTRE."""
    contract = {"permanent": "CDI", "contract": "AUTRE"}.get((contract_type or "").lower(), "INCONNU")
    work_time = {"full_time": "FULL_TIME", "part_time": "PART_TIME"}.get((contract_time or "").lower(), "INCONNU")
    return contract, work_time


# --------------------------------------------------------------------------
# Salaire
# --------------------------------------------------------------------------
HOURS_PER_YEAR = 1607  # base légale française, pour annualiser un taux horaire
ANNUAL_MIN, ANNUAL_MAX = 8_000, 400_000  # au-delà : valeur aberrante, ignorée

_FT_SALARY_RE = re.compile(
    r"(Mensuel|Annuel|Horaire)\s+de\s+([\d.,]+)\s+Euros?(?:\s+à\s+([\d.,]+)\s+Euros?)?", re.I
)
_FT_PERIODS = {"mensuel": "MONTH", "annuel": "YEAR", "horaire": "HOUR"}
_UNIT_PERIODS = {"YEAR": "YEAR", "MONTH": "MONTH", "DAY": "DAY", "HOUR": "HOUR", "WEEK": "WEEK"}


def _num(text) -> float | None:
    try:
        return float(str(text).replace(",", "."))
    except (TypeError, ValueError):
        return None


def parse_salary_ft(libelle: str | None) -> dict:
    """France Travail : « Mensuel de 1900.0 Euros à 2400.0 Euros » ou « Annuel de 28000.0 Euros ».
    Certains libellés annoncent « Mensuel » avec des montants annuels
    (ex. « Mensuel de 43000.0 Euros ») : la période est corrigée en YEAR."""
    if not libelle:
        return {}
    match = _FT_SALARY_RE.search(libelle)
    if not match:
        return {}
    low, high = _num(match.group(2)), _num(match.group(3))
    period = _FT_PERIODS[match.group(1).lower()]
    corrected = False
    if period == "MONTH" and max(v for v in (low, high) if v is not None) >= 12_000:
        period, corrected = "YEAR", True
    return {"min": low, "max": high if high is not None else low, "period": period, "period_corrected": corrected}


def parse_salary_schemaorg(unit, min_value, max_value, value) -> dict:
    low = _num(min_value) if min_value is not None else _num(value)
    high = _num(max_value) if max_value is not None else _num(value)
    low = low if low is not None else high
    high = high if high is not None else low
    if low is None:
        return {}
    return {"min": low, "max": high, "period": _UNIT_PERIODS.get(str(unit).upper()) if unit else None,
            "period_corrected": False}


def annualize(amount: float | None, period: str | None) -> float | None:
    """Équivalent annuel, seulement quand il a un sens : un tarif journalier
    freelance (DAY) n'est pas converti. Les valeurs aberrantes sont ignorées."""
    if amount is None or not period:
        return None
    factor = {"YEAR": 1, "MONTH": 12, "HOUR": HOURS_PER_YEAR}.get(period)
    if factor is None:
        return None
    annual = amount * factor
    return annual if ANNUAL_MIN <= annual <= ANNUAL_MAX else None


# --------------------------------------------------------------------------
# Identifiants
# --------------------------------------------------------------------------
def source_id_from_url(source: str, url: str | None) -> str | None:
    """HelloWork : .../emplois/84615734.html -> 84615734. Free-Work : dernier segment du chemin."""
    if not url:
        return None
    path = url.split("?")[0].split("#")[0].rstrip("/")
    if source == "hellowork":
        match = re.search(r"/(\d+)\.html$", path)
        return match.group(1) if match else path.rsplit("/", 1)[-1]
    return path.rsplit("/", 1)[-1]


# --------------------------------------------------------------------------
# Contrat déduit du texte (quand la source ne le donne pas)
# --------------------------------------------------------------------------
_CONTRACT_PATTERNS = {
    "ALTERNANCE": re.compile(r"\b(alternance|alternant|alternante|contrat d apprentissage|contrat de professionnalisation)\b"),
    "STAGE": re.compile(r"\b(stage|stagiaire|internship)\b"),
    "CDI": re.compile(r"\b(cdi|contrat a duree indeterminee|duree indeterminee)\b"),
    "CDD": re.compile(r"\b(cdd|contrat a duree determinee|duree determinee)\b"),
    "INTERIM": re.compile(r"\b(mission d interim|contrat d interim|travail temporaire)\b"),
    "FREELANCE": re.compile(r"\b(freelance|free lance|tjm|taux journalier|auto entrepreneur|portage salarial)\b"),
}
# Quand le texte cite plusieurs types (« CDI ou CDD »), on ne devine pas.
_DESCRIPTION_WINDOW = 2500  # le type de contrat est annoncé en début d'annonce


def _classes(text: str) -> set[str]:
    return {name for name, rx in _CONTRACT_PATTERNS.items() if rx.search(text)}


def infer_contract_from_text(title: str | None, description: str | None) -> str | None:
    """Déduit le contrat quand la source ne l'indique pas.

    1. Le titre fait foi s'il cite un seul type (« Alternance - Data Analyst »).
    2. Sinon, on lit le début de la description : un seul type cité -> retenu ;
       plusieurs types cités -> on ne tranche pas (None).
    Prudence volontaire : mieux vaut INCONNU qu'une valeur fausse."""
    in_title = _classes(norm_text(title or ""))
    if len(in_title) == 1:
        return next(iter(in_title))
    if len(in_title) > 1:
        return None
    in_desc = _classes(norm_text((description or "")[:_DESCRIPTION_WINDOW]))
    if len(in_desc) == 1:
        return next(iter(in_desc))
    return None