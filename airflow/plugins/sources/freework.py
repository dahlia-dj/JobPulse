"""
Connecteur pour Free-Work - offres d'emploi et missions freelance IT/data
(scraping HTTP simple, pas d'API publique).

Constats par sonde (2026-10-20, requêtes simples avec le user-agent du
projet, aucune protection anti-bot rencontrée) :
  - Liste : https://www.free-work.com/fr/tech-it/jobs?query={mot-clé}&page={n}
    La pagination est un paramètre d'URL (pas de bouton à cliquer), et la
    liste est rendue côté serveur : requests suffit, pas besoin de Playwright.
    ~16 liens /job-mission/ par page ; quelques offres (mises en avant)
    reviennent d'une page à l'autre, d'où la déduplication ci-dessous.
  - Chaque offre a une page détail /fr/tech-it/job-mission/{métier}/{slug}
    qui contient un bloc JSON-LD schema.org "JobPosting" (title, description,
    hiringOrganization, jobLocation, employmentType, baseSalary, datePosted,
    validThrough) -- plus stable que des classes CSS, comme pour HelloWork.
  - Offres expirées : la liste contient ~10 % de liens vers des offres déjà
    supprimées ou expirées (la page affiche « l'offre recherchée a été
    supprimée ou expirée », sans bloc JobPosting). Ce ne sont pas des
    offres : elles sont ignorées, pas écrites en Bronze avec des "N/A".
    Garde-fou : si plus de la moitié des pages visitées sont ignorées, on
    lève une erreur, car c'est le signe d'un changement de balisage du site
    et non d'offres expirées.

Conformité (à reporter dans docs/data_dictionary.md) :
  - robots.txt de free-work.com : seuls /login, /logout et /fw-deals sont
    interdits ; les listes, pages détail et la pagination sont autorisées.
  - Conditions générales (art. 7.3) : interdisent la reproduction d'un
    élément du site et l'exploitation professionnelle/commerciale, sans
    exception académique ; l'extraction visée y concerne la CVthèque, et le
    scraping n'est pas cité nommément. Zone grise assumée pour un usage
    académique (PFE) : stockage privé en Bronze, aucune redistribution,
    débit limité (SCRAPE_DELAY_SECONDS). À rouvrir si le projet sort du
    cadre académique.

Si le site se met à répondre 403/429 de façon durable (protection
anti-bot), ce connecteur s'arrête : il ne contourne pas les protections.
"""
from __future__ import annotations

import json
import logging
import math
import os
import re
import time
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from tenacity import retry, stop_after_attempt, wait_exponential

logger = logging.getLogger(__name__)

BASE_URL = "https://www.free-work.com"
SEARCH_URL = f"{BASE_URL}/fr/tech-it/jobs"
OFFER_PATH = "/job-mission/"
MAX_PAGES = 100  # garde-fou : ~16 offres par page, soit ~1600 offres au plus
REQUEST_TIMEOUT_SECONDS = 30
# On collecte un peu plus de liens que max_offers pour compenser les offres
# expirées (~10 %) qui seront ignorées ; max_offers reste le nombre d'offres
# valides renvoyées.
LINK_OVERFETCH_RATIO = 1.2
MAX_SKIPPED_RATIO = 0.5

# `or` plutôt que le seul défaut de .get() : docker-compose crée quand même
# la variable d'environnement (vide) si elle est absente du .env, donc
# .get(clé, defaut) renverrait "" et non le défaut.
SCRAPER_USER_AGENT = os.environ.get("SCRAPER_USER_AGENT") or (
    "Mozilla/5.0 (compatible; PFE-DataEngineering-Bot/1.0; usage academique)"
)
SCRAPE_DELAY_SECONDS = float(os.environ.get("SCRAPE_DELAY_SECONDS") or "3")


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10), reraise=True)
def _get(url: str, params: dict | None = None) -> requests.Response:
    response = requests.get(
        url,
        params=params,
        headers={"User-Agent": SCRAPER_USER_AGENT},
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    response.raise_for_status()
    return response


def _extract_offer_links(html: str) -> list[str]:
    """Liens d'offres d'une page de liste, dédupliqués, sans paramètres ni ancre."""
    links: list[str] = []
    for href in re.findall(r'href="([^"#?]+)', html):
        if OFFER_PATH in href:
            full = urljoin(BASE_URL, href)
            if full not in links:
                links.append(full)
    return links


def _extract_job_posting_jsonld(soup: BeautifulSoup) -> dict | None:
    """Cherche, parmi les blocs JSON-LD de la page (WebPage, JobPosting...),
    celui de type "JobPosting"."""
    for script in soup.find_all("script", type="application/ld+json"):
        if not script.string:
            continue
        try:
            data = json.loads(script.string)
        except (json.JSONDecodeError, TypeError):
            continue

        candidates = data if isinstance(data, list) else [data]
        for candidate in candidates:
            if isinstance(candidate, dict) and candidate.get("@type") == "JobPosting":
                return candidate
    return None


def _clean_html_text(raw: str | None) -> str:
    """La description du JSON-LD contient souvent du HTML : on la réduit à
    du texte brut pour l'extraction de compétences spaCy en aval."""
    if not raw:
        return ""
    return BeautifulSoup(raw, "html.parser").get_text(separator=" ", strip=True)


def _format_location(job_location) -> str:
    if isinstance(job_location, list):
        job_location = job_location[0] if job_location else None
    if not isinstance(job_location, dict):
        return "N/A"
    address = job_location.get("address")
    if not isinstance(address, dict):
        return "N/A"
    parts = (address.get("addressLocality"), address.get("addressRegion"), address.get("addressCountry"))
    return ", ".join(str(p) for p in parts if p and isinstance(p, str)) or "N/A"


def _fetch_offer_details(link: str) -> dict | None:
    """Récupère les champs de l'offre depuis le JSON-LD JobPosting de sa page
    détail. Renvoie None si la page n'est pas une offre exploitable (offre
    expirée/supprimée, ou erreur réseau persistante) : l'appelant l'ignore."""
    try:
        response = _get(link)
    except requests.RequestException as exc:
        logger.warning("Free-Work : échec requête page détail %s (%s)", link, exc)
        return None

    soup = BeautifulSoup(response.text, "lxml")
    job_posting = _extract_job_posting_jsonld(soup)

    if not job_posting:
        logger.warning(
            "Free-Work : offre ignorée, pas de bloc JSON-LD JobPosting (offre expirée ou supprimée probable) : %s",
            link,
        )
        return None

    details = {
        "title": job_posting.get("title") or "N/A",
        "company": "N/A",
        "description": _clean_html_text(job_posting.get("description")),
        "location": _format_location(job_posting.get("jobLocation")),
        "contract_type": job_posting.get("employmentType") or "N/A",
        "date_posted": job_posting.get("datePosted"),
        "valid_through": job_posting.get("validThrough"),
        "salary": None,
    }

    hiring_org = job_posting.get("hiringOrganization")
    if isinstance(hiring_org, dict):
        details["company"] = hiring_org.get("name") or "N/A"

    base_salary = job_posting.get("baseSalary")
    if isinstance(base_salary, (dict, list)):
        details["salary"] = base_salary

    return details


def _collect_links(max_offers: int | None, keyword: str) -> list[str]:
    all_links: list[str] = []
    seen: set[str] = set()

    for page_number in range(1, MAX_PAGES + 1):
        logger.info("Free-Work : liste, page %d", page_number)
        try:
            response = _get(SEARCH_URL, params={"query": keyword, "page": page_number})
        except requests.RequestException as exc:
            if page_number == 1:
                # Rien collecté du tout : on échoue visiblement plutôt que de
                # renvoyer une liste vide silencieuse (ex. blocage 403/429).
                raise
            logger.warning("Free-Work : arrêt à la page %d (%s), on garde ce qui est collecté.", page_number, exc)
            break

        new_links = [link for link in _extract_offer_links(response.text) if link not in seen]
        if not new_links:
            logger.info("Free-Work : aucune nouvelle offre à la page %d, fin de la liste.", page_number)
            break

        seen.update(new_links)
        all_links.extend(new_links)
        logger.info("Free-Work : %d nouvelles offres (total cumulé : %d)", len(new_links), len(all_links))

        if max_offers and len(all_links) >= max_offers:
            return all_links[:max_offers]

        time.sleep(SCRAPE_DELAY_SECONDS)  # respect du site, pas de rafale de requêtes

    return all_links


def collect(max_offers: int | None = 1500, keyword: str = "data") -> list[dict]:
    """Collecte les offres Free-Work : liens depuis la liste paginée, puis
    détails depuis le JSON-LD de chaque page d'offre.

    Args:
        max_offers: nombre maximum d'offres valides à renvoyer (les offres
            expirées, ignorées, ne comptent pas).
        keyword: mot-clé de recherche (filtrage fait ici, à la Collecte).

    Returns:
        Liste brute de dicts {title, company, description, location,
        contract_type, date_posted, valid_through, salary, link} -- pas de
        transformation métier ici, c'est le rôle de la couche Silver en aval.

    Raises:
        RuntimeError: si plus de la moitié des pages visitées sont ignorées
            (balisage du site probablement modifié).
    """
    link_limit = math.ceil(max_offers * LINK_OVERFETCH_RATIO) if max_offers else None
    links = _collect_links(link_limit, keyword)
    logger.info("Free-Work : %d liens collectés, récupération des détails (JSON-LD)...", len(links))

    all_offers: list[dict] = []
    skipped = 0
    for link in links:
        details = _fetch_offer_details(link)
        if details is None:
            skipped += 1
        else:
            all_offers.append({**details, "link": link})

        processed = len(all_offers) + skipped
        if processed % 50 == 0:
            logger.info("Free-Work : %d pages détail visitées (%d offres, %d ignorées)", processed, len(all_offers), skipped)

        if max_offers and len(all_offers) >= max_offers:
            break

        time.sleep(SCRAPE_DELAY_SECONDS)  # respect du site, une requête à la fois

    processed = len(all_offers) + skipped
    logger.info(
        "Free-Work : %d offres conservées, %d ignorées (expirées ou sans JobPosting) sur %d pages détail visitées.",
        len(all_offers), skipped, processed,
    )
    if processed and skipped / processed > MAX_SKIPPED_RATIO:
        raise RuntimeError(
            f"Free-Work : {skipped}/{processed} pages sans JobPosting (> {MAX_SKIPPED_RATIO:.0%}) : "
            "le balisage du site a probablement changé, collecte annulée."
        )

    return all_offers