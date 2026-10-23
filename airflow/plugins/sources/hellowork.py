"""
Connecteur pour HelloWork - offres d'emploi (scraping, pas d'API publique).

Sélecteurs et mécanique de pagination confirmés par observation directe du
site en conditions réelles (Playwright en mode non-headless) :
  - Recherche : https://www.hellowork.com/fr-fr/emploi/recherche.html?k=...&l=...
  - Chaque offre est un <a data-cy="offerTitle"> -- fournit le lien.
  - La pagination n'est PAS un paramètre d'URL : c'est un bouton
    <button name="p" value="{numéro_page}"> à cliquer, désactivé
    (attribut "disabled") sur la dernière page. HelloWork nécessite donc
    un navigateur headless (Playwright), comme WTTJ -- une page en
    requests+BeautifulSoup ne verrait qu'une pagination non fonctionnelle.
  - Titre/entreprise/description/lieu/contrat/date/salaire : PAS extraits
    de la page de liste (ses classes CSS utilitaires, ex. "tw-typo-l" /
    "tw-typo-s", ont changé entre fin septembre et le 19/10/2026 et sont
    devenues obsolètes du jour au lendemain -- confirmé : la collecte du
    19/10 a renvoyé "N/A" sur 100% des offres pour ces deux champs, et ne
    capturait de toute façon jamais la description). On va plutôt chercher
    chaque page détail (via le lien, fiable) et parser son bloc JSON-LD
    <script type="application/ld+json"> de type "JobPosting" -- balisage
    SEO/Google for Jobs, nettement plus stable que des classes CSS, et qui
    fournit en prime description/lieu/type de contrat/date/salaire. Les
    pages détail sont rendues côté serveur (SEO oblige) : un simple
    requests.get suffit, pas besoin de Playwright pour celles-ci.

  - Doublons : HelloWork remet des offres sponsorisées d'une page de liste à
    l'autre (~10 % des liens sur un run de 1000). Les liens sont dédupliqués.
  - Offres expirées : une page détail sans bloc JobPosting n'est pas une
    offre exploitable ; elle est ignorée (pas écrite en Bronze avec des
    "N/A"). Garde-fou : si plus de la moitié des pages visitées sont
    ignorées, on lève une erreur (changement de balisage probable).

⚠️ Ces sélecteurs peuvent changer si HelloWork fait évoluer son site. À
re-vérifier si le connecteur cesse de renvoyer des résultats.

Respect du site :
  - Rate limiting entre les pages ET entre les pages détail (SCRAPE_DELAY_SECONDS).
  - Un seul navigateur headless réutilisé pour toute la collecte.
  - Le robots.txt de hellowork.com interdit explicitement l'accès
    automatisé à ce chemin (vérifié) -- ce connecteur outrepasse donc une
    convention technique, pas une loi. Voir docs/data_dictionary.md et
    l'échange sur la sécurité/légalité du projet pour le contexte complet
    avant de l'activer en usage réel.
"""
from __future__ import annotations

import json
import logging
import math
import os
import time

import requests
from bs4 import BeautifulSoup
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright

logger = logging.getLogger(__name__)

SEARCH_URL = "https://www.hellowork.com/fr-fr/emploi/recherche.html"
MAX_PAGES = 50  # garde-fou, au-delà des références observées
DETAIL_TIMEOUT_SECONDS = 20
# On collecte un peu plus de liens que max_offers pour compenser les offres
# expirées qui seront ignorées ; max_offers reste le nombre d'offres valides
# renvoyées.
LINK_OVERFETCH_RATIO = 1.2
MAX_SKIPPED_RATIO = 0.5

# `or` plutôt que le seul défaut de .get() : docker-compose crée quand même
# la variable d'environnement (vide) si elle est absente du .env, donc
# .get(clé, defaut) renverrait "" et non le défaut. `or` traite "" comme
# absent, tout comme None.
SCRAPER_USER_AGENT = os.environ.get("SCRAPER_USER_AGENT") or (
    "Mozilla/5.0 (compatible; PFE-DataEngineering-Bot/1.0; usage academique)"
)
SCRAPE_DELAY_SECONDS = float(os.environ.get("SCRAPE_DELAY_SECONDS") or "3")
PAGE_TIMEOUT_MS = 120_000


def _dismiss_cookie_banner(page) -> None:
    """Ferme le bandeau de consentement cookies RGPD s'il est présent.

    Cause réelle observée en échec de production : le clic sur le bouton
    "page suivante" était intercepté indéfiniment par la modale de
    consentement (classes hw-cc-modal / hw-cc-notice-content), qui reste
    au-dessus du contenu tant qu'on n'a pas répondu. Best-effort : si aucun
    des sélecteurs ne correspond en 3s, on suppose qu'il n'y a pas de
    bandeau (déjà accepté, ou absent) et on continue sans bloquer.
    """
    for selector in (
        ".hw-cc-modal button:has-text('Tout accepter')",
        ".hw-cc-modal button:has-text('Accepter')",
        ".hw-cc-notice-buttons button:has-text('Accepter')",
        "button:has-text('Tout accepter')",
        "button:has-text(\"J'accepte\")",
    ):
        try:
            page.click(selector, timeout=3000)
            logger.info("HelloWork : bandeau cookies fermé via le sélecteur %r.", selector)
            return
        except PlaywrightTimeoutError:
            continue
    logger.info("HelloWork : aucun bandeau cookies détecté (ou déjà fermé).")


def _extract_job_posting_jsonld(soup: BeautifulSoup) -> dict | None:
    """Cherche, parmi tous les blocs JSON-LD de la page, celui de type
    "JobPosting" (une page peut en contenir plusieurs : fil d'Ariane,
    organisation, etc.)."""
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
    """Le champ description du JSON-LD contient souvent du HTML
    (listes à puces, paragraphes) -- on le réduit à du texte brut pour
    l'extraction de compétences spaCy en aval."""
    if not raw:
        return ""
    return BeautifulSoup(raw, "html.parser").get_text(separator=" ", strip=True)


def _fetch_offer_details(link: str) -> dict | None:
    """Récupère title/company/description/lieu/contrat/date/salaire depuis
    le JSON-LD JobPosting de la page détail. Renvoie None si la page n'est
    pas une offre exploitable (expirée/supprimée, ou erreur réseau) :
    l'appelant l'ignore."""
    details = {
        "title": "N/A",
        "company": "N/A",
        "description": "",
        "location": "N/A",
        "contract_type": "N/A",
        "date_posted": None,
        "salary": None,
    }

    try:
        response = requests.get(
            link,
            headers={"User-Agent": SCRAPER_USER_AGENT},
            timeout=DETAIL_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
    except requests.RequestException as exc:
        logger.warning("HelloWork : échec requête page détail %s (%s)", link, exc)
        return None

    soup = BeautifulSoup(response.text, "lxml")
    job_posting = _extract_job_posting_jsonld(soup)

    if not job_posting:
        logger.warning(
            "HelloWork : offre ignorée, pas de bloc JSON-LD JobPosting (offre expirée ou supprimée probable) : %s",
            link,
        )
        return None

    details["title"] = job_posting.get("title") or "N/A"
    details["description"] = _clean_html_text(job_posting.get("description"))

    hiring_org = job_posting.get("hiringOrganization")
    if isinstance(hiring_org, dict):
        details["company"] = hiring_org.get("name") or "N/A"

    job_location = job_posting.get("jobLocation")
    if isinstance(job_location, list):
        job_location = job_location[0] if job_location else None
    if isinstance(job_location, dict):
        address = job_location.get("address")
        if isinstance(address, dict):
            locality = address.get("addressLocality")
            region = address.get("addressRegion")
            details["location"] = ", ".join(part for part in (locality, region) if part) or "N/A"

    details["contract_type"] = job_posting.get("employmentType") or "N/A"
    details["date_posted"] = job_posting.get("datePosted")

    base_salary = job_posting.get("baseSalary")
    if isinstance(base_salary, dict):
        details["salary"] = base_salary

    return details


def collect(max_offers: int | None = 1500, keyword: str = "data", location: str = "France") -> list[dict]:
    """Collecte les offres d'emploi depuis HelloWork via Playwright, puis
    enrichit chaque offre via le JSON-LD de sa page détail.

    Args:
        max_offers: nombre maximum d'offres valides à renvoyer (les offres
            expirées, ignorées, ne comptent pas).
        keyword: mot-clé de recherche (filtrage fait ici, à la Collecte).
        location: localisation transmise au paramètre "l" de l'URL.

    Returns:
        Liste brute de dicts {title, company, description, location,
        contract_type, date_posted, salary, link} -- pas de transformation
        métier ici, c'est le rôle de la couche Silver en aval.

    Raises:
        RuntimeError: si plus de la moitié des pages visitées sont ignorées
            (balisage du site probablement modifié).
    """
    link_limit = math.ceil(max_offers * LINK_OVERFETCH_RATIO) if max_offers else None
    search_keyword = keyword.replace(" ", "+")
    url = f"{SEARCH_URL}?k={search_keyword}&l={location}"
    all_links: list[str] = []
    seen_links: set[str] = set()

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(
            headless=True,
            args=["--disable-dev-shm-usage", "--no-sandbox"],
        )
        page = browser.new_page(user_agent=SCRAPER_USER_AGENT)

        try:
            page.goto(url, wait_until="domcontentloaded", timeout=PAGE_TIMEOUT_MS)
            _dismiss_cookie_banner(page)

            page_number = 1
            while page_number <= MAX_PAGES:
                logger.info("HelloWork : scraping page %d", page_number)

                offers = page.query_selector_all('a[data-cy="offerTitle"]')
                logger.info("HelloWork : %d offres trouvées sur cette page", len(offers))

                if not offers:
                    logger.info("HelloWork : aucune offre trouvée, arrêt à la page %d.", page_number)
                    break

                for offer in offers:
                    link = offer.get_attribute("href")
                    if not link:
                        continue
                    if not link.startswith("https://www.hellowork.com"):
                        link = "https://www.hellowork.com" + link
                    if link in seen_links:  # offres sponsorisées répétées entre pages
                        continue
                    seen_links.add(link)
                    all_links.append(link)

                if link_limit and len(all_links) >= link_limit:
                    all_links = all_links[:link_limit]
                    break

                # Bouton "suivant" : <button name="p" value="{page_number + 1}">
                next_button = page.query_selector(f'button[name="p"][value="{page_number + 1}"]')

                if not next_button:
                    logger.info("HelloWork : dernière page atteinte - bouton suivant non trouvé.")
                    break

                if next_button.get_attribute("disabled"):
                    logger.info("HelloWork : dernière page atteinte - bouton désactivé.")
                    break

                next_button.click()
                page_number += 1
                time.sleep(SCRAPE_DELAY_SECONDS)  # respect du site, pas de rafale de requêtes
                page.wait_for_load_state("domcontentloaded")
        except Exception:
            # Capture de debug : visible dans ./airflow/logs/ côté hôte
            # (volume déjà monté), utile pour diagnostiquer un futur échec
            # sans avoir à rejouer la tâche en mode non-headless.
            try:
                page.screenshot(path="/opt/airflow/logs/hellowork_debug.png", full_page=True)
                logger.info("HelloWork : capture de debug sauvegardée dans airflow/logs/hellowork_debug.png")
            except Exception:
                pass
            raise
        finally:
            browser.close()

    logger.info("HelloWork : %d liens collectés, récupération des détails (JSON-LD)...", len(all_links))

    all_offers: list[dict] = []
    skipped = 0
    for link in all_links:
        details = _fetch_offer_details(link)
        if details is None:
            skipped += 1
        else:
            all_offers.append({**details, "link": link})

        processed = len(all_offers) + skipped
        if processed % 50 == 0:
            logger.info("HelloWork : %d pages détail visitées (%d offres, %d ignorées)", processed, len(all_offers), skipped)

        if max_offers and len(all_offers) >= max_offers:
            break

        time.sleep(SCRAPE_DELAY_SECONDS)  # respect du site, une requête à la fois

    processed = len(all_offers) + skipped
    logger.info(
        "HelloWork : %d offres conservées, %d ignorées (expirées ou sans JobPosting) sur %d pages détail visitées.",
        len(all_offers), skipped, processed,
    )
    if processed and skipped / processed > MAX_SKIPPED_RATIO:
        raise RuntimeError(
            f"HelloWork : {skipped}/{processed} pages sans JobPosting (> {MAX_SKIPPED_RATIO:.0%}) : "
            "le balisage du site a probablement changé, collecte annulée."
        )

    return all_offers