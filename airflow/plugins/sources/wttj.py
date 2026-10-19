"""
Connecteur pour Welcome to the Jungle - offres d'emploi (scraping, pas d'API
publique documentée). Site en JavaScript (SPA) : nécessite Playwright pour
exécuter le rendu avant extraction.

Sélecteurs et mécanique de pagination confirmés par observation directe du
site en conditions réelles (Playwright en mode non-headless) :
  - Recherche : https://www.welcometothejungle.com/fr/jobs?query={keyword}
  - Chaque offre est un <li data-testid="search-results-list-item-wrapper">
      - titre dans <h2><div role="mark">...</div></h2>
      - lien dans <a role="link" href="...">
      - l'entreprise n'est pas dans un élément dédié facilement sélectionnable :
        elle est déduite du slug de l'URL (/companies/{slug}/jobs/...)
  - Pagination par clic sur la flèche "suivante" (pas de paramètre d'URL) :
    un <a aria-disabled="..."> contenant un <svg alt="Right">, désactivé
    (aria-disabled="true") sur la dernière page.

⚠️ Ces sélecteurs peuvent changer si WTTJ fait évoluer son site. À
re-vérifier si le connecteur cesse de renvoyer des résultats.

Mur de consentement cookies -- diagnostic (captures de debug des
2026-10-19) : la navigation vers /fr/jobs reste bloquée sur la page
d'accueil, derrière un widget aux libellés "Tout refuser" / "Je choisis"
(pattern du CMP français Axeptio). Un clic ciblé sur ces libellés
(:has-text) ne l'atteint pas -- cohérent avec un widget logé dans une
iframe ou un Shadow DOM fermé, tous deux hors de portée d'un clic
Playwright classique sur `page`. Plutôt que de viser l'intérieur du
widget, on masque son élément hôte en CSS (accessible depuis l'extérieur
même si son contenu interne ne l'est pas) et on retente le clic en
parcourant aussi les iframes, en complément.

Respect du site :
  - Rate limiting entre les pages (SCRAPE_DELAY_SECONDS).
  - Un seul navigateur headless réutilisé pour toute la collecte.
"""
from __future__ import annotations

import logging
import os
import re
import time
from urllib.parse import quote

from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright

logger = logging.getLogger(__name__)

BASE_URL = "https://www.welcometothejungle.com"
SEARCH_URL = f"{BASE_URL}/fr/jobs"
RESULTS_ITEM_SELECTOR = 'li[data-testid="search-results-list-item-wrapper"]'
MAX_PAGES = 50  # garde-fou, au-delà des références observées

# `or` plutôt que le seul défaut de .get() : docker-compose crée quand même
# la variable d'environnement (vide) si elle est absente du .env, donc
# .get(clé, defaut) renverrait "" et non le défaut. `or` traite "" comme
# absent, tout comme None.
SCRAPER_USER_AGENT = os.environ.get("SCRAPER_USER_AGENT") or (
    "Mozilla/5.0 (compatible; PFE-DataEngineering-Bot/1.0; usage academique)"
)
SCRAPE_DELAY_SECONDS = float(os.environ.get("SCRAPE_DELAY_SECONDS") or "3")
PAGE_TIMEOUT_MS = 120_000
RESULTS_TIMEOUT_MS = 60_000

# Masque générique de tout widget de consentement cookies connu, ciblé sur
# l'élément hôte (accessible en CSS même si son contenu est en Shadow DOM
# fermé -- seul l'intérieur est inatteignable, pas le conteneur lui-même).
_COOKIE_WIDGET_KILL_CSS = """
axeptio_overlay, axeptio_widget,
[id*="axeptio" i], [class*="axeptio" i],
[id*="didomi" i], [class*="didomi" i],
[id*="onetrust" i], [class*="onetrust" i],
[class*="cookie-banner" i], [class*="cookie-consent" i],
[class*="cookie_banner" i], [class*="cookie_consent" i],
[class*="consent-banner" i], [class*="consent-modal" i],
[class*="consent_banner" i], [class*="consent_modal" i],
dialog[open], [role="dialog"], [role="alertdialog"] {
  display: none !important;
  visibility: hidden !important;
  pointer-events: none !important;
}
html, body {
  overflow: auto !important;
  position: static !important;
}
"""


def _safe_text(element, selector: str) -> str:
    el = element.query_selector(selector)
    return el.inner_text().strip() if el else "N/A"


def _extract_company_from_link(link: str) -> str:
    """L'entreprise n'a pas d'élément dédié fiable dans le DOM : on la
    déduit du slug de l'URL (/companies/ma-super-entreprise/jobs/...)."""
    match = re.search(r"/companies/([^/]+)/jobs", link)
    if match:
        return match.group(1).replace("-", " ").title()
    return "N/A"


def _dismiss_cookie_banner(page) -> None:
    """Neutralise le mur de consentement cookies.

    Double approche, best-effort :
    1. Injection CSS qui masque l'élément hôte du widget (fonctionne même
       si son contenu est en Shadow DOM fermé -- voir le docstring du
       module). C'est la mesure principale : elle ne dépend d'aucune
       hypothèse sur la structure interne du widget.
    2. Tentative de clic sur les libellés connus, y compris à l'intérieur
       des iframes de la page (certains CMP isolent leur widget dans une
       iframe) -- en complément, au cas où un consentement explicite soit
       réellement requis côté client avant de débloquer la navigation.
    """
    try:
        page.add_style_tag(content=_COOKIE_WIDGET_KILL_CSS)
        logger.info("WTTJ : CSS de masquage du widget cookies injecté.")
    except Exception as exc:  # best-effort, ne doit jamais bloquer la collecte
        logger.warning("WTTJ : échec injection CSS masquage cookies (%s)", exc)

    labels = ("Tout refuser", "Je choisis", "Tout accepter", "Accepter", "J'accepte", "Accept all")

    frames = [page] + list(page.frames)
    for frame in frames:
        for label in labels:
            try:
                frame.get_by_text(label, exact=False).first.click(timeout=2000)
                logger.info("WTTJ : bandeau cookies fermé via clic sur %r.", label)
                return
            except (PlaywrightTimeoutError, Exception):
                continue
    logger.info("WTTJ : aucun clic de bandeau cookies n'a abouti (masquage CSS seul appliqué).")


def collect(max_offers: int | None = 1500, keyword: str = "data") -> list[dict]:
    """Collecte les offres d'emploi depuis Welcome to the Jungle via Playwright.

    Args:
        max_offers: nombre maximum d'offres à récupérer.
        keyword: mot-clé de recherche (filtrage fait ici, à la Collecte).

    Returns:
        Liste brute de dicts {title, company, link} -- pas de transformation
        métier ici, c'est le rôle de la couche Silver en aval.
    """
    url = f"{SEARCH_URL}?query={quote(keyword)}"
    all_offers: list[dict] = []

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(
            headless=True,
            args=["--disable-dev-shm-usage", "--no-sandbox"],
        )
        page = browser.new_page(user_agent=SCRAPER_USER_AGENT)

        try:
            page.goto(url, wait_until="domcontentloaded", timeout=PAGE_TIMEOUT_MS)
            page.wait_for_timeout(1500)  # laisse le temps au widget cookies de s'hydrater avant de le traiter
            _dismiss_cookie_banner(page)

            # Que le widget ait été cliqué ou seulement masqué en CSS, on
            # reprend la navigation vers l'URL de recherche : si le mur
            # bloquait réellement le routage côté client, ce rechargement
            # repart sur une page où il ne devrait plus interférer.
            page.goto(url, wait_until="domcontentloaded", timeout=PAGE_TIMEOUT_MS)
            page.add_style_tag(content=_COOKIE_WIDGET_KILL_CSS)

            page.wait_for_selector(RESULTS_ITEM_SELECTOR, timeout=RESULTS_TIMEOUT_MS)

            page_number = 1
            while page_number <= MAX_PAGES:
                logger.info("WTTJ : scraping page %d", page_number)

                offers = page.query_selector_all(RESULTS_ITEM_SELECTOR)
                logger.info("WTTJ : %d offres trouvées sur cette page", len(offers))

                for offer in offers:
                    title = _safe_text(offer, 'h2 div[role="mark"]')
                    link_el = offer.query_selector('a[role="link"]')
                    href = link_el.get_attribute("href") if link_el else None

                    link = None
                    company = "N/A"
                    if href:
                        link = BASE_URL + href
                        company = _extract_company_from_link(link)

                    all_offers.append({"title": title, "company": company, "link": link})

                if max_offers and len(all_offers) >= max_offers:
                    all_offers = all_offers[:max_offers]
                    break

                # Flèche "suivante" : <a aria-disabled="..."><svg alt="Right">
                # (:has() cible directement le <a> parent, plus robuste que de
                # remonter depuis le <svg> via evaluate_handle/closest)
                next_link = page.query_selector('a[aria-disabled]:has(svg[alt="Right"])')

                if not next_link:
                    logger.info("WTTJ : bouton suivant introuvable, fin du scraping.")
                    break

                if next_link.get_attribute("aria-disabled") == "true":
                    logger.info("WTTJ : dernière page atteinte.")
                    break

                next_link.click()
                page_number += 1
                time.sleep(SCRAPE_DELAY_SECONDS)  # respect du site, pas de rafale de requêtes
                page.wait_for_selector(RESULTS_ITEM_SELECTOR, timeout=RESULTS_TIMEOUT_MS)
        except Exception:
            # Capture de debug : visible dans ./airflow/logs/ côté hôte
            # (volume déjà monté). Essentiel ici puisque la cause de
            # l'échec (bandeau cookies ? sélecteur obsolète ? blocage
            # anti-bot ?) n'est pas confirmée -- la capture permettra de
            # trancher sans avoir à rejouer en mode non-headless.
            try:
                page.screenshot(path="/opt/airflow/logs/wttj_debug.png", full_page=True)
                logger.info("WTTJ : capture de debug sauvegardée dans airflow/logs/wttj_debug.png")
            except Exception:
                pass
            raise
        finally:
            browser.close()

    return all_offers