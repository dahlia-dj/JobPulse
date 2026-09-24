"""
Connecteur pour l'API Adzuna - offres d'emploi.

Documentation : https://developer.adzuna.com/docs/search
Authentification : app_id + app_key en query string (pas d'OAuth).
Pagination : /search/{page}, results_per_page (max 50).
"""
from __future__ import annotations

import logging
import os

import requests
from tenacity import retry, stop_after_attempt, wait_exponential

logger = logging.getLogger(__name__)

BASE_URL = "https://api.adzuna.com/v1/api/jobs/{country}/search/{page}"
RESULTS_PER_PAGE = 50  # maximum autorisé par Adzuna


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def _fetch_page(country: str, page: int, app_id: str, app_key: str, extra_params: dict) -> list[dict]:
    url = BASE_URL.format(country=country, page=page)
    params = {
        "app_id": app_id,
        "app_key": app_key,
        "results_per_page": RESULTS_PER_PAGE,
        "content-type": "application/json",
        **extra_params,
    }
    response = requests.get(url, params=params, timeout=20)
    response.raise_for_status()
    return response.json().get("results", [])


def collect(max_offers: int | None = 1500, **search_params) -> list[dict]:
    """Collecte les offres d'emploi depuis l'API Adzuna.

    Args:
        max_offers: nombre maximum d'offres à récupérer (None = tout récupérer
            dans la limite raisonnable de pages pour éviter une boucle infinie).
        **search_params: filtres optionnels de l'API (ex: what="data engineer").

    Returns:
        Liste brute des offres telles que renvoyées par l'API (pas de transformation).
    """
    app_id = os.environ["ADZUNA_APP_ID"]
    app_key = os.environ["ADZUNA_APP_KEY"]
    country = os.environ.get("ADZUNA_COUNTRY", "fr")

    if not app_id or not app_key:
        raise RuntimeError(
            "ADZUNA_APP_ID / ADZUNA_APP_KEY manquants. "
            "Créez un compte sur https://developer.adzuna.com et renseignez le fichier .env"
        )

    all_offers: list[dict] = []
    page = 1
    max_pages = 200  # garde-fou pour éviter une boucle infinie si l'API ne renvoie jamais []

    while page <= max_pages:
        offers = _fetch_page(country, page, app_id, app_key, search_params)
        if not offers:
            break

        all_offers.extend(offers)
        logger.info("Adzuna : page %d, %d offres récupérées (total cumulé : %d)", page, len(offers), len(all_offers))

        if max_offers and len(all_offers) >= max_offers:
            all_offers = all_offers[:max_offers]
            break

        page += 1

    return all_offers
