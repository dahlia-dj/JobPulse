"""
Connecteur pour l'API Adzuna - offres d'emploi.

Documentation : https://developer.adzuna.com/docs/search
Authentification : app_id + app_key en query string (pas d'OAuth).
Pagination : /search/{page}, results_per_page (max 50).

Ancienneté : seules les offres publiées depuis MIN_POSTED_DATE (variable
d'environnement, par défaut 2026-01-01) sont collectées. Adzuna est un agrégateur
qui conserve la date de première indexation : sans ce filtre, des annonces
restées en ligne depuis 2023-2024 remontaient.
"""
from __future__ import annotations

import logging
import os
from datetime import date

import requests
from tenacity import retry, stop_after_attempt, wait_exponential

logger = logging.getLogger(__name__)

BASE_URL = "https://api.adzuna.com/v1/api/jobs/{country}/search/{page}"
RESULTS_PER_PAGE = 50  # maximum autorisé par Adzuna
MIN_POSTED_DATE = date.fromisoformat(os.environ.get("MIN_POSTED_DATE", "2026-01-01"))


def _max_days_old() -> int:
    """Nombre de jours entre MIN_POSTED_DATE et aujourd'hui (paramètre max_days_old de l'API)."""
    return max(1, (date.today() - MIN_POSTED_DATE).days + 1)


def _posted_since_min_date(offer: dict) -> bool:
    """Deuxième sécurité : on vérifie aussi la date « created » (ex. 2026-03-14T09:12:00Z)."""
    created = (offer.get("created") or "")[:10]
    try:
        return date.fromisoformat(created) >= MIN_POSTED_DATE
    except ValueError:
        return True  # date absente ou illisible : on garde, la couche Silver tranchera


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

    # Offres publiées depuis MIN_POSTED_DATE, les plus récentes d'abord
    # (les valeurs passées explicitement à collect() restent prioritaires).
    search_params = {"max_days_old": _max_days_old(), "sort_by": "date", **search_params}
    logger.info("Adzuna : offres publiées depuis le %s (max_days_old=%s)",
                MIN_POSTED_DATE, search_params["max_days_old"])

    all_offers: list[dict] = []
    page = 1
    max_pages = 200  # garde-fou pour éviter une boucle infinie si l'API ne renvoie jamais []

    while page <= max_pages:
        offers = _fetch_page(country, page, app_id, app_key, search_params)
        if not offers:
            break

        kept = [o for o in offers if _posted_since_min_date(o)]
        if len(kept) < len(offers):
            logger.info("Adzuna : %d offres antérieures au %s écartées", len(offers) - len(kept), MIN_POSTED_DATE)
        all_offers.extend(kept)
        logger.info("Adzuna : page %d, %d offres récupérées (total cumulé : %d)", page, len(offers), len(all_offers))

        if max_offers and len(all_offers) >= max_offers:
            all_offers = all_offers[:max_offers]
            break

        page += 1

    return all_offers