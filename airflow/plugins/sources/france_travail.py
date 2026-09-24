"""
Connecteur pour l'API France Travail (ex Pôle Emploi) - offres d'emploi.

Documentation : https://francetravail.io/produits-partenaires/catalogue/offres-emploi
Authentification : OAuth2 client credentials (realm partenaire).
Pagination : header Range (offset-limit), max 150 offres par appel.
"""
from __future__ import annotations

import logging
import os
from typing import Any

import requests
from tenacity import retry, stop_after_attempt, wait_exponential

logger = logging.getLogger(__name__)

TOKEN_URL = "https://entreprise.francetravail.fr/connexion/oauth2/access_token?realm=%2Fpartenaire"
SEARCH_URL = "https://api.francetravail.io/partenaire/offresdemploi/v2/offres/search"
PAGE_SIZE = 150  # maximum autorisé par l'API par appel


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def _get_access_token() -> str:
    client_id = os.environ["FRANCE_TRAVAIL_CLIENT_ID"]
    client_secret = os.environ["FRANCE_TRAVAIL_CLIENT_SECRET"]
    scope = os.environ.get("FRANCE_TRAVAIL_SCOPE", "api_offresdemploiv2 o2dsoffre")

    if not client_id or not client_secret:
        raise RuntimeError(
            "FRANCE_TRAVAIL_CLIENT_ID / FRANCE_TRAVAIL_CLIENT_SECRET manquants. "
            "Créez une application sur https://francetravail.io et renseignez le fichier .env"
        )

    response = requests.post(
        TOKEN_URL,
        data={
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
            "scope": scope,
        },
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        timeout=15,
    )
    response.raise_for_status()
    return response.json()["access_token"]


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def _fetch_page(token: str, start: int, end: int, params: dict) -> tuple[list[dict], int | None]:
    """Récupère une page d'offres. Retourne (offres, total_disponible)."""
    headers = {"Authorization": f"Bearer {token}"}
    query = dict(params)
    query["range"] = f"{start}-{end}"

    response = requests.get(SEARCH_URL, headers=headers, params=query, timeout=20)

    # 206 = pagination partielle (comportement normal de cette API), 200 = tout reçu
    if response.status_code not in (200, 206):
        response.raise_for_status()

    data = response.json()
    offers = data.get("resultats", [])

    content_range = response.headers.get("Content-Range")  # ex: "offres 0-149/1234"
    total = None
    if content_range and "/" in content_range:
        try:
            total = int(content_range.split("/")[-1])
        except ValueError:
            total = None

    return offers, total


def collect(max_offers: int | None = 1500, **search_params: Any) -> list[dict]:
    """Collecte les offres d'emploi depuis l'API France Travail.

    Args:
        max_offers: nombre maximum d'offres à récupérer (None = tout récupérer).
        **search_params: filtres optionnels de l'API (ex: motsCles="data engineer").

    Returns:
        Liste brute des offres telles que renvoyées par l'API (pas de transformation).
    """
    token = _get_access_token()
    all_offers: list[dict] = []
    start = 0

    while True:
        end = start + PAGE_SIZE - 1
        offers, total = _fetch_page(token, start, end, search_params)

        if not offers:
            break

        all_offers.extend(offers)
        logger.info("France Travail : %d offres récupérées (total cumulé : %d)", len(offers), len(all_offers))

        if max_offers and len(all_offers) >= max_offers:
            all_offers = all_offers[:max_offers]
            break
        if total is not None and end + 1 >= total:
            break

        start += PAGE_SIZE

    return all_offers
