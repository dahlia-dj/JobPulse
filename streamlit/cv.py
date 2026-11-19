"""
Lecture d'un CV au format PDF et extraction des compétences.

Le texte du PDF est extrait avec pypdf, puis analysé par extract_skills() de la
couche Silver (silver/enrich.py) : même catalogue de 92 compétences et même
PhraseMatcher spaCy que pour les offres. Les compétences du CV et celles des
offres sont donc directement comparables.

Le dossier airflow/plugins/silver est monté en lecture seule dans le conteneur
Streamlit (voir docker-compose) : le catalogue n'existe qu'à un seul endroit.
"""
from __future__ import annotations

import io

from pypdf import PdfReader

MAX_PAGES = 5  # un CV dépasse rarement 2 pages ; on borne la lecture


def pdf_text(data: bytes) -> str:
    reader = PdfReader(io.BytesIO(data))
    pages = reader.pages[:MAX_PAGES]
    return "\n".join((page.extract_text() or "") for page in pages)


def skills_from_cv(data: bytes) -> tuple[list[str], int]:
    """Renvoie (compétences détectées, nombre de caractères lus)."""
    from silver.enrich import extract_skills

    text = pdf_text(data)
    return sorted(set(extract_skills("", text))), len(text.strip())
