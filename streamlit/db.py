"""
Accès à l'entrepôt Gold (PostgreSQL, base job_offers_dwh, schéma gold).

- Connexion en lecture seule, requêtes paramétrées (aucun texte saisi n'est
  concaténé dans le SQL), résultats mis en cache 10 minutes.
- Les filtres de la barre latérale (métier, région, contrat, expérience, source,
  offres actives) s'appliquent à toutes les requêtes via FILTRE_SQL.
"""
from __future__ import annotations

import os

import pandas as pd
import psycopg2
import streamlit as st

CACHE_TTL = 600  # secondes : la couche Gold n'est rechargée qu'une fois par jour


def _connect():
    return psycopg2.connect(
        host=os.environ.get("POSTGRES_DWH_HOST") or "postgres",
        port=int(os.environ.get("POSTGRES_DWH_PORT") or 5432),
        dbname=os.environ.get("POSTGRES_DWH_DB") or "job_offers_dwh",
        user=os.environ.get("POSTGRES_DWH_USER") or "pfe_admin",
        password=os.environ.get("POSTGRES_DWH_PASSWORD", ""),
        connect_timeout=5,
        options="-c default_transaction_read_only=on",
    )


@st.cache_data(ttl=CACHE_TTL, show_spinner=False)
def query(sql: str, params: dict | None = None) -> pd.DataFrame:
    with _connect() as conn, conn.cursor() as cur:
        cur.execute(sql, params or {})
        cols = [c.name for c in cur.description]
        return pd.DataFrame(cur.fetchall(), columns=cols)


# ------------------------------------------------------------------ filtres communs
BASE_FROM = """
FROM gold.fact_job_offer f
JOIN gold.dim_job_family jf ON jf.job_family_key = f.job_family_key
JOIN gold.dim_location l    ON l.location_key = f.location_key
JOIN gold.dim_contract ct   ON ct.contract_key = f.contract_key
JOIN gold.dim_experience e  ON e.experience_key = f.experience_key
JOIN gold.dim_source s      ON s.source_key = f.source_key
"""
FILTRE_SQL = """
WHERE jf.in_scope
  AND (NOT %(actives)s OR f.is_active)
  AND (%(metiers)s::text[]     IS NULL OR jf.job_family_label = ANY(%(metiers)s))
  AND (%(regions)s::text[]     IS NULL OR l.region            = ANY(%(regions)s))
  AND (%(contrats)s::text[]    IS NULL OR ct.contract_label   = ANY(%(contrats)s))
  AND (%(experiences)s::text[] IS NULL OR e.experience_label  = ANY(%(experiences)s))
  AND (%(sources)s::text[]     IS NULL OR s.source_name       = ANY(%(sources)s))
"""


def filtres(metiers=None, regions=None, contrats=None, experiences=None, sources=None, actives=False) -> dict:
    """Paramètres SQL des filtres ; une liste vide signifie « pas de filtre »."""
    return {"metiers": metiers or None, "regions": regions or None, "contrats": contrats or None,
            "experiences": experiences or None, "sources": sources or None, "actives": bool(actives)}


def offres_filtrees_cte(p: dict) -> str:
    return f"WITH base AS (SELECT f.* {BASE_FROM} {FILTRE_SQL})"


# ------------------------------------------------------------------ listes
def skills() -> pd.DataFrame:
    return query("SELECT skill_name, skill_category FROM gold.dim_skill ORDER BY skill_category, skill_name")


def filter_values() -> dict[str, list[str]]:
    q = lambda sql: query(sql).iloc[:, 0].tolist()
    return {
        "metiers": q("SELECT job_family_label FROM gold.dim_job_family WHERE in_scope ORDER BY job_family_label"),
        "regions": q("SELECT DISTINCT region FROM gold.dim_location ORDER BY region"),
        "contrats": q("SELECT DISTINCT contract_label FROM gold.dim_contract ORDER BY contract_label"),
        "experiences": q("SELECT experience_label FROM gold.dim_experience ORDER BY sort_order"),
        "sources": q("SELECT source_name FROM gold.dim_source ORDER BY source_key"),
    }


def last_update():
    df = query("SELECT max(run_at) AS run_at FROM gold.etl_runs")
    return df.iloc[0, 0] if not df.empty else None


# ------------------------------------------------------------------ tableau de bord
def kpis(p: dict) -> dict:
    return query(offres_filtrees_cte(p) + """
        SELECT COUNT(*)                                                          AS offres,
               COUNT(*) FILTER (WHERE is_active)                                 AS actives,
               COUNT(*) FILTER (WHERE first_seen_key = (SELECT max(first_seen_key) FROM gold.fact_job_offer)) AS nouvelles,
               COUNT(*) FILTER (WHERE NOT is_active)                             AS disparues,
               COUNT(salary_annual_avg)                                          AS avec_salaire,
               PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY salary_annual_avg)    AS salaire_median,
               COUNT(DISTINCT company_key)                                       AS entreprises,
               COUNT(*) FILTER (WHERE contract_key IN (SELECT contract_key FROM gold.dim_contract
                                                       WHERE contract_type = 'CDI')) AS cdi
        FROM base""", p).iloc[0].to_dict()


def by_region(p: dict) -> pd.DataFrame:
    return query(offres_filtrees_cte(p) + """
        SELECT l.region, COUNT(*) AS nb_offres,
               PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY b.salary_annual_avg) AS salaire_median
        FROM base b JOIN gold.dim_location l USING (location_key)
        GROUP BY l.region ORDER BY nb_offres DESC""", p)


def by_city(p: dict) -> pd.DataFrame:
    return query(offres_filtrees_cte(p) + """
        SELECT l.city AS ville, l.region, AVG(l.latitude) AS lat, AVG(l.longitude) AS lon, COUNT(*) AS nb_offres
        FROM base b JOIN gold.dim_location l USING (location_key)
        WHERE l.latitude IS NOT NULL
        GROUP BY l.city, l.region ORDER BY nb_offres DESC""", p)


def top_skills(p: dict, n: int = 15) -> pd.DataFrame:
    return query(offres_filtrees_cte(p) + """
        SELECT sk.skill_name AS competence, sk.skill_category AS categorie, COUNT(*) AS nb_offres,
               ROUND(100.0 * COUNT(*) / NULLIF((SELECT COUNT(*) FROM base), 0), 1) AS pourcentage
        FROM gold.bridge_offer_skill bo JOIN base USING (offer_key)
        JOIN gold.dim_skill sk USING (skill_key)
        GROUP BY 1, 2 ORDER BY nb_offres DESC LIMIT %(n)s""", {**p, "n": n})


def skills_heatmap(p: dict, n_skills: int = 12) -> pd.DataFrame:
    """% des offres de chaque métier qui citent chacune des compétences les plus demandées."""
    return query(offres_filtrees_cte(p) + """,
        par_metier AS (SELECT job_family_key, COUNT(*) AS n FROM base GROUP BY job_family_key),
        top AS (SELECT skill_key FROM gold.bridge_offer_skill JOIN base USING (offer_key)
                GROUP BY skill_key ORDER BY COUNT(*) DESC LIMIT %(n)s)
        SELECT jf.job_family_label AS metier, sk.skill_name AS competence,
               ROUND(100.0 * COUNT(*) / pm.n, 1) AS pourcentage
        FROM gold.bridge_offer_skill bo JOIN base b USING (offer_key)
        JOIN top USING (skill_key)
        JOIN gold.dim_skill sk USING (skill_key)
        JOIN gold.dim_job_family jf ON jf.job_family_key = b.job_family_key
        JOIN par_metier pm ON pm.job_family_key = b.job_family_key
        WHERE pm.n >= 20
        GROUP BY jf.job_family_label, sk.skill_name, pm.n""", {**p, "n": n_skills})


def salary_by_family(p: dict) -> pd.DataFrame:
    return query(offres_filtrees_cte(p) + """
        SELECT jf.job_family_label AS metier, COUNT(*) AS nb_offres,
               PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY b.salary_annual_avg) AS q1,
               PERCENTILE_CONT(0.5)  WITHIN GROUP (ORDER BY b.salary_annual_avg) AS mediane,
               PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY b.salary_annual_avg) AS q3
        FROM base b JOIN gold.dim_job_family jf USING (job_family_key)
        WHERE b.salary_annual_avg IS NOT NULL
        GROUP BY 1 HAVING COUNT(*) >= 10 ORDER BY mediane DESC""", p)


def by_contract(p: dict) -> pd.DataFrame:
    return query(offres_filtrees_cte(p) + """
        SELECT ct.contract_label AS contrat, COUNT(*) AS nb_offres
        FROM base b JOIN gold.dim_contract ct USING (contract_key)
        GROUP BY 1 ORDER BY nb_offres DESC""", p)


def by_month(p: dict) -> pd.DataFrame:
    return query(offres_filtrees_cte(p) + """
        SELECT d.year_month AS mois, COUNT(*) AS nb_offres
        FROM base b JOIN gold.dim_date d ON d.date_key = b.date_posted_key
        WHERE d.full_date >= (current_date - INTERVAL '12 months')
        GROUP BY 1 ORDER BY 1""", p)


# ------------------------------------------------------------------ Mon Profil Matcher
MATCH_SQL = """
WITH base AS (SELECT f.* {base_from} {filtre}),
choix AS (SELECT skill_key FROM gold.dim_skill WHERE skill_name = ANY(%(skills)s)),
communes AS (SELECT bo.offer_key, COUNT(*) AS nb_communes
             FROM gold.bridge_offer_skill bo JOIN choix USING (skill_key) GROUP BY bo.offer_key),
scores AS (
    SELECT b.*, m.nb_communes,
           100 * (%(poids)s * m.nb_communes::float / %(n_profil)s
                + (1 - %(poids)s) * m.nb_communes::float / b.skills_count) AS score
    FROM base b JOIN communes m USING (offer_key)
    WHERE (%(salaire_min)s::numeric IS NULL OR b.salary_annual_max >= %(salaire_min)s)
)
"""


def _match_params(skills_choisies, p, salaire_min, poids=0.6) -> dict:
    return {**p, "skills": list(skills_choisies), "n_profil": max(len(skills_choisies), 1),
            "salaire_min": salaire_min or None, "poids": poids}


def match_offers(skills_choisies: list[str], p: dict, salaire_min=None, limite: int = 5) -> pd.DataFrame:
    if not skills_choisies:
        return pd.DataFrame()
    sql = MATCH_SQL.format(base_from=BASE_FROM, filtre=FILTRE_SQL) + """
    SELECT sc.offer_id, sc.title, c.company_name, l.city, l.region,
           ct.contract_label, e.experience_label, jf.job_family_label,
           sc.salary_annual_min, sc.salary_annual_max, d.full_date AS date_publication,
           sc.url, s.source_name, sc.skills_count, sc.nb_communes, sc.score, sc.is_active,
           ARRAY(SELECT sk.skill_name FROM gold.bridge_offer_skill b2 JOIN gold.dim_skill sk USING (skill_key)
                 WHERE b2.offer_key = sc.offer_key ORDER BY sk.skill_name) AS competences_offre
    FROM scores sc
    JOIN gold.dim_company c     ON c.company_key = sc.company_key
    JOIN gold.dim_location l    ON l.location_key = sc.location_key
    JOIN gold.dim_contract ct   ON ct.contract_key = sc.contract_key
    JOIN gold.dim_experience e  ON e.experience_key = sc.experience_key
    JOIN gold.dim_job_family jf ON jf.job_family_key = sc.job_family_key
    JOIN gold.dim_source s      ON s.source_key = sc.source_key
    JOIN gold.dim_date d        ON d.date_key = sc.date_posted_key
    ORDER BY sc.score DESC, sc.nb_communes DESC, d.full_date DESC
    LIMIT %(limite)s"""
    return query(sql, {**_match_params(skills_choisies, p, salaire_min), "limite": limite})


def match_map(skills_choisies: list[str], p: dict, salaire_min=None, score_min: float = 50) -> pd.DataFrame:
    """Offres compatibles (score >= score_min) agrégées par ville, pour la carte."""
    if not skills_choisies:
        return pd.DataFrame()
    sql = MATCH_SQL.format(base_from=BASE_FROM, filtre=FILTRE_SQL) + """
    SELECT l.city AS ville, l.region, AVG(l.latitude) AS lat, AVG(l.longitude) AS lon,
           COUNT(*) AS nb_offres, ROUND(AVG(sc.score)) AS score_moyen
    FROM scores sc JOIN gold.dim_location l ON l.location_key = sc.location_key
    WHERE sc.score >= %(score_min)s
    GROUP BY l.city, l.region"""
    return query(sql, {**_match_params(skills_choisies, p, salaire_min), "score_min": score_min})


def skills_to_learn(skills_choisies: list[str], p: dict, n: int = 8) -> pd.DataFrame:
    return query(offres_filtrees_cte(p) + """
        SELECT sk.skill_name AS competence, sk.skill_category AS categorie,
               ROUND(100.0 * COUNT(*) / NULLIF((SELECT COUNT(*) FROM base), 0), 1) AS pourcentage
        FROM gold.bridge_offer_skill bo JOIN base USING (offer_key)
        JOIN gold.dim_skill sk USING (skill_key)
        WHERE NOT (sk.skill_name = ANY(%(skills)s))
        GROUP BY 1, 2 ORDER BY COUNT(*) DESC LIMIT %(n)s""", {**p, "skills": list(skills_choisies), "n": n})


def by_family(p: dict) -> pd.DataFrame:
    return query(offres_filtrees_cte(p) + """
        SELECT jf.job_family_label AS metier, COUNT(*) AS nb_offres
        FROM base b JOIN gold.dim_job_family jf USING (job_family_key)
        GROUP BY 1 ORDER BY nb_offres DESC""", p)


def by_skill_category(p: dict) -> pd.DataFrame:
    """Part des offres qui citent au moins une compétence de chaque catégorie."""
    return query(offres_filtrees_cte(p) + """
        SELECT sk.skill_category AS categorie,
               ROUND(100.0 * COUNT(DISTINCT bo.offer_key) / NULLIF((SELECT COUNT(*) FROM base), 0), 1) AS pourcentage
        FROM gold.bridge_offer_skill bo JOIN base USING (offer_key)
        JOIN gold.dim_skill sk USING (skill_key)
        GROUP BY 1 ORDER BY pourcentage DESC""", p)
