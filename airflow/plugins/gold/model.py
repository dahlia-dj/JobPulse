"""
Construction du schéma en étoile à partir de la table Silver (PySpark).

Entrée  : DataFrame Silver (une ligne par offre unique, 36 colonnes).
Sortie  : un DataFrame par table Gold, colonnes dans l'ordre du DDL (schema.py).

Clés :
- petites dimensions (source, contrat, famille, expérience, études, compétence) :
  clés entières fixes, issues de listes de référence ;
- offre, entreprise, localisation : empreinte 64 bits (xxhash64) de la clé naturelle.
  Une même offre garde donc la même clé d'un chargement à l'autre, même en
  reconstruction complète.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

NOT_SET = "Non renseignée"

SOURCES = [  # code, nom affiché, méthode d'accès
    ("france_travail", "France Travail", "API"),
    ("adzuna", "Adzuna", "API"),
    ("hellowork", "HelloWork", "Scraping"),
    ("freework", "Free-Work", "Scraping"),
]
CONTRACTS = [
    ("CDI", "CDI"), ("CDD", "CDD"), ("FREELANCE", "Freelance"), ("INTERIM", "Intérim"),
    ("STAGE", "Stage"), ("ALTERNANCE", "Alternance"), ("AUTRE", "Autre"), ("INCONNU", "Non précisé"),
]
WORK_TIMES = [("FULL_TIME", "Temps plein"), ("PART_TIME", "Temps partiel"), ("INCONNU", "Non précisé")]
JOB_FAMILIES = [  # code, libellé, dans le périmètre
    ("DATA_ENGINEER", "Data Engineer", True),
    ("DATA_ANALYST", "Data Analyst", True),
    ("DATA_SCIENTIST", "Data Scientist", True),
    ("ML_IA", "Machine Learning / IA", True),
    ("DATA_ARCHITECT", "Architecte Data", True),
    ("BI", "Business Intelligence", True),
    ("DATA_GOVERNANCE", "Gouvernance des données", True),
    ("MANAGEMENT", "Management / pilotage", True),
    ("CONSULTANT_DATA", "Consultant Data", True),
    ("AUTRE", "Autre", True),
    ("HORS_PERIMETRE", "Hors périmètre", False),
]
EXPERIENCE = [
    ("JUNIOR", "Junior (0 à 2 ans)", 1), ("CONFIRME", "Confirmé (3 à 5 ans)", 2),
    ("SENIOR", "Senior (6 ans et plus)", 3), ("INCONNU", "Non précisé", 9),
]
EDUCATION = [
    ("BAC+2", "Bac+2", 2), ("BAC+3", "Bac+3", 3), ("BAC+4", "Bac+4", 4),
    ("BAC+5", "Bac+5", 5), ("DOCTORAT", "Doctorat", 8), ("INCONNU", "Non précisé", 9),
]
SKILL_CATEGORIES = {
    "LANGAGE": "Langages", "BASE_DE_DONNEES": "Bases de données", "BIG_DATA": "Big Data",
    "CLOUD": "Cloud", "ETL_ORCHESTRATION": "ETL et orchestration", "BI_VISUALISATION": "BI et visualisation",
    "ML_IA": "Machine learning et IA", "DEVOPS": "DevOps", "GOUVERNANCE_QUALITE": "Gouvernance et qualité",
    "METHODOLOGIE": "Méthodologie et ERP",
}
MONTHS = ["janvier", "février", "mars", "avril", "mai", "juin", "juillet", "août",
          "septembre", "octobre", "novembre", "décembre"]
DAYS = ["lundi", "mardi", "mercredi", "jeudi", "vendredi", "samedi", "dimanche"]


def _with_unknowns(ref: list[tuple], silver: DataFrame, col: str, tail: tuple) -> list[tuple]:
    """Ajoute à une liste de référence les éventuelles valeurs Silver inconnues
    (le chargement ne doit jamais échouer sur une nouvelle valeur)."""
    known = {r[0] for r in ref}
    extra = [r[col] for r in silver.select(col).distinct().collect() if r[col] is not None and r[col] not in known]
    return ref + [(v, v, *tail) for v in sorted(extra)]


def _small_dim(spark, rows, columns, key):
    data = [(i + 1, *r) for i, r in enumerate(rows)]
    return spark.createDataFrame(data, [key, *columns])


def dim_date(spark: SparkSession, silver: DataFrame) -> DataFrame:
    bounds = silver.select(
        F.least(F.min(F.coalesce(F.to_date("date_posted"), F.col("first_seen"))), F.min("first_seen")).alias("lo"),
        F.max(F.greatest(F.to_date("date_posted"), F.col("last_seen"))).alias("hi"),
    ).first()
    days = spark.createDataFrame([(bounds["lo"], bounds["hi"])], "lo date, hi date") \
        .select(F.explode(F.sequence("lo", "hi")).alias("d"))
    month_name = F.element_at(F.array(*[F.lit(m) for m in MONTHS]), F.month("d"))
    dow = ((F.dayofweek("d") + 5) % 7) + 1  # Spark : 1 = dimanche ; ici 1 = lundi
    return days.select(
        F.date_format("d", "yyyyMMdd").cast("int").alias("date_key"),
        F.col("d").alias("full_date"),
        F.dayofmonth("d").cast("short").alias("day"),
        dow.cast("short").alias("day_of_week"),
        F.element_at(F.array(*[F.lit(x) for x in DAYS]), dow).alias("day_name"),
        F.weekofyear("d").cast("short").alias("week_of_year"),
        F.month("d").cast("short").alias("month"),
        month_name.alias("month_name"),
        F.quarter("d").cast("short").alias("quarter"),
        F.year("d").cast("short").alias("year"),
        F.date_format("d", "yyyy-MM").alias("year_month"),
        (dow >= 6).alias("is_weekend"),
    )


def _location_key(city, dept, region):
    return F.xxhash64(F.coalesce(city, F.lit("")), F.coalesce(dept, F.lit("")), F.coalesce(region, F.lit("")))


def _company_key(norm_name):
    """Clé d'entreprise = empreinte du nom NORMALISÉ (minuscules, sans accents,
    ponctuation ni forme juridique) : « Thales », « THALES » et « THALES SA »
    partagent la même clé."""
    return F.xxhash64(F.coalesce(norm_name, F.lit("")))


def _with_company_norm(silver: DataFrame) -> DataFrame:
    from silver.parsing import norm_company  # même normalisation que le dédoublonnage Silver
    norm_udf = F.udf(lambda s: norm_company(s) if s else "", T.StringType())
    return silver.withColumn("company_norm", norm_udf(F.col("company")))


def dim_company(silver: DataFrame) -> DataFrame:
    """Une ligne par entreprise normalisée. Nom affiché : la variante la plus
    fréquente dans les offres (à égalité, la première par ordre alphabétique)."""
    from pyspark.sql import Window
    counts = silver.groupBy("company_norm", "company").count()
    best = Window.partitionBy("company_norm").orderBy(F.col("count").desc(), F.col("company").asc_nulls_last())
    return (
        counts.withColumn("rk", F.row_number().over(best)).filter("rk = 1")
        .select(
            _company_key(F.col("company_norm")).alias("company_key"),
            F.when(F.col("company_norm") == "", F.lit(NOT_SET)).otherwise(F.col("company")).alias("company_name"),
        )
    )


def build_star_schema(spark: SparkSession, silver: DataFrame) -> dict[str, DataFrame]:
    from silver.enrich import SKILL_CATALOG  # même catalogue que l'extraction

    tables: dict[str, DataFrame] = {}
    tables["dim_date"] = dim_date(spark, silver)

    sources = _with_unknowns(SOURCES, silver, "source", ("API",))
    tables["dim_source"] = _small_dim(spark, sources, ["source_code", "source_name", "access_method"], "source_key")

    contracts = _with_unknowns(CONTRACTS, silver, "contract_type", ())
    work_times = _with_unknowns(WORK_TIMES, silver, "work_time", ())
    combos = [(c, cl, w, wl) for c, cl in contracts for w, wl in work_times]
    tables["dim_contract"] = _small_dim(
        spark, combos, ["contract_type", "contract_label", "work_time", "work_time_label"], "contract_key")

    families = _with_unknowns(JOB_FAMILIES, silver, "job_family", (True,))
    tables["dim_job_family"] = _small_dim(
        spark, families, ["job_family_code", "job_family_label", "in_scope"], "job_family_key")

    exp = _with_unknowns(EXPERIENCE, silver, "experience_level", (9,))
    tables["dim_experience"] = _small_dim(
        spark, exp, ["experience_level", "experience_label", "sort_order"], "experience_key")

    edu = _with_unknowns(EDUCATION, silver, "education_level", (9,))
    tables["dim_education"] = _small_dim(
        spark, edu, ["education_level", "education_label", "sort_order"], "education_key")

    # Coordonnées d'une ville : médiane des coordonnées fournies par les offres de cette ville
    # (France Travail, Adzuna). Les offres HelloWork / Free-Work d'une même ville en profitent.
    coords = (
        silver.where(F.col("city").isNotNull() & F.col("latitude").isNotNull() & F.col("longitude").isNotNull())
        .groupBy("city", "region")
        .agg(F.percentile_approx("latitude", 0.5).alias("latitude"),
             F.percentile_approx("longitude", 0.5).alias("longitude"))
    )
    locs = silver.select("city", "department_code", "region").distinct().alias("l")
    tables["dim_location"] = (
        locs.join(coords.alias("c"),
                  (F.col("l.city") == F.col("c.city")) & F.col("l.region").eqNullSafe(F.col("c.region")), "left")
        .select(
            _location_key(F.col("l.city"), F.col("l.department_code"), F.col("l.region")).alias("location_key"),
            F.col("l.city").alias("city"), F.col("l.department_code").alias("department_code"),
            F.coalesce(F.col("l.region"), F.lit(NOT_SET)).alias("region"),
            F.lit("France").alias("country"),
            F.round("c.latitude", 5).alias("latitude"), F.round("c.longitude", 5).alias("longitude"),
        )
    )
    silver = _with_company_norm(silver)
    tables["dim_company"] = dim_company(silver)

    catalog = sorted(SKILL_CATALOG.items(), key=lambda kv: (kv[1][0], kv[0].lower()))
    skill_rows = [(name, SKILL_CATEGORIES.get(cat, cat)) for name, (cat, _) in catalog]
    seen = {s[0] for s in skill_rows}  # compétence hors catalogue (ne devrait pas arriver)
    extra = sorted({r["s"] for r in silver.select(F.explode("skills").alias("s")).distinct().collect()} - seen)
    skill_rows += [(s, "Autre") for s in extra]
    tables["dim_skill"] = _small_dim(spark, skill_rows, ["skill_name", "skill_category"], "skill_key")

    # ------------------------------------------------------------ table de faits
    def lookup(dim: str, *pairs) -> DataFrame:
        d = tables[dim]
        return d.select(*[d[c].alias(a) for c, a in pairs])

    f = (
        silver
        .join(F.broadcast(lookup("dim_source", ("source_key", "source_key"), ("source_code", "source"))), "source", "left")
        .join(F.broadcast(lookup("dim_contract", ("contract_key", "contract_key"), ("contract_type", "contract_type"),
                                 ("work_time", "work_time"))), ["contract_type", "work_time"], "left")
        .join(F.broadcast(lookup("dim_job_family", ("job_family_key", "job_family_key"),
                                 ("job_family_code", "job_family"))), "job_family", "left")
        .join(F.broadcast(lookup("dim_experience", ("experience_key", "experience_key"),
                                 ("experience_level", "experience_level"))), "experience_level", "left")
        .join(F.broadcast(lookup("dim_education", ("education_key", "education_key"),
                                 ("education_level", "education_level"))), "education_level", "left")
    )
    lo, hi = F.col("salary_annual_min"), F.col("salary_annual_max")
    date_key = lambda c: F.date_format(c, "yyyyMMdd").cast("int")
    tables["fact_job_offer"] = f.select(
        F.xxhash64("offer_id").alias("offer_key"),
        "offer_id", "source_key", "contract_key", "job_family_key", "experience_key", "education_key",
        _location_key(F.col("city"), F.col("department_code"), F.col("region")).alias("location_key"),
        _company_key(F.col("company_norm")).alias("company_key"),
        date_key(F.coalesce(F.to_date("date_posted"), F.col("first_seen"))).alias("date_posted_key"),
        date_key("first_seen").alias("first_seen_key"),
        date_key("last_seen").alias("last_seen_key"),
        "title", "url",
        F.round(lo, 2).alias("salary_annual_min"),
        F.round(hi, 2).alias("salary_annual_max"),
        F.round(F.when(lo.isNotNull() & hi.isNotNull(), (lo + hi) / 2).otherwise(F.coalesce(lo, hi)), 2)
         .alias("salary_annual_avg"),
        F.col("experience_years_min").cast("short").alias("experience_years_min"),
        F.coalesce(F.size("skills"), F.lit(0)).cast("short").alias("skills_count"),
        F.col("snapshots_seen").cast("short").alias("snapshots_seen"),
        (F.datediff("last_seen", "first_seen") + 1).alias("days_online"),
        F.coalesce("is_active", F.lit(False)).alias("is_active"),
        F.coalesce("is_merged", F.lit(False)).alias("is_merged"),
        F.coalesce("contract_inferred", F.lit(False)).alias("contract_inferred"),
    )

    skills = tables["dim_skill"].select("skill_key", F.col("skill_name").alias("skill"))
    tables["bridge_offer_skill"] = (
        silver.select(F.xxhash64("offer_id").alias("offer_key"), F.explode("skills").alias("skill"))
        .join(F.broadcast(skills), "skill", "inner")
        .select("offer_key", "skill_key").distinct()
    )
    return tables