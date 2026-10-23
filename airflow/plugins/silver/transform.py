"""
Transformation Bronze -> Silver (PySpark).

Entrée : les fichiers JSON Bronze téléchargés localement, un dossier par
source (`{bronze_root}/{source}/date=.../{source}_{AAAAMMJJ}_{HHMMSS}.json`).
Sortie : une table `job_offers` au schéma commun aux 4 sources, une ligne par
offre réelle.

Étapes (voir l'annexe G du cahier des charges) :
  1. lecture de tous les instantanés Bronze d'une source (mode complet) ;
  2. mapping de chaque source vers le schéma commun (titre, entreprise, lieu,
     contrat, salaire, dates, description nettoyée) ;
  3. déduplication dans une même source : clé (source, id), on garde la
     ligne de l'instantané le plus récent, avec first_seen / last_seen /
     is_active ;
  4. fusion des doublons entre sources : même titre + entreprise + ville
     (normalisés) vus sur au moins 2 sources -> une seule ligne.
"""
from __future__ import annotations

import logging

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from . import parsing as P

logger = logging.getLogger(__name__)

SOURCES = ("france_travail", "adzuna", "hellowork", "freework")
# En cas de fusion, source préférée pour les champs texte (description la plus riche d'abord,
# puis ce rang pour départager).
SOURCE_RANK = {"france_travail": 1, "hellowork": 2, "freework": 3, "adzuna": 4}

LOCATION_T = T.StructType([
    T.StructField("city", T.StringType()),
    T.StructField("department_code", T.StringType()),
    T.StructField("region", T.StringType()),
])
CONTRACT_T = T.StructType([T.StructField("contract_type", T.StringType()), T.StructField("work_time", T.StringType())])
SALARY_T = T.StructType([
    T.StructField("min", T.DoubleType()),
    T.StructField("max", T.DoubleType()),
    T.StructField("period", T.StringType()),
    T.StructField("period_corrected", T.BooleanType()),
])

# --- UDF (le volume, quelques milliers d'offres par jour, rend un UDF Python largement suffisant) ---
clean_description_udf = F.udf(P.clean_description, T.StringType())
clean_title_udf = F.udf(P.clean_title, T.StringType())
norm_title_udf = F.udf(P.norm_title, T.StringType())
norm_company_udf = F.udf(P.norm_company, T.StringType())
norm_text_udf = F.udf(P.norm_text, T.StringType())
annualize_udf = F.udf(P.annualize, T.DoubleType())
source_id_udf = F.udf(P.source_id_from_url, T.StringType())
loc_text_udf = F.udf(lambda s: tuple(P.parse_location_text(s).values()), LOCATION_T)
loc_ft_udf = F.udf(lambda a, b, c: tuple(P.parse_location_ft(a, b, c).values()), LOCATION_T)
loc_adzuna_udf = F.udf(lambda a, b: tuple(P.parse_location_adzuna(a, b).values()), LOCATION_T)
contract_schemaorg_udf = F.udf(lambda v: P.map_contract_schemaorg(v), CONTRACT_T)
contract_ft_udf = F.udf(lambda a, b, c, d: P.map_contract_ft(a, b, c, d), CONTRACT_T)
from . import enrich as EN

skills_udf = F.udf(lambda t, d: EN.extract_skills(t, d), T.ArrayType(T.StringType()))
exp_years_udf = F.udf(lambda d, e: EN.extract_experience_years(d, e), T.IntegerType())
exp_level_udf = F.udf(lambda y, t, c: EN.experience_level(y, t, c), T.StringType())
education_udf = F.udf(lambda d: EN.extract_education_level(d), T.StringType())
job_family_udf = F.udf(lambda t: EN.classify_job_family(t), T.StringType())
infer_contract_udf = F.udf(lambda t, d: P.infer_contract_from_text(t, d), T.StringType())
contract_adzuna_udf = F.udf(lambda a, b: P.map_contract_adzuna(a, b), CONTRACT_T)
salary_ft_udf = F.udf(
    lambda s: (lambda r: (r.get("min"), r.get("max"), r.get("period"), r.get("period_corrected")))(P.parse_salary_ft(s)),
    SALARY_T,
)
salary_schemaorg_udf = F.udf(
    lambda u, lo, hi, v: (lambda r: (r.get("min"), r.get("max"), r.get("period"), r.get("period_corrected")))(
        P.parse_salary_schemaorg(u, lo, hi, v)
    ),
    SALARY_T,
)


def _has(df: DataFrame, path: str) -> bool:
    """Vrai si la colonne (éventuellement imbriquée, ex. « salary.value.unitText ») existe :
    le schéma Bronze peut évoluer d'un instantané à l'autre."""
    fields, dtype = df.schema.fields, None
    for i, part in enumerate(path.split(".")):
        match = next((f for f in fields if f.name == part), None)
        if match is None:
            return False
        dtype = match.dataType
        fields = dtype.fields if isinstance(dtype, T.StructType) else []
        if i < len(path.split(".")) - 1 and not isinstance(dtype, T.StructType):
            return False
    return True


def _col(df: DataFrame, path: str, dtype: str = "string") -> F.Column:
    """Colonne si elle existe, sinon NULL (même type) : une source qui perd un champ ne casse pas Silver."""
    if not _has(df, path):
        return F.lit(None).cast(dtype)
    column = F.col(".".join(f"`{p}`" for p in path.split(".")))
    return column.cast(dtype)


def read_bronze(spark: SparkSession, bronze_root: str, source: str) -> DataFrame | None:
    """Lit tous les instantanés d'une source et ajoute `source_file` et `ingestion_ts`
    (UTC, lu dans le nom du fichier : {source}_{AAAAMMJJ}_{HHMMSS}.json)."""
    path = f"{bronze_root}/{source}/*/*.json"
    try:
        df = spark.read.option("multiLine", True).json(path)
    except Exception as exc:  # AnalysisException : aucun fichier pour cette source
        logger.warning("Aucun fichier Bronze lisible pour %s (%s)", source, str(exc)[:120])
        return None
    if not df.columns:
        return None
    df = df.withColumn("source_file", F.input_file_name())
    stamp = F.regexp_extract("source_file", r"_(\d{8})_(\d{6})\.json$", 0)
    stamp = F.regexp_replace(F.regexp_replace(stamp, r"\.json$", ""), r"^_", "")
    return df.withColumn("ingestion_ts", F.to_timestamp(F.regexp_replace(stamp, "_", ""), "yyyyMMddHHmmss"))


def _common(df: DataFrame, source: str, **cols: F.Column) -> DataFrame:
    """Colonnes communes, toujours dans le même ordre et avec les mêmes types."""
    spec = {
        "source_id": "string", "title": "string", "title_raw": "string", "company": "string", "description_raw": "string",
        "city": "string", "department_code": "string", "region": "string",
        "latitude": "double", "longitude": "double",
        "contract_type": "string", "work_time": "string",
        "salary_min": "double", "salary_max": "double", "salary_period": "string",
        "salary_period_corrected": "boolean", "salary_estimated": "boolean",
        "rome_code": "string", "rome_label": "string", "sector": "string", "experience": "string",
        "skills_declared": "array<string>",
        "date_posted": "timestamp", "date_updated": "timestamp", "valid_through": "timestamp",
        "url": "string",
    }
    selected = [F.lit(source).alias("source")]
    for name, dtype in spec.items():
        selected.append(cols.get(name, F.lit(None)).cast(dtype).alias(name))
    return df.select(*selected, "ingestion_ts", "source_file")


def map_adzuna(df: DataFrame) -> DataFrame:
    loc = loc_adzuna_udf(_col(df, "location.area", "array<string>"), _col(df, "location.display_name"))
    contract = contract_adzuna_udf(_col(df, "contract_type"), _col(df, "contract_time"))
    label = _col(df, "category.label")
    return _common(
        df, "adzuna",
        source_id=_col(df, "id"), title=clean_title_udf(_col(df, "title")), title_raw=_col(df, "title"),
        company=_col(df, "company.display_name"),
        description_raw=_col(df, "description"),
        city=loc["city"], department_code=loc["department_code"], region=loc["region"],
        latitude=_col(df, "latitude", "double"), longitude=_col(df, "longitude", "double"),
        contract_type=contract["contract_type"], work_time=contract["work_time"],
        salary_min=_col(df, "salary_min", "double"), salary_max=_col(df, "salary_max", "double"),
        salary_period=F.when(_col(df, "salary_min", "double").isNotNull(), F.lit("YEAR")),  # Adzuna : montants annuels
        salary_period_corrected=F.lit(False),
        salary_estimated=_col(df, "salary_is_predicted") == "1",
        sector=F.when(label != "Unknown", label),
        date_posted=_col(df, "created", "timestamp"), url=_col(df, "redirect_url"),
    )


def map_france_travail(df: DataFrame) -> DataFrame:
    loc = loc_ft_udf(_col(df, "lieuTravail.libelle"), _col(df, "lieuTravail.codePostal"), _col(df, "lieuTravail.commune"))
    contract = contract_ft_udf(_col(df, "typeContrat"), _col(df, "natureContrat"), _col(df, "alternance", "boolean"),
                               _col(df, "dureeTravailLibelle"))
    salary = salary_ft_udf(_col(df, "salaire.libelle"))
    # competences = tableau d'objets {code, exigence, libelle} : F.col("competences.libelle") donne la liste des libellés
    skills = F.col("competences.libelle") if _has(df, "competences") else F.lit(None)
    return _common(
        df, "france_travail",
        source_id=_col(df, "id"), title=clean_title_udf(_col(df, "intitule")), title_raw=_col(df, "intitule"),
        company=_col(df, "entreprise.nom"),
        description_raw=_col(df, "description"),
        city=loc["city"], department_code=loc["department_code"], region=loc["region"],
        latitude=_col(df, "lieuTravail.latitude", "double"), longitude=_col(df, "lieuTravail.longitude", "double"),
        contract_type=contract["contract_type"], work_time=contract["work_time"],
        salary_min=salary["min"], salary_max=salary["max"], salary_period=salary["period"],
        salary_period_corrected=salary["period_corrected"], salary_estimated=F.lit(False),
        rome_code=_col(df, "romeCode"), rome_label=_col(df, "romeLibelle"),
        sector=_col(df, "secteurActiviteLibelle"), experience=_col(df, "experienceLibelle"),
        skills_declared=skills,
        date_posted=_col(df, "dateCreation", "timestamp"), date_updated=_col(df, "dateActualisation", "timestamp"),
        url=_col(df, "origineOffre.urlOrigine"),
    )


def _map_scraped(df: DataFrame, source: str) -> DataFrame:
    """HelloWork et Free-Work : même format (extrait du JSON-LD JobPosting)."""
    loc = loc_text_udf(_col(df, "location"))
    contract = contract_schemaorg_udf(_col(df, "contract_type"))
    salary = salary_schemaorg_udf(
        _col(df, "salary.value.unitText"), _col(df, "salary.value.minValue", "double"),
        _col(df, "salary.value.maxValue", "double"), _col(df, "salary.value.value"),
    )
    company = _col(df, "company")
    return _common(
        df, source,
        source_id=source_id_udf(F.lit(source), _col(df, "link")), title=clean_title_udf(_col(df, "title")),
        title_raw=_col(df, "title"),
        company=F.when(F.upper(F.trim(company)) != "N/A", F.trim(company)),
        description_raw=_col(df, "description"),
        city=loc["city"], department_code=loc["department_code"], region=loc["region"],
        contract_type=contract["contract_type"], work_time=contract["work_time"],
        salary_min=salary["min"], salary_max=salary["max"], salary_period=salary["period"],
        salary_period_corrected=salary["period_corrected"], salary_estimated=F.lit(False),
        date_posted=_col(df, "date_posted", "timestamp"), valid_through=_col(df, "valid_through", "timestamp"),
        url=_col(df, "link"),
    )


MAPPERS = {
    "france_travail": map_france_travail,
    "adzuna": map_adzuna,
    "hellowork": lambda df: _map_scraped(df, "hellowork"),
    "freework": lambda df: _map_scraped(df, "freework"),
}


def build_snapshot_rows(spark: SparkSession, bronze_root: str) -> tuple[DataFrame, dict]:
    """Toutes les lignes de tous les instantanés Bronze, au schéma commun, nettoyées."""
    frames, report = [], {}
    for source in SOURCES:
        raw = read_bronze(spark, bronze_root, source)
        if raw is None:
            report[source] = {"bronze_rows": 0}
            continue
        mapped = MAPPERS[source](raw)
        report[source] = {"bronze_rows": mapped.count()}
        frames.append(mapped)
    if not frames:
        raise RuntimeError("Aucune donnée Bronze trouvée : rien à transformer.")
    df = frames[0]
    for other in frames[1:]:
        df = df.unionByName(other)

    df = df.withColumn("description", clean_description_udf("description_raw")).drop("description_raw")
    df = df.withColumn("has_description", F.length("description") >= 50)
    # Contrat déduit du titre / début de description quand la source ne le donne pas.
    inferred = infer_contract_udf("title_raw", "description")  # titre d'origine : « Stage - … » y est encore
    df = (df.withColumn("contract_inferred", (F.col("contract_type") == "INCONNU") & inferred.isNotNull())
            .withColumn("contract_type", F.when(F.col("contract_inferred"), inferred).otherwise(F.col("contract_type"))))
    # Lignes inexploitables (offre expirée écrite avec des « N/A » par d'anciennes versions des connecteurs).
    before = df.count()
    df = df.filter(F.col("title").isNotNull() & F.col("source_id").isNotNull())
    report["rows_without_title_dropped"] = before - df.count()
    df = (df.withColumn("title_norm", norm_title_udf("title"))
            .withColumn("company_norm", norm_company_udf("company"))
            .withColumn("city_norm", norm_text_udf("city")))
    return df, report


def dedup_within_source(df: DataFrame) -> DataFrame:
    """Une ligne par (source, source_id) : celle du dernier instantané, avec
    first_seen / last_seen (dates) et is_active (offre présente dans le
    dernier instantané de sa source)."""
    key = Window.partitionBy("source", "source_id")
    ranked = Window.partitionBy("source", "source_id").orderBy(F.col("ingestion_ts").desc())
    latest_of_source = F.max("ingestion_ts").over(Window.partitionBy("source"))
    return (
        df.withColumn("first_seen", F.to_date(F.min("ingestion_ts").over(key)))
          .withColumn("last_seen", F.to_date(F.max("ingestion_ts").over(key)))
          .withColumn("is_active", F.max("ingestion_ts").over(key) == latest_of_source)
          .withColumn("snapshots_seen", F.count("*").over(key))
          .withColumn("_rn", F.row_number().over(ranked))
          .filter("_rn = 1").drop("_rn")
    )


def merge_across_sources(df: DataFrame) -> DataFrame:
    """Fusionne les offres présentes sur au moins 2 sources : même titre, entreprise
    et ville normalisés (l'entreprise doit être connue). La ligne conservée est celle
    qui a la description la plus longue ; salaire et contrat manquants sont repris des
    autres sources. Les offres sans entreprise ou vues sur une seule source ne sont pas fusionnées."""
    rank_expr = F.create_map(*[x for s, r in SOURCE_RANK.items() for x in (F.lit(s), F.lit(r))])
    df = df.withColumn("_source_rank", rank_expr[F.col("source")])
    mergeable = F.col("company_norm") != ""
    df = df.withColumn("dedup_key", F.when(mergeable, F.concat_ws("|", "title_norm", "company_norm", "city_norm")))
    group = Window.partitionBy("dedup_key")
    ordered = group.orderBy(F.length("description").desc(), F.col("_source_rank").asc(), F.col("source_id").asc())
    full = ordered.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

    df = (
        df.withColumn("_n_sources", F.size(F.collect_set("source").over(group)))
          .withColumn("sources", F.when(F.col("dedup_key").isNotNull(), F.array_sort(F.collect_set("source").over(group)))
                                   .otherwise(F.array("source")))
          .withColumn("urls", F.when(F.col("dedup_key").isNotNull(), F.array_sort(F.collect_set("url").over(group)))
                                .otherwise(F.array("url")))
          .withColumn("first_seen", F.when(F.col("dedup_key").isNotNull(), F.min("first_seen").over(group)).otherwise(F.col("first_seen")))
          .withColumn("last_seen", F.when(F.col("dedup_key").isNotNull(), F.max("last_seen").over(group)).otherwise(F.col("last_seen")))
          .withColumn("is_active", F.when(F.col("dedup_key").isNotNull(), F.max(F.col("is_active").cast("int")).over(group) == 1)
                                    .otherwise(F.col("is_active")))
    )
    # Valeurs manquantes complétées par les autres sources du groupe (salaire, contrat).
    for name in ("salary_min", "salary_max", "salary_period", "salary_estimated"):
        df = df.withColumn(name, F.coalesce(F.col(name), F.first(name, ignorenulls=True).over(full)))
    df = df.withColumn("contract_type", F.when(
        (F.col("contract_type") == "INCONNU") & F.col("dedup_key").isNotNull(),
        F.coalesce(F.first(F.when(F.col("contract_type") != "INCONNU", F.col("contract_type")), ignorenulls=True).over(full),
                   F.col("contract_type"))).otherwise(F.col("contract_type")))
    df = df.withColumn("_rn", F.when(F.col("_n_sources") > 1, F.row_number().over(ordered)).otherwise(F.lit(1)))
    # Dans un groupe multi-sources, une seule ligne ; sinon toutes les lignes (offres distinctes de même titre).
    return df.filter("_rn = 1").withColumn("is_merged", F.col("_n_sources") > 1).drop("_rn", "_n_sources", "_source_rank")


FINAL_COLUMNS = [
    # identité
    "offer_id", "source", "url",
    # contenu
    "title", "company", "description",
    # lieu
    "city", "department_code", "region", "latitude", "longitude",
    # contrat
    "contract_type", "contract_inferred", "work_time",
    # salaire
    "salary_min", "salary_max", "salary_period", "salary_currency",
    "salary_annual_min", "salary_annual_max",
    # classification (renseignée par certaines sources seulement)
    "rome_code", "rome_label", "sector",
    # enrichissement (spaCy + règles)
    "skills", "job_family", "experience_years_min", "experience_level", "education_level",
    # dates et suivi
    "date_posted", "date_updated", "valid_through",
    "first_seen", "last_seen", "is_active", "snapshots_seen", "is_merged",
]


def enrich(df: DataFrame) -> DataFrame:
    """Enrichissement spaCy / règles, appliqué UNE fois par offre unique (après dédoublonnage)."""
    return (
        df.withColumn("skills", skills_udf("title", "description"))
          .withColumn("experience_years_min", exp_years_udf("description", "experience"))
          .withColumn("experience_level", exp_level_udf("experience_years_min", "title", "contract_type"))
          .withColumn("education_level", education_udf("description"))
          .withColumn("job_family", job_family_udf("title"))
    )


def finalize(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("offer_id", F.sha1(F.concat_ws("|", "source", "source_id")))
          .withColumn("salary_currency", F.when(F.col("salary_min").isNotNull(), F.lit("EUR")))
          .withColumn("salary_annual_min", annualize_udf("salary_min", "salary_period"))
          .withColumn("salary_annual_max", annualize_udf("salary_max", "salary_period"))
          .select(*FINAL_COLUMNS)
    )


def build_silver(spark: SparkSession, bronze_root: str) -> tuple[DataFrame, dict]:
    snapshots, report = build_snapshot_rows(spark, bronze_root)
    unique_by_source = dedup_within_source(snapshots)
    report["unique_offers_within_sources"] = unique_by_source.count()
    merged = merge_across_sources(unique_by_source)
    silver = finalize(enrich(merged))
    return silver, report