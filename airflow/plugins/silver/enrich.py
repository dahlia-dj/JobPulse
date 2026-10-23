"""
Enrichissement des offres (étape spaCy de la couche Silver).

Appliqué UNE SEULE FOIS par offre, après le dédoublonnage (annexe G du CDC).
Quatre enrichissements, tous à base de règles explicites et vérifiables :

  1. skills               compétences techniques normalisées (spaCy PhraseMatcher
                          + catalogue SKILL_CATALOG, réutilisable pour dim_skill du Gold)
  2. experience_*         années d'expérience demandées + niveau JUNIOR/CONFIRME/SENIOR
  3. education_level      niveau d'études minimum demandé (BAC+2 ... DOCTORAT)
  4. job_family           famille de métier déduite du titre

Pas de modèle statistique à télécharger : on utilise le tokenizer français de
spaCy (`spacy.blank("fr")`) et son PhraseMatcher. Résultat déterministe,
rapide, et expliquable en soutenance.
"""
from __future__ import annotations

import re

from .parsing import norm_text, unaccent

# --------------------------------------------------------------------------
# 1. Compétences
# --------------------------------------------------------------------------
# canonique -> (catégorie, [variantes écrites dans les annonces])
# Les variantes sont comparées sans tenir compte des majuscules.
# Volontairement ABSENTS car trop ambigus : « R » seul, « C » seul, « Go » seul,
# « SAS » seul (forme juridique), « Fabric » seul, « Synapse » seul.
SKILL_CATALOG: dict[str, tuple[str, list[str]]] = {
    # Langages
    "Python": ("LANGAGE", ["python", "python3"]),
    "SQL": ("LANGAGE", ["sql", "t-sql", "tsql", "pl/sql", "plsql"]),
    "Java": ("LANGAGE", ["java"]),
    "Scala": ("LANGAGE", ["scala"]),
    "R": ("LANGAGE", ["langage r", "rstudio", "r studio", "r shiny"]),
    "JavaScript": ("LANGAGE", ["javascript", "typescript"]),
    "C++": ("LANGAGE", ["c++"]),
    "C#": ("LANGAGE", ["c#"]),
    "Go": ("LANGAGE", ["golang", "langage go"]),
    "Bash": ("LANGAGE", ["bash", "shell scripting", "scripts shell"]),
    "VBA": ("LANGAGE", ["vba"]),
    "DAX": ("LANGAGE", ["dax"]),
    "SAS": ("LANGAGE", ["sas viya", "sas base", "sas enterprise guide", "programmation sas"]),
    # Bases de données
    "PostgreSQL": ("BASE_DE_DONNEES", ["postgresql", "postgres"]),
    "MySQL": ("BASE_DE_DONNEES", ["mysql", "mariadb"]),
    "SQL Server": ("BASE_DE_DONNEES", ["sql server", "mssql", "ms sql"]),
    "Oracle": ("BASE_DE_DONNEES", ["oracle"]),
    "MongoDB": ("BASE_DE_DONNEES", ["mongodb", "mongo db"]),
    "NoSQL": ("BASE_DE_DONNEES", ["nosql"]),
    "Cassandra": ("BASE_DE_DONNEES", ["cassandra"]),
    "Neo4j": ("BASE_DE_DONNEES", ["neo4j"]),
    "Redis": ("BASE_DE_DONNEES", ["redis"]),
    "Elasticsearch": ("BASE_DE_DONNEES", ["elasticsearch", "elastic search", "opensearch"]),
    "ClickHouse": ("BASE_DE_DONNEES", ["clickhouse"]),
    # Big Data
    "Spark": ("BIG_DATA", ["spark", "pyspark", "apache spark", "spark sql"]),
    "Hadoop": ("BIG_DATA", ["hadoop", "hdfs"]),
    "Hive": ("BIG_DATA", ["hive"]),
    "Kafka": ("BIG_DATA", ["kafka"]),
    "Flink": ("BIG_DATA", ["flink"]),
    "Databricks": ("BIG_DATA", ["databricks"]),
    "Snowflake": ("BIG_DATA", ["snowflake"]),
    "BigQuery": ("BIG_DATA", ["bigquery", "big query"]),
    "Redshift": ("BIG_DATA", ["redshift"]),
    "Delta Lake": ("BIG_DATA", ["delta lake"]),
    "Trino": ("BIG_DATA", ["trino", "presto"]),
    "Data Lake": ("BIG_DATA", ["data lake", "datalake", "data lakehouse", "lakehouse"]),
    "Data Warehouse": ("BIG_DATA", ["data warehouse", "datawarehouse", "entrepôt de données", "entrepot de donnees"]),
    # Cloud
    "AWS": ("CLOUD", ["aws", "amazon web services", "s3", "amazon s3"]),
    "Azure": ("CLOUD", ["azure", "microsoft azure"]),
    "GCP": ("CLOUD", ["gcp", "google cloud", "google cloud platform"]),
    "Azure Data Factory": ("CLOUD", ["azure data factory", "adf"]),
    "Microsoft Fabric": ("CLOUD", ["microsoft fabric", "ms fabric"]),
    "Azure Synapse": ("CLOUD", ["azure synapse", "synapse analytics"]),
    # ETL / orchestration
    "Airflow": ("ETL_ORCHESTRATION", ["airflow", "apache airflow"]),
    "dbt": ("ETL_ORCHESTRATION", ["dbt"]),
    "Talend": ("ETL_ORCHESTRATION", ["talend"]),
    "Informatica": ("ETL_ORCHESTRATION", ["informatica"]),
    "NiFi": ("ETL_ORCHESTRATION", ["nifi"]),
    "SSIS": ("ETL_ORCHESTRATION", ["ssis"]),
    "Alteryx": ("ETL_ORCHESTRATION", ["alteryx"]),
    "ETL": ("ETL_ORCHESTRATION", ["etl", "elt"]),
    "Dagster": ("ETL_ORCHESTRATION", ["dagster"]),
    # BI / visualisation
    "Power BI": ("BI_VISUALISATION", ["power bi", "powerbi"]),
    "Tableau": ("BI_VISUALISATION", ["tableau software", "tableau desktop", "tableau server", "tableau prep", "tableau public"]),
    "Qlik": ("BI_VISUALISATION", ["qlik", "qlikview", "qlik sense"]),
    "Looker": ("BI_VISUALISATION", ["looker", "looker studio"]),
    "Metabase": ("BI_VISUALISATION", ["metabase"]),
    "Superset": ("BI_VISUALISATION", ["superset"]),
    "Excel": ("BI_VISUALISATION", ["excel"]),
    "Streamlit": ("BI_VISUALISATION", ["streamlit"]),
    # ML / IA
    "Machine Learning": ("ML_IA", ["machine learning", "apprentissage automatique", "ml"]),
    "Deep Learning": ("ML_IA", ["deep learning", "apprentissage profond"]),
    "NLP": ("ML_IA", ["nlp", "traitement du langage naturel", "traitement automatique du langage"]),
    "LLM": ("ML_IA", ["llm", "llms", "large language model"]),
    "IA Générative": ("ML_IA", ["genai", "gen ai", "ia générative", "ia generative", "intelligence artificielle générative", "ai générative", "generative ai"]),
    "MLOps": ("ML_IA", ["mlops", "ml ops"]),
    "TensorFlow": ("ML_IA", ["tensorflow"]),
    "PyTorch": ("ML_IA", ["pytorch"]),
    "scikit-learn": ("ML_IA", ["scikit-learn", "scikit learn", "sklearn"]),
    "Pandas": ("ML_IA", ["pandas"]),
    "NumPy": ("ML_IA", ["numpy"]),
    "Computer Vision": ("ML_IA", ["computer vision", "vision par ordinateur"]),
    "MLflow": ("ML_IA", ["mlflow"]),
    "Dataiku": ("ML_IA", ["dataiku"]),
    "RAG": ("ML_IA", ["rag", "retrieval augmented generation"]),
    # DevOps / infra
    "Docker": ("DEVOPS", ["docker"]),
    "Kubernetes": ("DEVOPS", ["kubernetes", "k8s"]),
    "Git": ("DEVOPS", ["git", "github", "gitlab", "bitbucket"]),
    "CI/CD": ("DEVOPS", ["ci/cd", "cicd", "ci-cd", "jenkins", "gitlab ci", "github actions"]),
    "Terraform": ("DEVOPS", ["terraform"]),
    "Linux": ("DEVOPS", ["linux", "unix"]),
    "Ansible": ("DEVOPS", ["ansible"]),
    "FastAPI": ("DEVOPS", ["fastapi", "flask", "django"]),
    # Gouvernance / qualité
    "Data Governance": ("GOUVERNANCE_QUALITE", ["data governance", "gouvernance des données", "gouvernance des donnees", "gouvernance de la donnée", "gouvernance de la donnee", "gouvernance data"]),
    "Data Quality": ("GOUVERNANCE_QUALITE", ["data quality", "qualité des données", "qualite des donnees", "qualité de la donnée", "qualite de la donnee", "qualité de données", "qualite de donnees"]),
    "RGPD": ("GOUVERNANCE_QUALITE", ["rgpd", "gdpr"]),
    "Data Catalog": ("GOUVERNANCE_QUALITE", ["data catalog", "data catalogue", "collibra", "alation"]),
    "MDM": ("GOUVERNANCE_QUALITE", ["mdm", "master data management"]),
    "Data Modeling": ("GOUVERNANCE_QUALITE", ["data modeling", "modélisation de données", "modelisation de donnees", "modélisation des données", "modelisation des donnees", "modélisation dimensionnelle", "schéma en étoile", "schema en etoile"]),
    # Méthodes / ERP
    "Agile": ("METHODOLOGIE", ["agile", "scrum", "kanban"]),
    "JIRA": ("METHODOLOGIE", ["jira", "confluence"]),
    "SAP": ("METHODOLOGIE", ["sap", "sap bw", "sap hana"]),
}

_nlp = None
_matcher = None


def _load_matcher():
    """Chargé une fois par processus Python (donc une fois par worker Spark)."""
    global _nlp, _matcher
    if _matcher is None:
        import spacy
        from spacy.matcher import PhraseMatcher

        _nlp = spacy.blank("fr")
        _matcher = PhraseMatcher(_nlp.vocab, attr="LOWER")
        for canonical, (_cat, aliases) in SKILL_CATALOG.items():
            patterns = list(_nlp.tokenizer.pipe(aliases))
            _matcher.add(canonical, patterns)
    return _nlp, _matcher


def extract_skills(title: str | None, description: str | None) -> list[str]:
    """Compétences normalisées trouvées dans le titre et la description."""
    text = f"{title or ''}\n{description or ''}".strip()
    if not text:
        return []
    from spacy.util import filter_spans

    nlp, matcher = _load_matcher()
    doc = nlp.make_doc(text)
    spans = filter_spans(list(matcher(doc, as_spans=True)))  # « SQL Server » l'emporte sur « SQL »
    return sorted({s.label_ for s in spans})


# --------------------------------------------------------------------------
# 2. Expérience
# --------------------------------------------------------------------------
_EXP_PATTERNS = [
    # « 3 à 5 ans d'expérience », « 3-5 ans d'expérience » -> borne basse
    re.compile(r"(\d{1,2})\s*(?:a|-|to)\s*\d{1,2}\s*ans?\s*(?:d\s*|de\s*)?(?:experience|exp\b)"),
    # « 5 ans d'expérience », « 5+ ans d'expérience »
    re.compile(r"(\d{1,2})\s*\+?\s*ans?\s*(?:d\s*|de\s*)?(?:experience|exp\b)"),
    # « expérience de 3 ans », « expérience d'au moins 3 ans », « expérience minimum de 3 ans »
    re.compile(r"experience\s*(?:professionnelle\s*)?(?:significative\s*)?(?:de\s*|d\s*)?(?:au moins\s*|minimum\s*(?:de\s*)?|plus de\s*|superieure? a\s*)?(\d{1,2})\s*ans?"),
    # « minimum 3 ans », « au moins 3 ans »
    re.compile(r"(?:minimum|au moins|min\.?)\s*(?:de\s*)?(\d{1,2})\s*ans?"),
    # « 3 ans minimum »
    re.compile(r"(\d{1,2})\s*ans?\s*(?:minimum|min\b|au minimum|ou plus)"),
]
# « forte de 20 ans d'expérience », « depuis 15 ans » : ancienneté de l'entreprise, pas une exigence.
_EXP_COMPANY_CUE = re.compile(r"(?:forte?s?\s+de|fort\s+de|depuis|plus de \d+\s+ans\s+d existence|notre|nos|le groupe)\s*$")
_BEGINNER = re.compile(r"\b(debutant|debutante|jeune diplome|sortie? d ecole|premiere experience)\b")


def _ft_experience(declared: str | None) -> int | None:
    """Libellé structuré France Travail : « 2 An(s) », « 12 Mois », « Débutant accepté »."""
    if not isinstance(declared, str) or not declared:
        return None
    t = unaccent(declared).lower()
    if "debutant" in t:
        return 0
    m = re.match(r"\s*(\d+)\s*an", t)
    if m:
        return int(m.group(1))
    m = re.match(r"\s*(\d+)\s*mois", t)
    if m:
        return max(1, round(int(m.group(1)) / 12))
    return None  # « Expérience exigée » sans durée


def extract_experience_years(description: str | None, declared: str | None = None) -> int | None:
    """Années d'expérience minimales demandées (None si non précisé)."""
    ft = _ft_experience(declared)
    if ft is not None:
        return ft
    text = re.sub(r"\s+", " ", unaccent(description or "").lower().replace("'", " ").replace("’", " "))
    for rx in _EXP_PATTERNS:
        for m in rx.finditer(text):
            years = int(m.group(1))
            if years > 20 or _EXP_COMPANY_CUE.search(text[max(0, m.start() - 20):m.start()]):
                continue
            return years
    if _BEGINNER.search(text):
        return 0
    return None


def experience_level(years: int | None, title: str | None, contract_type: str | None) -> str:
    """JUNIOR (0-2 ans), CONFIRME (3-5 ans), SENIOR (6 ans et +), sinon INCONNU."""
    if contract_type in ("STAGE", "ALTERNANCE"):
        return "JUNIOR"
    if years is not None:
        return "JUNIOR" if years <= 2 else "CONFIRME" if years <= 5 else "SENIOR"
    t = norm_text(title or "")
    if re.search(r"\b(junior|debutant|jr)\b", t):
        return "JUNIOR"
    if re.search(r"\b(senior|sr|lead|principal|expert|staff)\b", t):
        return "SENIOR"
    if re.search(r"\bconfirme\b", t):
        return "CONFIRME"
    return "INCONNU"


# --------------------------------------------------------------------------
# 3. Niveau d'études (minimum demandé)
# --------------------------------------------------------------------------
_EDU_RULES = [
    ("BAC+2", re.compile(r"\bbac\s*(?:\+|plus)?\s*2\b|\bbts\b|\bdut\b|\bniveau\s*(?:bac\s*\+\s*)?2\b")),
    ("BAC+3", re.compile(r"\bbac\s*(?:\+|plus)?\s*3\b|\blicence\b|\bbachelor\b|\bbut\s+(?:informatique|stid|mmi)\b")),
    ("BAC+4", re.compile(r"\bbac\s*(?:\+|plus)?\s*4\b|\bm1\b")),
    ("BAC+5", re.compile(
        r"\bbac\s*(?:\+|plus)?\s*5\b|\bm2\b|\bmsc\b|\bgrande ecole\b|\bmaster\s*(?:2|ii)\b"
        r"|\b(?:diplome|titulaire|formation|cursus|ecole)\s*(?:d|de|du|en)?\s*(?:un\s*)?(?:master|ingenieur)\b"
        r"|\bingenieur de formation\b|\becole d ingenieurs?\b|\bmaster\s+(?:en|of)\b"
        r"|\bmaster\b(?!\s*(?:data|class|chef|d))(?<!scrum master)")),
    ("DOCTORAT", re.compile(r"\bdoctorat\b|\bphd\b|\bbac\s*\+\s*8\b")),
]
_EDU_ORDER = {"BAC+2": 2, "BAC+3": 3, "BAC+4": 4, "BAC+5": 5, "DOCTORAT": 8}


def extract_education_level(description: str | None) -> str:
    """Niveau d'études MINIMUM cité (« Bac+3 à Bac+5 » -> BAC+3), sinon INCONNU."""
    text = unaccent(description or "").lower().replace("'", " ").replace("’", " ")
    found = [lvl for lvl, rx in _EDU_RULES if rx.search(text)]
    if not found:
        return "INCONNU"
    return min(found, key=_EDU_ORDER.get)


# --------------------------------------------------------------------------
# 5. Famille de métier (d'après le titre)
# --------------------------------------------------------------------------
# L'ordre compte : la première règle qui correspond l'emporte.
_FAMILY_RULES = [
    ("HORS_PERIMETRE", r"data cent(?:er|re)|data entry|data collection|saisie|frigoriste|electricite|electricien|genie civil|btp|chantier"),
    ("DATA_SCIENTIST", r"data scien|scientifique des donnees|data science|statisticien"),
    ("ML_IA", r"machine learning|\bml\b|mlops|\bai engineer|\bia engineer|ingenieur (?:ia|ai)\b|deep learning|\bnlp\b|computer vision|\bllm|genai|ia generative|ingenieur intelligence artificielle|prompt engineer"),
    ("DATA_ARCHITECT", r"architect|data model(?:er|eur|euse|ler)"),
    ("DATA_ENGINEER", r"data engineer|data infrastructure|infrastructure data|ingenieur (?:data|donnees|big data|etl)|ingenieur de donnees|integrateur de donnees|integratrice de donnees|dataops|data ops|\betl\b|big data|developpe(?:ur|use)?(?: se)? (?:data|big data|etl|spark|talend|python data)|data developer|analytics engineer|tech lead data|data platform|ingenieur logiciel data|cloud data|data ingenieur|engineer.*\bdata\b|\bdata\b.*engineer|lead (?:technique )?data|data integration|data integrateur|talend|admin(?:istrateur)? (?:data|base)|\bdba\b|integrateur data|ingenieur.*\bdata\b"),
    ("DATA_ANALYST", r"data analy|analyste (?:de )?donnee|analyste data|analyste bi|\banalyste\b|business analyst|analytics|data insight|analyste de donnees|chargee? d etudes? (?:data|statistique)"),
    ("BI", r"\bbi\b|business intelligence|decisionnel|power ?bi|qlik|reporting|dataviz|data visuali"),
    ("DATA_GOVERNANCE", r"gouvernance|data steward|data owner|data quality|qualite (?:des |de la )?donnee|\bdpo\b|data protection|master data|\bmdm\b|data manager|data office|data catalog|conformite|data management|data privacy|data governance|data controleur|gestionnaire (?:de )?(?:donnees|data)|data controller"),
    ("DATA_ANALYST", r"\banalyst\b"),  # après la gouvernance : « Data Quality Analyst » reste en gouvernance
    ("MANAGEMENT", r"chefs?(?: fe| e)? de projets?|chefs?(?: fe| e)? de mission|solution owner|project manager|product owner|product manager|\bpmo\b|scrum master|responsable|manager|directeur|directrice|head of|\bcto\b|\bcdo\b|delivery|\bpo\b"),
    ("CONSULTANT_DATA", r"consultant|consultante|expert|referent data|data expert"),
]
_FAMILY_RULES = [(name, re.compile(rx)) for name, rx in _FAMILY_RULES]


def classify_job_family(title: str | None) -> str:
    t = norm_text(title or "")
    if not t:
        return "AUTRE"
    # norm_text enlève les caractères spéciaux ; on garde les mots pour les règles ci-dessus.
    for name, rx in _FAMILY_RULES:
        if rx.search(t):
            return name
    return "AUTRE"