# Power BI — connexion, modèle et mesures DAX

## 1. Se connecter à l'entrepôt Gold

1. Power BI Desktop → **Obtenir les données** → **Base de données PostgreSQL**.
2. Serveur : `localhost:5432` · Base de données : `job_offers_dwh` · Mode : **Importer**.
3. Identifiants : onglet **Base de données**, utilisateur et mot de passe PostgreSQL de ton `.env`
   (`POSTGRES_USER` / `POSTGRES_PASSWORD`).
4. Dans le navigateur, coche :
   - `gold.fact_job_offer`, `gold.bridge_offer_skill`
   - `gold.dim_date`, `gold.dim_source`, `gold.dim_contract`, `gold.dim_job_family`,
     `gold.dim_experience`, `gold.dim_education`, `gold.dim_location`, `gold.dim_company`, `gold.dim_skill`
   - `gold.etl_runs` (date de dernière mise à jour)
   - facultatif : les vues `gold.gold_top_skills`, `gold.gold_repartition_geographique`,
     `gold.gold_statistiques_salaires`, `gold.gold_tendances_recrutement` (tableaux tout faits).
5. **Renomme les tables** sans le préfixe « gold » (double-clic sur le nom) : `fact_job_offer`, `dim_date`…
   Les mesures ci-dessous utilisent ces noms.

## 2. Relations (vue Modèle)

Toutes en **plusieurs-à-un (\*:1)**, filtrage **unique**, sauf indication contraire.

| De (table de faits) | Vers (dimension) | Remarque |
|---|---|---|
| `fact_job_offer[source_key]` | `dim_source[source_key]` | |
| `fact_job_offer[contract_key]` | `dim_contract[contract_key]` | |
| `fact_job_offer[job_family_key]` | `dim_job_family[job_family_key]` | |
| `fact_job_offer[experience_key]` | `dim_experience[experience_key]` | |
| `fact_job_offer[education_key]` | `dim_education[education_key]` | |
| `fact_job_offer[location_key]` | `dim_location[location_key]` | |
| `fact_job_offer[company_key]` | `dim_company[company_key]` | |
| `fact_job_offer[date_posted_key]` | `dim_date[date_key]` | **active** (date de publication) |
| `fact_job_offer[first_seen_key]` | `dim_date[date_key]` | **inactive** (utilisée par USERELATIONSHIP) |
| `fact_job_offer[last_seen_key]` | `dim_date[date_key]` | **inactive** |
| `bridge_offer_skill[offer_key]` | `fact_job_offer[offer_key]` | filtrage croisé **dans les deux sens** |
| `bridge_offer_skill[skill_key]` | `dim_skill[skill_key]` | |

Pourquoi « dans les deux sens » sur la table de liaison : quand tu choisis une compétence dans un
segment, le filtre doit remonter `dim_skill → bridge_offer_skill → fact_job_offer` pour ne garder
que les offres qui la citent.

Réglages complémentaires :
- `dim_date` : **Marquer comme table de dates** (colonne `full_date`).
- Trier `dim_date[month_name]` par `month`, `dim_date[day_name]` par `day_of_week`.
- Trier `dim_experience[experience_label]` et `dim_education[education_label]` par `sort_order`.
- `dim_location[region]` : catégorie de données **État ou province** ; `dim_location[city]` : **Ville** ;
  `latitude` / `longitude` : **Latitude** / **Longitude** (pour les cartes).
- Masquer les colonnes de clés (`*_key`) dans la vue Rapport.
- Filtre au niveau du rapport : `dim_job_family[in_scope] = Vrai` (exclut les 116 offres hors périmètre).

## 3. Mesures DAX

Crée une table vide « Mesures » (Accueil → Entrer des données → OK) et ajoute-y les mesures.

```DAX
Nb offres = COUNTROWS ( fact_job_offer )

Offres actives = CALCULATE ( [Nb offres], fact_job_offer[is_active] = TRUE () )

Offres disparues = CALCULATE ( [Nb offres], fact_job_offer[is_active] = FALSE () )

% offres actives = DIVIDE ( [Offres actives], [Nb offres] )

Nouvelles offres (dernière collecte) =
VAR derniere_collecte = CALCULATE ( MAX ( fact_job_offer[first_seen_key] ), REMOVEFILTERS () )
RETURN CALCULATE ( [Nb offres], fact_job_offer[first_seen_key] = derniere_collecte )

Offres par date de 1re observation =
CALCULATE ( [Nb offres], USERELATIONSHIP ( fact_job_offer[first_seen_key], dim_date[date_key] ) )

Offres avec salaire = CALCULATE ( [Nb offres], NOT ISBLANK ( fact_job_offer[salary_annual_avg] ) )

% offres avec salaire = DIVIDE ( [Offres avec salaire], [Nb offres] )

Salaire médian = MEDIAN ( fact_job_offer[salary_annual_avg] )

Salaire min moyen = AVERAGE ( fact_job_offer[salary_annual_min] )

Salaire max moyen = AVERAGE ( fact_job_offer[salary_annual_max] )

% CDI = DIVIDE ( CALCULATE ( [Nb offres], dim_contract[contract_type] = "CDI" ), [Nb offres] )

Offres citant la compétence = DISTINCTCOUNT ( bridge_offer_skill[offer_key] )

% des offres (compétence) =
DIVIDE ( [Offres citant la compétence], CALCULATE ( [Nb offres], REMOVEFILTERS ( dim_skill ) ) )

% du total affiché = DIVIDE ( [Nb offres], CALCULATE ( [Nb offres], ALLSELECTED () ) )

Nb entreprises = DISTINCTCOUNT ( fact_job_offer[company_key] )

Compétences par offre (moyenne) = AVERAGE ( fact_job_offer[skills_count] )

Durée de présence moyenne (jours) = AVERAGE ( fact_job_offer[days_online] )

Dernière mise à jour = MAX ( etl_runs[run_at] )
```

Formats : pourcentages en `%` (1 décimale), salaires en nombre entier avec séparateur de milliers et « € ».

## 4. Pages proposées (KPI validés)

| Page | Visuels | Mesures / champs |
|---|---|---|
| **Vue d'ensemble** | 4 cartes · anneau des contrats · barres par source | Nb offres, Offres actives, Nouvelles offres, Nb entreprises, % CDI |
| **Compétences** | barres « top 15 » · matrice métier × compétence · segment Métier | `dim_skill[skill_name]`, % des offres (compétence) |
| **Métiers et salaires** | barres par métier · barres min/médiane/max · table grille salariale | `dim_job_family`, Salaire médian, Salaire min/max moyen, `dim_contract`, `dim_experience` |
| **Géographie** | carte (région ou villes) · barres top villes | `dim_location[region]` / `city` / latitude-longitude, Offres actives |
| **Qualité des données** | cartes · barres par source | % offres avec salaire, Offres par date de 1re observation, Dernière mise à jour |

Segments communs (à synchroniser entre les pages) : Métier, Région, Type de contrat, Niveau d'expérience, Source.

À afficher à côté de chaque salaire : le nombre d'offres chiffrées ([Offres avec salaire]) — seules
~31 % des offres indiquent un salaire.
