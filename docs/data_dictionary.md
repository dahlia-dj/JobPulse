# Dictionnaire de données — Plateforme Big Data Offres d'Emploi

Ce document liste les champs ciblés pour alimenter le schéma Gold
(`fact_job_offer` et ses dimensions), leur justification fonctionnelle, et
leur disponibilité réelle selon la source de collecte. Il sert de référence
commune entre les couches Bronze, Silver et Gold, et de pièce de
documentation pour le rapport de PFE.

## Principe de collecte

En couche **Bronze**, aucune donnée n'est filtrée ni transformée : chaque
source est stockée telle quelle (JSON brut), pour garantir la traçabilité
et la rejouabilité du pipeline. Ce dictionnaire décrit les champs que la
couche **Silver** doit extraire ou déduire de ce brut pour construire le
modèle analytique final.

## Tableau de correspondance par source

| Champ cible | Justification | France Travail | Adzuna | HelloWork | Welcome to the Jungle |
|---|---|---|---|---|---|
| `id_offre_source` | Dédupliquer, tracer l'origine de chaque offre | Natif | Natif | À extraire de l'URL | À extraire de l'URL |
| `intitulé_poste` | Analyse par métier | Natif | Natif | Natif | Natif |
| `description` | Source pour l'extraction NLP des compétences | Natif | Natif | Natif | Natif |
| `entreprise` | Analyse par employeur | Natif | Natif (`display_name`) | Natif | Natif |
| `secteur_activité` | KPI "tendances par secteur" | Natif | Via `category.label` (moins précis) | Absent — à déduire du texte | Absent — à déduire du texte |
| `localisation` (ville, code postal) | KPI géographique | Natif, riche | Natif | Natif | Natif |
| `type_contrat` (CDI/CDD/Alternance...) | Filtre analytique clé | Natif | Natif (`contract_type`, moins détaillé) | Natif | Natif |
| `salaire_min` / `salaire_max` | KPI "évolution des salaires" | Natif quand renseigné par l'employeur | Souvent une **estimation algorithmique** (voir note) | Rarement structuré | Rarement structuré |
| `expérience_requise` | Segmentation junior/senior | Natif | Absent — à déduire du texte | Absent — à déduire du texte | Absent — à déduire du texte |
| `compétences` | KPI central du projet | **Liste structurée déjà fournie** | Absent — extraction spaCy nécessaire | Absent — extraction spaCy nécessaire | Absent — extraction spaCy nécessaire |
| `date_publication` | Fraîcheur des offres, filtrage incrémental | Natif | Natif | Natif (parfois relative, ex. "il y a 3 jours" — à parser) | Natif (parfois relative — à parser) |
| `code_ROME` | Nomenclature métier officielle, référentiel pivot | **Disponible uniquement sur cette source** | Absent | Absent | Absent |

## Notes importantes

### 1. Deux chemins d'extraction des compétences en couche Silver

France Travail fournit une liste de compétences déjà structurée par
l'API — aucune extraction NLP n'est nécessaire pour cette source. Pour
Adzuna, HelloWork et Welcome to la Jungle, les compétences ne sont
présentes qu'en texte libre dans le champ `description` et doivent être
extraites via spaCy. Le job Silver `silver_extract_skills.py` doit donc
gérer ces deux chemins distincts selon la source d'origine de l'offre.

### 2. Le code ROME comme référentiel pivot

Le code ROME (nomenclature officielle des métiers en France) n'est fourni
que par France Travail. Il représente une opportunité de catégoriser les
offres de façon fiable plutôt que de comparer des intitulés de poste en
texte libre entre 4 sources hétérogènes. Deux options pour les 3 autres
sources :
- mapper approximativement leurs intitulés vers ce référentiel (ambitieux,
  nécessiterait un modèle de classification supplémentaire) ;
- se limiter à une catégorisation par mots-clés du titre (plus simple,
  suffisant pour le périmètre de ce PFE).

### 3. Vigilance sur les salaires Adzuna

Le champ `salary_is_predicted` d'Adzuna indique quand un salaire est une
**estimation algorithmique** de leur part plutôt qu'une valeur réellement
affichée par l'employeur. Ce flag doit être conservé tel quel jusqu'en
couche Silver, où il faudra décider soit d'exclure les salaires prédits du
calcul du KPI "évolution des salaires", soit de les conserver avec un
indicateur de fiabilité distinct. Ne pas le faire risquerait de biaiser ce
KPI sans que cela soit visible dans les résultats finaux.

## Statut

Ce dictionnaire reflète l'état de la collecte prévue à ce stade du projet
(France Travail et Adzuna implémentés ; HelloWork et Welcome to the Jungle
à implémenter). Il devra être mis à jour si des champs supplémentaires
sont découverts lors de l'implémentation réelle des scrapers HelloWork et
Welcome to the Jungle.
