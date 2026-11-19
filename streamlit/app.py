"""
JobPulse — application Streamlit sur l'entrepôt Gold (PostgreSQL).

Barre latérale : filtres communs (métier, région, contrat, expérience, source, offres actives).
Onglets :
  1. Tableau de bord     : indicateurs, carte de France, compétences par métier, salaires...
  2. Mon Profil Matcher  : compétences saisies ou extraites d'un CV PDF -> meilleures offres,
                           carte des offres compatibles, compétences à développer.
  3. Mes favoris         : offres retenues, export CSV.

Lancement local : streamlit run app.py  (variables POSTGRES_DWH_* ; dossier silver/ dans le PYTHONPATH)
"""
from __future__ import annotations

import html
from datetime import datetime
from pathlib import Path

import pandas as pd
import streamlit as st

import charts
import db

ASSETS = Path(__file__).parent / "assets"
LOGO_SVG = (ASSETS / "logo.svg").read_text(encoding="utf-8").replace('width="128" height="128"', 'width="40" height="40"')

st.set_page_config(page_title="JobPulse · emploi data", page_icon=str(ASSETS / "logo.png"), layout="wide",
                   initial_sidebar_state="expanded")

st.markdown("""
<style>
.block-container {padding-top: 3.2rem; padding-bottom: 2rem;}
h3 {font-size: 1.15rem !important; margin-bottom: .2rem !important;}
.tile {background:#161b26; border:1px solid #232a38; border-radius:12px; padding:14px 16px; margin-bottom:12px;}
.tile .lbl {color:#8b98a9; font-size:.8rem; text-transform:uppercase; letter-spacing:.04em;}
.tile .val {color:#e6edf3; font-size:1.9rem; font-weight:700; line-height:1.25;}
.tile .up {color:#34d399; font-size:.85rem;} .tile .down {color:#f87171; font-size:.85rem;}
.tile .info {color:#8b98a9; font-size:.85rem;}
.offer {background:#161b26; border:1px solid #232a38; border-left:4px solid #38bdf8; border-radius:12px;
        padding:14px 18px; margin:6px 0 4px 0;}
.offer h4 {margin:0 0 4px 0; font-size:1.05rem; color:#e6edf3;}
.offer .meta {color:#8b98a9; font-size:.86rem; margin-bottom:8px;}
.offer .score {float:right; font-weight:800; font-size:1.25rem; color:#38bdf8;}
.chip {display:inline-block; padding:1px 9px; margin:2px 4px 2px 0; border-radius:10px; font-size:.78rem;}
.ok {background:rgba(52,211,153,.15); color:#34d399; border:1px solid rgba(52,211,153,.45);}
.miss {background:rgba(139,152,169,.12); color:#8b98a9; border:1px solid rgba(139,152,169,.3);}
.bar {height:5px; border-radius:3px; background:#232a38; margin:6px 0 10px 0;}
.bar > div {height:5px; border-radius:3px; background:linear-gradient(90deg,#38bdf8,#818cf8);}
[data-testid="stFileUploaderDropzoneInstructions"] > div > span {display:none;}
[data-testid="stFileUploaderDropzoneInstructions"] > div::before {content:"Dépose ton CV ici (PDF, 5 Mo max.)"; font-size:.9rem;}
[data-testid="stFileUploaderDropzoneInstructions"] small {display:none;}
[data-testid="stFileUploaderDropzone"] button {font-size:0;}
[data-testid="stFileUploaderDropzone"] button::after {content:"Parcourir"; font-size:.9rem;}
</style>""", unsafe_allow_html=True)


def nombre(x) -> str:
    return f"{int(x):,}".replace(",", " ") if x is not None and x == x else "—"


def euros(x) -> str:
    return f"{x:,.0f} €".replace(",", " ") if x is not None and x == x else "—"


def salaire(lo, hi) -> str:
    lo = None if lo is None or lo != lo else float(lo)
    hi = None if hi is None or hi != hi else float(hi)
    if lo is None and hi is None:
        return "Salaire non communiqué"
    if lo is None or hi is None or lo == hi:
        return euros(lo if lo is not None else hi) + " / an"
    return f"{euros(lo)} – {euros(hi)} / an"


def tile(label: str, value: str, sub: str = "", kind: str = "info") -> None:
    st.markdown(f'<div class="tile"><div class="lbl">{label}</div><div class="val">{value}</div>'
                f'<div class="{kind}">{sub}</div></div>', unsafe_allow_html=True)


def chart(fig) -> None:
    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})


# ===================================================================== barre latérale
try:
    fv = db.filter_values()
    maj = db.last_update()
except Exception as exc:  # base indisponible : message clair plutôt qu'une trace Python
    st.error(f"Impossible de se connecter à l'entrepôt Gold (PostgreSQL) : {exc}")
    st.stop()

with st.sidebar:
    st.markdown(f'<div style="display:flex;align-items:center;gap:10px;margin-bottom:4px">{LOGO_SVG}'
                '<span style="font-size:1.6rem;font-weight:700;color:#e6edf3">JobPulse</span></div>',
                unsafe_allow_html=True)
    st.caption("Le marché de l'emploi data en France")
    metiers = st.multiselect("Métier", fv["metiers"], placeholder="Tous")
    regions = st.multiselect("Région", fv["regions"], placeholder="Toutes")
    contrats = st.multiselect("Type de contrat", fv["contrats"], placeholder="Tous")
    experiences = st.multiselect("Niveau d'expérience", fv["experiences"], placeholder="Tous")
    sources = st.multiselect("Source", fv["sources"], placeholder="Toutes")
    actives = st.toggle("Offres encore en ligne uniquement", value=False)
    st.divider()
    if maj is not None:
        st.caption(f"Dernière mise à jour des données : {maj:%d/%m/%Y à %H:%M}")
    st.caption("Sources : France Travail, Adzuna, HelloWork, Free-Work")

P = db.filtres(metiers, regions, contrats, experiences, sources, actives)
st.session_state.setdefault("favoris", {})

tab_dash, tab_match, tab_fav = st.tabs(
    ["Tableau de bord", "🎯 Mon Profil Matcher", f"⭐ Mes favoris ({len(st.session_state['favoris'])})"])

# ===================================================================== 1. Tableau de bord
with tab_dash:
    k = db.kpis(P)
    if not k["offres"]:
        st.warning("Aucune offre ne correspond aux filtres choisis.")
    else:
        left, center, right = st.columns([1.05, 2.6, 1.55], gap="medium")
        with left:
            st.markdown("### Indicateurs")
            tile("Offres analysées", nombre(k["offres"]), f"↑ {nombre(k['nouvelles'])} nouvelles à la dernière collecte", "up")
            tile("Offres actives", nombre(k["actives"]), f"↓ {nombre(k['disparues'])} plus en ligne", "down")
            tile("Salaire médian", euros(k["salaire_median"]), f"sur {nombre(k['avec_salaire'])} offres chiffrées")
            tile("Entreprises", nombre(k["entreprises"]), "qui recrutent")
            st.markdown("### Taux clés")
            chart(charts.donuts([(100 * k["cdi"] / k["offres"], "offres<br>en CDI", "#34d399"),
                                 (100 * k["avec_salaire"] / k["offres"], "salaire<br>affiché", "#f87171")]))

        with center:
            st.markdown("### Répartition géographique")
            chart(charts.france_map(db.by_region(P), db.by_city(P)))

            st.markdown("### Les 10 compétences les plus demandées pour un métier")
            choix_metier = st.selectbox("Métier", fv["metiers"],
                                        index=fv["metiers"].index("Data Engineer") if "Data Engineer" in fv["metiers"] else 0,
                                        label_visibility="collapsed")
            p_metier = db.filtres([choix_metier], regions, contrats, experiences, sources, actives)
            sk_metier = db.top_skills(p_metier, 10)
            if sk_metier.empty:
                st.info("Pas d'offre pour ce métier avec les filtres choisis.")
            else:
                chart(charts.hbar(sk_metier, "pourcentage", "competence", height=360, suffix=" %"))
                st.caption(f"Part des offres « {choix_metier} » qui citent chaque compétence.")

        with right:
            st.markdown("### Top compétences")
            ts = db.top_skills(P, 15)
            st.dataframe(
                ts[["competence", "pourcentage"]], hide_index=True, use_container_width=True, height=565,
                column_config={
                    "competence": st.column_config.TextColumn("Compétence"),
                    "pourcentage": st.column_config.ProgressColumn(
                        "% des offres", format="%.1f %%", min_value=0,
                        max_value=float(max(ts["pourcentage"].max(), 1)) if not ts.empty else 100),
                })

        b1, b2, b3 = st.columns(3, gap="medium")
        with b1:
            st.markdown("### Salaires par métier")
            sal = db.salary_by_family(P)
            if sal.empty:
                st.info("Pas assez de salaires pour cette sélection.")
            else:
                chart(charts.salary_ranges(sal))
                st.caption("Barre : 50 % central des salaires · losange : médiane (métiers avec au moins 10 offres chiffrées).")
        with b2:
            st.markdown("### Types de contrat")
            chart(charts.hbar(db.by_contract(P), "nb_offres", "contrat", "#818cf8"))
        with b3:
            st.markdown("### Publications (12 derniers mois)")
            chart(charts.area_by_month(db.by_month(P)))
            st.caption("Date de publication des offres encore visibles lors des collectes : "
                       "les mois récents sont donc les mieux représentés.")

# ===================================================================== 2. Mon Profil Matcher
def toggle_favori(rec: dict) -> None:
    fav = st.session_state["favoris"]
    if rec["offer_id"] in fav:
        del fav[rec["offer_id"]]
    else:
        fav[rec["offer_id"]] = rec


with tab_match:
    catalogue = db.skills()["skill_name"].tolist()
    st.session_state.setdefault("profil", [])

    cin, cout = st.columns([1, 2.3], gap="large")
    with cin:
        st.markdown("### Ton profil")
        mode = st.radio("Comment renseigner tes compétences ?", ["Les choisir dans la liste", "Importer mon CV (PDF)"],
                        label_visibility="collapsed")
        if mode == "Importer mon CV (PDF)":
            fichier = st.file_uploader("Ton CV au format PDF", type=["pdf"],
                                       help="Le fichier est lu en mémoire puis oublié : il n'est jamais enregistré.")
            if fichier is not None and st.session_state.get("cv_id") != fichier.file_id:
                try:
                    import cv
                    trouvees, n_car = cv.skills_from_cv(fichier.getvalue())
                    st.session_state["cv_id"] = fichier.file_id
                    st.session_state["profil"] = [s for s in trouvees if s in catalogue]
                    st.session_state["cv_info"] = (len(st.session_state["profil"]), n_car)
                except Exception as exc:
                    st.error(f"Lecture du CV impossible : {exc}")
            if st.session_state.get("cv_info"):
                n, n_car = st.session_state["cv_info"]
                if n_car < 50:
                    st.warning("Le PDF ne contient presque pas de texte (CV scanné ?). Choisis tes compétences dans la liste.")
                else:
                    st.success(f"{n} compétence(s) détectée(s) dans ton CV. Tu peux en ajouter ou en retirer.")
        choix = st.multiselect("Tes compétences", catalogue, key="profil", placeholder="Ex. Python, SQL, Power BI…")
        salaire_min = st.number_input("Salaire annuel souhaité (minimum)", 0, 200000, 0, 5000,
                                      help="0 = pas de critère de salaire")
        nb = st.slider("Nombre d'offres à afficher", 3, 20, 5)
        st.caption("Les filtres de la barre latérale (métier, région, contrat…) s'appliquent aussi ici.")

    with cout:
        if not choix:
            st.markdown("### Les offres qui te correspondent")
            st.info("Choisis tes compétences ou importe ton CV pour voir les offres qui te correspondent le mieux.")
        else:
            res = db.match_offers(choix, P, salaire_min or None, nb)
            st.markdown(f"### Les {len(res)} offres qui te correspondent le mieux")
            if res.empty:
                st.warning("Aucune offre ne correspond. Élargis les filtres ou ajoute des compétences.")
            mine = set(choix)
            for _, o in res.iterrows():
                comp = list(o["competences_offre"])
                chips = "".join(f'<span class="chip ok">✓ {html.escape(s)}</span>' for s in comp if s in mine)
                chips += "".join(f'<span class="chip miss">{html.escape(s)}</span>' for s in comp if s not in mine)
                lieu = ", ".join(x for x in [o["city"], o["region"]] if x)
                st.markdown(f"""<div class="offer">
<span class="score">{o['score']:.0f} %</span>
<h4>{html.escape(o['title'])}</h4>
<div class="meta">🏢 {html.escape(o['company_name'])} &nbsp;·&nbsp; 📍 {html.escape(lieu)} &nbsp;·&nbsp;
📄 {html.escape(o['contract_label'])} &nbsp;·&nbsp; 🎓 {html.escape(o['experience_label'])} &nbsp;·&nbsp;
💶 {salaire(o['salary_annual_min'], o['salary_annual_max'])} &nbsp;·&nbsp; 🗓️ {o['date_publication']:%d/%m/%Y}
&nbsp;·&nbsp; {html.escape(o['source_name'])}</div>
<div class="bar"><div style="width:{min(o['score'], 100):.0f}%"></div></div>
<div style="font-size:.86rem;color:#c9d3df"><b>{o['nb_communes']}/{o['skills_count']}</b> compétences demandées que tu possèdes : {chips}</div>
</div>""", unsafe_allow_html=True)
                rec = {"offer_id": o["offer_id"], "titre": o["title"], "entreprise": o["company_name"],
                       "ville": o["city"] or "", "region": o["region"], "contrat": o["contract_label"],
                       "experience": o["experience_label"], "salaire": salaire(o["salary_annual_min"], o["salary_annual_max"]),
                       "score": round(float(o["score"])), "competences": ", ".join(comp),
                       "date_publication": o["date_publication"], "source": o["source_name"], "lien": o["url"] or "",
                       "ajoutee_le": datetime.now().strftime("%d/%m/%Y %H:%M")}
                is_fav = o["offer_id"] in st.session_state["favoris"]
                a, b, _ = st.columns([1, 1, 1.4])
                a.button("★ Dans mes favoris" if is_fav else "☆ Ajouter aux favoris", key=f"fav_{o['offer_id']}",
                         on_click=toggle_favori, args=(rec,), use_container_width=True)
                if o["url"]:
                    b.link_button("Postuler ↗", o["url"], use_container_width=True)

            if not res.empty:
                with st.expander("Comment le score est-il calculé ?"):
                    st.markdown("Score = **60 %** × part de *tes* compétences utilisées par l'offre + **40 %** × part des "
                                "compétences *demandées par l'offre* que tu possèdes. À score égal, l'offre la plus "
                                "récente passe devant. Les compétences sont extraites automatiquement des annonces.")

    if choix:
        st.divider()
        st.markdown("### Où se trouvent les offres compatibles ?")
        m1, m2 = st.columns([2.3, 1], gap="large")
        with m2:
            seuil = st.slider("Score minimum pris en compte", 20, 100, 50, 5, format="%d %%",
                              help="Seules les offres qui atteignent ce score apparaissent sur la carte.")
        mp = db.match_map(choix, P, salaire_min or None, seuil)
        with m1:
            if mp.empty:
                st.info("Aucune offre compatible à ce niveau de score.")
            else:
                reg = mp.groupby("region", as_index=False)["nb_offres"].sum()
                chart(charts.france_map(reg, mp.dropna(subset=["lat", "lon"]), height=430, label="Offres compatibles"))
        with m2:
            if not mp.empty:
                st.metric("Offres compatibles", nombre(mp["nb_offres"].sum()))
                st.caption("Couleur : nombre d'offres par région · bulles : villes dont la position est connue.")

        st.divider()
        st.markdown("### Compétences à développer")
        learn = db.skills_to_learn(choix, P, 8)
        cible = ", ".join(metiers) if metiers else "tous les métiers data"
        st.caption(f"Les plus demandées ({cible}) parmi celles que tu n'as pas encore.")
        chart(charts.hbar(learn, "pourcentage", "competence", "#a78bfa", height=330, suffix=" %"))

# ===================================================================== 3. Mes favoris
with tab_fav:
    fav = st.session_state["favoris"]
    st.markdown("### Mes offres favorites")
    if not fav:
        st.info("Aucune offre en favori pour l'instant. Ajoute-en depuis l'onglet « Mon Profil Matcher ».")
    else:
        df = pd.DataFrame(list(fav.values()))
        st.dataframe(
            df[["titre", "entreprise", "ville", "contrat", "salaire", "score", "lien"]], hide_index=True,
            use_container_width=True,
            column_config={"titre": "Offre", "entreprise": "Entreprise", "ville": "Ville", "contrat": "Contrat",
                           "salaire": "Salaire",
                           "score": st.column_config.ProgressColumn("Score", format="%d %%", min_value=0, max_value=100),
                           "lien": st.column_config.LinkColumn("Lien", display_text="Postuler ↗")})
        c1, c2, c3 = st.columns([1.3, 1.3, 2])
        csv = df.drop(columns=["offer_id"]).to_csv(index=False, sep=";").encode("utf-8-sig")  # lisible par Excel
        c1.download_button("⬇️ Exporter en CSV", csv, file_name=f"offres_favorites_{datetime.now():%Y%m%d}.csv",
                           mime="text/csv", use_container_width=True)
        if c2.button("🗑️ Vider la liste", use_container_width=True):
            st.session_state["favoris"] = {}
            st.rerun()
        a_retirer = c3.multiselect("Retirer certaines offres", options=list(fav),
                                   format_func=lambda i: f"{fav[i]['titre']} — {fav[i]['entreprise']}",
                                   label_visibility="collapsed", placeholder="Retirer certaines offres…")
        if a_retirer:
            for i in a_retirer:
                fav.pop(i, None)
            st.rerun()
        st.caption("Les favoris sont gardés pendant la session (ils disparaissent si la page est rechargée) : "
                   "pense à les exporter en CSV.")
