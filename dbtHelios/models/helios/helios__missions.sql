with all_inspections as (

    select
        missions.identifiant_de_la_mission,
        missions.code_theme_igas,
        missions.theme_igas,
        missions.code_theme_regional,
        missions.theme_regional,
        missions.type_de_mission,
        missions.type_de_planification,
        missions.modalite_de_la_mission,
        missions.cd_finess,
        SUBSTRING(missions.date_reelle_visite, 7, 4) || '-' ||
             SUBSTRING(missions.date_reelle_visite, 4, 2) || '-' ||
             SUBSTRING(missions.date_reelle_visite, 1, 2) AS date_reelle_visite,
        SUBSTRING(missions.date_reelle_rapport, 7, 4) || '-' ||
            SUBSTRING(missions.date_reelle_rapport, 4, 2) || '-' ||
            SUBSTRING(missions.date_reelle_rapport, 1, 2) AS date_reelle_rapport,
        missions.nombre_d_ecarts,
        missions.nombre_de_remarques,
        missions.injonction,
        missions.prescription,
        missions.recommandation,
        missions.saisine_cng,
        missions.saisine_juridiction_ordinale,
        missions.saisine_parquet,
        missions.autre_saisine,
        missions.statut_de_la_mission,

        decisions.type_de_decision,
        decisions.complement,
        decisions.theme_decision,
        decisions.sous_theme_decision,
        decisions.nombre_de_decisions,
        decisions.statut_de_decision,
        case 
            when length(decisions.finess_geographique) = 8 then '0' || decisions.finess_geographique
            else decisions.finess_geographique
        end as finess_geographique,
        case 
            when length(decisions.finess_de_rattcahement) = 8 then '0' || decisions.finess_de_rattcahement
            else decisions.finess_de_rattcahement
        end as finess_de_rattcahement,
        decisions.date_de_realisation,
        decisions.etat_d_avancement

    from {{ ref('staging__helios_siicea_missions') }} as missions

    left join {{ ref('staging__sa_siicea_decisions') }} as decisions
        on missions.identifiant_de_la_mission = decisions.identifiant_de_la_mission
    where cd_finess != ''
)
select distinct
    identifiant_de_la_mission as "Identifiant de la mission",
    code_theme_igas as "Code thème IGAS",
    theme_igas as "Thème IGAS",
    code_theme_regional as "Code thème régional",
    theme_regional as "Thème régional",
    type_de_mission as "Type de mission",
    type_de_planification as "Type de planification",
    modalite_de_la_mission as "Modalité de la mission",
    cd_finess as "Code FINESS",
    CAST(
        CASE 
            WHEN date_reelle_visite = '--' THEN NULL
            ELSE 
                substr(date_reelle_visite, 1, 10)
        END AS DATE) AS "Date réelle Visite",
    CAST(
        CASE 
            WHEN date_reelle_rapport = '--' THEN NULL
            ELSE 
                substr(date_reelle_rapport, 1, 10)
        END AS DATE) AS "Date réelle Rapport",
    nombre_d_ecarts as "Nombre d écarts",
    nombre_de_remarques as "Nombre de remarques",
    injonction as "Injonction",
    prescription as "Prescription",
    recommandation as "Recommandation",
    saisine_cng as "Saisine CNG",
    saisine_juridiction_ordinale as "Saisine juridiction/ordinale",
    saisine_parquet as "Saisine parquet",
    autre_saisine as "Autre saisine",
    statut_de_la_mission as "Statut de la mission"
from all_inspections