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
        case 
            when length(missions.finess_geographique) = 8 then '0' || missions.finess_geographique
            else missions.finess_geographique
        end as cd_finess,

        missions.date_reelle_visite,
        missions.date_reelle_rapport,
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
        decisions.nombre,
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

    where missions.cd_finess != ''
)

select distinct
    identifiant_de_la_mission,
    code_theme_igas,
    theme_igas,
    code_theme_regional,
    theme_regional,
    type_de_mission,
    type_de_planification,
    modalite_de_la_mission,
    cd_finess,
    substr(date_reelle_visite, 1, 10) as date_reelle_visite,
    substr(date_reelle_rapport, 1, 10) as date_reelle_rapport,
    nombre_d_ecarts,
    nombre_de_remarques,
    injonction,
    prescription,
    recommandation,
    saisine_cng,
    saisine_juridiction_ordinale,
    saisine_parquet,
    autre_saisine,
    statut_de_la_mission

from all_inspections