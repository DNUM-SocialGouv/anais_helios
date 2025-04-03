with all_inspections as (

    select
        missions."Identifiant de la mission",
        missions."Code thème IGAS",
        missions."Thème IGAS",
        missions."Code Thème régional",
        missions."Thème régional",
        missions."Type de mission",
        missions."Type de planification",
        missions."Modalité de la mission",

        case 
            when length(missions."Code FINESS") = 8 then '0' || missions."Code FINESS"
            else missions."Code FINESS"
        end as "Code FINESS",

        missions."Date réelle ""Visite""" as "Date réelle Visite",
        missions."Date réelle ""Rapport""" as "Date réelle Rapport",
        missions."Nombre décarts",
        missions."Nombre de remarques",
        missions.Injonction,
        missions.Prescription,
        missions.Recommandation,
        missions."Saisine CNG",
        missions."Saisine juridiction/ordinale",
        missions."Saisine parquet",
        missions."Autre saisine",
        missions."Statut de la mission",

        decisions."Type_de_decision",
        decisions.Complement,
        decisions."Theme_Decision",
        decisions."Sous_theme_Decision",
        decisions.Nombre,
        decisions."Statut_de_decision",
        case 
            when length(decisions."Identifiant_FINESS_geographique") = 8 then '0' || decisions."Identifiant_FINESS_geographique"
            else decisions."Identifiant_FINESS_geographique"
        end as "Identifiant_FINESS_geographique",
        case 
            when length(decisions."Identifiant_FINESS_de_rattachement") = 8 then '0' || decisions."Identifiant_FINESS_de_rattachement"
            else decisions."Identifiant_FINESS_de_rattachement"
        end as "Identifiant_FINESS_de_rattachement",
        decisions."Date_de_realisation",
        decisions."Etat_davancement"

    from "duckdb_database"."main"."staging__helios_siicea_missions" as missions

    left join "duckdb_database"."main"."staging__sa_siicea_suites" as decisions
        on missions."Identifiant de la mission" = decisions."Identifiant_de_la_mission"

    where missions."Code FINESS" != ''
)

select distinct
    "Identifiant de la mission",
    "Code thème IGAS",
    "Thème IGAS",
    "Code Thème régional",
    "Thème régional",
    "Type de mission",
    "Type de planification",
    "Modalité de la mission",
    "Code FINESS",
    substr("Date réelle Visite", 1, 10) as "Date réelle Visite",
    substr("Date réelle Rapport", 1, 10) as "Date réelle Rapport",
    "Nombre décarts",
    "Nombre de remarques",
    Injonction,
    Prescription,
    Recommandation,
    "Saisine CNG",
    "Saisine juridiction/ordinale",
    "Saisine parquet",
    "Autre saisine",
    "Statut de la mission"

from all_inspections