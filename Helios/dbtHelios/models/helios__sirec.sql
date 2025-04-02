-- Modèle DBT corrigé pour helios__sirec avec construction de "Motifs IGAS"

with base as (
    select
        cast("N° FINESS/RPPS" as varchar) as "N° FINESS/RPPS",
        cast("N° FINESS/RPPS" as varchar) || substr("Date de réception à l’ARS", 1, 4) as IDENTIFIANT,
        substr("Date de réception à l’ARS", 1, 4) as ANNEE_DE_RECEPTION,
        "Numéro de la réclamation",
        case
            when "Motifs IGAS sortie" is not null and trim("Motifs IGAS sortie") != '' then "Motifs IGAS sortie"
            else "Motifs IGAS entrée"
        end as "Motifs IGAS",
        case
            when "Statut de la réclamation" in ('Affectation', 'Réponse', 'Traitement') then 'en cours'
            when "Statut de la réclamation" = 'Clôture' then 'clot'
        end as STATUT_AGREG
    from {{ ref('staging__helios_sirec') }}
    where length(cast("N° FINESS/RPPS" as varchar)) > 4 and length(cast("N° FINESS/RPPS" as varchar)) <= 9
),

motifs_split as (
    select
        b.IDENTIFIANT,
        b."N° FINESS/RPPS",
        b.ANNEE_DE_RECEPTION,
        b."Numéro de la réclamation",
        b.STATUT_AGREG,
        trim(motif_value) as MOTIFS_IGAS_SPLIT
    from base b,
         unnest(string_split("Motifs IGAS", '|')) as motif(motif_value)
),

motifs_classified as (
    select
        IDENTIFIANT,
        "N° FINESS/RPPS",
        ANNEE_DE_RECEPTION,
        STATUT_AGREG,
        "Numéro de la réclamation",
        case when MOTIFS_IGAS_SPLIT like '%Hôtellerie-locaux-restauration%' then '10'
             when MOTIFS_IGAS_SPLIT like '%organisation ou de fonctionnement%' then '11'
             when MOTIFS_IGAS_SPLIT like '%qualité des soins médicaux%' then '12'
             when MOTIFS_IGAS_SPLIT like '%qualité des soins paramédicaux%' then '13'
             when MOTIFS_IGAS_SPLIT like '%établissement ou d%un professionnel%' then '14'
             when MOTIFS_IGAS_SPLIT like '%attitude des professionnels%' then '15'
             when MOTIFS_IGAS_SPLIT like '%droits des usagers%' then '16'
             when MOTIFS_IGAS_SPLIT like '%Facturation et honoraires%' then '17'
             when MOTIFS_IGAS_SPLIT like '%Santé-environnementale%' then '18'
             when MOTIFS_IGAS_SPLIT like '%esthétique réglementées%' then '19'
             when MOTIFS_IGAS_SPLIT like '%A renseigner%' then '155'
             when MOTIFS_IGAS_SPLIT like '%COVID-19%' then '156'
             else null end as code_motif
    from motifs_split
),

agreg_motif as (
    select
        IDENTIFIANT,
        "N° FINESS/RPPS",
        ANNEE_DE_RECEPTION,
        STATUT_AGREG,
        code_motif,
        count(distinct "Numéro de la réclamation") as NB_RECLAMATIONS
    from motifs_classified
    where code_motif is not null
    group by all
),

pivoted as (
    select
        IDENTIFIANT,
        "N° FINESS/RPPS",
        ANNEE_DE_RECEPTION,

        max(case when STATUT_AGREG = 'en cours' then NB_RECLAMATIONS end) as ENCOURS_NB_RECLA_TOTAL,
        max(case when STATUT_AGREG = 'clot' then NB_RECLAMATIONS end) as CLOT_NB_RECLA_TOTAL,

        max(case when code_motif = '10' and STATUT_AGREG = 'en cours' then NB_RECLAMATIONS end) as ENCOURS_NB_RECLA_MOTIF_10,
        max(case when code_motif = '11' and STATUT_AGREG = 'en cours' then NB_RECLAMATIONS end) as ENCOURS_NB_RECLA_MOTIF_11,
        max(case when code_motif = '12' and STATUT_AGREG = 'en cours' then NB_RECLAMATIONS end) as ENCOURS_NB_RECLA_MOTIF_12,
        max(case when code_motif = '13' and STATUT_AGREG = 'en cours' then NB_RECLAMATIONS end) as ENCOURS_NB_RECLA_MOTIF_13,
        max(case when code_motif = '14' and STATUT_AGREG = 'en cours' then NB_RECLAMATIONS end) as ENCOURS_NB_RECLA_MOTIF_14,
        max(case when code_motif = '15' and STATUT_AGREG = 'en cours' then NB_RECLAMATIONS end) as ENCOURS_NB_RECLA_MOTIF_15,
        max(case when code_motif = '16' and STATUT_AGREG = 'en cours' then NB_RECLAMATIONS end) as ENCOURS_NB_RECLA_MOTIF_16,
        max(case when code_motif = '17' and STATUT_AGREG = 'en cours' then NB_RECLAMATIONS end) as ENCOURS_NB_RECLA_MOTIF_17,
        max(case when code_motif = '18' and STATUT_AGREG = 'en cours' then NB_RECLAMATIONS end) as ENCOURS_NB_RECLA_MOTIF_18,
        max(case when code_motif = '19' and STATUT_AGREG = 'en cours' then NB_RECLAMATIONS end) as ENCOURS_NB_RECLA_MOTIF_19,
        max(case when code_motif = '155' and STATUT_AGREG = 'en cours' then NB_RECLAMATIONS end) as ENCOURS_NB_RECLA_MOTIF_155,
        max(case when code_motif = '156' and STATUT_AGREG = 'en cours' then NB_RECLAMATIONS end) as ENCOURS_NB_RECLA_MOTIF_156,

        max(case when code_motif = '10' and STATUT_AGREG = 'clot' then NB_RECLAMATIONS end) as CLOT_NB_RECLA_MOTIF_10,
        max(case when code_motif = '11' and STATUT_AGREG = 'clot' then NB_RECLAMATIONS end) as CLOT_NB_RECLA_MOTIF_11,
        max(case when code_motif = '12' and STATUT_AGREG = 'clot' then NB_RECLAMATIONS end) as CLOT_NB_RECLA_MOTIF_12,
        max(case when code_motif = '13' and STATUT_AGREG = 'clot' then NB_RECLAMATIONS end) as CLOT_NB_RECLA_MOTIF_13,
        max(case when code_motif = '14' and STATUT_AGREG = 'clot' then NB_RECLAMATIONS end) as CLOT_NB_RECLA_MOTIF_14,
        max(case when code_motif = '15' and STATUT_AGREG = 'clot' then NB_RECLAMATIONS end) as CLOT_NB_RECLA_MOTIF_15,
        max(case when code_motif = '16' and STATUT_AGREG = 'clot' then NB_RECLAMATIONS end) as CLOT_NB_RECLA_MOTIF_16,
        max(case when code_motif = '17' and STATUT_AGREG = 'clot' then NB_RECLAMATIONS end) as CLOT_NB_RECLA_MOTIF_17,
        max(case when code_motif = '18' and STATUT_AGREG = 'clot' then NB_RECLAMATIONS end) as CLOT_NB_RECLA_MOTIF_18,
        max(case when code_motif = '19' and STATUT_AGREG = 'clot' then NB_RECLAMATIONS end) as CLOT_NB_RECLA_MOTIF_19,
        max(case when code_motif = '155' and STATUT_AGREG = 'clot' then NB_RECLAMATIONS end) as CLOT_NB_RECLA_MOTIF_155,
        max(case when code_motif = '156' and STATUT_AGREG = 'clot' then NB_RECLAMATIONS end) as CLOT_NB_RECLA_MOTIF_156
    from agreg_motif
    group by IDENTIFIANT, "N° FINESS/RPPS", ANNEE_DE_RECEPTION
)

select
    IDENTIFIANT,
    case when length(cast("N° FINESS/RPPS" as varchar)) = 8 then '0' || "N° FINESS/RPPS" else "N° FINESS/RPPS" end as NDEG_FINESS_RPPS,
    ANNEE_DE_RECEPTION,
    ENCOURS_NB_RECLA_TOTAL,
    ENCOURS_NB_RECLA_MOTIF_10,
    ENCOURS_NB_RECLA_MOTIF_11,
    ENCOURS_NB_RECLA_MOTIF_12,
    ENCOURS_NB_RECLA_MOTIF_13,
    ENCOURS_NB_RECLA_MOTIF_14,
    ENCOURS_NB_RECLA_MOTIF_15,
    ENCOURS_NB_RECLA_MOTIF_16,
    ENCOURS_NB_RECLA_MOTIF_17,
    ENCOURS_NB_RECLA_MOTIF_18,
    ENCOURS_NB_RECLA_MOTIF_19,
    ENCOURS_NB_RECLA_MOTIF_155,
    ENCOURS_NB_RECLA_MOTIF_156,
    CLOT_NB_RECLA_TOTAL,
    CLOT_NB_RECLA_MOTIF_10,
    CLOT_NB_RECLA_MOTIF_11,
    CLOT_NB_RECLA_MOTIF_12,
    CLOT_NB_RECLA_MOTIF_13,
    CLOT_NB_RECLA_MOTIF_14,
    CLOT_NB_RECLA_MOTIF_15,
    CLOT_NB_RECLA_MOTIF_16,
    CLOT_NB_RECLA_MOTIF_17,
    CLOT_NB_RECLA_MOTIF_18,
    CLOT_NB_RECLA_MOTIF_19,
    CLOT_NB_RECLA_MOTIF_155,
    CLOT_NB_RECLA_MOTIF_156
from pivoted