-- Modèle DBT corrigé pour helios__sirec avec construction de motifs_igas

with base as (
    select
        cast(n_finess_rpps as varchar) as n_finess_rpps,
        cast(n_finess_rpps as varchar) || substr(date_de_reception_a_l_ars, 1, 4) as identifiant,
        cast(substr(date_de_reception_a_l_ars, 1, 4) as integer) as annee_de_reception,
        numero_de_la_reclamation,
        case
            when motifs_igas_sortie != '' and trim(motifs_igas_sortie) != '' then motifs_igas_sortie
            else motifs_igas_entree
        end as motifs_igas,
        case
            when statut_de_la_reclamation in ('Affectation', 'Réponse', 'Traitement') then 'en cours'
            when statut_de_la_reclamation = 'Clôture' then 'clot'
        end as statut_agreg
    from {{ ref('staging__helios_sirec') }}
    where length(cast(n_finess_rpps as varchar)) > 4 and length(cast(n_finess_rpps as varchar)) <= 9
),

motifs_split as (
    select
        b.identifiant,
        b.n_finess_rpps,
        b.annee_de_reception,
        b.numero_de_la_reclamation,
        b.statut_agreg,
        trim(motif_value) as motifs_igas_split
    from base b,
         {{ dbtStaging.split_string_by_pipe('motifs_igas', 'b') }}
),

motifs_classified as (
    select
        identifiant,
        n_finess_rpps,
        annee_de_reception,
        statut_agreg,
        numero_de_la_reclamation,
        case when motifs_igas_split like '%Hôtellerie-locaux-restauration%' then '10'
             when motifs_igas_split like '%organisation ou de fonctionnement%' then '11'
             when motifs_igas_split like '%qualité des soins médicaux%' then '12'
             when motifs_igas_split like '%qualité des soins paramédicaux%' then '13'
             when motifs_igas_split like '%établissement ou d%un professionnel%' then '14'
             when motifs_igas_split like '%attitude des professionnels%' then '15'
             when motifs_igas_split like '%droits des usagers%' then '16'
             when motifs_igas_split like '%Facturation et honoraires%' then '17'
             when motifs_igas_split like '%Santé-environnementale%' then '18'
             when motifs_igas_split like '%esthétique réglementées%' then '19'
             when motifs_igas_split like '%A renseigner%' then '155'
             when motifs_igas_split like '%COVID-19%' then '156'
            --  else '' 
             end as code_motif
    from motifs_split
),
agreg_motif as (
    select
        identifiant,
        n_finess_rpps,
        annee_de_reception,
        statut_agreg,
        code_motif,
        count(distinct numero_de_la_reclamation) as nb_reclamations
    from motifs_classified
    -- where code_motif != ''
    group by
        identifiant,
        n_finess_rpps,
        annee_de_reception,
        statut_agreg,
        code_motif
),
pivoted as (
    select
        identifiant,
        n_finess_rpps,
        annee_de_reception,
        cast(max(case when statut_agreg = 'en cours' then nb_reclamations end) as integer) as encours_nb_recla_total,
        cast(max(case when statut_agreg = 'clot' then nb_reclamations end) as integer) as clot_nb_recla_total,
        cast(max(case when code_motif = '10' and statut_agreg = 'en cours' then nb_reclamations end) as integer) as encours_nb_recla_10,
        cast(max(case when code_motif = '11' and statut_agreg = 'en cours' then nb_reclamations end) as integer) as encours_nb_recla_11,
        cast(max(case when code_motif = '12' and statut_agreg = 'en cours' then nb_reclamations end) as integer) as encours_nb_recla_12,
        cast(max(case when code_motif = '13' and statut_agreg = 'en cours' then nb_reclamations end) as integer) as encours_nb_recla_13,
        cast(max(case when code_motif = '14' and statut_agreg = 'en cours' then nb_reclamations end) as integer) as encours_nb_recla_14,
        cast(max(case when code_motif = '15' and statut_agreg = 'en cours' then nb_reclamations end) as integer) as encours_nb_recla_15,
        cast(max(case when code_motif = '16' and statut_agreg = 'en cours' then nb_reclamations end) as integer) as encours_nb_recla_16,
        cast(max(case when code_motif = '17' and statut_agreg = 'en cours' then nb_reclamations end) as integer) as encours_nb_recla_17,
        cast(max(case when code_motif = '18' and statut_agreg = 'en cours' then nb_reclamations end) as integer) as encours_nb_recla_18,
        cast(max(case when code_motif = '19' and statut_agreg = 'en cours' then nb_reclamations end) as integer) as encours_nb_recla_19,
        cast(max(case when code_motif = '155' and statut_agreg = 'en cours' then nb_reclamations end) as integer) as encours_nb_recla_155,
        cast(max(case when code_motif = '156' and statut_agreg = 'en cours' then nb_reclamations end) as integer) as encours_nb_recla_156,
        cast(max(case when code_motif = '10' and statut_agreg = 'clot' then nb_reclamations end) as integer) as clot_nb_recla_motif_10,
        cast(max(case when code_motif = '11' and statut_agreg = 'clot' then nb_reclamations end) as integer) as clot_nb_recla_motif_11,
        cast(max(case when code_motif = '12' and statut_agreg = 'clot' then nb_reclamations end) as integer) as clot_nb_recla_motif_12,
        cast(max(case when code_motif = '13' and statut_agreg = 'clot' then nb_reclamations end) as integer) as clot_nb_recla_motif_13,
        cast(max(case when code_motif = '14' and statut_agreg = 'clot' then nb_reclamations end) as integer) as clot_nb_recla_motif_14,
        cast(max(case when code_motif = '15' and statut_agreg = 'clot' then nb_reclamations end) as integer) as clot_nb_recla_motif_15,
        cast(max(case when code_motif = '16' and statut_agreg = 'clot' then nb_reclamations end) as integer) as clot_nb_recla_motif_16,
        cast(max(case when code_motif = '17' and statut_agreg = 'clot' then nb_reclamations end) as integer) as clot_nb_recla_motif_17,
        cast(max(case when code_motif = '18' and statut_agreg = 'clot' then nb_reclamations end) as integer) as clot_nb_recla_motif_18,
        cast(max(case when code_motif = '19' and statut_agreg = 'clot' then nb_reclamations end) as integer) as clot_nb_recla_motif_19,
        cast(max(case when code_motif = '155' and statut_agreg = 'clot' then nb_reclamations end) as integer) as clot_nb_recla_motif_155,
        cast(max(case when code_motif = '156' and statut_agreg = 'clot' then nb_reclamations end) as integer) as clot_nb_recla_motif_156
    from agreg_motif
    group by
        identifiant,
        n_finess_rpps,
        annee_de_reception
)

select
    identifiant as "IDENTIFIANT",
    case when length(cast(n_finess_rpps as varchar)) = 8 then '0' || n_finess_rpps else n_finess_rpps end AS "NDEG_FINESS_RPPS",
    annee_de_reception AS "ANNEE_DE_RECEPTION",
    encours_nb_recla_total AS "ENCOURS_NB_RECLA_TOTAL",
    encours_nb_recla_10 AS "ENCOURS_NB_RECLA_MOTIF_10",
    encours_nb_recla_11 AS "ENCOURS_NB_RECLA_MOTIF_11",
    encours_nb_recla_12 AS "ENCOURS_NB_RECLA_MOTIF_12",
    encours_nb_recla_13 AS "ENCOURS_NB_RECLA_MOTIF_13",
    encours_nb_recla_14 AS "ENCOURS_NB_RECLA_MOTIF_14",
    encours_nb_recla_15 AS "ENCOURS_NB_RECLA_MOTIF_15",
    encours_nb_recla_16 AS "ENCOURS_NB_RECLA_MOTIF_16",
    encours_nb_recla_17 AS "ENCOURS_NB_RECLA_MOTIF_17",
    encours_nb_recla_18 AS "ENCOURS_NB_RECLA_MOTIF_18",
    encours_nb_recla_19 AS "ENCOURS_NB_RECLA_MOTIF_19",
    encours_nb_recla_155 AS "ENCOURS_NB_RECLA_MOTIF_155",
    encours_nb_recla_156 AS "ENCOURS_NB_RECLA_MOTIF_156",
    clot_nb_recla_total AS "CLOT_NB_RECLA_TOTAL",
    clot_nb_recla_motif_10 AS "CLOT_NB_RECLA_MOTIF_10",
    clot_nb_recla_motif_11 AS "CLOT_NB_RECLA_MOTIF_11",
    clot_nb_recla_motif_12 AS "CLOT_NB_RECLA_MOTIF_12",
    clot_nb_recla_motif_13 AS "CLOT_NB_RECLA_MOTIF_13",
    clot_nb_recla_motif_14 AS "CLOT_NB_RECLA_MOTIF_14",
    clot_nb_recla_motif_15 AS "CLOT_NB_RECLA_MOTIF_15",
    clot_nb_recla_motif_16 AS "CLOT_NB_RECLA_MOTIF_16",
    clot_nb_recla_motif_17 AS "CLOT_NB_RECLA_MOTIF_17",
    clot_nb_recla_motif_18 AS "CLOT_NB_RECLA_MOTIF_18",
    clot_nb_recla_motif_19 AS "CLOT_NB_RECLA_MOTIF_19",
    clot_nb_recla_motif_155 AS "CLOT_NB_RECLA_MOTIF_155",
    clot_nb_recla_motif_156 AS "CLOT_NB_RECLA_MOTIF_156"
from pivoted