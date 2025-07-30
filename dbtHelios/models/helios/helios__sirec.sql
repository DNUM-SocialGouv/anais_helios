-- Modèle DBT corrigé pour helios__sirec avec construction de motifs_igas

with base as (
    select
        cast(n_finess_rpps as varchar) as n_finess_rpps,
        cast(n_finess_rpps as varchar) || substr(date_de_reception_a_l_ars, 1, 4) as identifiant,
        substr(date_de_reception_a_l_ars, 1, 4) as annee_de_reception,
        numero_de_la_reclamation,
        case
            when motifs_igas_sortie is not null and trim(motifs_igas_sortie) != '' then motifs_igas_sortie
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
         {{ split_string_by_pipe('motifs_igas', 'b') }}
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
             else null end as code_motif
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
    where code_motif is not null
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
        max(case when statut_agreg = 'en cours' then nb_reclamations end) as encours_nb_recla_total,
        max(case when statut_agreg = 'clot' then nb_reclamations end) as clot_nb_recla_total,
        max(case when code_motif = '10' and statut_agreg = 'en cours' then nb_reclamations end) as encours_nb_recla_10,
        max(case when code_motif = '11' and statut_agreg = 'en cours' then nb_reclamations end) as encours_nb_recla_11,
        max(case when code_motif = '12' and statut_agreg = 'en cours' then nb_reclamations end) as encours_nb_recla_12,
        max(case when code_motif = '13' and statut_agreg = 'en cours' then nb_reclamations end) as encours_nb_recla_13,
        max(case when code_motif = '14' and statut_agreg = 'en cours' then nb_reclamations end) as encours_nb_recla_14,
        max(case when code_motif = '15' and statut_agreg = 'en cours' then nb_reclamations end) as encours_nb_recla_15,
        max(case when code_motif = '16' and statut_agreg = 'en cours' then nb_reclamations end) as encours_nb_recla_16,
        max(case when code_motif = '17' and statut_agreg = 'en cours' then nb_reclamations end) as encours_nb_recla_17,
        max(case when code_motif = '18' and statut_agreg = 'en cours' then nb_reclamations end) as encours_nb_recla_18,
        max(case when code_motif = '19' and statut_agreg = 'en cours' then nb_reclamations end) as encours_nb_recla_19,
        max(case when code_motif = '155' and statut_agreg = 'en cours' then nb_reclamations end) as encours_nb_recla_155,
        max(case when code_motif = '156' and statut_agreg = 'en cours' then nb_reclamations end) as encours_nb_recla_156,
        max(case when code_motif = '10' and statut_agreg = 'clot' then nb_reclamations end) as clot_nb_recla_motif_10,
        max(case when code_motif = '11' and statut_agreg = 'clot' then nb_reclamations end) as clot_nb_recla_motif_11,
        max(case when code_motif = '12' and statut_agreg = 'clot' then nb_reclamations end) as clot_nb_recla_motif_12,
        max(case when code_motif = '13' and statut_agreg = 'clot' then nb_reclamations end) as clot_nb_recla_motif_13,
        max(case when code_motif = '14' and statut_agreg = 'clot' then nb_reclamations end) as clot_nb_recla_motif_14,
        max(case when code_motif = '15' and statut_agreg = 'clot' then nb_reclamations end) as clot_nb_recla_motif_15,
        max(case when code_motif = '16' and statut_agreg = 'clot' then nb_reclamations end) as clot_nb_recla_motif_16,
        max(case when code_motif = '17' and statut_agreg = 'clot' then nb_reclamations end) as clot_nb_recla_motif_17,
        max(case when code_motif = '18' and statut_agreg = 'clot' then nb_reclamations end) as clot_nb_recla_motif_18,
        max(case when code_motif = '19' and statut_agreg = 'clot' then nb_reclamations end) as clot_nb_recla_motif_19,
        max(case when code_motif = '155' and statut_agreg = 'clot' then nb_reclamations end) as clot_nb_recla_motif_155,
        max(case when code_motif = '156' and statut_agreg = 'clot' then nb_reclamations end) as clot_nb_recla_motif_156
    from agreg_motif
    group by
        identifiant,
        n_finess_rpps,
        annee_de_reception
)

select
    identifiant,
    case when length(cast(n_finess_rpps as varchar)) = 8 then '0' || n_finess_rpps else n_finess_rpps end as n_finess_rpps,
    annee_de_reception,
    encours_nb_recla_total,
    encours_nb_recla_10,
    encours_nb_recla_11,
    encours_nb_recla_12,
    encours_nb_recla_13,
    encours_nb_recla_14,
    encours_nb_recla_15,
    encours_nb_recla_16,
    encours_nb_recla_17,
    encours_nb_recla_18,
    encours_nb_recla_19,
    encours_nb_recla_155,
    encours_nb_recla_156,
    clot_nb_recla_total,
    clot_nb_recla_motif_10,
    clot_nb_recla_motif_11,
    clot_nb_recla_motif_12,
    clot_nb_recla_motif_13,
    clot_nb_recla_motif_14,
    clot_nb_recla_motif_15,
    clot_nb_recla_motif_16,
    clot_nb_recla_motif_17,
    clot_nb_recla_motif_18,
    clot_nb_recla_motif_19,
    clot_nb_recla_motif_155,
    clot_nb_recla_motif_156
from pivoted