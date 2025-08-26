-- Modèle DBT pour la vue sivss_to_helios

select
    structure_intitule,
    numero_sivss,
    date_reception,
    famille_principale,
    nature_principale,
    autre_signal_libelle,
    famille_secondaire,
    nature_secondaire,
    autre_signal_secondaire_libelle,
    est_eigs,
    consequences_personne_exposee,
    reclamation,
    declarant_est_anonyme,
    declarant_qualite_fonction,
    declarant_categorie,
    declarant_organisme_type,
    declarant_etablissement_type,
    declarant_organisme_numero_finess,
    declarant_organisme_nom,
    declarant_organisme_region,
    declarant_organisme_departement,
    declarant_organisme_code_postal,
    declarant_organisme_commune,
    declarant_organisme_code_insee,
    survenue_cas_collectivite,
    scc_organisme_type,
    scc_etablissement_type,
    scc_organisme_nom,
    scc_organisme_finess,
    scc_organisme_region,
    scc_organisme_departement,
    scc_organisme_code_postal,
    scc_organisme_commune,
    scc_organisme_code_insee,
    etat,
    support_signalement,
    date_cloture,
    motif_cloture
from {{ ref('staging__helios_sivss') }}
where
    length(date_reception) <= 10
    and (
        date_reception  = '--'
        or substr(date_reception , 1, 4) in {{ dbtStaging.get_x_previous_year(x=2)}} -- A confirmer ('2022', '2023', '2024')
    )