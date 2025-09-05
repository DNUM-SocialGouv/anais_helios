-- Modèle DBT pour la vue sivss_to_helios

select
    structure_intitule as "STRUCTURE_INTITULE",
    numero_sivss as "NUMERO_SIVSS",
    CAST(
        CASE 
            WHEN date_reception = '--' THEN NULL
            ELSE 
                substr(date_reception, 1, 10)
        END AS DATE
    ) AS "DATE_RECEPTION",
    famille_principale as "FAMILLE_PRINCIPALE",
    nature_principale as "NATURE_PRINCIPALE",
    autre_signal_libelle as "AUTRE_SIGNAL_LIBELLE",
    famille_secondaire as "FAMILLE_SECONDAIRE",
    nature_secondaire as "NATURE_SECONDAIRE",
    autre_signal_secondaire_libelle as "AUTRE_SIGNAL_SECONDAIRE_LIBELLE",
    est_eigs as "EST_EIGS",
    consequences_personne_exposee as "CONSEQUENCES_PERSONNE_EXPOSEE",
    reclamation as "RECLAMATION",
    declarant_est_anonyme as "DECLARANT_EST_ANONYME",
    declarant_qualite_fonction as "DECLARANT_QUALITE_FONCTION",
    declarant_categorie as "DECLARANT_CATEGORIE",
    declarant_organisme_type as "DECLARANT_ORGANISME_TYPE",
    declarant_etablissement_type as "DECLARANT_ETABLISSEMENT_TYPE",
    declarant_organisme_numero_finess as "DECLARANT_ORGANISME_NUMERO_FINESS",
    declarant_organisme_nom as "DECLARANT_ORGANISME_NOM",
    declarant_organisme_region as "DECLARANT_ORGANISME_REGION",
    declarant_organisme_departement as "DECLARANT_ORGANISME_DEPARTEMENT",
    declarant_organisme_code_postal as "DECLARANT_ORGANISME_CODE_POSTAL",
    declarant_organisme_commune as "DECLARANT_ORGANISME_COMMUNE",
    declarant_organisme_code_insee as "DECLARANT_ORGANISME_CODE_INSEE",
    survenue_cas_collectivite as "SURVENUE_CAS_COLLECTIVITE",
    scc_organisme_type as "SCC_ORGANISME_TYPE",
    scc_etablissement_type as "SCC_ETABLISSEMENT_TYPE",
    scc_organisme_nom as "SCC_ORGANISME_NOM",
    scc_organisme_finess as "SCC_ORGANISME_FINESS",
    scc_organisme_region as "SCC_ORGANISME_REGION",
    scc_organisme_departement as "SCC_ORGANISME_DEPARTEMENT",
    scc_organisme_code_postal as "SCC_ORGANISME_CODE_POSTAL",
    scc_organisme_commune as "SCC_ORGANISME_COMMUNE",
    scc_organisme_code_insee as "SCC_ORGANISME_CODE_INSEE",
    etat as "ETAT",
    support_signalement as "SUPPORT_SIGNALEMENT",
    CAST(
        CASE 
            WHEN date_cloture = '--' THEN NULL
            ELSE 
                substr(date_cloture, 1, 10)
        END AS DATE
    ) AS "DATE_CLOTURE",
    motif_cloture as "MOTIF_CLOTURE"
from {{ ref('staging__helios_sivss') }}
where
    length(date_reception) <= 10
    and (
        date_reception  = '--'
        or substr(date_reception , 1, 4) in {{ dbtStaging.get_x_previous_year(x=3)}} -- A confirmer ('2022', '2023', '2024')
    )