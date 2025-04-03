-- Modèle DBT pour la vue sivss_to_helios

select
    STRUCTURE_INTITULE,
    NUMERO_SIVSS,
    DATE_RECEPTION,
    FAMILLE_PRINCIPALE,
    NATURE_PRINCIPALE,
    AUTRE_SIGNAL_LIBELLE,
    FAMILLE_SECONDAIRE,
    NATURE_SECONDAIRE,
    AUTRE_SIGNAL_SECONDAIRE_LIBELLE,
    EST_EIGS,
    CONSEQUENCES_PERSONNE_EXPOSEE,
    RECLAMATION,
    DECLARANT_EST_ANONYME,
    DECLARANT_QUALITE_FONCTION,
    DECLARANT_CATEGORIE,
    DECLARANT_ORGANISME_TYPE,
    DECLARANT_ETABLISSEMENT_TYPE,
    DECLARANT_ORGANISME_NUMERO_FINESS,
    DECLARANT_ORGANISME_NOM,
    DECLARANT_ORGANISME_REGION,
    DECLARANT_ORGANISME_DEPARTEMENT,
    DECLARANT_ORGANISME_CODE_POSTAL,
    DECLARANT_ORGANISME_COMMUNE,
    DECLARANT_ORGANISME_CODE_INSEE,
    SURVENUE_CAS_COLLECTIVITE,
    SCC_ORGANISME_TYPE,
    SCC_ETABLISSEMENT_TYPE,
    SCC_ORGANISME_NOM,
    SCC_ORGANISME_FINESS,
    SCC_ORGANISME_REGION,
    SCC_ORGANISME_DEPARTEMENT,
    SCC_ORGANISME_CODE_POSTAL,
    SCC_ORGANISME_COMMUNE,
    SCC_ORGANISME_CODE_INSEE,
    ETAT,
    SUPPORT_SIGNALEMENT,
    DATE_CLOTURE,
    MOTIF_CLOTURE
from "duckdb_database"."main"."staging__helios_sivss"
where
    length(DATE_CLOTURE) <= 10
    and (
        DATE_CLOTURE = '--'
        or substr(DATE_CLOTURE, 1, 4) in ('2022', '2023', '2024')
    )