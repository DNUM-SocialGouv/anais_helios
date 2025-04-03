
  
  create view "duckdb_database"."main"."staging__matrice_sirec__dbt_tmp" as (
    

WITH sirec AS (
    SELECT * FROM "duckdb_database"."main"."staging__sa_sirec"
)

SELECT 
    "Numéro de la réclamation" AS numero_reclamation,
    "Ancien numéro de la réclamation" AS ancien_numero_reclamation,
    "Numéro Démat Social" AS numero_demat_social,
    "Service gestionnaire" AS service_gestionnaire,
    "Statut de la réclamation" AS statut_reclamation,
    "Services en lecture" AS services_en_lecture,
    Signalement AS signalement,
    "Date de la demande du requérant" AS date_demande_requerant,
    "Date de réception à l’ARS" AS date_reception_ars,
    "Service de premier niveau" AS service_premier_niveau,
    "Date de réception au service de premier niveau" AS date_reception_service_premier_niveau,
    Description AS description,
    "Date de création de la réclamation" AS date_creation_reclamation,
    "Domaine fonctionnel" AS domaine_fonctionnel,
    "Mode de réception" AS mode_reception,
    "Prioritaire : Oui/Non" AS prioritaire_oui_non,
    "Précisions sur le caractère prioritaire" AS precisions_caractere_prioritaire,
    "Département de la réclamation" AS departement_reclamation,
    "Destinataire(s) de la réclamation" AS destinataires_reclamation,
    "Destinataire primaire" AS destinataire_primaire,
    "Destinataire secondaire" AS destinataire_secondaire,
    "Saisine du procureur par requérant" AS saisine_procureur_requerant,
    "Institutions de provenance" AS institutions_provenance,
    "Date de réception à l'institution de provenance" AS date_reception_institution_provenance,
    reponse_attendue,
    "Courrier signalé : Oui/Non" AS courrier_signale_oui_non,
    "Le requérant est" AS le_requerant_est,
    "Le requérant est anonyme" AS le_requerant_est_anonyme,
    "Le requérant souhaite garder l'anonymat" AS le_requerant_souhaite_garder_anonymat,
    "Statut du requérant" AS statut_requerant,
    "Plus de 2 réclamations déposées" AS plus_de_2_reclamations_deposees,
    "Usager (victime) non identifiée" AS usager_victime_non_identifiee,
    "Sans mis en cause" AS sans_mis_en_cause,
    CAST("N° FINESS/RPPS" AS VARCHAR) AS numero_finess_rpps,  -- ✅ Conversion ici
    Autre_Type AS autre_type,
    "Nom structure" AS nom_structure,
    Adresse AS adresse,
    "Code postal" AS code_postal,
    Ville AS ville,
    "Service pour les établissements sanitaires" AS service_etablissements_sanitaires,
    "Observations du mis en cause" AS observations_mis_en_cause,
    "Motifs IGAS entrée" AS motifs_igas_entree,
    "Motifs IGAS sortie" AS motifs_igas_sortie,
    "Niveau de compétence de traitement de la réclamation" AS niveau_competence_traitement_reclamation,
    "Institution(s) partenaire(s)" AS institutions_partenaires,
    "Précisions sur le niveau de compétence de traitement de la réclamation" AS precisions_niveau_competence_traitement_reclamation,
    "Envoi d'un accusé réception" AS envoi_accuse_reception,
    "Date d'envoi de l'AR" AS date_envoi_ar,
    "Précisions sur le non envoi de l'AR" AS precisions_non_envoi_ar,
    "Date de transfert à l'institution compétente" AS date_transfert_institution_competente,
    "Date de prise en charge par le service gestionnaire" AS date_prise_en_charge_service_gestionnaire,
    "Type de traitement" AS type_traitement,
    "Précisions sur le type de traitement" AS precisions_type_traitement,
    "Réclamation en lien avec un ou plusieurs signalements" AS reclamation_lien_signalements,
    "Numéro(s) de signalement(s) associé(s)" AS numeros_signalements_associes,
    "Date d'examen en commission" AS date_examen_commission,
    "Type d'action" AS type_action,
    "Mesures prises par le mis en cause" AS mesures_mis_en_cause,
    "Mesures à l'initiative de" AS mesures_a_initiative,
    "Réponse au requérant par l'ARS" AS reponse_requerant_ars,
    "Date de réponse au requérant" AS date_reponse_requerant,
    "Précisions sur la réponse au requérant" AS precisions_reponse_requerant,
    "Date de réponse  à l'institution de provenance" AS date_reponse_institution_provenance,
    "Motif de clôture" AS motif_cloture,
    Commentaire AS commentaire,
    "Date de clôture" AS date_cloture,
    "Siège ARS" AS siege_ars,
    "Motifs déclarés" AS motifs_declares
FROM sirec
WHERE LENGTH(CAST("N° FINESS/RPPS" AS VARCHAR)) IN (8, 9)
AND "Date de réception à l’ARS" < '2024-12-31'
  );
