-- sirec_to_helios source
 
CREATE VIEW sirec_to_helios AS
-- requete finale
SELECT
IDENTIFIANT
, IIF(LENGTH("N° FINESS/RPPS")=8, "0" || "N° FINESS/RPPS", "N° FINESS/RPPS") AS NDEG_FINESS_RPPS
, ANNEE_DE_RECEPTION
, ENCOURS_NB_RECLA_TOTAL
, ENCOURS_NB_RECLA_MOTIF_10
, ENCOURS_NB_RECLA_MOTIF_11
, ENCOURS_NB_RECLA_MOTIF_12
, ENCOURS_NB_RECLA_MOTIF_13
, ENCOURS_NB_RECLA_MOTIF_14
, ENCOURS_NB_RECLA_MOTIF_15
, ENCOURS_NB_RECLA_MOTIF_16
, ENCOURS_NB_RECLA_MOTIF_17
, ENCOURS_NB_RECLA_MOTIF_18
, ENCOURS_NB_RECLA_MOTIF_19
, ENCOURS_NB_RECLA_MOTIF_155
, ENCOURS_NB_RECLA_MOTIF_156
, CLOT_NB_RECLA_TOTAL
, CLOT_NB_RECLA_MOTIF_10
, CLOT_NB_RECLA_MOTIF_11
, CLOT_NB_RECLA_MOTIF_12
, CLOT_NB_RECLA_MOTIF_13
, CLOT_NB_RECLA_MOTIF_14
, CLOT_NB_RECLA_MOTIF_15
, CLOT_NB_RECLA_MOTIF_16
, CLOT_NB_RECLA_MOTIF_17
, CLOT_NB_RECLA_MOTIF_18
, CLOT_NB_RECLA_MOTIF_19
, CLOT_NB_RECLA_MOTIF_155
, CLOT_NB_RECLA_MOTIF_156
FROM
	-- requête pour avoir le nb d réclamations totales par statut
	(SELECT
	statuts.IDENTIFIANT,
	statuts."N° FINESS/RPPS",
	statuts.ANNEE_DE_RECEPTION,
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
	FROM
		(SELECT
		IDENTIFIANT,
		"N° FINESS/RPPS",
		ANNEE_DE_RECEPTION,
		MAX(ENCOURS_TOTAL) AS ENCOURS_NB_RECLA_TOTAL,
		MAX(CLOT_TOTAL) AS CLOT_NB_RECLA_TOTAL
		FROM
			(SELECT
			IDENTIFIANT,
			"N° FINESS/RPPS",
			ANNEE_DE_RECEPTION,
			STATUT_AGREG,
			NB_RECLAMATIONS,
			CASE
				WHEN STATUT_AGREG = "en cours" THEN NB_RECLAMATIONS
			END AS ENCOURS_TOTAL,
			CASE
				WHEN STATUT_AGREG = "clot" THEN NB_RECLAMATIONS
			END AS CLOT_TOTAL
			FROM
				(SELECT
					"N° FINESS/RPPS" || ANNEE_DE_RECEPTION AS IDENTIFIANT,
					"N° FINESS/RPPS",
					ANNEE_DE_RECEPTION,
					STATUT_AGREG,
					COUNT(DISTINCT "Numéro de la réclamation") AS NB_RECLAMATIONS
				FROM
					(SELECT
						--"Numéro de la réclamation",
						"Numéro de la réclamation",
						"Statut de la réclamation",
						Signalement,
						"N° FINESS/RPPS",
						"Nom structure",
						Adresse,
						--Adresse_1,
						"Date de réception à l’ARS",
						"Motifs IGAS",
						"Siège ARS",
						SUBSTR("Date de réception à l’ARS",1,4) as ANNEE_DE_RECEPTION,
						CASE
							WHEN
								"Statut de la réclamation" = 'Affectation'
								OR "Statut de la réclamation" = 'Réponse'
								OR "Statut de la réclamation" = 'Traitement'
							THEN 'en cours'
							WHEN "Statut de la réclamation" = 'Clôture'
							THEN 'clot'
						END as STATUT_AGREG
					FROM "HeliosSirec") statut_query
					GROUP BY
					"N° FINESS/RPPS",
					ANNEE_DE_RECEPTION,
					STATUT_AGREG) agreg_statut_query
			) agreg_col_statut_query
		GROUP BY
		IDENTIFIANT,
		"N° FINESS/RPPS",
		ANNEE_DE_RECEPTION
		) statuts
		LEFT JOIN (
		SELECT
		IDENTIFIANT,
		"N° FINESS/RPPS",
		ANNEE_DE_RECEPTION,
		MAX(ENCOURS_NB_RECLA_MOTIF_10) AS ENCOURS_NB_RECLA_MOTIF_10,
		MAX(ENCOURS_NB_RECLA_MOTIF_11) AS ENCOURS_NB_RECLA_MOTIF_11,
		MAX(ENCOURS_NB_RECLA_MOTIF_12) AS ENCOURS_NB_RECLA_MOTIF_12,
		MAX(ENCOURS_NB_RECLA_MOTIF_13) AS ENCOURS_NB_RECLA_MOTIF_13,
		MAX(ENCOURS_NB_RECLA_MOTIF_14) AS ENCOURS_NB_RECLA_MOTIF_14,
		MAX(ENCOURS_NB_RECLA_MOTIF_15) AS ENCOURS_NB_RECLA_MOTIF_15,
		MAX(ENCOURS_NB_RECLA_MOTIF_16) AS ENCOURS_NB_RECLA_MOTIF_16,
		MAX(ENCOURS_NB_RECLA_MOTIF_17) AS ENCOURS_NB_RECLA_MOTIF_17,
		MAX(ENCOURS_NB_RECLA_MOTIF_18) AS ENCOURS_NB_RECLA_MOTIF_18,
		MAX(ENCOURS_NB_RECLA_MOTIF_19) AS ENCOURS_NB_RECLA_MOTIF_19,
		MAX(ENCOURS_NB_RECLA_MOTIF_155) AS ENCOURS_NB_RECLA_MOTIF_155,
		MAX(ENCOURS_NB_RECLA_MOTIF_156) AS ENCOURS_NB_RECLA_MOTIF_156,
		MAX(CLOT_NB_RECLA_MOTIF_10) AS CLOT_NB_RECLA_MOTIF_10,
		MAX(CLOT_NB_RECLA_MOTIF_11) AS CLOT_NB_RECLA_MOTIF_11,
		MAX(CLOT_NB_RECLA_MOTIF_12) AS CLOT_NB_RECLA_MOTIF_12,
		MAX(CLOT_NB_RECLA_MOTIF_13) AS CLOT_NB_RECLA_MOTIF_13,
		MAX(CLOT_NB_RECLA_MOTIF_14) AS CLOT_NB_RECLA_MOTIF_14,
		MAX(CLOT_NB_RECLA_MOTIF_15) AS CLOT_NB_RECLA_MOTIF_15,
		MAX(CLOT_NB_RECLA_MOTIF_16) AS CLOT_NB_RECLA_MOTIF_16,
		MAX(CLOT_NB_RECLA_MOTIF_17) AS CLOT_NB_RECLA_MOTIF_17,
		MAX(CLOT_NB_RECLA_MOTIF_18) AS CLOT_NB_RECLA_MOTIF_18,
		MAX(CLOT_NB_RECLA_MOTIF_19) AS CLOT_NB_RECLA_MOTIF_19,
		MAX(CLOT_NB_RECLA_MOTIF_155) AS CLOT_NB_RECLA_MOTIF_155,
		MAX(CLOT_NB_RECLA_MOTIF_156) AS CLOT_NB_RECLA_MOTIF_156
		FROM
			(SELECT
			*,
			-- statut en cours
			CASE
				WHEN MOTIFS_IGAS_SPLIT LIKE "%Hôtellerie-locaux-restauration%" AND STATUT_AGREG = "en cours" THEN NB_RECLAMATIONS
			END AS ENCOURS_NB_RECLA_MOTIF_10,
			CASE
				WHEN MOTIFS_IGAS_SPLIT LIKE "%Problème d?organisation ou de fonctionnement de l?établissement ou du service%" AND STATUT_AGREG = "en cours" THEN NB_RECLAMATIONS
			END AS ENCOURS_NB_RECLA_MOTIF_11,
			CASE
				WHEN MOTIFS_IGAS_SPLIT LIKE "%Problème de qualité des soins médicaux%" AND STATUT_AGREG = "en cours" THEN NB_RECLAMATIONS
			END AS ENCOURS_NB_RECLA_MOTIF_12,
			CASE
				WHEN MOTIFS_IGAS_SPLIT LIKE "%Problème de qualité des soins paramédicaux%" AND STATUT_AGREG = "en cours" THEN NB_RECLAMATIONS
			END AS ENCOURS_NB_RECLA_MOTIF_13,
			CASE
				WHEN MOTIFS_IGAS_SPLIT LIKE "%Recherche d?établissement ou d?un professionnel%" AND STATUT_AGREG = "en cours" THEN NB_RECLAMATIONS
			END AS ENCOURS_NB_RECLA_MOTIF_14,
			CASE
				WHEN MOTIFS_IGAS_SPLIT LIKE "%Mise en cause attitude des professionnels%" AND STATUT_AGREG = "en cours" THEN NB_RECLAMATIONS
			END AS ENCOURS_NB_RECLA_MOTIF_15,
			CASE
				WHEN MOTIFS_IGAS_SPLIT LIKE "%Informations et droits des usagers%" AND STATUT_AGREG = "en cours" THEN NB_RECLAMATIONS
			END AS ENCOURS_NB_RECLA_MOTIF_16,
			CASE
				WHEN MOTIFS_IGAS_SPLIT LIKE "%Facturation et honoraires%" AND STATUT_AGREG = "en cours" THEN NB_RECLAMATIONS
			END AS ENCOURS_NB_RECLA_MOTIF_17,
			CASE
				WHEN MOTIFS_IGAS_SPLIT LIKE "%Santé-environnementale%" AND STATUT_AGREG = "en cours" THEN NB_RECLAMATIONS
			END AS ENCOURS_NB_RECLA_MOTIF_18,
			CASE
				WHEN MOTIFS_IGAS_SPLIT LIKE "%Activités d?esthétique réglementées%" AND STATUT_AGREG = "en cours" THEN NB_RECLAMATIONS
			END AS ENCOURS_NB_RECLA_MOTIF_19,
			CASE
				WHEN MOTIFS_IGAS_SPLIT LIKE "%A renseigner%" AND STATUT_AGREG = "en cours" THEN NB_RECLAMATIONS
			END AS ENCOURS_NB_RECLA_MOTIF_155,
			CASE
				WHEN MOTIFS_IGAS_SPLIT LIKE "%COVID-19%" AND STATUT_AGREG = "en cours" THEN NB_RECLAMATIONS
			END AS ENCOURS_NB_RECLA_MOTIF_156,
			-- statut clot
			CASE
				WHEN MOTIFS_IGAS_SPLIT LIKE "%Hôtellerie-locaux-restauration%" AND STATUT_AGREG = "clot" THEN NB_RECLAMATIONS
			END AS CLOT_NB_RECLA_MOTIF_10,
			CASE
				WHEN MOTIFS_IGAS_SPLIT LIKE "%Problème d?organisation ou de fonctionnement de l?établissement ou du service%" AND STATUT_AGREG = "clot" THEN NB_RECLAMATIONS
			END AS CLOT_NB_RECLA_MOTIF_11,
			CASE
				WHEN MOTIFS_IGAS_SPLIT LIKE "%Problème de qualité des soins médicaux%" AND STATUT_AGREG = "clot" THEN NB_RECLAMATIONS
			END AS CLOT_NB_RECLA_MOTIF_12,
			CASE
				WHEN MOTIFS_IGAS_SPLIT LIKE "%Problème de qualité des soins paramédicaux%" AND STATUT_AGREG = "clot" THEN NB_RECLAMATIONS
			END AS CLOT_NB_RECLA_MOTIF_13,
			CASE
				WHEN MOTIFS_IGAS_SPLIT LIKE "%Recherche d?établissement ou d?un professionnel%" AND STATUT_AGREG = "clot" THEN NB_RECLAMATIONS
			END AS CLOT_NB_RECLA_MOTIF_14,
			CASE
				WHEN MOTIFS_IGAS_SPLIT LIKE "%Mise en cause attitude des professionnels%" AND STATUT_AGREG = "clot" THEN NB_RECLAMATIONS
			END AS CLOT_NB_RECLA_MOTIF_15,
			CASE
				WHEN MOTIFS_IGAS_SPLIT LIKE "%Informations et droits des usagers%" AND STATUT_AGREG = "clot" THEN NB_RECLAMATIONS
			END AS CLOT_NB_RECLA_MOTIF_16,
			CASE
				WHEN MOTIFS_IGAS_SPLIT LIKE "%Facturation et honoraires%" AND STATUT_AGREG = "clot" THEN NB_RECLAMATIONS
			END AS CLOT_NB_RECLA_MOTIF_17,
			CASE
				WHEN MOTIFS_IGAS_SPLIT LIKE "%Santé-environnementale%" AND STATUT_AGREG = "clot" THEN NB_RECLAMATIONS
			END AS CLOT_NB_RECLA_MOTIF_18,
			CASE
				WHEN MOTIFS_IGAS_SPLIT LIKE "%Activités d?esthétique réglementées%" AND STATUT_AGREG = "clot" THEN NB_RECLAMATIONS
			END AS CLOT_NB_RECLA_MOTIF_19,
			CASE
				WHEN MOTIFS_IGAS_SPLIT LIKE "%A renseigner%" AND STATUT_AGREG = "clot" THEN NB_RECLAMATIONS
			END AS CLOT_NB_RECLA_MOTIF_155,
			CASE
				WHEN MOTIFS_IGAS_SPLIT LIKE "%COVID-19%" AND STATUT_AGREG = "clot" THEN NB_RECLAMATIONS
			END AS CLOT_NB_RECLA_MOTIF_156
			FROM
				(SELECT
				"N° FINESS/RPPS" || ANNEE_DE_RECEPTION AS IDENTIFIANT,
				"N° FINESS/RPPS",
				ANNEE_DE_RECEPTION,
				STATUT_AGREG,
				MOTIFS_IGAS_SPLIT,
				COUNT(DISTINCT "Numéro de la réclamation") AS NB_RECLAMATIONS
				FROM
					(SELECT
					"Numéro de la réclamation",
					MOTIFS_IGAS_SPLIT,
					"N° FINESS/RPPS",
					ANNEE_DE_RECEPTION,
					STATUT_AGREG
					FROM
						(SELECT
						split_query."Numéro de la réclamation",
						TRIM(split_query."Motifs IGAS") as MOTIFS_IGAS_SPLIT,
						src."Numéro de la réclamation",
						src."Statut de la réclamation",
						src.Signalement,
						src."N° FINESS/RPPS",
						src."Nom structure",
						src.Adresse,
						--src.Adresse_1,
						src."Date de réception à l’ARS",
						src."Motifs IGAS",
						src."Siège ARS",
						SUBSTR("Date de réception à l’ARS",1,4) as ANNEE_DE_RECEPTION,
						CASE
							WHEN
								"Statut de la réclamation" = 'Affectation'
								OR "Statut de la réclamation" = 'Réponse'
								OR "Statut de la réclamation" = 'Traitement'
							THEN 'en cours'
							WHEN "Statut de la réclamation" = 'Clôture'
							THEN 'clot'
						END as STATUT_AGREG
						FROM
						-- split les motifs IGAS et créé 1 ligne par valeur trouvée
							(WITH RECURSIVE split(
							    "Numéro de la réclamation",
							    "Motifs IGAS",
							    str
								) AS (
							    -- Initial select to set up the recursion with appended '|' for easier splitting
							    SELECT
							        "Numéro de la réclamation",
							        '',
							        "Motifs IGAS" || '|'
							    FROM "HeliosSirec"
							    UNION ALL
							    -- Recursive step: take the current string, split on the first '|', and process the remainder
							    SELECT
							        "Numéro de la réclamation",
							        TRIM(SUBSTR(str, 0, INSTR(str, '|'))), -- Extract the first value before the '|'
							        SUBSTR(str, INSTR(str, '|') + 1) -- Remainder of the string after the '|'
							    FROM split
							    WHERE str != ''
							    AND INSTR(str, '|') > 0 -- Ensure there's something left to split
							)
							SELECT
							    "Numéro de la réclamation",
							    "Motifs IGAS"
							FROM split
							WHERE "Motifs IGAS" != '' -- Remove empty results	
							) split_query
						LEFT JOIN "HeliosSirec" src ON split_query."Numéro de la réclamation" = src."Numéro de la réclamation") join_split_query
					WHERE (MOTIFS_IGAS_SPLIT LIKE "%Problème d?organisation ou de fonctionnement de l?établissement ou du service%"
					OR MOTIFS_IGAS_SPLIT LIKE "%A renseigner%"
					OR MOTIFS_IGAS_SPLIT LIKE "%Informations et droits des usagers%"
					OR MOTIFS_IGAS_SPLIT LIKE "%Problème de qualité des soins médicaux%"
					OR MOTIFS_IGAS_SPLIT LIKE "%Mise en cause attitude des professionnels%"
					OR MOTIFS_IGAS_SPLIT LIKE "%Problème de qualité des soins paramédicaux%"
					OR MOTIFS_IGAS_SPLIT LIKE "%Recherche d?établissement ou d?un professionnel%"
					OR MOTIFS_IGAS_SPLIT LIKE "%Hôtellerie-locaux-restauration%"
					OR MOTIFS_IGAS_SPLIT LIKE "%Facturation et honoraires%"
					OR MOTIFS_IGAS_SPLIT LIKE "%Activités d?esthétique réglementées%"
					OR MOTIFS_IGAS_SPLIT LIKE "%Santé-environnementale%"
					OR MOTIFS_IGAS_SPLIT LIKE "%COVID-19%")
					) filter_motifs_query
				GROUP BY
				"N° FINESS/RPPS",
				ANNEE_DE_RECEPTION,
				STATUT_AGREG,
				MOTIFS_IGAS_SPLIT) agreg_motifs_query
			) agreg_motifs_col_query
		GROUP BY
		IDENTIFIANT,
		"N° FINESS/RPPS",
		ANNEE_DE_RECEPTION) motifs ON statuts.IDENTIFIANT = motifs.IDENTIFIANT) requete_finale
-- filtre pour exclure les RPPS en supposant que tous les numéros <= 9 caractères sont des FINESS
WHERE LENGTH("N° FINESS/RPPS") > 4 AND LENGTH("N° FINESS/RPPS") <= 9;