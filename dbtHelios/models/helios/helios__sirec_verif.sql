-- sirec_to_helios source

with sirec_to_helios as (
-- requete finale
SELECT 
identifiant
, {{ iif_replacement("LENGTH(n_finess_rpps)=8", "'0' || n_finess_rpps", "n_finess_rpps") }} AS NDEG_FINESS_RPPS
, annee_de_reception
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
	statuts.identifiant,
	statuts.n_finess_rpps,
	statuts.annee_de_reception,
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
		identifiant,
		n_finess_rpps,
		annee_de_reception,
		MAX(ENCOURS_TOTAL) AS ENCOURS_NB_RECLA_TOTAL,
		MAX(CLOT_TOTAL) AS CLOT_NB_RECLA_TOTAL
		FROM
			(SELECT 
			identifiant,
			n_finess_rpps,
			annee_de_reception,
			STATUT_AGREG,
			NB_RECLAMATIONS,
			CASE 
				WHEN STATUT_AGREG = 'en cours' THEN NB_RECLAMATIONS
			END AS ENCOURS_TOTAL,
			CASE 
				WHEN STATUT_AGREG = 'clot' THEN NB_RECLAMATIONS
			END AS CLOT_TOTAL
			FROM 
				(SELECT 
					n_finess_rpps || annee_de_reception AS identifiant,
					n_finess_rpps,
					annee_de_reception,
					STATUT_AGREG,
					COUNT(DISTINCT numero_de_la_reclamation) AS NB_RECLAMATIONS
				FROM 
					(SELECT
						--numero_de_la_reclamation,
						numero_de_la_reclamation,
						statut_de_la_reclamation,
						Signalement,
						n_finess_rpps,
						nom_structure,
						Adresse,
						--Adresse_1,
						date_de_reception_a_l_ars,
						case
							when motifs_igas_sortie is not null and trim(motifs_igas_sortie) != '' then motifs_igas_sortie
							else motifs_igas_entree
						end as motifs_igas,
						siege_ars,
						SUBSTR(date_de_reception_a_l_ars,1,4) as annee_de_reception,
						CASE
							WHEN 
								statut_de_la_reclamation = 'Affectation'
								OR statut_de_la_reclamation = 'Réponse'
								OR statut_de_la_reclamation = 'Traitement'
							THEN 'en cours'
							WHEN statut_de_la_reclamation = 'Clôture'
							THEN 'clot'
						END as STATUT_AGREG
					FROM {{ ref('staging__helios_sirec') }}) statut_query
					GROUP BY
					n_finess_rpps,
					annee_de_reception,
					STATUT_AGREG) agreg_statut_query
			) agreg_col_statut_query
		GROUP BY 
		identifiant,
		n_finess_rpps,
		annee_de_reception
		) statuts
		LEFT JOIN (
		SELECT 
		identifiant,
		n_finess_rpps,
		annee_de_reception,
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
				WHEN MOTIFS_IGAS_SPLIT LIKE '%Hôtellerie-locaux-restauration%' AND STATUT_AGREG = 'en cours' THEN NB_RECLAMATIONS
			END AS ENCOURS_NB_RECLA_MOTIF_10,
			CASE
				WHEN MOTIFS_IGAS_SPLIT LIKE '%organisation ou de fonctionnement%' AND STATUT_AGREG = 'en cours' THEN NB_RECLAMATIONS
			END AS ENCOURS_NB_RECLA_MOTIF_11,
			CASE
				WHEN MOTIFS_IGAS_SPLIT LIKE '%qualité des soins médicaux%' AND STATUT_AGREG = 'en cours' THEN NB_RECLAMATIONS
			END AS ENCOURS_NB_RECLA_MOTIF_12,
			CASE
				WHEN MOTIFS_IGAS_SPLIT LIKE '%qualité des soins paramédicaux%' AND STATUT_AGREG = 'en cours' THEN NB_RECLAMATIONS
			END AS ENCOURS_NB_RECLA_MOTIF_13,
			CASE
				WHEN MOTIFS_IGAS_SPLIT LIKE '%établissement ou d%un professionnel%' AND STATUT_AGREG = 'en cours' THEN NB_RECLAMATIONS
			END AS ENCOURS_NB_RECLA_MOTIF_14,
			CASE
				WHEN MOTIFS_IGAS_SPLIT LIKE '%attitude des professionnels%' AND STATUT_AGREG = 'en cours' THEN NB_RECLAMATIONS
			END AS ENCOURS_NB_RECLA_MOTIF_15,
			CASE
				WHEN MOTIFS_IGAS_SPLIT LIKE '%droits des usagers%' AND STATUT_AGREG = 'en cours' THEN NB_RECLAMATIONS
			END AS ENCOURS_NB_RECLA_MOTIF_16,
			CASE
				WHEN MOTIFS_IGAS_SPLIT LIKE '%Facturation et honoraires%' AND STATUT_AGREG = 'en cours' THEN NB_RECLAMATIONS
			END AS ENCOURS_NB_RECLA_MOTIF_17,
			CASE
				WHEN MOTIFS_IGAS_SPLIT LIKE '%Santé-environnementale%' AND STATUT_AGREG = 'en cours' THEN NB_RECLAMATIONS
			END AS ENCOURS_NB_RECLA_MOTIF_18,
			CASE
				WHEN MOTIFS_IGAS_SPLIT LIKE '%esthétique réglementées%' AND STATUT_AGREG = 'en cours' THEN NB_RECLAMATIONS
			END AS ENCOURS_NB_RECLA_MOTIF_19,
			CASE
				WHEN MOTIFS_IGAS_SPLIT LIKE '%A renseigner%' AND STATUT_AGREG = 'en cours' THEN NB_RECLAMATIONS
			END AS ENCOURS_NB_RECLA_MOTIF_155,
			CASE
				WHEN MOTIFS_IGAS_SPLIT LIKE '%COVID-19%' AND STATUT_AGREG = 'en cours' THEN NB_RECLAMATIONS
			END AS ENCOURS_NB_RECLA_MOTIF_156,
			-- statut clot
			CASE
				WHEN MOTIFS_IGAS_SPLIT LIKE '%Hôtellerie-locaux-restauration%' AND STATUT_AGREG = 'clot' THEN NB_RECLAMATIONS
			END AS CLOT_NB_RECLA_MOTIF_10,
			CASE
				WHEN MOTIFS_IGAS_SPLIT LIKE '%organisation ou de fonctionnement%' AND STATUT_AGREG = 'clot' THEN NB_RECLAMATIONS
			END AS CLOT_NB_RECLA_MOTIF_11,
			CASE
				WHEN MOTIFS_IGAS_SPLIT LIKE '%qualité des soins médicaux%' AND STATUT_AGREG = 'clot' THEN NB_RECLAMATIONS
			END AS CLOT_NB_RECLA_MOTIF_12,
			CASE
				WHEN MOTIFS_IGAS_SPLIT LIKE '%qualité des soins paramédicaux%' AND STATUT_AGREG = 'clot' THEN NB_RECLAMATIONS
			END AS CLOT_NB_RECLA_MOTIF_13,
			CASE
				WHEN MOTIFS_IGAS_SPLIT LIKE '%établissement ou d%un professionnel%' AND STATUT_AGREG = 'clot' THEN NB_RECLAMATIONS
			END AS CLOT_NB_RECLA_MOTIF_14,
			CASE
				WHEN MOTIFS_IGAS_SPLIT LIKE '%attitude des professionnels%' AND STATUT_AGREG = 'clot' THEN NB_RECLAMATIONS
			END AS CLOT_NB_RECLA_MOTIF_15,
			CASE
				WHEN MOTIFS_IGAS_SPLIT LIKE '%droits des usagers%' AND STATUT_AGREG = 'clot' THEN NB_RECLAMATIONS
			END AS CLOT_NB_RECLA_MOTIF_16,
			CASE
				WHEN MOTIFS_IGAS_SPLIT LIKE '%Facturation et honoraires%' AND STATUT_AGREG = 'clot' THEN NB_RECLAMATIONS
			END AS CLOT_NB_RECLA_MOTIF_17,
			CASE
				WHEN MOTIFS_IGAS_SPLIT LIKE '%Santé-environnementale%' AND STATUT_AGREG = 'clot' THEN NB_RECLAMATIONS
			END AS CLOT_NB_RECLA_MOTIF_18,
			CASE
				WHEN MOTIFS_IGAS_SPLIT LIKE '%esthétique réglementées%' AND STATUT_AGREG = 'clot' THEN NB_RECLAMATIONS
			END AS CLOT_NB_RECLA_MOTIF_19,
			CASE
				WHEN MOTIFS_IGAS_SPLIT LIKE '%A renseigner%' AND STATUT_AGREG = 'clot' THEN NB_RECLAMATIONS
			END AS CLOT_NB_RECLA_MOTIF_155,
			CASE
				WHEN MOTIFS_IGAS_SPLIT LIKE '%COVID-19%' AND STATUT_AGREG = 'clot' THEN NB_RECLAMATIONS
			END AS CLOT_NB_RECLA_MOTIF_156
			FROM
				(SELECT 
				n_finess_rpps || annee_de_reception AS identifiant,
				n_finess_rpps,
				annee_de_reception,
				STATUT_AGREG,
				MOTIFS_IGAS_SPLIT,
				COUNT(DISTINCT numero_de_la_reclamation) AS NB_RECLAMATIONS
				FROM
					(SELECT 
					numero_de_la_reclamation,
					MOTIFS_IGAS_SPLIT,
					n_finess_rpps,
					annee_de_reception,
					STATUT_AGREG
					FROM 
						(SELECT
						split_query.numero_de_la_reclamation,
						TRIM(split_query.motifs_igas) as MOTIFS_IGAS_SPLIT,
						src.statut_de_la_reclamation,
						src.Signalement,
						src.n_finess_rpps,
						src.nom_structure,
						src.Adresse,
						--src.Adresse_1,
						src.date_de_reception_a_l_ars,
						case
							when src.motifs_igas_sortie is not null and trim(src.motifs_igas_sortie) != '' then src.motifs_igas_sortie
							else src.motifs_igas_entree
						end,
						src.siege_ars,
						SUBSTR(date_de_reception_a_l_ars,1,4) as annee_de_reception,
						CASE
							WHEN 
								statut_de_la_reclamation = 'Affectation'
								OR statut_de_la_reclamation = 'Réponse'
								OR statut_de_la_reclamation = 'Traitement'
							THEN 'en cours'
							WHEN statut_de_la_reclamation = 'Clôture'
							THEN 'clot'
						END as STATUT_AGREG
						FROM
						-- split les motifs IGAS et créé 1 ligne par valeur trouvée
							(WITH RECURSIVE split(
							    numero_de_la_reclamation,
							    motifs_igas,
							    str
								) AS (
							    -- Initial select to set up the recursion with appended '|' for easier splitting
							    SELECT 
							        numero_de_la_reclamation, 
							        '',
									(case
										when motifs_igas_sortie is not null and trim(motifs_igas_sortie) != '' then motifs_igas_sortie
										else motifs_igas_entree
									end) || '|' as motifs_igas 
							    FROM {{ ref('staging__helios_sirec') }}
							    UNION ALL
							    -- Recursive step: take the current string, split on the first '|', and process the remainder
								SELECT
									numero_de_la_reclamation,
									TRIM(SUBSTRING(str FROM 1 FOR POSITION('|' IN str) - 1)) AS first_value,
									SUBSTRING(str FROM POSITION('|' IN str) + 1) AS remainder
								FROM split
								WHERE str != ''
								AND POSITION('|' IN str) > 0 -- Ensure there's something left to split
							)
							SELECT 
							    numero_de_la_reclamation, 
							    motifs_igas
							FROM split
							WHERE motifs_igas != '' -- Remove empty results	
							) split_query
						LEFT JOIN {{ ref('staging__helios_sirec') }} src ON split_query.numero_de_la_reclamation = src.numero_de_la_reclamation) join_split_query
					WHERE (MOTIFS_IGAS_SPLIT LIKE '%organisation ou de fonctionnement%'
					OR MOTIFS_IGAS_SPLIT LIKE '%A renseigner%'
					OR MOTIFS_IGAS_SPLIT LIKE '%droits des usagers%'
					OR MOTIFS_IGAS_SPLIT LIKE '%qualité des soins médicaux%'
					OR MOTIFS_IGAS_SPLIT LIKE '%attitude des professionnels%'
					OR MOTIFS_IGAS_SPLIT LIKE '%qualité des soins paramédicaux%'
					OR MOTIFS_IGAS_SPLIT LIKE '%établissement ou d%un professionnel%'
					OR MOTIFS_IGAS_SPLIT LIKE '%Hôtellerie-locaux-restauration%'
					OR MOTIFS_IGAS_SPLIT LIKE '%Facturation et honoraires%'
					OR MOTIFS_IGAS_SPLIT LIKE '%esthétique réglementées%'
					OR MOTIFS_IGAS_SPLIT LIKE '%Santé-environnementale%'
					OR MOTIFS_IGAS_SPLIT LIKE '%COVID-19%')
					) filter_motifs_query
				GROUP BY 
				n_finess_rpps,
				annee_de_reception,
				STATUT_AGREG,
				MOTIFS_IGAS_SPLIT) agreg_motifs_query
			) agreg_motifs_col_query 
		GROUP BY 
		identifiant,
		n_finess_rpps,
		annee_de_reception) motifs ON statuts.identifiant = motifs.identifiant) requete_finale
-- filtre pour exclure les RPPS en supposant que tous les numéros <= 9 caractères sont des FINESS
WHERE LENGTH(n_finess_rpps) > 4 AND LENGTH(n_finess_rpps) <= 9
)
select * from sirec_to_helios