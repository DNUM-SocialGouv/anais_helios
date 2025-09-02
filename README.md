# Anais Helios
Pipeline de l'étape de Helios de la plateforme ANAIS

# Installation & Lancement du projet DBT

Cette section décrit les étapes nécessaires pour installer les dépendances, configurer DBT, instancier la base de données si besoin, et exécuter le projet.

---

## 1. Installation des dépendances via UV

Le projet utilise [UV] pour la gestion des dépendances Python.  
Voici les étapes à suivre pour initialiser l’environnement :

```bash

# 1. Se placer dans le dossier du projet
cd chemin/vers/le/projet

# 2. Vérifier que uv est installé
uv --version
pip install uv # Si pas installé

# 3. Installer les dépendances
uv sync
```

---
## 2. ⚙️Configuration du projet

Plusieurs fichiers doivent être correctement configurés et placés dans le bon répertoire pour le bon fonctionnement de la pipeline.

### 2.1 Fichier `profiles.yml`

DBT nécessite un fichier de configuration appelé `profiles.yml`, qui contient les informations de connexion à la base de données.

#### Où se trouve le fichier ?

Il doit se trouver dans le répertoire suivant :
- **Linux/macOS** : `~/.dbt/profiles.yml`
- **Windows** : `C:\Users\<NomUtilisateur>\.dbt\profiles.yml`

> Si le dossier `.dbt` n’existe pas encore, vous pouvez le créer manuellement.  

#### Où placer le fichier ?

Il doit être placé dans à la racine du projet Anais_helios (au même niveau que le README et pyproject.toml) :
- **VM Cegedim** : `~/anais_helios/profiles.yml`
- **Local** : `C:\Users\<NomUtilisateur>\...\<projet>\profiles.yml`
 
Le fichier `profiles.yml` est disponible à la racine du repo.  


#### Que contient le fichier ?

Il contient les informations relatives aux bases de données des différents projets :
- Staging (DuckDB et postgres)
- Helios (DuckDB et postgres)
- Matrice (DuckDB et postgres)
- InspectionControlePA et InspectionControlePH (DuckDB et postgres)
- CertDC (DuckDB et postgres)

Exemple :
```yaml
Helios:
  target: local
  outputs:
    anais:
      type: postgres
      host: xx.xx.xx.xx
      user: <Nom_utilisateur>
      password: "{{ env_var('HELIOS_PASSWORD') }}"
      port: xxxx
      schema: public
      dbname: helios
    local:
      type: duckdb
      path: data/helios/duckdb_database.duckdb
      schema: main
```

Seul le mot de passe des bases postgres n'est pas indiqué dans le fichier profiles.yml -> il est indiqué dans le `.env`

### 2.2 Fichier `.env`

Ce fichier `.env` contient les mots de passe d'accès aux bases postgres et au SFTP.
Il est secret, donc indisponible sur le git.
Il doit donc être créé manuellement.

#### Où placer le fichier ?

Il doit être placé dans à la racine du projet Anais_helios (au même niveau que le README et pyproject.toml) :
- **VM Cegedim** : `~/anais_helios/.env`
- **Local** : `C:\Users\<NomUtilisateur>\...\<projet>\.env`
 
Le fichier `.env` est disponible à la racine du repo.

#### Que contient le fichier ?

Il contient les variables suivantes, avec leurs valeurs entre guillement `" "` :

- `SFTP_HOST = "<host du SFTP>"`
- `SFTP_PORT = <port du SFTP>`
- `SFTP_USERNAME = "<nom de l'utilisateur>"`
- `SFTP_PASSWORD = "<Mot de passe du SFTP>"`
- `STAGING_PASSWORD = "<Mot de passe de la base staging>"`
- `HELIOS_PASSWORD = "<Mot de passe de la base helios>"`
- `INSPECTION_CONTROLE_ADMIN_PA_PASSWORD = "<Mot de passe de la base inspection_controle>"`
- `INSPECTION_CONTROLE_ADMIN_PH_PASSWORD = "<Mot de passe de la base inspection_controle>"`
- `MATRICE_PA_PASSWORD = "<Mot de passe de la base matrice>"`
- `MATRICE_PH_PASSWORD = "<Mot de passe de la base matrice>"`
- `CERTDC_PASSWORD = "<Mot de passe de la base certelec_dc>"`


### 2.3 ⚙️ Fichier `metadata.yml`

Le fichier `metadata.yml` contient le paramétrage relatif aux fichiers en entrée et en sortie et aux répertoires du projet Helios.

#### Section *directory*

Contient les répertoires du projet :
```yaml
  local_directory_input: "input/<Nom_projet>/"
  local_directory_output: "output/<Nom_projet>/"
  models_directory: "<dbtNom_projet>"
  create_table_directory: "output_sql/<Nom_projet>/"
  remote_directory_input: "/SCN_BDD/<Nom_projet>/input"
  remote_directory_output: "/SCN_BDD/<Nom_projet>/output"
```

Avec :
- `local_directory_input` = répertoire en local où sont trouvables les fichiers csv en entrée.
- `local_directory_output` = répertoire en local où sont enregistrés les fichiers csv en sortie.
- `models_directory` = répertoire dans lequel sont enregistrés les modèles dbt du projet.
- `create_table_directory` = répertoire où sont enregistrés les fichiers SQL Create table.
- `remote_directory_input` = répertoire SFTP où sont enregistrés les fichiers csv des tables d'origine en sortie. Ce répertoire existe pour faciliter la recette. Le projet Staging ne nécessite pas de valeur -> `null`
- `remote_directory_output` = répertoire SFTP où sont enregistrés les fichiers csv en sortie. Le projet Staging ne nécessite pas de valeur -> `null`

#### Section *files_to_download*

Contient les informations relatives aux fichiers csv provenant du SFTP.
La section `files_to_download` (fichier à récupérer) contient :

``` yaml
  files_to_download:
    - path: "/SCN_BDD/INSERN"
      keyword: "DNUM_TdB_CertDc"
      file: "sa_insern.csv"
```

Avec :
- `path` = Chemin du fichier sur le SFTP.
- `keyword` = Terme dans le nom du fichier qui permet de le distinguer des autres fichiers. Le fichier le plus récent contenant ce terme sera récupéré.
- `file` = Nom d'enregistrement du fichier une fois importé.

Section inutilisée en local.


#### Section *table_to_copy*, *input_to_download* et *files_to_upload*

La section `table_to_copy` indique les tables de staging à copier dans la base du projet.
La section `input_to_download` indique les tables à envoyer en csv dans le remote_directory_input. Nécessaire pour la recette.
La section `files_to_upload` indique les vues à envoyer en csv dans le remote_directory_output.

```yaml
  table_to_copy:
    <Nom_de_la_vue_sql>: <nom_de_la_table>
    ...
    <Nom_de_la_vue_sql>: <nom_de_la_table>
  input_to_download:
    <Nom_de_la_vue_sql>: <radical_du_fichier_csv_exporté>
    ...
    <Nom_de_la_vue_sql>: <radical_du_fichier_csv_exporté>
  files_to_upload:
    <Nom_de_la_vue_sql>: <radical_du_fichier_csv_exporté>
    ...
    <Nom_de_la_vue_sql>: <radical_du_fichier_csv_exporté>
```

Avec :
- `<Nom_de_la_vue_sql>`: le nom de la vue SQL dans la base de données. Aussi le nom du modèle déployé
- `<radical_du_fichier_csv_exporté>`: radical du nom du csv à exporter. La date du jour d'exécution sera ajouté automatiquement au nom du fichier exporté : `'<radical_du_fichier_csv_exporté>_<date_du_jour>.csv'`. On peut également préciser le répertoire de destination dans le nom.

### 2.4 Fichier `sources.yml`

Ce fichier `sources.yml` contient la liste des des tables sources nécessaires pour lancer les modèles dbt, pour local et pour anais. 

#### Où placer le fichier ?

Il doit être placé au même endroit que les modèles dbt du projet (au même niveau que le README et pyproject.toml) :
- **VM Cegedim** : `~/anais_helios/<dbtNom_Projet>/models/sources.yml`
- **Local** : `C:\Users\<NomUtilisateur>\...\<projet>\<dbtNom_Projet>\models\sources.yml`

#### Que contient le fichier ?

Il contient la liste des tables sources nécessaires (celles créées via CREATE TABLE) pour le lancement des modèles dbt.
La liste est dupliquée pour les deux sources : local (public) et anais (main).

```yaml
sources:
  - name: main
    tables:
      - name: sa_insern
      ...
      - name: sa_tdb_esms
  - name: public
    tables:
      - name: sa_insern
      ...
      - name: sa_tdb_esms
```
Avec main ou public respectivement le nom des schémas des bases postgres et duckDB. Nom indiqué dans `profiles.yml`.

### 2.5 Récupération des tables de Staging sur Helios en local


Pour exécuter Helios en local, il est nécessaire de répondre à au moins une de ces deux méthodes.
En effet, Helios récupère les tables d'origine nécessaires de staging à partir de la base Staging. En local, il s'agit de la base DuckDB.

#### Méthode 1:

Placer la base DuckDB Staging dans le répertoire correspondant ->`data/staging/duckdb_database.duckdb`.

Lors de l'exécution de la pipeline avec Helios, les tables indiquées dans la section `table_to_copy` du fichier `metadata.yml` sont copiées de Staging et collées dans Helios.


#### Méthode 2:

Si vous n'avez pas la base Staging duckDB à disposition, voici la seconde méthode.

Placer les fichiers .sql CREATE TABLE des tables d'origines nécessaires dans le répertoire `output_sql/helios/`.

Placer les fichiers .csv de données des tables d'origines nécessaires dans le répertoire `input/helios/`. 

Renommer les fichiers .csv pour qu'ils correspondent au nom indiqué dans la section `table_to_copy` du fichier `metadata.yml`.

Lors de l'exécution de la pipeline avec Helios, les tables seront créées à partir des fichiers .sql CREATE TABLE et les données injectées via les fichiers .csv.

La 1ère méthode est préférable, car plus rapide à exécuter et évite des potentielles erreurs déjà corrigées dans Staging.

---
## 3. Lancement du pipeline :

L'ensemble de la Pipeline est exécutée depuis le `main.py`.
La Pipeline exécutée est celle du package `anais_pipeline` dans la branche du même nom du repo anais_staging. Elle est importée comme un package dans le `pyproject.toml`.

### 3.1 Exécution de la pipeline pour Helios:

```bash
# Se placer dans anais_helios
cd anais_helios

#  Lancer le `main.py`
uv run main.py --env "local" --profile "Helios"
```
Avec env = 'local' ou 'anais' selon votre environnement de travail
et profile = 'Helios'

#### Pipeline Helios sur env 'local':
1. Création de la base DuckDB si inexistante.
2. Connexion à la base DuckDB.
3. Méthode 1 : Récupération des tables d'origine nécessaires à Helios à partir de la base staging. 
3. Méthode 2 : Création des tables d'origine nécessaires à Helios à partir des fichiers CREATE TABLE .sql (`output_sql/helios/`) -> injection des données dans les tables à partir des fichiers de données .csv (`input/helios/`).
4. Historisation des données pour chaque table vers les tables `z<nom_de_la_table` avec indication que la date d'injection dans la colonne `date_ingestion`.
5. Vérification de la réussite de l'injection.
7. Fermeture de la connexion à la base DuckDB.
8. Exécution de la commande `run dbt` -> Création des vues relatives au projet.
9. Export des vues Helios vers le répertoire `output/helios/`.


#### Pipeline Helios sur env 'anais':
1. Connexion à la base Postgres.
2. Récupération des tables d'origine nécessaires à Helios à partir de la base staging.
3. Historisation des données pour chaque table vers les tables `z<nom_de_la_table` avec indication que la date d'injection dans la colonne `date_ingestion`.
4. Exécution de la commande `run dbt` -> Création des vues relatives au projet.
5. Export des vues Helios vers le répertoire `output/helios/`.
6. Export des vues Helios vers le SFTP `/SCN_BDD/HELIOS/output/`.
7. Fermeture de la connexion à la base Postgres.


### 3.2 Exécution de parties de la pipeline
#### Importation seule des fichiers depuis le SFTP

Pour seulement importer les fichiers .csv du SFTP vers le répertoire local :

```bash
uv run -m pipeline.utils.sftp_sync --env "local" --profile "Helios"

```

#### Exécution du dbt run

Pour seulement exécuter le dbt run afin de tester le fonctionnement des modèles :

```bash
uv run -m pipeline.utils.dbt_tools --env "local" --profile "Helios"
```

### 4. Mettre à jour les packages Staging

Dans le cas où les packages ont été modifiés, et vous souhaitez les appliquées à Helios, voici la manupilation à faire.

#### 4.1 Pipeline

Pour rappel, le package `anais_pipeline` de la pipeline est présent dans la branche du même nom du repo anais_staging.

Pour la mettre à jour dans le projet Helios, il suffit de supprimer le fichier `uv.lock` à la racine du projet.

Ce fichier permet de figer les versions des packages dont le package `anais_pipeline` lors de l'exécution.

Néanmoins, avec la suppression du fichier, lors de l'exécution du projet Helios, un nouveau fichier `uv.lock` sera créé, avec le package `anais_pipeline` à jour.

Les autres packages utilisés resteront à la même version que précédemment, tant que leur version n'est pas modifiée manuellement dans le fichier `pyproject.toml`.


#### 4.2 dbtStaging

Pour rappel, le package `dbtStaging` est présent dans la branche `dev` du repo anais_staging.

Pour la mettre à jour dans le projet Helios, il suffit de supprimer le fichier `package-lock.yml` présent dans le répertoire `dbtHelios/`.

Avec la suppression du fichier, lors de l'exécution du projet Helios, un nouveau fichier `package-lock.yml` sera créé, avec le package `dbtStaging` à jour.

# Documentation et architecture du projet
## 1. Déployement de la documentation
En cours

## 2. Architecture du projet

```plaintext
.
├── data
│   ├── helios
│   │   └── duckdb_database.duckdb
│   └── staging
│       └── duckdb_database.duckdb
├── dbtHelios
│   ├── analyses
│   ├── dbt_packages
│   │   └── dbtStaging
│   ├── dbt_project.yml
│   ├── helios_tables.txt
│   ├── helios_views.txt
│   ├── logs
│   ├── models
│   │   ├── helios
│   │   ├── schema.yml
│   │   └── sources.yml
│   ├── package-lock.yml
│   ├── packages.yml
│   ├── seeds
│   ├── snapshots
│   ├── target
│   └── tests
├── input
│   └── helios
│       ├── sa_siicea_decisions.csv
│       ├── sa_siicea_missions_real.csv
│       ├── sa_sirec.csv
│       └── sa_sivss.csv
├── logs
│   ├── dbt.log
│   ├── log_anais.log
│   └── log_local.log
├── output
│   └── helios
├── output_sql
│   └── helios
│       ├── sa_siicea_decisions.sql
│       ├── sa_siicea_missions_real.sql
│       ├── sa_sirec.sql
│       └── sa_sivss.sql
├── poetry.lock
├── profiles.yml
├── main.py
├── metadata.yml
├── pyproject.toml
├── README.md
└── uv.lock
```

## 3. Utilités des fichiers
### 3.1 Fichiers à la racine `./`
Répertoire d'orchestration de la pipeline Python.

- `.env `: Fichier secret contenant le paramétrage vers le SFTP et les mots de passe des bases de données postgres.
- `metadata.yml` : Contient les configurations du projets et la liste des fichiers .csv provenant du SFTP.
- `main.py` : Programme d'exécution de la pipeline.
- `output_sql/` : Répertoire qui contient les fichiers .sql de création de table (CREATE TABLE).
- `logs/` : Répertoire des logs local et anais.
- `data/` : Répertoire des bases de données DuckDB.
- `input/` : Répertoire de stockage des fichiers .csv en entrée.
- `output/` : Répertoire de stockage des fichiers .csv en sortie.
- `profiles.yml` : Contient les informations relatives aux bases des différents projets.
- `pyproject.toml` : Fichier contenant les dépendances et packages nécessaires pour le lancement de la pipeline.

### 3.2 Fichiers dans dbtHelios `./dbtHelios/`
Répertoire de fonctionnement des modèles DBT -> création de vues SQL.

- `dbt_project.yml` : Fichier de configuration de DBT (obligatoire).
- `macros/` : Répertoire de stockage des macros jinja.
- `models/` : Répertoire de stockage des modèles dbt.
- `models/helios/sources.yml` : Fichier contenant le nom des tables sources nécessaires pour le lancement des modèles .

