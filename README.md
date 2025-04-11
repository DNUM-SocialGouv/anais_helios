# anais_helios

# Utilisation du layer Staging DBT

Cette section explique comment utiliser un projet DBT métier qui dépend du layer Staging DBT.  
Il est supposé que Poetry et les dépendances de base sont déjà installés. Si ce n’est pas le cas, merci de vous référer au guide d'installation du projet initial.

---

## 1. Configuration du package DBT

Le projet DBT métier inclut le projet initial sous forme de package.  
Pour cela, vérifiez que le fichier `packages.yml` contient bien la référence au package :

```yaml
packages:
  - local: ../chemin/vers/le/projet-initial
```

> Le chemin doit être relatif à la racine du projet métier DBT.

Ensuite, mettez à jour les packages :

```bash
dbt deps
```

---

## 2. Configuration du fichier `profiles.yml`

Assurez-vous que le fichier `profiles.yml` est bien en place dans :

- **Linux/macOS** : `~/.dbt/profiles.yml`
- **Windows** : `C:\Users\<VotreNom>\.dbt\profiles.yml`

Le profil doit être compatible avec les connexions définies dans le projet DBT métier.

---

## 3. Lancement de DBT

Vous pouvez maintenant exécuter DBT comme pour n'importe quel projet :

```bash
# Vérifier la configuration
dbt debug

# Exécuter les modèles
dbt run

```

---