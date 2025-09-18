# DECP processing

> version 2.0 ([notes de version](https://github.com/ColinMaudry/decp-processing/blob/main/CHANGELOG.md))

Projet de traitement et de publication de meilleures données sur les marchés publics attribués en France. Vous pouvez consulter, filtrer et télécharger
ces données sur le site [decp.info](https://decp.info). Enfin la section [À propos](https://decp.info/a-propos) décrit les objectifs du projet et regroupe toutes les informations clés.

Les données sont rassemblées et traitées tous les matins, du lundi au vendredi,
et [publiées sur data.gouv.fr](https://www.data.gouv.fr/fr/datasets/donnees-essentielles-de-la-commande-publique-consolidees-format-tabulaire/)
aux formats parquet, CSV et SQLite.

Ce projet prend sa source dans la complexité de la publication des données par le Ministère des Finances :

- code source de l'agrégation [ouvert mais très peu lisible](https://github.com/139bercy/decp-arr2022)
- documentation incomplète et éparpillée
  - https://github.com/139bercy/decp-arr2022/blob/main/README.md
  - https://www.data.gouv.fr/fr/datasets/5cd57bf68b4c4179299eb0e9
  - https://data.economie.gouv.fr/pages/donnees-essentielles-de-la-commande-publique/
- schéma de données DECP 2 [complexe à utiliser](https://github.com/ColinMaudry/decp-processing/issues/4)

Vous trouverez des informations sur le contexte, le cadre réglementaire et les données de la commande publique sur [le wiki](https://github.com/ColinMaudry/decp-processing/wiki).

Ce projet se veut collaboratif et à l'écoute des besoins des usagers potentiels : entreprises, acteurs publics, journalistes, chercheurs et chercheuses, citoyens et citoyennes.

Pour me contacter vous pouvez ouvrir un "issue" sur Github ou me contacter par email [colin+decp@maudry.com](mailto:colin+decp@maudry.com).

## Données

Les données consolidées proviennent intégralement de sources ouvertes. Les détails des sources de données utilisées peuvent
être consultés sur [decp.info](https://decp.info/a-propos).

## Pré-requis

- Python 3.9 ou plus récent
- cargo ([installation rapide](https://rustup.rs))
- si sauvegarde dans PostgreSQL :
  - sur Debian/Ubuntu : `sudo apt install libpq-dev` (pour builder le module `psycopg2`)
  - créer la base de données cible

## Bases techniques

Cet outil repose grandement sur trois logiciels libres :

- [prefect](https://docs.prefect.io/v3/get-started) pour l'orchestration, le monitoring, la gestion du cache
- [polars](https://docs.pola.rs) pour la manipulation de données tabulaires en flux
- [ijson](https://pypi.org/project/ijson/) pour la manipulation de données JSON en flux

## Installation

### En utilisant un environnement virtuel (recommandé)

Je vous recommande d'utiliser un environnement virtuel Python pour isoler l'installation des dépendances :

```bash
python -m venv .venv
source .venv/bin/activate
```

Installez les dépendances :

```bash
pip install .
```

Pour les contributeurices, installez les dépendances de développement et l'auto-formatage au moment des commits :

```bash
pip install .[dev]
pre-commit install
# à chaque commit black et cie se lancent et reformattent les fichiers si besoin, ça peut demander de "git add"
# de nouveau pour prendre en compte le reformatage dans le commit

# installation des dépendances sous zsh
pip install .'[dev]'
```

Faites une copie du fichier template.env, renommez-le en .env et adaptez les valeurs :

```shell
cp template.env .env
nano .env
```

### Installation sur le serveur pour les déploiements (Linux)

Ces instructions supposent que le serveur [prefect](https://docs.prefect.io/v3/get-started) est installé, configuré et démarré.

Il suppose également que le work pool "local" a été créé.

1. Suivre les instructions d'installation ci-dessus
2. Démarrer le serveur prefect
3. Adapter les chemins dans `systemd/prefect-worker.service`
4. Copier `systemd/prefect-worker.service` dans le répertoire `/etc/systemd/system`
5. Activer et démarrer le service

```bash
systemctl enable prefect-worker.service
systemctl start prefect-worker.service
```

Un nouveau worker doit apparaître dans l'interface de gestion de prefect.

### Avec Docker sous Windows, peu testé

Construire et lancer le container

```bash
script\docker_build_and_run.bat
```

Démarrer le serveur prefect une fois dans le container

```bash
script\start_server_in_docker.sh
```

Le serveur est accessible sur le navigateur à l'adresse http://127.0.0.1:4200/

## Lancer le traitement des données (pour le développement en local)

Le pré-traitement des données SIRENE doit être fait une fois pour que le traitement principal soit fonctionnel.

```bash
pytest tests/test_sirene_preprocess.py
```

Lancement du traitement principal (data_tables + decp.info) via un serveur prefect à usage unique

```bash
python src/flows.py
```

## Lancer le traitement des données (sur le serveur Prefect configuré dans .env)

Le déploiement sur le serveur déploie à la fois un run quotidien de traitement des données et un run activable à la demande.

Attention, la version de prefect du client utilisé pour le déploiement et celle utilisée pour le serveur doivent être identiques. Cela est normalement garanti par la version configurée dans `pyproject.toml`.

1. Suivre les instructions de la section "Installation sur le serveur pour les déploiements"
2. Vérifier que le `.env` est bien configuré, ce sont ces variables qui seront utilisées par les run du serveur.
3. Déployer sur le serveur :

```bash
python src/deploy.py
```

4. Le run se lancera tous les jours selon la configuration cron. Si tu souhaites exécuter le run maintenant :

```bash
prefect deployment run decp-processing
```

## Test

Pour lancer les tests unitaires :

### Du pre-process des données SIRENE

Ce traitement doit être fait une fois pour que le test du traitement principal soit fonctionnel.

```bash
pytest tests/test_sirene_preprocess.py
```

### Du traitement principal (data tables + decp.info) (les autres tests de /tests sont moins maintenus)

```bash
pytest tests/test_main.py
```

# Contributeurs ❤️

- Colin Maudry (développeur principal)
- [FranckMaseo](https://github.com/frankmaseo)
- [Thomas Louf](https://github.com/tlouf)
