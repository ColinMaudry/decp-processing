# DECP processing

Projet de traitement et de publication de meilleures données sur les marchés publics attribués en France. Ce projet prend sa source dans la complexité de la publication des données faite par le Ministère des Finances :

- code source de l'agrégation des données [fermé](https://github.com/139bercy/decp-rama-v2/blob/main/README.md)
- documentation incomplète et éparpillée
    - https://github.com/139bercy/decp-rama-v2/blob/main/README.md
    - https://www.data.gouv.fr/fr/datasets/5cd57bf68b4c4179299eb0e9
    - https://data.economie.gouv.fr/pages/donnees-essentielles-de-la-commande-publique/
- schéma de données DECP 2 [complexe à utiliser](https://github.com/ColinMaudry/decp-processing/issues/4)

Ce projet se veut collaboratif et à l'écoute des besoins des usagers potentiels : entreprises, acteurs publics, journalistes, chercheurs et chercheuses, citoyens et citoyennes.

Pour me contacter vous pouvez ouvrir un "issue" sur Github ou me contacter par email [colin+decp@maudry.com](mailto:colin+decp@maudry.com).

## Données

Les données produites sont les mêmes données que celles publiées par le Ministère des Finances sur data.economie.gouv.fr. J'ai choisi de prendre ces données comme source et non les DECP au format réglementaire JSON car les premières ont été nettoyées et améliorées ([code](https://github.com/139bercy/decp-augmente)) par le Ministère, ce qui me fait moins de travail.

Elles sont mises à dispositions aux formats CSV, Parquet et SQLite.

Vous pouvez...

- les télécharger sur [data.gouv.fr](https://www.data.gouv.fr/fr/datasets/donnees-essentielles-de-la-commande-publique-consolidees-format-tabulaire/) (vous trouverez aussi plus d'informations sur ces données)
- les visualiséer, les filtrer et télécharger sur [decp.info](https://decp.info)


## Pré-requis

- Python 3.8 ou plus récent
- cargo ([installation rapide](https://rustup.rs))
- fichier .env avec l'adresse des fichiers source

## Installation

### En utilisant un environnement virtuel (recommandé)
Je vous recommande d'utiliser un environnement virtuel Python pour isoler l'installation des dépendances :

```bash
virtualenv .venv
```

Activez l'environnement virtuel :

```bash
source .venv/bin/activate
```

Installez les dépendances :

```bash
pip install .
```

Installez les dépendances de développement :

```bash
pip install .[dev]

# sous zsh

pip install .'[dev]'
```

### Avec Docker
#### Pour windows
Construire et lancer le container
```bash
./script/docker_build_and_run.bat
```

Démarrer le serveur prefect une fois dans le container
```bash
./script/start_server_in_docker.sh
```
Le serveur est accessible sur le navigateur à l'adresse http://127.0.0.1:4200/

Lancer le script
```bash
python src/flows.py
```
WARNING : En testant le script se kill (OOM) au moment de la commande "save_to_sqlite(df, "datalab", "data.economie.2019.2022")", sauf si on a sauté les premiers save_to_sqlite. Est ce un soucis seulement sous docker ?

## Test

Pour lancer les tests unitaires :

```
pytest
```
