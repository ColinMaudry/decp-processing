"""Schéma des appels d'offres (DEAO) : contrat avis-lot des adaptateurs et schéma publié."""

import json
from dataclasses import dataclass
from pathlib import Path

import polars as pl

STATUTS = ("ouvert", "clos", "annule", "pre-information")
TYPES = ("Fournitures", "Services", "Travaux")


@dataclass(frozen=True)
class Field:
    name: str
    dtype: pl.DataType
    title: str
    description: str
    enum: tuple[str, ...] | None = None


S = pl.String
DATE = pl.Date
LISTE = pl.List(pl.String)
DATETIME = pl.Datetime("us", "Europe/Paris")

PUBLISHED_FIELDS: tuple[Field, ...] = (
    Field(
        "uid",
        S,
        "Identifiant unique",
        "Identifiant unique de l'appel d'offres et du lot, de la forme "
        "<sourceDataset>_<idAvisInitial>_<lotId>. Stable tant que l'avis initial "
        "(ou, à défaut, le plus ancien avis connu) de la chaîne d'avis ne change pas.",
    ),
    Field(
        "referenceAcheteur",
        S,
        "Référence acheteur",
        "Référence interne attribuée à l'appel d'offres par l'acheteur, quand elle est renseignée dans l'avis.",
    ),
    Field(
        "acheteur_id",
        S,
        "SIRET acheteur",
        "Identifiant de l'établissement de l'acheteur (SIRET), référencé dans la base SIRENE de l'INSEE. "
        "Peut être nul si l'avis ne le mentionne pas et qu'il n'a pas pu être déduit.",
    ),
    Field(
        "acheteur_id_source",
        S,
        "Origine du SIRET acheteur",
        "Origine du SIRET de l'acheteur : 'avis' s'il figure dans l'avis, 'dictionnaire' "
        "s'il a été déduit d'autres avis du même acheteur, nul sinon.",
    ),
    Field(
        "acheteur_nom",
        S,
        "Nom acheteur",
        "Nom de l'acheteur : dénomination SIRENE si le SIRET de l'acheteur est "
        "trouvé dans SIRENE, sinon nom tel que renseigné dans l'avis.",
    ),
    Field(
        "objet",
        S,
        "Objet",
        "Objet de l'appel d'offres, tel que renseigné par l'acheteur dans l'avis.",
    ),
    Field(
        "lotId",
        S,
        "Identifiant du lot",
        "Numéro du lot au sein de l'appel d'offres. '0' signifie que la procédure "
        "n'est pas divisée en lots (ou que l'avis ne détaille pas ses lots).",
    ),
    Field(
        "lotIntitule",
        S,
        "Intitulé du lot",
        "Intitulé du lot, quand l'appel d'offres est divisé en lots.",
    ),
    Field(
        "description",
        S,
        "Description",
        "Description de l'appel d'offres ou du lot : nature et étendue des besoins.",
    ),
    Field(
        "montantEstime",
        pl.Float64,
        "Montant estimé",
        "Montant estimé hors-taxes de l'appel d'offres ou du lot, en euros, "
        "quand l'acheteur l'a renseigné.",
    ),
    Field(
        "type",
        S,
        "Type",
        "Type d'appel d'offres : fournitures, services ou travaux.",
        TYPES,
    ),
    Field(
        "codeCPV",
        S,
        "Code CPV",
        "Catégorie de bien, service ou travaux achetés, selon le Vocabulaire commun pour les marchés publics (CPV).",
    ),
    Field(
        "procedure",
        S,
        "Procédure",
        "Le type de procédure de l'appel d'offres, selon les libellés de l'avis BOAMP.",
    ),
    Field(
        "techniques",
        LISTE,
        "Techniques",
        "Les techniques d'achat utilisées pour l'appel d'offres (accord-cadre, système d'acquisition dynamique, etc.).",
    ),
    Field(
        "dureeMois",
        pl.Int64,
        "Durée (mois)",
        "Durée en mois du marché envisagé.",
    ),
    Field(
        "datePublicationInitiale",
        DATE,
        "Date de publication initiale",
        "Date de publication de l'avis initial, à défaut du plus ancien avis disponible.",
    ),
    Field(
        "dateDerniereModification",
        DATE,
        "Date de dernière modification",
        "Date du dernier rectificatif ou de l'annulation. Nulle s'il n'y a que l'avis initial.",
    ),
    Field(
        "dateLimiteReponse",
        DATETIME,
        "Date limite de réponse",
        "Date et heure limite (fuseau Europe/Paris) de remise des offres ou des candidatures.",
    ),
    Field(
        "lieuExecution_code",
        S,
        "Code lieu exécution",
        "Code du lieu d'exécution de l'appel d'offres. Le type de code est renseigné par 'Type code lieu exécution'.",
    ),
    Field(
        "lieuExecution_typeCode",
        S,
        "Type code lieu exécution",
        "Type du code du lieu d'exécution.",
    ),
    Field(
        "acheteur_categorie",
        S,
        "Catégorie de l'acheteur",
        "Catégorie de l'acheteur selon son code juridique INSEE.",
    ),
    Field(
        "acheteur_commune_code",
        S,
        "Commune de l'acheteur (code)",
        "Code de la commune où se trouve l'acheteur.",
    ),
    Field(
        "acheteur_commune_nom",
        S,
        "Commune de l'acheteur",
        "Nom de la commune où se trouve l'acheteur.",
    ),
    Field(
        "acheteur_departement_code",
        S,
        "Département de l'acheteur (code)",
        "Code du département où se trouve l'acheteur.",
    ),
    Field(
        "acheteur_departement_nom",
        S,
        "Département de l'acheteur",
        "Nom du département où se trouve l'acheteur.",
    ),
    Field(
        "acheteur_region_code",
        S,
        "Région de l'acheteur (code)",
        "Code de la région où se trouve l'acheteur.",
    ),
    Field(
        "acheteur_region_nom",
        S,
        "Région de l'acheteur",
        "Nom de la région où se trouve l'acheteur.",
    ),
    Field(
        "acheteur_latitude",
        pl.Float64,
        "Latitude de l'acheteur",
        "Latitude des coordonnées géographiques de l'acheteur.",
    ),
    Field(
        "acheteur_longitude",
        pl.Float64,
        "Longitude de l'acheteur",
        "Longitude des coordonnées géographiques de l'acheteur.",
    ),
    Field(
        "statut",
        S,
        "Statut",
        "Statut de l'appel d'offres à la date du traitement. 'annule' si un avis d'annulation "
        "existe ; 'pre-information' pour un avis de pré-information (seule date connue : la "
        "publication) ; sinon, si la date limite de réponse est renseignée : 'clos' si elle est "
        "passée, 'ouvert' sinon ; si elle est nulle : 'ouvert' si l'avis initial date de moins de "
        "90 jours (DEAO_STATUT_DELAI_JOURS), 'clos' au-delà. Limite : un rectificatif legacy qui "
        "repousse la date limite ne la met pas à jour (texte libre), l'appel d'offres peut donc "
        "passer 'clos' à tort ; 'rectifie' et 'infosRectificatifs' permettent de le repérer.",
        STATUTS,
    ),
    Field(
        "rectifie",
        pl.Boolean,
        "Rectifié",
        "Vrai si l'appel d'offres a fait l'objet d'au moins un avis rectificatif.",
    ),
    Field(
        "infosRectificatifs",
        LISTE,
        "Informations rectificatives",
        "Textes décrivant les rectifications ou l'annulation, un par avis rectificatif de la chaîne.",
    ),
    Field(
        "urlConsultation",
        S,
        "URL de consultation",
        "Adresse où consulter le dossier de consultation et déposer une offre, quand elle est renseignée.",
    ),
    Field(
        "urlAvis",
        S,
        "URL de l'avis",
        "Lien BOAMP vers le dernier avis de la chaîne d'avis.",
    ),
    Field(
        "idAvisInitial",
        S,
        "Identifiant de l'avis initial",
        "Identifiant (IDWEB) de l'avis initial de la chaîne d'avis ; à défaut, de l'avis le plus ancien connu.",
    ),
    Field(
        "idDernierAvis",
        S,
        "Identifiant du dernier avis",
        "Identifiant (IDWEB) du dernier avis de la chaîne d'avis.",
    ),
    Field(
        "avisLies",
        LISTE,
        "Avis liés",
        "Identifiants (IDWEB) de tous les avis de la chaîne (initial, rectificatifs, annulation), triés.",
    ),
    Field(
        "sourceDataset",
        S,
        "Source dataset",
        "Code du jeu de données dont proviennent les données de cet appel d'offres.",
    ),
    Field(
        "sourceFile",
        S,
        "Source fichier",
        "Lien vers le fichier de données ouvertes dont proviennent les données de cet appel d'offres.",
    ),
)

_DESCRIPTIVES = (
    "referenceAcheteur",
    "acheteur_id",
    "acheteur_nom",
    "objet",
    "lotId",
    "lotIntitule",
    "description",
    "montantEstime",
    "type",
    "codeCPV",
    "procedure",
    "techniques",
    "dureeMois",
    "dateLimiteReponse",
    "lieuExecution_code",
    "lieuExecution_typeCode",
    "urlConsultation",
    "urlAvis",
    "sourceDataset",
    "sourceFile",
)

_PUBLISHED_DTYPES = {f.name: f.dtype for f in PUBLISHED_FIELDS}

AVIS_LOT_SCHEMA: dict[str, pl.DataType] = {
    **{name: _PUBLISHED_DTYPES[name] for name in _DESCRIPTIVES},
    "idAvis": S,
    "avisPrecedent": S,
    "typeAvis": S,
    "preInformation": pl.Boolean,
    "avisComplet": pl.Boolean,
    "datePublicationAvis": DATE,
    "infosRectificatif": S,
    "formatSource": S,
    "acheteur_adresse_voie": S,
    "acheteur_adresse_cp": S,
    "acheteur_adresse_ville": S,
    "acheteur_adresse_nuts": S,
}


def published_schema() -> dict[str, pl.DataType]:
    return dict(_PUBLISHED_DTYPES)


def conform_avis(df: pl.DataFrame) -> pl.DataFrame:
    """Complète (null), caste et ordonne un DataFrame selon AVIS_LOT_SCHEMA."""
    inconnues = [c for c in df.columns if c not in AVIS_LOT_SCHEMA]
    if inconnues:
        raise ValueError(f"Colonnes inconnues du contrat avis-lot : {inconnues}")
    return df.select(
        [
            pl.col(name).cast(dtype)
            if name in df.columns
            else pl.lit(None, dtype=dtype).alias(name)
            for name, dtype in AVIS_LOT_SCHEMA.items()
        ]
    )


def empty_avis_frame() -> pl.DataFrame:
    return pl.DataFrame(schema=AVIS_LOT_SCHEMA)


def _table_schema_type(dtype: pl.DataType) -> str:
    if isinstance(dtype, pl.List):
        return "array"
    if isinstance(dtype, pl.Datetime):
        return "datetime"
    return {
        pl.String: "string",
        pl.Int64: "integer",
        pl.Float64: "number",
        pl.Boolean: "boolean",
        pl.Date: "date",
    }[dtype]


def write_table_schema(path: Path) -> Path:
    """Écrit le Table Schema JSON du schéma publié."""
    fields = []
    for f in PUBLISHED_FIELDS:
        entry = {
            "name": f.name,
            "type": _table_schema_type(f.dtype),
            "title": f.title,
            "description": f.description,
        }
        if f.enum:
            entry["enum"] = list(f.enum)
        fields.append(entry)
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"fields": fields}, ensure_ascii=False, indent=2))
    return path
