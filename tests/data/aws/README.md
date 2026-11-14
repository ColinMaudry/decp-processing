# Fixtures AWS (www.marches-publics.info)

Pages publiques capturées le 14/11/2026 (15 requêtes séquentielles espacées de plus d'1 s,
`User-Agent` de `CLOSED_USER_AGENT`, une seule session HTTP) ; aucun lien DCE suivi.

- `rechercher.html` : GET `/Annonces/rechercher` (formulaire, liste des départements).
- `liste_trop_large.html` : POST `/Annonces/lister`, `IDR=X`, `IDN=X` (« Votre recherche trouve 4093 résultats »).
- `liste_13_S_p1.html` : POST `IDN=S`, `IDR=13` (67 services, page 1).
- `liste_25.html` : POST `IDN=X`, `IDR=25` (trois onglets).
- `avis_2026314252.html` : avis alloti (3 lots), Audincourt.
- `avis_2026275758.html` : avis non alloti, rectifié, Grand Besançon Métropole.
- `avis_2026260690.html` : avis alloti (4 lots), rectifié, variantes déclarées par lot (SOLEAM).

Pas de `liste_13_S_p2.html` : `GET /Annonces/lister?pager_s=2` dans la même session
renvoie la page du formulaire de recherche (deux essais, avec ou sans `Referer`).

- `aucun_resultat.html` : POST `IDR=84`, `IDN=S`, `IDP=AOE`, recherche à 0 résultat : le site
  renvoie le formulaire avec « Aucun résultat trouvé. Veuillez modifier votre recherche »,
  sans badge.

- `liste_07_T_PA_dl.html` : POST `IDR=7`, `IDN=T`, `IDP=PA` (badge travaux 4) : 2 avis
  `MPI-pub-` et 2 avis `DL-pub-` dont les liens DCE/RC pointent vers
  `dl.aws-achat.info` (autre numérotation IDM).
