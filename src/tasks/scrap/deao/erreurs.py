"""Erreurs communes aux scrapers de plateformes DEAO (sources fermées, dataset b)."""

import polars as pl


class ScrapPartiel(RuntimeError):
    """Le passage a produit des lignes publiables mais doit être signalé en échec
    (ex. archivage S3 des DCE interrompu) : scrap_closed publie `lignes`, puis
    compte la source parmi les erreurs du flow."""

    def __init__(self, message: str, lignes: pl.DataFrame):
        super().__init__(message)
        self.lignes = lignes
