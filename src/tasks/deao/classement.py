"""Classement d'un avis : retenu ou non, typeAvis, preInformation."""

from dataclasses import dataclass


@dataclass(frozen=True)
class Classement:
    nature: str | None
    retenu: bool
    typeAvis: str | None
    preInformation: bool | None
    formatSource: str
