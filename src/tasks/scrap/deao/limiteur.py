"""Limitation du débit des requêtes vers un site tiers."""

import threading
import time
from collections.abc import Callable


class Limiteur:
    """Impose un intervalle minimal entre deux requêtes, tous fils confondus."""

    def __init__(
        self,
        intervalle: float,
        horloge: Callable[[], float] = time.monotonic,
        dormir: Callable[[float], None] = time.sleep,
    ):
        self.intervalle = intervalle
        self.horloge = horloge
        self.dormir = dormir
        self._verrou = threading.Lock()
        self._prochain = 0.0

    def attendre(self) -> None:
        with self._verrou:
            maintenant = self.horloge()
            attente = self._prochain - maintenant
            if attente > 0:
                self.dormir(attente)
                maintenant += attente
            self._prochain = maintenant + self.intervalle
