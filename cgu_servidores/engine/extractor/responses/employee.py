from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Honorarios:
    advocaticios: Path
    jetons: Path


@dataclass(frozen=True)
class Servidores:
    afastamentos: Path
    cadastro: Path
    observacoes: Path
    remuneracao: Path


@dataclass(frozen=True)
class Militares:
    cadastro: Path
    observacoes: Path
    remuneracao: Path


@dataclass(frozen=True)
class Response:
    honorarios: Honorarios
    servidores_bacen: Servidores
    servidores_siape: Servidores
    militares: Militares
