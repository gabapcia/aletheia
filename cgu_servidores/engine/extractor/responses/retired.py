from dataclasses import dataclass
from pathlib import Path


@dataclass
class Content:
    cadastro: Path
    observacoes: Path
    remuneracao: Path


@dataclass
class Response:
    bacen: Content
    siape: Content
    militar: Content
