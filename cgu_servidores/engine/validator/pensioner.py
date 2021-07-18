import re
from pathlib import Path
from zipfile import ZipFile
from .exceptions import InvalidFilenames, WrongNumberOfFiles


class PensionerValidator:
    # First layer constants
    FIRST_LAYER_N_FILES = 3
    FIRST_LAYER_BACEN_PATTERN = r'^\d{6}_Pensionistas/\d{6}_Pensionistas_BACEN\.zip$'
    FIRST_LAYER_SIAPE_PATTERN = r'^\d{6}_Pensionistas/\d{6}_Pensionistas_SIAPE\.zip$'
    FIRST_LAYER_MILITAR_PATTERN = r'^\d{6}_Pensionistas/\d{6}_Pensionistas_DEFESA\.zip$'
    FIRST_LAYER_PATTERNS = [
        FIRST_LAYER_BACEN_PATTERN,
        FIRST_LAYER_SIAPE_PATTERN,
        FIRST_LAYER_MILITAR_PATTERN,
    ]

    # Files constants
    CONTENT_N_FILES = 3
    CONTENT_CADASTRO_PATTERN = r'^\d{6}_Cadastro\.csv$'
    CONTENT_OBSERVACOES_PATTERN = r'^\d{6}_Observacoes\.csv$'
    CONTENT_REMUNERACAO_PATTERN = r'^\d{6}_Remuneracao\.csv$'
    CONTENT_PATTERNS = [
        CONTENT_CADASTRO_PATTERN,
        CONTENT_OBSERVACOES_PATTERN,
        CONTENT_REMUNERACAO_PATTERN,
    ]

    def __init__(self, zip_path: Path) -> None:
        self._path = zip_path

    def validate(self) -> None:
        with ZipFile(self._path, 'r') as z:
            self._validate_first_layer(z)
            self._validate_bacen(z)
            self._validate_siape(z)
            self._validate_militares(z)

    def _validate_filenames(self, filenames: list[str], patterns: list[str]) -> bool:
        for pattern in patterns:
            match = False
            for filename in filenames:
                if re.match(pattern, filename):
                    match = True
                    break

            if not match:
                return False

        return True

    def _validate_first_layer(self, zip: ZipFile) -> None:
        filenames = zip.namelist()

        if len(filenames) != PensionerValidator.FIRST_LAYER_N_FILES:
            raise WrongNumberOfFiles(len(filenames), PensionerValidator.FIRST_LAYER_N_FILES, filenames)

        valid = self._validate_filenames(filenames, PensionerValidator.FIRST_LAYER_PATTERNS)
        if not valid:
            raise InvalidFilenames(filenames)

    def _validate_second_layer(self, zip: ZipFile) -> None:
        filenames = zip.namelist()

        if len(filenames) != PensionerValidator.CONTENT_N_FILES:
            raise WrongNumberOfFiles(len(filenames), PensionerValidator.CONTENT_N_FILES, filenames)

        valid = self._validate_filenames(filenames, PensionerValidator.CONTENT_PATTERNS)
        if not valid:
            raise InvalidFilenames(filenames)

    def _validate_bacen(self, zip: ZipFile) -> None:
        for filename in zip.namelist():
            if re.match(PensionerValidator.FIRST_LAYER_BACEN_PATTERN, filename):
                break

        with zip.open(filename, 'r') as f, ZipFile(f, 'r') as z:
            self._validate_second_layer(z)

    def _validate_siape(self, zip: ZipFile) -> None:
        for filename in zip.namelist():
            if re.match(PensionerValidator.FIRST_LAYER_SIAPE_PATTERN, filename):
                break

        with zip.open(filename, 'r') as f, ZipFile(f, 'r') as z:
            self._validate_second_layer(z)

    def _validate_militares(self, zip: ZipFile) -> None:
        for filename in zip.namelist():
            if re.match(PensionerValidator.FIRST_LAYER_MILITAR_PATTERN, filename):
                break

        with zip.open(filename, 'r') as f, ZipFile(f, 'r') as z:
            self._validate_second_layer(z)
