import re
from pathlib import Path
from zipfile import ZipFile
from .exceptions import InvalidFilenames, WrongNumberOfFiles


class EmployeeValidator:
    # First layer constants
    FIRST_LAYER_N_FILES = 6
    FIRST_LAYER_HONORARIOS_ADVOCATICIOS_PATTERN = r'^\d{6}_Servidores/\d{6}_Honorarios_Advocaticios\.zip$'
    FIRST_LAYER_HONORARIOS_JETONS_PATTERN = r'^\d{6}_Servidores/\d{6}_Honorarios_Jetons\.zip$'
    FIRST_LAYER_MILITARES_PATTERN = r'^\d{6}_Servidores/\d{6}_Militares\.zip$'
    FIRST_LAYER_SERVIDORES_BACEN_PATTERN = r'^\d{6}_Servidores/\d{6}_Servidores_BACEN\.zip$'
    FIRST_LAYER_SERVIDORES_SIAPE_PATTERN = r'^\d{6}_Servidores/\d{6}_Servidores_SIAPE\.zip$'
    FIRST_LAYER_FILE_PATTERNS = [
        r'^\d{6}_Servidores/$',
        FIRST_LAYER_HONORARIOS_ADVOCATICIOS_PATTERN,
        FIRST_LAYER_HONORARIOS_JETONS_PATTERN,
        FIRST_LAYER_MILITARES_PATTERN,
        FIRST_LAYER_SERVIDORES_BACEN_PATTERN,
        FIRST_LAYER_SERVIDORES_SIAPE_PATTERN,
    ]

    # "Honorarios Advocaticios" constants
    HA_N_FILES = 1
    HA_FILE_PATTERN = r'^\d{6}_HonorariosAdvocaticios\.csv$'

    # "Honorarios jetons" contants
    HJ_N_FILES = 1
    HJ_FILE_PATTERN = r'^\d{6}_Honorarios\(Jetons\)\.csv$'

    # "Militares" constants
    M_N_FILES = 3
    M_CADASTRO_PATTERN = r'^\d{6}_Cadastro\.csv$'
    M_OBSERVACOES_PATTERN = r'^\d{6}_Observacoes\.csv$'
    M_REMUNERACAO_PATTERN = r'^\d{6}_Remuneracao\.csv$'
    M_FILE_PATTERNS = [
        M_CADASTRO_PATTERN,
        M_OBSERVACOES_PATTERN,
        M_REMUNERACAO_PATTERN,
    ]

    # "Servidores BACEN" constants
    SBACEN_N_FILES = 4
    SBACEN_AFASTAMENTOS_PATTNER = r'^\d{6}_Afastamentos\.csv$'
    SBACEN_CADASTRO_PATTNER = r'^\d{6}_Cadastro\.csv$'
    SBACEN_OBSERVACOES_PATTNER = r'^\d{6}_Observacoes\.csv$'
    SBACEN_REMUNERACAO_PATTNER = r'^\d{6}_Remuneracao\.csv$'
    SBACEN_FILE_PATTERNS = [
        SBACEN_AFASTAMENTOS_PATTNER,
        SBACEN_CADASTRO_PATTNER,
        SBACEN_OBSERVACOES_PATTNER,
        SBACEN_REMUNERACAO_PATTNER,
    ]

    # "Servidores SIAPE" constants
    SSIAPE_N_FILES = 4
    SSIAPE_AFASTAMENTOS_PATTNER = r'^\d{6}_Afastamentos\.csv$'
    SSIAPE_CADASTRO_PATTNER = r'^\d{6}_Cadastro\.csv$'
    SSIAPE_OBSERVACOES_PATTNER = r'^\d{6}_Observacoes\.csv$'
    SSIAPE_REMUNERACAO_PATTNER = r'^\d{6}_Remuneracao\.csv$'
    SSIAPE_FILE_PATTERNS = [
        SSIAPE_AFASTAMENTOS_PATTNER,
        SSIAPE_CADASTRO_PATTNER,
        SSIAPE_OBSERVACOES_PATTNER,
        SSIAPE_REMUNERACAO_PATTNER,
    ]

    def __init__(self, path: Path) -> None:
        self._path = path

    def validate(self) -> None:
        with ZipFile(self._path, 'r') as z:
            self._validate_first_layer(z)
            self._validate_honorarios_advocaticios(z)
            self._validate_honorarios_jetons(z)
            self._validate_militares(z)
            self._validate_servidores_bacen(z)
            self._validate_servidores_siape(z)

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

    def _validate_first_layer(self, z: ZipFile) -> None:
        filenames = z.namelist()

        if len(filenames) != EmployeeValidator.FIRST_LAYER_N_FILES:
            raise WrongNumberOfFiles(len(filenames), EmployeeValidator.FIRST_LAYER_N_FILES, filenames)

        valid = self._validate_filenames(filenames, EmployeeValidator.FIRST_LAYER_FILE_PATTERNS)
        if not valid:
            raise InvalidFilenames(filenames)

    def _validate_honorarios_advocaticios(self, zip: ZipFile) -> None:
        for filename in zip.namelist():
            if re.match(EmployeeValidator.FIRST_LAYER_HONORARIOS_ADVOCATICIOS_PATTERN, filename):
                break

        with zip.open(filename, 'r') as f, ZipFile(f, 'r') as z:
            filenames = z.namelist()
            if len(filenames) != EmployeeValidator.HA_N_FILES:
                raise WrongNumberOfFiles(len(filenames), EmployeeValidator.HA_N_FILES, filenames)

            valid = self._validate_filenames(filenames, [EmployeeValidator.HA_FILE_PATTERN])
            if not valid:
                raise InvalidFilenames(filenames)

    def _validate_honorarios_jetons(self, zip: ZipFile) -> None:
        for filename in zip.namelist():
            if re.match(EmployeeValidator.FIRST_LAYER_HONORARIOS_JETONS_PATTERN, filename):
                break

        with zip.open(filename, 'r') as f, ZipFile(f, 'r') as z:
            filenames = z.namelist()
            if len(filenames) != EmployeeValidator.HJ_N_FILES:
                raise WrongNumberOfFiles(len(filenames), EmployeeValidator.HJ_N_FILES, filenames)

            valid = self._validate_filenames(filenames, [EmployeeValidator.HJ_FILE_PATTERN])
            if not valid:
                raise InvalidFilenames(filenames)

    def _validate_militares(self, zip: ZipFile) -> None:
        for filename in zip.namelist():
            if re.match(EmployeeValidator.FIRST_LAYER_MILITARES_PATTERN, filename):
                break

        with zip.open(filename, 'r') as f, ZipFile(f, 'r') as z:
            filenames = z.namelist()
            if len(filenames) != EmployeeValidator.M_N_FILES:
                raise WrongNumberOfFiles(len(filenames), EmployeeValidator.M_N_FILES, filenames)

            valid = self._validate_filenames(filenames, EmployeeValidator.M_FILE_PATTERNS)
            if not valid:
                raise InvalidFilenames(filenames)

    def _validate_servidores_bacen(self, zip: ZipFile) -> None:
        for filename in zip.namelist():
            if re.match(EmployeeValidator.FIRST_LAYER_SERVIDORES_BACEN_PATTERN, filename):
                break

        with zip.open(filename, 'r') as f, ZipFile(f, 'r') as z:
            filenames = z.namelist()
            if len(filenames) != EmployeeValidator.SBACEN_N_FILES:
                raise WrongNumberOfFiles(len(filenames), EmployeeValidator.SBACEN_N_FILES, filenames)

            valid = self._validate_filenames(filenames, EmployeeValidator.SBACEN_FILE_PATTERNS)
            if not valid:
                raise InvalidFilenames(filenames)

    def _validate_servidores_siape(self, zip: ZipFile) -> None:
        for filename in zip.namelist():
            if re.match(EmployeeValidator.FIRST_LAYER_SERVIDORES_SIAPE_PATTERN, filename):
                break

        with zip.open(filename, 'r') as f, ZipFile(f, 'r') as z:
            filenames = z.namelist()
            if len(filenames) != EmployeeValidator.SSIAPE_N_FILES:
                raise WrongNumberOfFiles(len(filenames), EmployeeValidator.SSIAPE_N_FILES, filenames)

            valid = self._validate_filenames(filenames, EmployeeValidator.SSIAPE_FILE_PATTERNS)
            if not valid:
                raise InvalidFilenames(filenames)
