import re
from pathlib import Path
from zipfile import ZipFile
from .responses.employee import Honorarios, Servidores, Militares, Response


class EmployeeExtractor:
    HONORARIOS_ADVOCATICIOS_PATTERN = r'^\d{6}_Servidores/\d{6}_Honorarios_Advocaticios\.zip$'
    HONORARIOS_JETONS_PATTERN = r'^\d{6}_Servidores/\d{6}_Honorarios_Jetons\.zip$'
    MILITARES_PATTERN = r'^\d{6}_Servidores/\d{6}_Militares\.zip$'
    SERVIDORES_BACEN_PATTERN = r'^\d{6}_Servidores/\d{6}_Servidores_BACEN\.zip$'
    SERVIDORES_SIAPE_PATTERN = r'^\d{6}_Servidores/\d{6}_Servidores_SIAPE\.zip$'

    SERVIDORES_AFASTAMENTOS_PATTNER = r'^\d{6}_Afastamentos\.csv$'
    SERVIDORES_CADASTRO_PATTNER = r'^\d{6}_Cadastro\.csv$'
    SERVIDORES_OBSERVACOES_PATTNER = r'^\d{6}_Observacoes\.csv$'
    SERVIDORES_REMUNERACAO_PATTNER = r'^\d{6}_Remuneracao\.csv$'

    MILITARES_CADASTRO_PATTERN = r'^\d{6}_Cadastro\.csv$'
    MILITARES_OBSERVACOES_PATTERN = r'^\d{6}_Observacoes\.csv$'
    MILITARES_REMUNERACAO_PATTERN = r'^\d{6}_Remuneracao\.csv$'

    def __init__(self, zip_path: Path, output_folder: Path) -> None:
        self._path = zip_path
        self._output = output_folder

    def extract(self) -> Response:
        with ZipFile(self._path, 'r') as z:
            honorarios = self._extract_honorarios(z)
            bacen, siape = self._extract_servidores(z)
            militares = self._extract_militares(z)

        r = Response(
            honorarios=honorarios,
            militares=militares,
            servidores_bacen=bacen,
            servidores_siape=siape,
        )
        return r

    def _extract_honorarios(self, zip: ZipFile) -> Honorarios:
        output = self._output / 'honorarios'
        output.mkdir(exist_ok=True)

        advocaticios = None
        jetons = None
        for filename in zip.namelist():
            if re.match(EmployeeExtractor.HONORARIOS_ADVOCATICIOS_PATTERN, filename):
                advocaticios = filename
            elif re.match(EmployeeExtractor.HONORARIOS_JETONS_PATTERN, filename):
                jetons = filename

        with zip.open(advocaticios, 'r') as f, ZipFile(f, 'r') as z:
            advocaticios_path = output / z.namelist()[0]
            z.extractall(output)

        with zip.open(jetons, 'r') as f, ZipFile(f, 'r') as z:
            jetons_path = output / z.namelist()[0]
            z.extractall(output)

        r = Honorarios(
            advocaticios=advocaticios_path.rename(output / 'advocaticios.csv'),
            jetons=jetons_path.rename(output / 'jetons.csv'),
        )
        return r

    def _extract_servidores(self, zip: ZipFile) -> tuple[Servidores, Servidores]:
        output = self._output / 'servidores'
        output.mkdir(exist_ok=True)

        bacen = None
        siape = None
        for filename in zip.namelist():
            if re.match(EmployeeExtractor.SERVIDORES_BACEN_PATTERN, filename):
                bacen = filename
            elif re.match(EmployeeExtractor.SERVIDORES_SIAPE_PATTERN, filename):
                siape = filename

        def extract(target: str, prefix: str) -> Servidores:
            with zip.open(target, 'r') as f, ZipFile(f, 'r') as z:
                filenames = z.namelist()
                afastamentos = output / list(filter(lambda n: re.match(EmployeeExtractor.SERVIDORES_AFASTAMENTOS_PATTNER, n), filenames))[0]
                cadastro = output / list(filter(lambda n: re.match(EmployeeExtractor.SERVIDORES_CADASTRO_PATTNER, n), filenames))[0]
                observacoes = output / list(filter(lambda n: re.match(EmployeeExtractor.SERVIDORES_OBSERVACOES_PATTNER, n), filenames))[0]
                remuneracao = output / list(filter(lambda n: re.match(EmployeeExtractor.SERVIDORES_REMUNERACAO_PATTNER, n), filenames))[0]
                z.extractall(output)

            files = Servidores(
                afastamentos=afastamentos.rename(output / f'{prefix}_afastamentos.csv'),
                cadastro=cadastro.rename(output / f'{prefix}_cadastro.csv'),
                observacoes=observacoes.rename(output / f'{prefix}_observacoes.csv'),
                remuneracao=remuneracao.rename(output / f'{prefix}_remuneracao.csv'),
            )
            return files

        bacen = extract(bacen, 'bacen')
        siape = extract(siape, 'siape')
        return bacen, siape

    def _extract_militares(self, zip: ZipFile) -> Militares:
        output = self._output / 'militares'
        output.mkdir(exist_ok=True)

        militares = list(filter(lambda p: re.match(EmployeeExtractor.MILITARES_PATTERN, p), zip.namelist()))[0]

        with zip.open(militares, 'r') as f, ZipFile(f, 'r') as z:
            filenames = z.namelist()
            cadastro = output / list(filter(lambda p: re.match(EmployeeExtractor.MILITARES_CADASTRO_PATTERN, p), filenames))[0]
            observacoes = output / list(filter(lambda p: re.match(EmployeeExtractor.MILITARES_OBSERVACOES_PATTERN, p), filenames))[0]
            remuneracao = output / list(filter(lambda p: re.match(EmployeeExtractor.MILITARES_REMUNERACAO_PATTERN, p), filenames))[0]
            z.extractall(output)

        r = Militares(
            cadastro=cadastro.rename(output / 'cadastro.csv'),
            observacoes=observacoes.rename(output / 'observacoes.csv'),
            remuneracao=remuneracao.rename(output / 'remuneracao.csv'),
        )
        return r
