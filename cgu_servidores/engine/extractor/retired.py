import re
from pathlib import Path
from zipfile import ZipFile
from .responses.retired import Content, Response


class RetiredExtractor:
    BACEN_PATTERN = r'^\d{6}_Aposentados/\d{6}_Aposentados_BACEN\.zip$'
    SIAPE_PATTERN = r'^\d{6}_Aposentados/\d{6}_Aposentados_SIAPE\.zip$'
    MILITAR_PATTERN = r'^\d{6}_Aposentados/\d{6}_Reserva_Reforma_Militares\.zip$'

    CADASTRO_PATTERN = r'^\d{6}_Cadastro\.csv$'
    OBSERVACOES_PATTERN = r'^\d{6}_Observacoes\.csv$'
    REMUNERACAO_PATTERN = r'^\d{6}_Remuneracao\.csv$'

    def __init__(self, zip_path: Path, output_folder: Path) -> None:
        self._path = zip_path
        self._output = output_folder

    def extract(self) -> Response:
        with ZipFile(self._path, 'r') as z:
            bacen, siape = self._extract_servidores(z)
            militares = self._extract_militares(z)

        r = Response(
            militar=militares,
            bacen=bacen,
            siape=siape,
        )
        return r

    def _extract_servidores(self, zip: ZipFile) -> tuple[Content, Content]:
        output = self._output / 'servidores'
        output.mkdir(exist_ok=True)

        bacen = None
        siape = None
        for filename in zip.namelist():
            if re.match(RetiredExtractor.BACEN_PATTERN, filename):
                bacen = filename
            elif re.match(RetiredExtractor.SIAPE_PATTERN, filename):
                siape = filename

        def extract(target: str, prefix: str) -> Content:
            with zip.open(target, 'r') as f, ZipFile(f, 'r') as z:
                filenames = z.namelist()
                cadastro = output / list(filter(lambda n: re.match(RetiredExtractor.CADASTRO_PATTERN, n), filenames))[0]
                observacoes = output / list(filter(lambda n: re.match(RetiredExtractor.OBSERVACOES_PATTERN, n), filenames))[0]
                remuneracao = output / list(filter(lambda n: re.match(RetiredExtractor.REMUNERACAO_PATTERN, n), filenames))[0]
                z.extractall(output)

            files = Content(
                cadastro=cadastro.rename(output / f'{prefix}_cadastro.csv'),
                observacoes=observacoes.rename(output / f'{prefix}_observacoes.csv'),
                remuneracao=remuneracao.rename(output / f'{prefix}_remuneracao.csv'),
            )
            return files

        bacen = extract(bacen, 'bacen')
        siape = extract(siape, 'siape')
        return bacen, siape

    def _extract_militares(self, zip: ZipFile) -> Content:
        output = self._output / 'militares'
        output.mkdir(exist_ok=True)

        militares = list(filter(lambda p: re.match(RetiredExtractor.MILITAR_PATTERN, p), zip.namelist()))[0]

        with zip.open(militares, 'r') as f, ZipFile(f, 'r') as z:
            filenames = z.namelist()
            cadastro = output / list(filter(lambda p: re.match(RetiredExtractor.CADASTRO_PATTERN, p), filenames))[0]
            observacoes = output / list(filter(lambda p: re.match(RetiredExtractor.OBSERVACOES_PATTERN, p), filenames))[0]
            remuneracao = output / list(filter(lambda p: re.match(RetiredExtractor.REMUNERACAO_PATTERN, p), filenames))[0]
            z.extractall(output)

        r = Content(
            cadastro=cadastro.rename(output / 'cadastro.csv'),
            observacoes=observacoes.rename(output / 'observacoes.csv'),
            remuneracao=remuneracao.rename(output / 'remuneracao.csv'),
        )
        return r
