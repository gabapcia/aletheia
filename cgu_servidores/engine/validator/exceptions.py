class BaseValidatorException(Exception):
    pass


class InvalidFilenames(BaseValidatorException):
    def __init__(self, filenames: list[str]) -> None:
        self.filenames = filenames

    def __str__(self) -> str:
        return 'Zip file does not contain all the expected files'


class WrongNumberOfFiles(BaseValidatorException):
    def __init__(self, got: int, want: int, filenames: list[str]) -> None:
        self.got = got
        self.want = want
        self.filenames = filenames

    def __str__(self) -> str:
        return f'Expected {self.want} files but got {self.got}'
