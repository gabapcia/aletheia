class BaseRfbCnpjException(Exception):
    pass


class CompaniesNotInserted(BaseRfbCnpjException):
    def __str__(self) -> str:
        return 'Companies not inserted yet'
