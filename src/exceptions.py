__all__ = ['InvalidInstrumentException', 'InstrumentDoesNotExistsException']


class InvalidInstrumentException(Exception):

    def __init__(self, message: str):
        self.message = message


class InstrumentDoesNotExistsException(Exception):

    def __init__(self, message: str):
        self.message = message
