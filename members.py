class Member():

    def __init__(self, host, port) -> None:
        self._host = host
        self._port = port
    
    @property
    def host(self):
        return self._host
    
    @property
    def port(self):
        return self._port