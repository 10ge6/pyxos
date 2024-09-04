class IdProposta:
    def __init__(self, id:int, porta:int):
        self._id = id
        self._porta = porta
    
    def __str__(self):
        return f"{self._id}:{self._porta}"
    
    def __lt__(self, value) -> bool:
        return self._id*self._porta < value._id*value._porta
    
    def __eq__(self, value) -> bool:
        return self._id*self._porta == value._id*value._porta
    
    def __ne__(self, value) -> bool:
        return not self.__eq__(value)
    
    @property
    def id(self):
        return self._id
    
    @property
    def porta(self):
        return self._porta
    