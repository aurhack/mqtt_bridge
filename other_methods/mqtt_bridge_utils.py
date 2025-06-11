
# This class helps on the access fast deliver members in a json-compatible format  
class json_wrapper_t:
    
    def __init__(self, name, value):
        self._name = name
        self._value = value
    
    # So we can do :
    # class.member
    # class.member.json
    
    @property
    def json(self):
            return dict({self._name : self._value})

    def __getattr__(self, attr): 
        return getattr(self._value, attr)

    def __str__(self):
        return str(self._value)

    def __repr__(self):
        return repr(self._value)

    def __int__(self):
        return int(self._value)

    def __float__(self):
        return float(self._value)

    def __eq__(self, other):
        return self._value == other

    def __add__(self, other):
        return self._value + other

    @property
    def value(self):
        return self._value
