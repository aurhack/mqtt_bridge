
# This class wraps a value and allows attribute access and JSON compatibility
class json_wrapper_t:
    
    def __init__(self, name, value):
        self._name = name
        self._value = value

    # Access to the raw value
    @property
    def raw(self):
        return self._value

    # JSON representation for dict-style export
    @property
    def json(self):
        return {self._name: self._value}

    # Fallback for JSON serialization
    def __getstate__(self):
        return self._value

    def __reduce__(self):
        return (lambda x: x, (self._value,))

    # Basic representations
    def __repr__(self):
        return repr(self._value)

    def __str__(self):
        return str(self._value)

    # Type conversions
    def __int__(self):
        return int(self._value)

    def __float__(self):
        return float(self._value)

    def __bool__(self):
        return bool(self._value)

    # Comparisons
    def __eq__(self, other):
        return self._value == other

    def __ne__(self, other):
        return self._value != other

    def __lt__(self, other):
        return self._value < other

    def __le__(self, other):
        return self._value <= other

    def __gt__(self, other):
        return self._value > other

    def __ge__(self, other):
        return self._value >= other

    # Arithmetic
    def __add__(self, other):
        return self._value + other

    def __radd__(self, other):
        return other + self._value

    def __sub__(self, other):
        return self._value - other

    def __rsub__(self, other):
        return other - self._value

    def __mul__(self, other):
        return self._value * other

    def __rmul__(self, other):
        return other * self._value

    def __truediv__(self, other):
        return self._value / other

    def __rtruediv__(self, other):
        return other / self._value

    def __floordiv__(self, other):
        return self._value // other

    def __rfloordiv__(self, other):
        return other // self._value

    def __mod__(self, other):
        return self._value % other

    def __rmod__(self, other):
        return other % self._value

    def __pow__(self, other):
        return self._value ** other

    def __rpow__(self, other):
        return other ** self._value

    # Unary
    def __neg__(self):
        return -self._value

    def __pos__(self):
        return +self._value

    def __abs__(self):
        return abs(self._value)

    def __invert__(self):
        return ~self._value

    # Container support
    def __len__(self):
        return len(self._value)

    def __getitem__(self, key):
        return self._value[key]

    def __setitem__(self, key, value):
        self._value[key] = value

    def __delitem__(self, key):
        del self._value[key]

    def __contains__(self, item):
        return item in self._value

    def __iter__(self):
        return iter(self._value)

    # Delegate attribute access
    def __getattr__(self, attr):
        return getattr(self._value, attr)

    # Hash support
    def __hash__(self):
        return hash(self._value)
