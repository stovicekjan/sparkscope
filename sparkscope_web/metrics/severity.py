from enum import Enum


class Severity(Enum):
    NONE = 0
    LOW = 1
    HIGH = 2

    def color(self):
        if self == Severity.NONE:
            color = "#009220"  # green
        elif self == Severity.LOW:
            color = "#fac744"  # yellow
        else:
            color = "#ff7575"  # red
        return color


    """
    Overload comparison operators, so that Severities are directly comparable
    """
    def __ge__(self, other):
        return self.value >= other.value

    def __le__(self, other):
        return self.value <= other.value

    def __gt__(self, other):
        return self.value > other.value

    def __lt__(self, other):
        return self.value < other.value

