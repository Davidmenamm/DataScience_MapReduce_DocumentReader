# Implements the Reduce element of the MapReduce Pattern
# Input: ordered dictionary with value as a list of multiple values
# Output: a new dictionary with value as an int, the sum of the multiple values

# Imports
from collections import defaultdict

# Class Reduce


class Reduce:
    # Constructor
    def __init__(self, dictGroupByKey):
        self.dictGroupByKey = dictGroupByKey

    # Getter and Setter
    def setDictGroupByKey(self, newDict):
        self.dictGroupByKey = newDict

    # Function join all value list from keys, in one single int sum
    def reducerResume(self):
        dictResume = {}
        for k, v in self.dictGroupByKey.items():
            sumValues = sum(v)
            dictResume[k] = sumValues

        return dictResume
