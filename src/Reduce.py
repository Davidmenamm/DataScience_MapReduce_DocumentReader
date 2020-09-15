# Implements the Reduce element of the MapReduce Pattern
# Input: ordered dictionary with value as a list of multiple values
# Output: a new dictionary with value as an int, the sum of the multiple values

# Imports
from collections import defaultdict
from utils import pickleWrite

# Class Reduce


class Reduce:
    # Num of reducer
    numReducer = None
    outputPath = f'src/ReducersOutput/Reducer_(REDUCER_NUMBER).pkl'

    # Constructor
    def __init__(self, dictGroupByKey):
        self.dictGroupByKey = dictGroupByKey

    # Getter and Setter
    def setDictGroupByKey(self, newDict):
        self.dictGroupByKey = newDict

    def getOutputPath(self):
        return self.outputPath

        # Function join all value list from keys, in one single int sum

    def reducerResume(self, reducerNum):
        dictResume = {}
        for k, v in self.dictGroupByKey.items():
            sumValues = sum(v)
            dictResume[k] = sumValues

        # save to pickle file
        Reduce.numReducer = reducerNum
        self.outputPath = self.outputPath.replace(
            f'(REDUCER_NUMBER)', f'{Reduce.numReducer}')
        pickleWrite(dictResume, self.outputPath)

        return dictResume
