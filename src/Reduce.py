# Implements the Reduce element of the MapReduce Pattern
# Input: ordered dictionary with value as a list of multiple values
# Output: a new dictionary with value as an int, the sum of the multiple values

# Imports
from collections import defaultdict
from utils import pickleWrite
import threading

# Class Reduce


class Reduce (threading.Thread):
    # Num of reducer
    reduceNumber = None
    outputPath = f'src/(e)ReducersOutput/Reducer_(REDUCER_NUMBER).pkl'

    # Constructor

    def __init__(self, dictGroupByKey, threadLock):
        threading.Thread.__init__(self)
        self.dictGroupByKey = dictGroupByKey
        self.threadLock = threadLock
        self.dictResume = None

    # Getter and Setter
    def setDictGroupByKey(self, newDict):
        self.dictGroupByKey = newDict

    def getOutputPath(self):
        return self.outputPath

    def getDictResume(self):
        return self.dictResume

        # Function join all value list from keys, in one single int sum

    def run(self):
        self.dictResume = {}
        for k, v in self.dictGroupByKey.items():
            sumValues = sum(v)
            self.dictResume[k] = sumValues

        # save to pickle file
        self.outputPath = self.outputPath.replace(
            f'(REDUCER_NUMBER)', f'{Reduce.reduceNumber}')
        Reduce.reduceNumber = Reduce.reduceNumber + 1  # next map number
        pickleWrite(self.dictResume, self.outputPath)
