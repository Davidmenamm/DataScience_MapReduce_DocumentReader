# Implements the Map element of the MapReduce Pattern, adds a combiner
# Input: chunk of the text to be processed
# Output: dictionary(defaultdict) representing frequency of words in this chunk of text

# Imports
from collections import defaultdict
from utils import pickleWrite
import threading

# Class Key-value-pair for allowing duplicate keys


class KeyVal:
    # Constructor
    def __init__(self, keyName):
        self.keyName = keyName

    # Activate when printing obj of this class
    def __repr__(self):
        return "'"+self.keyName+"'"

# Class Map


class Map (threading.Thread):
    # Static variable to manage current map number
    mapNumber = None
    mapPath = None

    # Constructor

    def __init__(self, filePath, threadLock):
        threading.Thread.__init__(self)
        self.filePath = filePath
        self.outputDict = None
        self.threadLock = threadLock

    # Getter and setter
    def getMapPath(self):
        return self.mapPath

    def getOutputDict(self):
        return self.outputDict

    # Function to erase unwanted characters
    def unwantedChar(self, word):
        charList = ['\\', '`', '*', '_', '{', '}', '[', ']', '?', '¿', ''
                    '(', ')', '>', '#', '+', '-', '.', ',', ':', ';', '%',
                    '!', '¡', '\'', '\"', '$', '0', '1', '2', '3', '4',
                    '5', '6', '7', '8', '9']

        for ch in charList:
            #self.tests += f'{ch}:{word}\n'
            if ch in word:
                word = word.replace(ch, '')
                #self.tests += f'****PASSED***:{word}\n'

        return word

    # Function to represent a combiner, to aid Map efficiency
    # Receives duplicate key dictionary representation
    def combiner(self, duplicateKeyDict):
        combineDict = defaultdict(int)
        dictItems = duplicateKeyDict.items()

        for k, v in dictItems:
            combineDict[k.keyName] += v

        return combineDict

    # Funtion to produce the key-values for Map Output
    def run(self):
        self.threadLock.acquire()
        # Critical Section
        dict = {}
        # read file line by line, store all words in array
        words = []
        f = open(self.filePath, 'r')
        while True:
            line = f.readline()
            if not line:
                break
            words.extend(line.split())
        for word in words:
            word = self.unwantedChar(word).lower()
            # dictionary with object as key, to represent duplicate keys
            dict[KeyVal(word)] = 1
        # combiner
        self.outputDict = self.combiner(dict)
        # write to pickle file
        self.mapPath = f'src/(c)MapsOutput/Map_{Map.mapNumber}.pkl'
        pickleWrite(self.outputDict, self.mapPath)
        Map.mapNumber = Map.mapNumber + 1  # next map number

        # Free lock, for next thread to run
        self.threadLock.release()
