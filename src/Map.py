# Implements the Map element of the MapReduce Pattern, adds a combiner
# Input: chunk of the text to be processed
# Output: dictionary(defaultdict) representing frequency of words in this chunk of text

# Imports
from collections import defaultdict

# Class Key-value-pair for allowing duplicate keys


class KeyVal:
    # Constructor
    def __init__(self, keyName):
        self.keyName = keyName

    # Activate when printing obj of this class
    def __repr__(self):
        return "'"+self.keyName+"'"


# Class Map
class Map:
    # Constructor
    def __init__(self, sectionOfText, size):
        self.sectionOfText = sectionOfText
        self.size = size

    # Function to erase unwanted characters
    def unwantedChar(self, word):
        charList = ['\\', '`', '*', '_', '{', '}', '[', ']',
                    '(', ')', '>', '#', '+', '-', '.', ',',
                    '!', '$', '\'', '0', '1', '2', '3', '4',
                    '5', '6', '7', '8', '9']
        for ch in charList:
            if ch in word:
                word = word.replace(ch, '')
        return word

    # Function to represent a combiner, to aid Map efficiency
    # Receives duplicate key dictionary representation
    def combiner(self, duplicateKeyDict):
        combineDict = defaultdict(int)
        dictItems = duplicateKeyDict.items()

        for k, v in dictItems:
            combineDict[k.keyName] += v

        return combineDict

    # Funtion to produce the key-values
    def getDictionary(self):
        dict = {}
        words = self.sectionOfText.split()

        for word in words:
            word = self.unwantedChar(word).lower()

            # dictionary with object as key, to represent duplicate keys
            dict[KeyVal(word)] = 1

        combineDict = self.combiner(dict)

        return combineDict
