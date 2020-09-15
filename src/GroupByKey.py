# Orders the output of Maps, by joining values of duplicate keys under the same key
# Input: all dictionaries from the Maps
# Output: one ordered dictionary representation all the Maps

# Imports
from collections import defaultdict
from utils import pickleWrite

# Class GroupByKey


class GroupByKey:
    # output path
    outputPath = None

    # Constructor
    def __init__(self, arrOutputMaps):
        self.arrOutputMaps = arrOutputMaps

    # Getter and setter functions
    def getOutputPath(self):
        return self.outputPath

    # Function to order Map outputs
    def orderMaps(self):
        # join all dictionary items in a list
        listDicts = []
        for dictionary in self.arrOutputMaps:
            listDicts.extend(list(dictionary.items()))

        # build a new dictionary with the list, with no duplicate keys
        joinDicts = defaultdict(list)

        for k, v in listDicts:
            joinDicts[k].append(v)

        # Save to pickle file
        self.outputPath = f'src/(d)GrpByKeyOutput/GroupByKey.pkl'
        pickleWrite(joinDicts, self.outputPath)

        return joinDicts
