# Orders the output of Maps, by joining values of duplicate keys under the same key
# Input: all dictionaries from the Maps
# Output: one ordered dictionary representation all the Maps

# Imports
from collections import defaultdict

# Class GroupByKey


class GroupByKey:
    # Constructor
    def __init__(self, arrOutputMaps):
        self.arrOutputMaps = arrOutputMaps

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

        return joinDicts
