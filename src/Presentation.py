# Presents the results for the program

# imports
from Map import Map
from GroupByKey import GroupByKey
from Reduce import Reduce
from Coordinator import Coordinator

# print dictionary in alphabeticall order

# Test Map Reduce Pattern

# Coordinator instance
coordinator = Coordinator()

# 1ST STEP (MAP)

# Instantitate necesary Maps
maps = coordinator.assignMaps()
print('\n***** 1st Step: Map Output is: *****')
print('\n***** Map Objects *****\n', maps)

# Map dictionary outputs
mapOutputs = coordinator.runMaps()
print('\n***** Map Outputs *****\n', mapOutputs)

# Apply combinators to maps
combOutputs = coordinator.applyCombiners(mapOutputs)
print('\n***** Map Combiner Outputs *****\n\n', combOutputs)

# 2ND STEP (Group By Key)


# PREVIOUS ORGANIZATION

# Read Document Line by line an create necesary Maps
# One new map for every 25 lines in the text
# count = 0
# textSection = ''

# Maps
# map1 = Map(testText1)
# map2 = Map(testText2)
# Dictionaries output of Maps
# dict1 = map1.getDictionary()
# dict2 = map2.getDictionary()

# 1ST STEP (MAP)
# print('\n1st Step: Map Output is:')

# print('\nMap1:')
# print(alphDict(dict1))
# print('\nMap2:')
# print(alphDict(dict2))

# 2ND STEP (Group By Key)
# dictOutputs = [dict1, dict2]
# group = GroupByKey(dictOutputs)
# groupDict = group.orderMaps()
# print('\n2nd Step: Group By Key:\n')
# print(alphDict(groupDict))

# 3RD STEP (Reduce)
# reduce = Reduce(groupDict)
# resume = reduce.resume()
# print('\n3rd Step: Reduce:\n')
# print(alphDict(resume))
