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
print('\n***** Number of Map Objects *****\n', len(maps))

# Map dictionary outputs
mapOutputs = coordinator.runMaps()
print('\n***** Map Outputs *****\n', mapOutputs)

# Apply combinators to maps
# combOutputs = coordinator.applyCombiners(mapOutputs)
# print('\n***** Map Combiner Outputs *****\n\n', combOutputs)

# 2ND STEP (Group By Key)
groupByKeys = coordinator.groupByKey(mapOutputs)
print('\n***** 2nd Step: Group By Key:*****\n')
print(groupByKeys)

# 3RD STEP (Reduce)
arrReducers = coordinator.createReducers()
reducersDicts = coordinator.startReducers(groupByKeys, arrReducers)
finalResume = coordinator.finalResume(reducersDicts)

print('\n*****3rd Step: Reduce:*****\n')
count = 1
for dict in reducersDicts:
    print(f'ReducerOutput {count}\n', dict)
    count = count + 1

print('\nFINAL RESUME\n', finalResume)


# Testing files prints
