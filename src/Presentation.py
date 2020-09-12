# Presents the results for the program

# imports
from Map import Map
from GroupByKey import GroupByKey
from Reduce import Reduce

# print dictionary in alphabeticall order


def alphDict(dictionary):
    index = 1
    items = dictionary.items()
    toPrint = ''
    for k, v in sorted(items):
        toPrint = toPrint + f' {k}:{v} '
        index = index + 1

    return toPrint


# Test Map Reduce Pattern
testText1 = 'When antimatter particles interact with matter particles,\
 they annihilate each other and produce energy. This has led engineers\
  to speculate that antimatter-powered spacecraft might be an efficient\
   way to explore the universe. NASA cautions there is a huge catch with\
   this idea: it takes about $100 billion to create a milligram of antimatter.\
    While research can get by on a lot less antimatter, this is the minimum that \
    would be needed for application. '

testText2 = "To be commercially viable, this price would have to drop by about\
     a factor of 10,000, the agency wrote. Power generation creates another headache:\
          It costs far more energy to create antimatter than the energy one could get\
 back from an antimatter reaction."

# Maps
map1 = Map(testText1, 0)
map2 = Map(testText2, 0)
# Dictionaries output of Maps
dict1 = map1.getDictionary()
dict2 = map2.getDictionary()

# 1ST STEP (MAP)
print('\n1st Step: Map Output is:')

print('\nMap1:')
print(alphDict(dict1))
print('\nMap2:')
print(alphDict(dict2))

# 2ND STEP (Group By Key)
dictOutputs = [dict1, dict2]
group = GroupByKey(dictOutputs)
groupDict = group.orderMaps()
print('\n2nd Step: Group By Key:\n')
print(alphDict(groupDict))

# 3RD STEP (Reduce)
reduce = Reduce(groupDict)
resume = reduce.resume()
print('\n3rd Step: Reduce:\n')
print(alphDict(resume))
