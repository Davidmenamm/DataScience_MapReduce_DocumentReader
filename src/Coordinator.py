# In charge of coordinating the elements in the map reduce pattern
# Input: None
# Output: necesary elements and behaviour for every step of the pattern

# imports
from Map import Map
from GroupByKey import GroupByKey
from Reduce import Reduce
from collections import defaultdict
from utils import pickleRead, printDefaultDict, objToFile
import threading
from pathlib import Path

# Class Coordinator


class Coordinator:
    # Atributes
    # to contain necesary maps
    arrMaps = []
    arrReducers = []
    numReducers = 0
    numLines = 100
    mapOutputPaths = []
    groupOutputPath = None
    reduOutputPaths = []
    initialPath = None
    finalPath = f'src/(f)FinalOutput/final.txt'

    # constructor
    def __init__(self, initialPath):
        Map.mapNumber = 1  # Restart Map Number
        Reduce.reduceNumber = 1  # Restart Reduce Number
        self.initialPath = initialPath

    # Getter and Setter
    def getNumReducer(self):
        return self.numReducers

    # Methods

    # Function to sort repeated key representation dictionaries
    def alphDict(self, dictionary):
        index = 1
        items = dictionary.items()
        toPrint = ''
        for k, v in sorted(items):
            toPrint = toPrint + f' {k}:{v} '
            index = index + 1
        return toPrint

    # Function to create and assign necesary maps reads from txt file
    def assignMaps(self):
        # Print document path
        extension = Path(self.initialPath).suffix
        print('extension ', extension)

        if extension != '.txt':
            print('Error! choose an appropriate extension')
        else:
            # open to read file
            file1 = open(self.initialPath, 'r')
            # counter
            countLines = 1
            countMaps = 1
            # to save necesary text section for each Map
            textSection = ''

            # Lock to synchronize threads
            threadLock = threading.Lock()

            while True:
                # Add a line
                line = file1.readline()
                textSection = textSection + line

                # New map for every assigned num of lines
                # Assing a new textfile to that Map
                if (countLines % self.numLines == 0):
                    # write section to text file
                    f = open(f'src/(b)DividedInput/Input_{countMaps}.txt', 'w')
                    f.write(textSection)
                    f.close()
                    countMaps = countMaps + 1
                    # create new Map object and pass the path
                    self.arrMaps.append(Map(f.name, threadLock))
                    textSection = ''
                # Advance count
                countLines += 1

                # if line is empty, stop the reading
                if not line:
                    if textSection != '':
                        # write section to text file
                        f = open(
                            f'src/(b)DividedInput/Input_{countMaps}.txt', 'w')
                        f.write(textSection)
                        f.close()
                        self.arrMaps.append(Map(f.name, threadLock))
                    break

    def runMaps(self):
        mapOutputs = []
        count = 1
        for map in self.arrMaps:
            map.start()
            map.join()
            mapOutputs.append(map.getOutputDict())
            self.mapOutputPaths.append(map.getMapPath())

            # print to file to visualize
            objToFile(mapOutputs, f'src/(c)MapsOutput/Map_{count}.txt')
            count = count + 1
        # Wait for map threads to finish
        # for mapThread in self.arrMaps:
        #     mapThread.join()

    # Function to Group Map output by key, in one new dictionary
    def groupByKey(self):
        outputMaps = []

        # Read pickles and store in array all map output dictionaries
        for path in self.mapOutputPaths:
            mapDict = pickleRead(path)
            outputMaps.append(mapDict)

        # initiate group by key process
        group = GroupByKey(outputMaps)
        groupDict = group.orderMaps()

        # save group by key output path
        self.groupOutputPath = group.getOutputPath()

        # print to file to visualize
        objToFile(groupDict, f'src/(d)GrpByKeyOutput/GroupByKey.txt')

    # Function creates reducer objects according to number of Maps
    # Reducer objects number is 1/3 of Map objects number

    def createReducers(self):
        relation = 3  # Relation of Map and Reduce number

        # Lock to synchronize threads
        threadLock = threading.Lock()

        # set the number of reducers needed
        # avoid dividing by zero error
        if relation <= len(self.arrMaps):
            self.numReducers = len(
                self.arrMaps)//relation + 1  # floor division
        else:
            self.numReducers = 2  # 2 reducers minimum
        for num in range(self.numReducers):
            self.arrReducers.append(Reduce(None, threadLock))

    # Function to Divide the groupKeys into the reducers
    def startReducers(self):
        finalDicts = []

        # Get pickle object from file
        dictGroupByKey = pickleRead(self.groupOutputPath)

        # The number of tuples each Reduce will have
        # The last reduce could have this number or less
        if self.numReducers > 1:
            sectionLen = len(dictGroupByKey.items())//(self.numReducers-1)
        else:
            sectionLen = len(dictGroupByKey.items())//(self.numReducers)

        listOfDict = list(dictGroupByKey.items())

        count = 1
        rangeSize = len(listOfDict)//sectionLen + 1

        for idx in range(rangeSize):
            # define start and end cut of all sections
            startCut = (count-1)*(sectionLen-1)
            endCut = count*(sectionLen-1)

            # current reducer to access
            currentReducer = self.arrReducers[count-1]

            # run the section cuttings in the array
            if idx == rangeSize-1:
                # cut last section of list and append it
                dictSection = dict(listOfDict[startCut:])
                currentReducer.setDictGroupByKey(dictSection)
                currentReducer.start()
                currentReducer.join()
                dictReducer = currentReducer.getDictResume
                finalDicts.append(dictReducer)
                # Get output file path
                self.reduOutputPaths.append(currentReducer.getOutputPath())
                # print to file to visualize
                objToFile(
                    dictReducer, f'src/(e)ReducersOutput/Reducer_{count}.txt')
            else:
                # cut normal list sections and append
                dictSection = dict(listOfDict[startCut:endCut])
                currentReducer.setDictGroupByKey(dictSection)
                currentReducer.start()
                currentReducer.join()
                dictReducer = currentReducer.getDictResume
                finalDicts.append(dictReducer)
                # Get output file path
                self.reduOutputPaths.append(currentReducer.getOutputPath())
                # print to file to visualize
                objToFile(
                    dictReducer, f'src/(e)ReducersOutput/Reducer_{count}.txt')

            count = count + 1

    # Function to produce the final output of all Reduce
    def finalResume(self):
        finalDict = defaultdict(int)
        reduceOutputs = []

        # Read from pickle files
        for path in self.reduOutputPaths:
            reduceDict = pickleRead(path)
            reduceOutputs.append(reduceDict)

        # Resume all Reduce
        for dictOutput in reduceOutputs:
            items = dictOutput.items()
            for k, v in items:
                finalDict[k] += v

        # Print final Result to txt file
        printDefaultDict(finalDict, self.finalPath)
