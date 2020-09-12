# In charge of coordinating the elements in the map reduce pattern
# Input: None
# Output: necesary elements and behaviour for every step of the pattern

# imports
from Map import Map

# Class Coordinator


class Coordinator:
    # Atributes
    # to contain necesary maps
    arrMaps = []

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
        # open to read file
        file1 = open(f'src\IncomingText.txt', 'r')
        # counter
        count = 1
        # to save necesary text section for each Map
        textSection = ''

        while True:
            # Add a line
            line = file1.readline()
            textSection = textSection + line

            # Create new map for every 25 lines
            if (count % 25 == 0):
                self.arrMaps.append(Map(textSection))
                textSection = ''

            # Advance count
            count += 1

            # if line is empty, stop the reading
            if not line:
                if textSection != '':
                    self.arrMaps.append(Map(textSection))
                break
        return self.arrMaps

    # Function to run maps
    # Input: Map Objects
    # Output: array of the dictionary outputs for all the Maps
    def runMaps(self):
        mapOutputs = []

        for map in self.arrMaps:
            mapOutputs.append(map.getDictionary())

        return mapOutputs

    # Function to apply combiners to map outputs
    def applyCombiners(self, arrMapOutputs):
        arrCombineOutput = []
        count = 0
        for map in self.arrMaps:
            arrCombineOutput.append(map.combiner(arrMapOutputs[count]))
        count = count + 1

        return arrCombineOutput
