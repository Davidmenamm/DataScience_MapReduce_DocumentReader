# Function to reutilize through files

# imports
import pickle
import textwrap

# Write object to pickle file


def pickleWrite(obj, pathWrite):
    # saved = None  # boolean to assure pickle saving works
    pickle_out = open(pathWrite, 'wb')
    pickle.dump(obj, pickle_out)
    pickle_out.close()

# Read object from pickle file


def pickleRead(pathRead):
    # saved = None  # boolean to assure pickle saving works
    pickle_in = open(pathRead, 'rb')
    obj = pickle.load(pickle_in)
    pickle_in.close()
    return obj


# Print object in string, with good visibility
def objToFile(obj, path):
    # object to string, and limit line length
    objToString = str(obj)
    limLineWidth = textwrap.fill(objToString, width=80)
    # write to file
    f = open(path, "w")
    f.write(limLineWidth)
    f.close()


# Print to file default dict in an ordered manner
def printDefaultDict(defaultDict, fileName):
    f = open(f'src/(f)FinalOutput/final.txt', 'w')
    result = ''
    count = 1
    for k, v in defaultDict.items():
        result += f'{k}: {v}  '
        if (count % 5 == 0):
            result += '\n'
        count = count + 1
    f.write(result)
    f.close()
