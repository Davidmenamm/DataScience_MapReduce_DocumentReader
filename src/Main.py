# Display the results for the program and begin it
# Test Map Reduce Pattern

# imports
from Map import Map
from GroupByKey import GroupByKey
from Reduce import Reduce
from Coordinator import Coordinator
from tkinter import filedialog as fd

# File dialog to extract file
filename = fd.askopenfilename()

# Create a new Coordinator instance
coordinator = Coordinator(filename)

# 1ST STEP (MAP):

# Assign the text files to the Maps
coordinator.assignMaps()

# Run the Maps
coordinator.runMaps()

# 2ND STEP (Group By Key):
coordinator.groupByKey()

# 3RD STEP (Reduce):

# Create Reducers
coordinator.createReducers()

# Run Reducers
coordinator.startReducers()

# FINAL RESUME OF EVERYTHING:
coordinator.finalResume()
