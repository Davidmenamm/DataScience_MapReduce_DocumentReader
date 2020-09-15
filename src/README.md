# DataScience_MapReduce_DocumentReader
Prints the frequency of words in a text document.
Uses the optimized Map Reduce pattern.

Preservation of information:
Pickle files are used between stages of the Map Reduce to preserve
information and avoid placing all in program memory

Parallelization elements:
Python threading library with locks and join, to achieve multiple
threads on Maps and multiple threads on Reduce

Printing of information to review:
In all important intermediate logical stages, current process
information is printed as objects to string

Coordinator Element:
In charge of organizing the Maps, Reduce and GroupByKey from the Map Reduce Pattern

utils:
To reuse common functions in all files

output folders:
For every important intermediate logical stages
A pickle file and txt file, one for program reasons and the other for 
visualization, respectively.








