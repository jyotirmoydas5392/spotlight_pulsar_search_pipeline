import numpy as np
import shutil
import math
import os

for i in range(0,10):
    for j in range(0,10):
        shutil.copyfile("Simulation00_2DHS.txt","Simulation" + str(i+1) + str(j+1) + "_2DHS.txt")
        file = open("Simulation" + str(i+1) + str(j+1) + "_2DHS.txt", "r")
        replacement = ""
        # using the for loop
        for line in file:
            line = line.strip()
            changes = line.replace("/newdata/All_filterbanks/Simulation00.fil", "/newdata/All_filterbanks/Simulation" + str(i+1) + str(j+1) + ".fil")
            replacement = replacement + changes + "\n"
            
        file.close()
        # opening the file in write mode
        fout = open("Simulation" + str(i+1) + str(j+1) + "_2DHS.txt", "w")
        fout.write(replacement)
        fout.close()
