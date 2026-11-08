import numpy as np
import shutil
import math
import os

M2=np.loadtxt("parameter_space.dat",usecols=[0])
Pb=np.loadtxt("parameter_space.dat",usecols=[1])

G=6.674*10**(-11)
Mo=2*10**(30)
c=3*10**(8)
M1=1.4*Mo

A1=[]

for i in range(0,10):
    A0=[]
    for j in range(0,10):
        A=((((Pb[i]*24*3600)**2/(4*(math.pi)**2))*G*(M1+(M2[j]*Mo)))**(1/3.0))*((M2[j]*Mo)/(M1+(M2[j]*Mo)))/c
        A0.append(A)
    A1.append(A0)

for i in range(0,5):
    for j in range(0,10):
        shutil.copyfile("J0737-3039A.par","Simulation" + str(i+1) + str(j+1) + ".par")
        file = open("Simulation" + str(i+1) + str(j+1) + ".par", "r")
        replacement = ""
        # using the for loop
        for line in file:
            line = line.strip()
            changes = line.replace("1.2489", str('%.4f'%M2[j])).replace("0.10225156248",str('%.11f'%Pb[i])).replace("1.415032",str('%.6f'%A1[i][j]))
            replacement = replacement + changes + "\n"
            
        file.close()
        # opening the file in write mode
        fout = open("Simulation" + str(i+1) + str(j+1) + ".par", "w")
        fout.write(replacement)
        fout.close()
        os.system('tempo2 -f Simulation' + str(i+1) + str(j+1) + '.par -pred "gmrt 53156.0000 53156.0070 750 550 12 12 10"')
        os.system("inject_pulsar -s 5.0 --pred t2pred.dat --prof prof.asc /newdata/All_filterbanks/GMRT_band4_real_noise_4k_81.92us.fil >  /newdata/All_filterbanks/Simulation" + str(i+1) + str(j+1) + ".fil")
        print("Simulation" + str(i+1) + str(j+1) + " done.")
