from subprocess import run
import shlex
import os
import numpy as np
mass=np.loadtxt("parameter_space1.dat",usecols=0)
orbperiod=np.loadtxt("parameter_space1.dat",usecols=1)
for i in range(5,6):
        for j in range(7,8):
                os.system("fake -period 22.6993785996 -dm 48.92 -snrpeak 0.2 -nbits 8 -nchans 4096 -tsamp 81.92 -tobs 600 -tstart 53156.0 -fch1 500.000 -foff -0.048828 -binary -bper " + str(orbperiod[i])+ " -bomega 0.0 -bphase 0.0 -bcmass " + str(mass[j]) + "> file" + str(i+1) + str(j+1) + ".fil")
                run(shlex.split("prepsubband -lodm 48.92 -dmstep 0 -numdms 1 -numout 7219200 -nsub 1024 file" + str(i+1) + str(j+1) + ".fil -o Simulation" + str(i+1) + str(j+1)))
                run(shlex.split("realfft -fwd Simulation" + str(i+1) + str(j+1) + "_DM48.92.dat"))
                run(shlex.split("accelsearch -zmax 000 -numharm 8 Simulation" + str(i+1) + str(j+1) + "_DM48.92.fft"))
                run(shlex.split("accelsearch -zmax 200 -numharm 8 Simulation" + str(i+1) + str(j+1) + "_DM48.92.fft"))
                run(shlex.split("accelsearch -zmax 200 -wmax 600 -numharm 8 Simulation" + str(i+1) + str(j+1) + "_DM48.92.fft"))

