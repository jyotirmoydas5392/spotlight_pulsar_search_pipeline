from subprocess import run
import shlex
import os
for i in range(8,10):
    for j in range(0,10):
        run(shlex.split("prepsubband -lodm 48.92 -dmstep 0 -numdms 1 -numout 7219200 -nsub 1024 Simulation" + str(i+1) + str(j+1) + ".fil -o Simulation" + str(i+1) + str(j+1)))
        run(shlex.split("realfft -fwd Simulation" + str(i+1) + str(j+1) + "_DM48.92.dat"))
        run(shlex.split("accelsearch -zmax 000 -numharm 8 Simulation" + str(i+1) + str(j+1) + "_DM48.92.fft"))
        run(shlex.split("accelsearch -zmax 050 -numharm 8 Simulation" + str(i+1) + str(j+1) + "_DM48.92.fft"))
        run(shlex.split("accelsearch -zmax 200 -numharm 8 Simulation" + str(i+1) + str(j+1) + "_DM48.92.fft"))
        run(shlex.split("accelsearch -zmax 200 -wmax 600 -numharm 8 Simulation" + str(i+1) + str(j+1) + "_DM48.92.fft"))

