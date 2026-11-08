import shlex
from subprocess import run
for i in range(0,10):
    for j in range(0,10):
        run(shlex.split("accelsearch -zmax 50 -numharm 8 Simulation" + str(i+1) + str(j+1) + "_DM48.92.fft"))
