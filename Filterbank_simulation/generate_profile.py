import matplotlib.pyplot as plt
import numpy as np
bins=277
#############  Gaussian shape gen.  ##################
duty=float(input("enter ducy cycle ( FWHM of gaussian in % of period): "))
frac=duty/100.0
sigma=frac*bins/2.355
gaussian=[]
x=[]
for r in range(0,bins):
	gaussian.append(np.exp(-(r-bins/2.0)**2/(2*(sigma)**2)))
	x.append(r)
############   profile generation   ###################
profile0=np.array(gaussian)
profile=profile0/max(profile0)
profile1=profile.tolist()
f1=open("prof.asc","w+")
for r in range(0,bins):
	f1.write(str(profile1[int(r)])+"\n")
f1.close()
plt.plot(x,profile)
plt.show()
