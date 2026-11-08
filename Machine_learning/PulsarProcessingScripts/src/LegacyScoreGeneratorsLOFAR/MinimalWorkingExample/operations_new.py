#!/share/apps/Python2.5/bin/python

from numpy import *
from scipy import std
from scipy.optimize import leastsq
from numpy import sin 
import makeprofile as mk
import pylab

#################################################################################################################
### module transforms data from hexadecimal in decimal system
#'''
def hex(list,nsub,nbin):	# input: numpy list 'list' with hexadecimal values, number of subbands 'nsub', number of bins 'nbin'

	#print list
	x,y = 0,0
	newlist = []
	while x < len(list):
		if list[x] != "\n":
			try:
				hex = list[x:x+2]
				#print hex
				newlist.append(int(hex,16))
				x += 2
				y += 1
			except ValueError:
				break
		else:
			x += 1
	a = array(newlist).reshape(nsub,nbin)
	return a
#'''

#################################################################################################################
### module outputs given x,y data in file
#'''
def listout(name,x,y,cand):		# input: name string 'name', data lists 'x' and 'y', candidate string 'cand'
	if x == []:
		x = range(len(y))
	if cand == "":
		output = open(name+".data","w")
		for i in range(len(y)):
			output.write(str(x[i])+"\t"+str(y[i])+"\n")
		output.close
	else:
	   	output = open(name+cand[:30]+".data","w")
		for i in range(len(y)):
			output.write(str(x[i])+"\t"+str(y[i])+"\n")
		output.close
#'''

#################################################################################################################
### module sorts a dictionary for different values
#'''
def sort_out(all,names):		# input: dictionary 'all', list 'names' with the names of values in the dictionary
	namestr = ""
	for i in range(len(names)):
		namestr += names[i] 
	help = []
	for i in range(len(names)):
		temp = ''
		for j in range(len(names[i])):
		   if (names[i][j] != ' ') and (names[i][j] != '\t'):
			temp += names[i][j]
		help.append(temp)
	names = help
	for l in range(len(names)):
		temp = dict_sort_by_value_n(all,l) 			# sort by value l 

		output = open("all_" + names[l] + ".data","w")		# output of sorted dictionary in file
		output.write("\t\t" + "candidate\t\t" + namestr + "\n")
		for (k,v) in temp:
		    help = str(k)
		    for i in range(len(v)):
		       hstr = str(v[i])
		       if len(hstr) > 7:
			  help += ("\t"+hstr[0:9])
		       else:
			  while len(hstr) < 8:
			     hstr += "0"
			  help += ("\t"+hstr)
		    output.write(help + "\n")
		output.close
#'''

#################################################################################################################
### the following module is based on a C-script by Aristeidis Noutsos
#'''
def subband_score(subbands, prof_bins, band_subbands, bestWidth):
	#block_bands = xmldata.getElementsByTagName("SubBands") 
	#frequency = block_bands[1].childNodes[0].data
	#prof_bins = int(block_bands[1].getAttribute("nBins"))
	#band_subbands = int(block_bands[1].getAttribute("nSub"))
	#subbands = hex(frequency, band_subbands, prof_bins)
	#bestWidth = float(xmldata.getElementsByTagName('Width')[1].childNodes[0].data)
	#print "hello"
	width_bins = int(ceil(bestWidth*prof_bins))
	subband_sums = []

	## CALCULATE THE AMPLITUDES FOR EACH SUBBAND USING A BOX-CAR EQUAL TO THE PULSE WIDTH  ##

	for i in range(band_subbands):
	   sums_vec = []
	   for j in range(prof_bins-width_bins+1):
		sum = 0
		for b in range(width_bins):
		   sum += subbands[i][j+b]
		sums_vec.append(sum)
	   subband_sums.append(sums_vec)

	## FIND THE MAXIMA OF THE AMPLITUDES FOR EACH SUBBAND ##

	max_bins, max_sums = [], []
	for i in range(len(subband_sums)):
	   max_sum = -10000.0
	   for j in range(len(subband_sums[i])):
	      if (subband_sums[i][j]>max_sum):
		max_sum = subband_sums[i][j]
       		max_bin = j+width_bins/2
	   max_bins.append(float(max_bin))
	   max_sums.append(max_sum)

	#pylab.title('Maxima of amplitudes for each subband')
	#pylab.plot(range(len(max_bins)),max_bins)
	#pylab.show()
	med = array(max_bins).mean()

	## CHECK HOW CLOSE TO EACH OTHER ARE THE POSITIONS OF THE MAXIMA ##

	count = 0
	var_med = 0.0

	for i in range(len(max_bins)):
	   if (abs(max_bins[i]-med) <= float(width_bins)):
	      count += 1
	      var_med += pow(max_bins[i]-med,2)
	
	if (count > 1):
	   var = var_med/float(count-1)
	else:
	   mean,var = 0,0
	   for i in range(len(max_bins)):
	      mean += max_bins[i]
	   mean /= float(len(max_bins))
	   for i in range(len(max_bins)):
	      var += pow(max_bins[i]-mean,2)
	   var /= float(len(max_bins)-1)

	stdev = sqrt(var) 

	########################  Linear correlation  #########################
	## Correlates the amplitudes across the pulse between subbands pairs ##
	m = 0
	sum = 0.0
	for i in range(len(subband_sums)):
	   k = i+1
	   while k < len(subband_sums):
	      cc = corrcoef(subband_sums[i], subband_sums[k])[0][1]
	      if str(cc)=="nan":
		k += 1
		sum += 0.0
		m += 1
	      else:
		sum += cc
		k += 1
		m += 1 
	mean_corr = sum/float(m) ## AVERAGE CORRELATION COEFFICIENT ACROSS ALL SUBBAND PAIRS

	rms = stdev/float(width_bins) ## RMS SCATTER OF THE MAXIMA NORMALISED TO THE PULSE WIDTH

	return rms, mean_corr
#'''

#################################################################################################################	
#'''
def zero_del(x,y):
	if x == []:
		x = range(len(y))
	i,j = 0,0
	x_temp,y_temp = [],[]
	while i < len(y):
		if y[i] != 0:
			x_temp.append(x[i])
			y_temp.append(y[i])
			i += 1
		else:
			i += 1
	return x_temp,y_temp
#'''

#################################################################################################################
### performs a gaussian fit with additional background term
#'''
def fit_gaussian_with_bg(xdata,ydata): # data should be numpy arrays 

	###### calculates the residuals ######
	def __residuals(paras, y, x):
		sigma, expect, maximum, bg = paras 
		err = y - (abs(maximum)*exp((-((x-expect)/sigma)**2)/2)+(bg))
		return err

	###### evaluates the gauss function in a given point ######
	def __peval(x, paras):
		sigma, expect, maximum, bg = paras
		return (abs(maximum)*exp((-((x-expect)/sigma)**2)/2)+(bg))

	###### perform gaussian fit ######
	if xdata == []:
	   xdata = range(len(ydata))
	expect = argmax(ydata)
	maximum = ydata[expect]
	sigma = 2*std(ydata)
	bg = mean(ydata)

#	if len(xdata) == len(ydata)+1:
#	   xdata = xdata[0:-1] 

	p0 = [sigma, expect, maximum, bg]
	plsq = leastsq(__residuals, p0, args=(ydata,xdata))
	fwhm = abs(2 * sqrt(2*log(2)) * plsq[0][0])
	fit = __peval(xdata, plsq[0])

        chisq = 0
        for i in range(len(ydata)):
	      chisq += (ydata[i]-fit[i])**2

	#pylab.title('fit_gaussian_with_bg')
	#pylab.plot(xdata, fit)
	#pylab.plot(xdata, ydata)
	#pylab.show()

	return plsq[0], fwhm, chisq/len(ydata), fit, xdata, ydata, p0
#'''

#################################################################################################################
### performs a gaussian fit on the input data
#'''
def fit_gaussian(xdata,ydata): # data should be numpy arrays 
	
	###### calculates the residuals ######
	def __residuals(paras, y, x):
		sigma, expect, maximum = paras 
		err = y - (abs(maximum)*exp((-((x-expect)/sigma)**2)/2))
		return err

	###### evaluates the gauss function in a given point ######
	def __peval(x, paras):
		sigma, expect, maximum = paras
		return (abs(maximum)*exp((-((x-expect)/sigma)**2)/2))

	###### reverses the order of the list entries ######
	def __mirror(list):
		temp = []
		for i in range(len(list)):
		   temp.append(list[abs(i-len(list)+1)])
		return temp

	if xdata == []:
	   xdata = range(len(ydata))

	exit,counter = 0,0
	pos = argmax(ydata)
	expect = xdata[pos]
	sigma = std(ydata)
	maximum = max(ydata)
	meansq = mean(ydata)**2
	temp = ydata

	if len(xdata) == len(ydata)+1:
	   xdata = xdata[0:-1] 
	length = len(xdata)
	
	###### check if maximum is on the border ######
	used = 0
	part1,part2 = [],[]
	if (pos == 0):
	      cut = ceil(len(ydata)/2)
	      part1 = ydata[:cut]
	      part2 = __mirror(part1)
	      ydata = list(part2)+list(part1)
	      used = 1
	elif (pos == len(xdata)-1):
	      cut = ceil(len(ydata)/2)
	      part1 = ydata[cut:]
	      part2 = __mirror(part1)
	      ydata = list(part1)+list(part2)
	      used = 1
	
	###### perform gaussian fit ######
	while exit == 0:
		p0 = [sigma, expect, maximum]
		plsq = leastsq(__residuals, p0, args=(ydata,xdata))
		if used == 1:
		   plsq[0][1] -= xdata[cut]
		   ydata = temp
		fwhm = abs(2 * sqrt(2*log(2)) * plsq[0][0])
		fit = __peval(xdata, plsq[0])

	        chisq = 0
	        for i in range(length):
		      chisq += (ydata[i]-fit[i])**2

		if (chisq > meansq*length) & (plsq[0][0] < 0.2*length) & (used==0):
		   counter += 1
		   temp = delete(temp,pos)
		   pos = argmax(temp)
		   expect = xdata[pos+counter]
		   if counter > 5:
			exit += 1
		else:
		   exit += 1

	#pylab.title('performs a gaussian fit on the input data')
	#pylab.plot(xdata, fit)
	#pylab.plot(xdata, ydata)
	#pylab.show()

	return plsq[0], fwhm, chisq, fit, xdata, ydata
#'''	

#################################################################################################################
### performs a double gaussian fit with background
#'''
def fit_gaussian_double(ydata,p0):

	###### calculates the residuals ######
	def __residuals(paras, y, x):
		sigma1, expect1, maximum1, bg1, sigma2, expect2, maximum2, bg2 = paras
		err = y - ((abs(maximum1)*exp((-((x-expect1)/abs(sigma1))**2)/2))+(abs(maximum2)*exp((-((x-expect2)/abs(sigma2))**2)/2))+(abs(bg1)+abs(bg2))/2)
		return err

	###### evaluates the function ######
	def __peval(x, paras):
		sigma1, expect1, maximum1, bg1, sigma2, expect2, maximum2, bg2 = paras
		return ((abs(maximum1)*exp((-((x-expect1)/abs(sigma1))**2)/2))+(abs(maximum2)*exp((-((x-expect2)/abs(sigma2))**2)/2))+(abs(bg1)+abs(bg2))/2)
	
	xdata = range(len(ydata))
	exit,counter = 0,0
	pos = argmax(ydata)

	###### perform gaussian fit ######
	plsq = leastsq(__residuals, p0, args=(ydata,xdata))
	fwhm1 = abs(2 * sqrt(2*log(2)) * plsq[0][0])
	fwhm2 = abs(2 * sqrt(2*log(2)) * plsq[0][4])
	fit = __peval(xdata, plsq[0])

        chisq = 0
        for i in range(len(ydata)):
	   #if fit[i] >= 1.:
	      chisq += (ydata[i]-fit[i])**2/len(ydata)

	#pylab.title('performs a double gaussian fit with background')
	#pylab.plot(range(len(fit)),fit)
	#pylab.show()

	return plsq[0], fwhm1, chisq, fit, xdata, ydata, fwhm2
#'''
	
#################################################################################################################
### test that tries a double gaussian fit of the profile
#'''
def test_fit(ydata):

	###### calculates the residuals ######	
	def __residuals(paras, y, x):
		sigma, expect, maximum, bg = paras 
		err = y - (abs(maximum)*exp((-((x-expect)/(2*sigma))**2)/2)+abs(bg))
		return err

	###### evaluates the function ######
	def __peval(x, paras):
		sigma, expect, maximum, bg = paras
		return (abs(maximum)*exp((-((x-expect)/(2*sigma))**2)/2)+abs(bg))

	xdata = range(len(ydata))
	pos = argmax(ydata)
	maximum = max(ydata)
	newx,newy = xdata,ydata
	tolerance,limit = 0,5
	length = len(xdata)

	####### delete first peak #######
	newx = delete(newx,pos)
	newy = delete(newy,pos)
	i = 1
	while i < len(ydata):
	   if ((pos-i) > 0) & ((pos+i) < len(ydata)):
	    if (ydata[pos-i] < ydata[pos-i+1]) & (ydata[pos+i] < ydata[pos+i-1]):
	      newx = delete(newx,pos-i)
	      newx = delete(newx,pos-i)
	      newy = delete(newy,pos-i)
	      newy = delete(newy,pos-i)
	    elif (ydata[pos-i]  >= ydata[pos-i+1]) or (ydata[pos+i] >= ydata[pos+i-1]) & (tolerance < limit):
	      newx = delete(newx,pos-i)
	      newx = delete(newx,pos-i)
   	      newy = delete(newy,pos-i)
	      newy = delete(newy,pos-i)
	      tolerance += 1
	    else:
	      break	

	   elif ((pos-i) < 0):
	    if (ydata[pos+i] < ydata[pos+i-1]):
	      newx = delete(newx,pos-i+1)
	      newy = delete(newy,pos-i+1)
	    elif (ydata[pos+i] >= ydata[pos+i-1]) & (tolerance < limit):
	      newx = delete(newx,pos-i+1)
	      newy = delete(newy,pos-i+1)
	      tolerance += 1
	    else:
	      break	

	   elif ((pos+i) > len(ydata)):
	    if (ydata[pos-i] < ydata[pos-i+1]):
	      newx = delete(newx,pos-i+1)
	      newy = delete(newy,pos-i+1)
	    elif (ydata[pos-i]  >= ydata[pos-i+1]) & (tolerance < limit):
	      newx = delete(newx,pos-i)
   	      newy = delete(newy,pos-i)
	      tolerance += 1
	    else:
	      break	

	   i += 1

	counter = 0 
	while counter < 8:
	   ####### new gaussian fit #######
	   exit = 0
	   npos = argmax(newy)
	   nexpect = newx[npos]
	   nsigma = std(newy)
	   nmaximum = max(newy)
	   nbg = mean(newy)	
	
	   np0 = [nsigma, nexpect, nmaximum, nbg]
	   plsq = leastsq(__residuals, np0, args=(newy,newx))
	   nfwhm = abs(2 * sqrt(2*log(2)) * plsq[0][0])
	   nfit = __peval(newx, plsq[0])

	   nchisq = 0
	   for i in range(len(newy)):
	      nchisq += (newy[i]-nfit[i])**2/len(newy)

	   ####### substraction to data #######	
	   newy = []
	   for i in range(len(ydata)):
	     evaly = __peval(xdata[i],plsq[0])
	     if (evaly <= ydata[i]):
		newy.append(ydata[i]-evaly+plsq[0][3])
	     elif (evaly > ydata[i]) & (xdata[i] > (plsq[0][2]-(1.5*nfwhm)/2)) & (xdata[i] < (plsq[0][2]+(1.5*nfwhm)/2)):
		newy.append(plsq[0][3])
	     else:
		newy.append(ydata[i])
	   newx = range(len(newy))
	   counter += 1
	   if counter == 7:
		store_p2 = plsq[0]
	   elif counter == 8:
		store_p1 = plsq[0]
	
	###### perform final gaussian fit ######
	p = list(store_p1) + list(store_p2)
 	finalfit = fit_gaussian_double(ydata,array(p))

	fit1 = __peval(xdata,store_p1)
	fit2 = __peval(xdata,store_p2)
	combifit = fit1+fit2-store_p1[3]-store_p2[3]+(store_p1[3]+store_p2[3])/2

        combi_chisq = 0
	for i in range(len(ydata)):
	   #if combifit[i] >= 1.:
	      combi_chisq += (ydata[i]-combifit[i])**2/len(ydata)

	combi_fwhm1 = abs(2 * sqrt(2*log(2)) * p[0])
	combi_fwhm2 = abs(2 * sqrt(2*log(2)) * p[4])
	#pylab.title('test that tries a double gaussian fit of the profile')
	#pylab.plot(xdata, combifit)
	#pylab.plot(xdata, ydata)
	if (finalfit[2] <= combi_chisq):
		pylab.plot(finalfit[4], finalfit[3])
		pylab.plot(finalfit[4], finalfit[5])
		#pylab.show()
	else:
		print "combi"
		pylab.plot(xdata, combifit)
		pylab.plot(xdata, ydata)
		#pylab.show()
	#print finalfit
	#print len(finalfit[0]), len(finalfit[1])


	#if (finalfit[2] <= combi_chisq):

	return finalfit
	#else:
        #return [p,combi_fwhm2,combi_chisq,combifit,xdata,ydata,combi_fwhm1]

        


   
#'''

#################################################################################################################
### performs a gaussian fit under the constrain that the expectation value is fixed
#'''
def fit_gaussian_fixed(xdata, ydata): # data should be numpy arrays 

	###### calculates the residuals ######
	def _residuals(paras, y, x, xmax):
		sigma, maximum = paras 
		err = y - (abs(maximum)*exp((-((x-xmax)/sigma)**2)/2))
		return err
	###### evaluates the function ######
	def _peval(x, paras, xmax):
		sigma, maximum = paras
		return (abs(maximum)*exp((-((x-xmax)/sigma)**2)/2))
	
	if xdata == []:
	   xdata = range(len(ydata))
	if len(xdata) == len(ydata)+1:
	   xdata = xdata[0:-1] 

	###### start parameter ######
	sigma = std(ydata)
	maximum = max(ydata)
	temp = array(ydata)
	xmax = xdata[29]

	
	###### perform fit ######
	p0 = [sigma, maximum]
	#plsq = leastsq(_residuals, p0, args=(ydata,xdata,xmax), maxfev=10000)
	plsq = leastsq(_residuals, p0, args=(ydata,xdata,xmax))
	
	fwhm = abs(2 * sqrt(2*log(2)) * plsq[0][0])
	fit = _peval(xdata, plsq[0],xmax)
	#print fit
	
        chisq = 0
        for i in range(len(ydata)):
	   #if fit[i] >= 1.:
	      chisq += (ydata[i]-fit[i])**2

#	pylab.title('performs a gaussian fit under the constrain that the expectation value is fixed')
#	pylab.plot(xdata, fit)
#	pylab.plot(xdata, ydata)
#	pylab.show()

	#
	
	return plsq[0], fwhm, chisq, fit, xmax
#'''	

#################################################################################################################
### performs a sine squared fit
#'''
def fit_sine_sqr(ydata,nmax,width): # data should be numpy arrays 

	###### calculates the residuals ######
	def __residuals(paras, y, x):
		A, f, phi = paras 
		err = y - (abs(A)*pow(sin(2*pi*f*x + phi),2))
		return err

	###### evaluates the function ######
	def __peval(x, paras):
		A, f, phi = paras
    		return abs(A)*pow(sin(2*pi*f*x + phi),2)

	###### start parameter ######
	xdata = array(range(len(ydata)))
	A = max(ydata)
	f = float(nmax/(len(ydata)-1.)/2.)
	if ydata[0] == 0:
	   phi = 0
	else:
	   phi = -1/(4*f)

	###### perform sine fit	######	
	p0 = (A,f,phi)
	plsq = leastsq(__residuals, p0, args=(ydata,xdata))
	fit = __peval(xdata, plsq[0])

	chisq = 0
	for i in range(len(ydata)):
	   #if ydata[i] >= 5.:
	      chisq += (ydata[i]-fit[i])**2

	#pylab.title('performs a sine squared fit')
	#pylab.plot(xdata, fit)
	#pylab.plot(xdata, ydata)
	#pylab.show()

	#print chisq
	#print chisq/pow(float(nmax),4)
	
	#return plsq[0], chisq, fit, xdata, ydata	
	return plsq[0], chisq/pow(float(nmax),4), fit, xdata, ydata
#'''

#################################################################################################################
### performs a sine fit on the profile data
#'''
def fit_sine(ydata,nmax,width): # ydata should be numpy array 
	
	###### calculates the residuals ######
	def __residuals(paras, y, x):
		A, f, phi, bg = paras 
		err = y - (abs(A)*sin(2*pi*f*x + phi)+abs(bg))
		return err
	
	###### evaluates the function ######
	def __peval(x, paras):
		A, f, phi, bg = paras
    		return abs(A)*sin(2*pi*f*x + phi)+abs(bg)
	
	###### start parameter ######
	
	xdata = array(range(len(ydata)))
	A0 = abs(max(ydata)-min(ydata))/2.
	f0 = float(nmax/(len(ydata)-1.))
	#f0 = float(width*len(ydata))
	bg0 = mean(ydata)
	if ydata[0] == bg0:
	   phi0 = 0
	elif ydata[0] < bg0: 
	   phi0 = -1/(4*f0)
	elif ydata[0] > bg0:
	   phi0 = +1/(4*f0)
	
	###### perform sine fit	######
	p0 = (A0,f0,phi0,bg0)
	#print p0
	
	plsq = leastsq(__residuals, p0, args=(ydata,xdata), maxfev=10000)
	
	fit = __peval(xdata, plsq[0])
	
	chisq = 0
	for i in range(len(ydata)):
	   #if ydata[i] >= 5.:
	      chisq += (ydata[i]-fit[i])**2
	      
	
	#pylab.title('performs a sine fit on the profile data')
	#pylab.plot(xdata, fit)
	#pylab.plot(xdata, ydata)
	#pylab.show()

	#print chisq
	#print chisq*pow(nmax,4)/100000000.


	#return plsq[0], chisq, fit, xdata, ydata
	return plsq[0], chisq*pow(nmax,4), fit, xdata, ydata
#'''

#################################################################################################################
### this module performs a sine and sine squared fit on the profile
#'''
def t3(p,width):
	
	p_mean, p_std, p_max, p_min = p.mean(), p.std(), p.max(), p.min()
	ressum = 0
	for i in range(len(p)):
	   ressum += abs(p_max-p_min)/2.-p[i]

	###### substracting background ######
	pm = p - p_mean - p_std
	for i in range(len(pm)):
	   if pm[i] < 0:
		pm[i] = 0
	

	###### find peaks ######
	tempx,tempy,max_list,newp = [],[],[],[]
	counter = 0
	for i in range(len(pm)):
	   if pm[i] != 0:
		tempy.append(pm[i])
		tempx.append(i)
	   elif counter < 4:
		tempy.append(pm[i])
		tempx.append(i)
		counter += 1
	   else:
		if max(tempy) != 0:			
		   max_list.append(tempx[argmax(tempy)])
		   newp += list(tempy)
		tempx,tempy = [],[]
		counter = 0
	
	###### locate and count maxima ######
	if (tempy != []):
	 if (max(tempy) != 0):			
	   max_list.append(tempx[argmax(tempy)])
	   newp += list(tempy)
	nmax = len(max_list)
	#print "nmax = ", nmax
	###### calculate difference between maxima ######
	if nmax > 0:
	   diff = delete(max_list,0)-delete(max_list,nmax-1)
	else:
	   diff = []
	
	###### delete zeros in newp ######
	newp2 = []
	counter,i = 0,0
	while i < len(newp):
	   if newp[i] != 0:
		newp2.append(newp[i])
		counter = 0
		i += 1
	   elif counter < 1:
		newp2.append(newp[i])
		counter += 1
		i += 1
	   else:
		i += 1
	
	###### calculate mean and standard deviation ######
#	if len(diff) > 1:
#	   mean_diff = array(diff).mean()
#	   std_diff  = array(diff).std()
#	elif len(diff) == 1:
#	   mean_diff = float(diff[0])
#	   std_diff  = float(diff[0])
#	else:	
#	   mean_diff = 60.
#	   std_diff  = 60.

	###### perform fits ######	
	newp_fit = fit_sine_sqr(newp2,nmax,width)
	
#	p_fit = fit_sine(p,ceil(1.5*nmax), width)
        
	p_fit = fit_sine(p,nmax, width)
	
	return p_fit, newp_fit, len(diff), ressum#, std_diff, mean_diff, newp2, pm
#'''

#################################################################################################################
### module that performs a double gaussian fit on data
#'''
def t2(ydata):
    part1,part2 = [],[]
    length = len(ydata)
    xdata = range(length)
    expect1 = argmax(ydata)
 
    ###### check if maximum is near borders of the interval ######
    used = 0
    if (expect1 < 15) or (expect1 >= 112):
	cut = int(ceil(length/2))
	part1 = ydata[:cut]
	part2 = ydata[cut:]
	ydata = list(part2) + list(part1)
	used = 1

    ###### perform gaussian fit ######
    result = test_fit(ydata)

    if used == 1:
	result[0][1] = result[0][1] + cut
	result[0][5] = result[0][5] + cut

    return result
#'''

#################################################################################################################
### module that performs a gaussian fit on data
#'''
def t1(ydata):
    xdata,part1,part2 = [],[],[]
    length = len(ydata)
    xdata = range(length)
    xmax = argmax(ydata)

    ###### check if maximum is near borders of the interval ######
    used = 0
    if (xmax < 15) or (xmax >= 112):
	cut = int(ceil(length/2))
	part1 = ydata[:cut]
	part2 = ydata[cut:]
	ydata = list(part2)+list(part1)
	used += 1

    ###### perform gaussian fit ######
    result = fit_gaussian_with_bg(xdata,ydata)

    if used == 1:
	result[0][1] = result[0][1]+cut
    return result
#'''

#################################################################################################################
### sorts numpy dictionary by key
#'''
def dict_sort_by_key(d):
     return sorted(d.items())
#'''

#################################################################################################################
### sorts numpy dictionary by value
#'''
def dict_sort_by_value(d):
     return sorted(d.items(), key=lambda (k,v): (v,k))
#'''

#################################################################################################################
### sorts numpy dictionary by value at position n
#'''
def dict_sort_by_value_n(d,n):
	nd,sd = {},[]
	###### create new dictionary by replacing each key with the value on position n ######
	for item in d:
	   temp = []
	   ival = d[item]
	   ikey = item
	   temp.append(ikey)
	   temp.append(ival)
	   nd[ival[n]] = temp
	###### sort new dictionary by key ######
	nd = dict_sort_by_key(nd)
	###### get original key back ######
	for item in nd:
	   nkey = item[1][0]
	   nval = item[1][1]
	   sd.append((nkey,nval))
	return sd
#'''

#################################################################################################################
### calclates the derivative of a list
#'''
def derivative(y):
	dy = []
	l = len(y)-1
	for i in range(l):
		dy.append(y[i] - y[i+1])
	return dy
#'''

#################################################################################################################
### normalizes a given numpy array to an interval 0...255
#'''
def normalize(p):
	minp = p.min()
	p -= minp
	maxp = p.max()
	if maxp != 0:
   	   p = p*(255./maxp)		   
	return p
#'''

#################################################################################################################
### calculates a cross profile of the subbands or subintegrations
#'''
def cross_profile(allbands, nbin_bands, nsub_bands):
	#block_bands = xmldata.getElementsByTagName('Sub'+section) 
	#frequency = block_bands[f].childNodes[0].data
	#nbin_bands = int(block_bands[f].getAttribute("nBins"))
	#nsub_bands = int(block_bands[f].getAttribute("nSub"))
	#allbands = hex(frequency, nsub_bands, nbin_bands)

	c_profile = []
	for i in range(nsub_bands):
		mean = allbands[i].mean()
		c_profile.append(mean)	
	return array(c_profile)
#'''

#################################################################################################################
### calculates the correlation of the profile with the subbands, -integrals
#'''
def profile_corr(allbands, nbin_bands, nsub_bands, p):
	#block_bands = xmldata.getElementsByTagName('Sub'+section)  
	#frequency = block_bands[1].childNodes[0].data
	#nbin_bands = int(block_bands[1].getAttribute("nBins"))
	#nsub_bands = int(block_bands[1].getAttribute("nSub"))
	#allbands = hex(frequency, nsub_bands, nbin_bands)

	corrlist = []
	for j in range(nsub_bands):
		coef = abs(corrcoef(allbands[j],p))
		if coef[0][1] > 0.0055:		
			corrlist.append(coef[0][1])
	return array(corrlist)
#'''

#################################################################################################################
### this function extracts the DM curve from the DM data block
#'''
def dm_curve(data):
	result,x,temp = [],[],[]
	for i in range(len(data)):
	   if (i+1)%128 == 0:
		result.append(max(temp))
		x.append(i-128)
		temp = []
	   else:
		temp.append(data[i])

	return array(result),array(x)
#'''

#################################################################################################################
### module performs fits of the DM curve
#'''
def dm_curve_fit(snr, dm, bary_period, width, dm_index, y_values):

	
	###### calculates the residuals ######
	def __residuals(paras, y, x):
		Amp,Prop,Shift,Up = paras 
		weff = sqrt(wint + pow(Prop*kdm*abs((dm + Shift)-x)*df/pow(f,3),2))
		#print Shift
		for wind in range(len(weff)):
			if ( weff[wind] > period ):
				weff[wind] = period
		#print period
		#print weff
		#print wint
		#print period-weff
		#print Amp, Prop, Up, weff
		SNR  = Up+Amp*sqrt((period-weff)/weff)
		err  = y - SNR
		#print SNR, err
		return err
	#lsq
	###### evaluates the function ######
	def __peval(x, paras):
		Amp,Prop,Shift,Up = paras
		#print Shift
	
		#print Prop*kdm*abs((dm + Shift)-x)*df/pow(f,3)
		weff = sqrt(wint + pow(Prop*kdm*abs((dm + Shift)-x)*df/pow(f,3),2))
		#print Shift
		for wind in range(len(weff)):
			if ( weff[wind] > period ):
				weff[wind] = period
		#print weff
		SNR  = Up+Amp*sqrt((period-weff)/weff)
		#print SNR
    		return SNR
	###### extract DM curve ######
	#dm_curve_all = array(mk.getdmfft(xmldata,1))
#	curve = dm_curve(y_values)
        curve=[]
	curve.append(y_values)
	curve.append(range(len(y_values)))
	ydata = curve[0]
	ydata = 255./max(ydata)*ydata
	length_all = len(y_values)
	length = len(ydata)

	###### extract x-scale for DM curve ######
	#read_data = list(xmldata.getElementsByTagName('DmIndex')[1].childNodes[0].data)
	#dm_index,temp = [],''
	#for i in range(len(read_data)):
	   #if (read_data[i] != "\n"):
		#temp += (read_data[i])
	   #else:
		#dm_index.append(temp)
		#temp = ''


	###### get start and end DM value and calculate step width ######
	dm_start,dm_end = float(dm_index[1]),float(dm_index[len(dm_index)-1])
	dm_step = abs(dm_start-dm_end)/length_all

	###### SNR and pulse parameters ######
	period = bary_period*1000.
	wint = (width*period)**2
	kdm = 8.3*10**6
	#df = 400
	#f = 1374
	df = 32
	f = 135
	#print period, wint
	peak = snr/sqrt((period-sqrt(wint))/sqrt(wint))

	###### scale x-data ######
	xdata = []
	for i in range(length):
		xdata.append(dm_start+curve[1][i]*dm_step)	
	xdata = array(xdata)

	###### calculate theoretic dm-curve from best values ######
	help = []
	#print length
	for i in range(length):
		weff = sqrt(wint + pow(kdm*abs(dm-xdata[i])*df/pow(f,3),2))
		if weff >period:
			weff = period
		SNR = sqrt((period-weff)/weff)
		#print SNR
		help.append(float(SNR))
	#print "help =", help
	theo = (255./max(help))*array(help)

	###### start parameter for fit #######
	Amp = (255./max(help))
	Prop,Shift  = 1,0
	p0 = (Amp,Prop,Shift,0)
	plsq = leastsq(__residuals, p0, args=(ydata,xdata))
	fit = __peval(xdata, plsq[0])

	###### chi square calculation ######
	chi_fit,chi_theo = 0,0
	for i in range(length):
	   if fit[i] >= 1.:
	      chi_fit  += (ydata[i]-fit[i])**2
	      chi_theo += (ydata[i]-theo[i])**2
	chi_fit  =  chi_fit/length
	chi_theo = chi_theo/length

	#print fit
	#pylab.title('module performs fits of the DM curve')
	#pylab.plot(xdata, fit)
	#pylab.plot(xdata, ydata)
	#pylab.show()

	return plsq[0], chi_fit, chi_theo, theo, fit, xdata, ydata, period, snr, dm, width, peak
#'''
