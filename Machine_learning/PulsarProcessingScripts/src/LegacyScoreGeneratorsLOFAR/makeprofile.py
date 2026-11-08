#!/share/apps/Python2.5/bin/python

#import os
#import operations as op
from xml.dom import minidom, EMPTY_NAMESPACE

def test_phcx(xmldata):
	###### read out data blocks ######
	profile_block = xmldata.getElementsByTagName('Profile')	
	subband_block = xmldata.getElementsByTagName('SubBands')
	datablock_block = xmldata.getElementsByTagName('DataBlock')
	###### test length of data in blocks ######
	if (len(profile_block)==len(subband_block)==len(datablock_block)==2):
		profile_points_opt = profile_block[1].childNodes[0].data	
		subband_points_fft = subband_block[0].childNodes[0].data	
		subband_points_opt = subband_block[1].childNodes[0].data
		datablock_points_fft = datablock_block[0].childNodes[0].data		
		datablock_points_opt = datablock_block[1].childNodes[0].data
		if (len(profile_points_opt)>100)&(len(subband_points_opt)>1000)&(len(subband_points_fft)>1000)&(len(datablock_points_opt)>1000):
			#"""&(len(datablock_points_fft)>1000)"""
		   subband_bins = int(subband_block[1].getAttribute("nBins"))
		   subband_subbands = int(subband_block[1].getAttribute("nSub"))
		   dmindex = list(xmldata.getElementsByTagName('DmIndex')[1].childNodes[0].data)
		   if (subband_bins==128)&(subband_subbands==16)&(len(dmindex)>100):
			bestwidth = float(xmldata.getElementsByTagName('Width')[1].childNodes[0].data)
			bestsnr = float(xmldata.getElementsByTagName('Snr')[1].childNodes[0].data)
			bestdm = float(xmldata.getElementsByTagName('Dm')[1].childNodes[0].data)
			bestbaryperiod = float(xmldata.getElementsByTagName('BaryPeriod')[1].childNodes[0].data)			
			if (bestwidth != "nan")&(bestsnr != "nan")&(bestdm != "nan")&(bestbaryperiod != "nan"):
			   return "OK"
			else:
			   return "FAILED1"
		   else:
			return "FAILED2"
		else:
		   print len(profile_points_opt), len(subband_points_opt), len(subband_points_fft), len(datablock_points_fft), len(datablock_points_opt)
		   return "FAILED3"
	else:
		return "FAILED4"

	
def getprofile(xmldata,f):
	dec_value = []
	block = xmldata.getElementsByTagName('Profile')
	print "block",block
	points = block[f].childNodes[0].data	
	x,y = 0,0
	while x < len(points):
		if points[x] != "\n":
			try:
				hex_value = points[x:x+2]
				dec_value.append(int(hex_value,16)) # now the profile (shape, unscaled) is stored in dec_value
				x = x+2
				y = y+1
			except ValueError:
				break
		else:
			x = x+1
	return dec_value

def getsubbands(xmldata):
	###### extract data ######
	dec_value = []
	block = xmldata.getElementsByTagName('SubBands') # this gets all of the bits with the title 'section'	
	points = block[1].childNodes[0].data
	###### transform data from hexadecimal to decimal values ######
	x,y=0,0
	while x < len(points):
		if points[x] != "\n":
			try:
				hex_value = points[x:x+2]
				dec_value.append(int(hex_value,16)) # now the profile (shape, unscaled) is stored in dec_value
				x = x+2
				y = y+1
			except ValueError:
				break
		else:
			x = x+1

	return dec_value

def getdmfft(xmldata,n):
	###### extract data ######
	dec_value = []
	block = xmldata.getElementsByTagName('DataBlock') # this gets all of the bits with the title 'section'	
	points = block[n].childNodes[0].data
	###### transform data from hexadecimal to decimal values ######
	x,y=0,0
	while x < len(points):
		if points[x] != "\n":
			try:
				hex_value = points[x:x+2]
				dec_value.append(int(hex_value,16)) # now the profile (shape, unscaled) is stored in dec_value
				x = x+2
				y = y+1
			except ValueError:
				break
		else:
			x = x+1

	return dec_value

def getdata(cand):
	###### extract data ######
	data = open(cand)
	read_data = data.read()
	data.close()
	###### read out data points and store them in a numpy list ######
	profile = []
	temp = ''
	for i in range(len(read_data)):
		if read_data[i] != "\n":
			temp += read_data[i]
		else:
			profile.append(int(temp))
			temp = ''
			i += 2
	return profile

def getdict(cand):
	###### extract data ######
	data = open(cand)
	read_data = data.read()
	data.close()
	###### read out data points and store them in a dictionary ######
	content = {}
	temp,key,value = '','',''
	for i in range(len(read_data)):
		if read_data[i] != "\n":
		   temp += read_data[i]
		else:
		   for j in range(len(temp)-2):
			key += temp[j]
		   value = temp[len(temp)-1]
		   content[key] = value
		   temp,key,value = '','',''
		   i += 2
	return content
 
def gen_patternfile(scores):
	###### read in pulsar data ######
	#print scores
	pulsars = getdict("pulsars")
	#print pulsars
	numbers = len(pulsars)
	keys   = pulsars.keys()
	values = pulsars.values()

	#print keys


	#strlen = 22
	strlen = len(scores[keys[0]])
	###### generate head of patternfile ######
	output  = open("patterfile_new.pat","w")
	output.write("SNNS pattern definition file V3.2\n")
	output.write("generated at Some day\n")
	output.write("\n")
	output.write("\n")
	output.write("\n")
	output.write("No. of patterns: "+str(numbers)+"\n")
	output.write("No. of input units: "+str(strlen)+"\n")
	output.write("No. of output units: 2"+"\n")
	output.write("\n")
	###### generate body of patternfile ######
	for cand in keys:
	   #output.write("# input pattern "+cand+"\n")
	   outstr = ''
	   for i in range(strlen):
	   	outstr += (str(scores[cand][i])+" ")
	   output.write(outstr+"\n")
	   if   pulsars[cand] == "1":
		output.write("1 0\n")
	   elif pulsars[cand] == "0":
		output.write("0 1\n")

	output.close()





	
def gen_patternfile_fann(scores):
	###### read in pulsar data ######
	#print scores
	pulsars = getdict("pulsars")
	#print pulsars
	numbers = len(pulsars)
	keys   = pulsars.keys()
	values = pulsars.values()

	#print keys


	#strlen = 22
	strlen = len(scores[keys[0]])
	###### generate head of patternfile ######
	output  = open("patterfile_fann_new.pat","w")
	output.write(str(numbers)+" "+str(strlen)+" "+"2"+"\n")

	###### generate body of patternfile ######
	for cand in keys:
	   #output.write("# input pattern "+cand+"\n")
	   outstr = ''
	   for i in range(strlen):
	   	outstr += (str(scores[cand][i])+" ")
	   output.write(outstr+"\n")
	   if   pulsars[cand] == "1":
		output.write("1 -1\n")
	   elif pulsars[cand] == "0":
		output.write("-1 1\n")

	output.close()
