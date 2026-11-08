"""
This code runs on python 2.4 or later.

Based on source code provided by Sam Bates, Dan Thornton, Jenny Green, and Ben Stappers.
Here I've simply cleaned up the code, added extra error checking code. 

I've also tested the code to ensure my changes did not change the functioning of the code.
I know for sure that the scores output by this code are mathematically identical to the scores
output by the original code. I know this for sure, since I recomputed the scores for
candidates genereated during the HTRU survey (score data stored at /local/scratch/cands) and
found the results to be exactly the same. 

Rob Lyon <robert.lyon@cs.man.ac.uk>

+-----------------------------------------------------------------------------------------+
+                       PLEASE RECORD ANY MODIFICATIONS YOU MAKE BELOW                    +
+-----------------------------------------------------------------------------------------+
+ Revision |   Author    | Description                                       |    DATE    +
+-----------------------------------------------------------------------------------------+

 Revision:1    Rob Lyon    Initial version of the re-written code.            07/01/2014 

 Revision:2    Rob Lyon    Added call to freedman_diaconis_rule(data)    29/01/2014
						   in ProfileHelper. This is used to compute
						   the ideal number of bins to use in a
						   histogram. Thanks to Sam bates for this
						   addition.

"""
# Numpy Imports:
from numpy import array
from numpy import histogram

# XML processing Imports:
from xml.dom import minidom

# Standard library Imports:
import gzip,glob,sys,os

# Custom file Imports:
import Utilities
import ProfileHelper
# Plotting imports (Comment out if not debugging!
import matplotlib.pyplot as plt

# ******************************
#
# CLASS DEFINITION
#
# ******************************

class test_22:
	"""				
	Generates 22 scores that describe the key features of pulsar candidate, from the
	candidate's own phcx file. The scores generated are as follows:
	
	Score number	Description of score																				Group
		1			Chi squared value from fitting since curve to pulse profile.									Sinusoid Fitting
		2			Chi squared value from fitting sine-squared curve to pulse profile. 							Sinusoid Fitting
		
		3			Number of peaks the program identifies in the pulse profile - 1.								Pulse Profile Tests
		4			Sum over residuals.																				Pulse Profile Tests
		
		5			Distance between expectation values of Gaussian and fixed Gaussian fits to profile histogram.	Gaussian Fitting
		6			Ratio of the maximum values of Gaussian and fixed Gaussian fits to profile histogram.			Gaussian Fitting
		7			Distance between expectation values of derivative histogram and profile histogram.				Gaussian Fitting	
		8			Full-width-half-maximum (FWHM) of Gaussian fit to pulse profile.								Gaussian Fitting
		9			Chi squared value from Gaussian fit to pulse profile.											Gaussian Fitting
		10			Smallest FWHM of double-Gaussian fit to pulse profile.											Gaussian Fitting
		11			Chi squared value from double Gaussian fit to pulse profile.									Gaussian Fitting
		
		12			Best period.																					Candidate Parameters
		13			Best SNR value.																					Candidate Parameters
		14			Best DM value.																					Candidate Parameters
		15			Best pulse width (original reported as Duty cycle (pulse width / period)).						Candidate Parameters
		
		16			SNR / SQRT( (P-W)/W ).																			Dispersion Measure (DM) Curve Fitting
		17			Difference between fitting factor, Prop, and 1.													Dispersion Measure (DM) Curve Fitting
		18			Difference between best DM value and optimised DM value from fit, mod(DMfit - DMbest).			Dispersion Measure (DM) Curve Fitting
		19			Chi squared value from DM curve fit.															Dispersion Measure (DM) Curve Fitting
		
		20			RMS of peak positions in all sub-bands.															Sub-band Scores
		21			Average correlation coefficient for each pair of sub-bands.										Sub-band Scores
		22			Sum of correlation coefficients between sub-bands and profile.									Sub-band Scores
		
	Check out Sam Bates' thesis for more information, "Surveys Of The Galactic Plane For Pulsars" 2011.
	
	"""
	
	# ******************************
	#
	# MAIN METHOD AND ENTRY POINT.
	#
	# ******************************

	def main(self,argv=None):
		"""
		Main entry point for the Application. Processes command line
		input and begins creating the scores.
	
		"""
		
		print "\n***********************************"
		print "| Executing score generation code |"
		print "***********************************"
		
		# Helper files.
		utils = Utilities.Utilities(True)
		ph = ProfileHelper.ProfileHelper(True)
		
		# Variables used to store stats on candidate scores generated,
		# and failure rate etc.
		#debug = True # Uncomment to debug explcitily, useful when modifying the code.
		debug = True
		candidatesProcessed = 0;
		successes = 0;
		failures = 0;
		
		histogramBins = 60
		
		for cand in glob.glob('*.phcx.gz'):
			
			candidatesProcessed+=1
			
			print "Processing candidate:\t" , cand
			output = open(cand + ".dat", 'w')	
			data = gzip.open(cand,'rb')
			xmldata = minidom.parse(data) # strip off xml data
			data.close()
			
			scores = [] # stores the scores generated for each candidate.
			
			################### Phcx file checks ####################################################################
			
			if debug == True:
				phcx_test = ph.wellFormed_phcx(xmldata)
				if phcx_test != True:
					print cand, " failed phcx-file test and will be excluded from analysis."
					continue
			
			
			try:
				################### Read profile data #################################################################################
				
				# Extracts data from this part of a candidate file. It contains details
				# of the profile in hexadecimal format. The data is extracted from the part
				# of the candidate file which resembles:
				# <Profile nBins='128' format='02X' min='-0.000310' max='0.000519'>
				#
				# Call to ph.getprofile() below will return a LIST data type of 128 integer data points.
				# Takes two parameters: the xml data and the profile data to use. 
				# Phcx files actually contain two profile sections (i.e. there are two <Profile>...</Profile> 
				# sections in the file) which can be read using the XML dom code by specifying the index of the
				# profile section to use. The first section profileIndex = 0 pretains to a profile obtained after the FFT,
				# the second, profileIndex = 1, to a profile that has been period and DM searched using PDMPD. We choose 1 here
				# as it should have a better SNR .... maybe.
				profileIndex = 1
				profileData = ph.getprofile(xmldata,profileIndex)
				
				# Simply convert list to an array data type (actually a numpy.ndarray):
				p = array(profileData)
				#print "Decimal version of profile:\n", p
				
				# START DUBUGGING CODE - REMOVE
				# Add code to iterate over the profile and add the intensities to a file unique to the candidate.
				profilePath = cand +".profile"
				try: 
					os.remove(profilePath)
				except Exception as e: print "No file to delete."
				
				for intensity in p:
					utils.appendToFile(profilePath, str(intensity)+",")
				# END DUBUGGING CODE - REMOVE
				
				# The function below wasn't used in the original code, but I've cleaned it up
				# and left it here in case anybody needs it.
				#utils.listOut("profile_",[],p,cand)
				
			except Exception as e: # catch *all* exceptions
				print "Error reading profile data using ProfileHelper.py (line 144 in test_22.py):\n\t", sys.exc_info()[0]
				print utils.format_exception(e)
				print cand, " did not have scores generated."
				failures+=1
				continue
				
			try:	
				################### Sinusoid Fitting ##########################################################################"
				
				sin_fit = ph.fitSineAndSineSquaredCureveToProfile(p)
				
				# Add first scores.
				scores.append(float(sin_fit[0])) # Score 1.  Chi-Squared value for sine fit to raw profile.
				scores.append(float(sin_fit[1])) # Score 2.  Chi-Squared value for sine-squared fit to amended profile.
				scores.append(float(sin_fit[2])) # Score 3.  Difference between maxima.
				scores.append(float(sin_fit[3])) # Score 4.  Sum over residuals.
				
				if(debug==True):
					print "\nScore 1. Chi-Squared value for sine fit to raw profile = ",sin_fit[0]
					print "Score 2. Chi-Squared value for sine-squared fit to amended profile = ",sin_fit[1]
					print "Score 3. Difference between maxima = ",sin_fit[2]
					print "Score 4. Sum over residuals = ",sin_fit[3]
				
			except Exception as e: # catch *all* exceptions
				print "Error computing scores 1-4 (Sinusoid Fitting) using ProfileHelper.py (line 164 in test_22.py):\n\t", sys.exc_info()[0]
				print utils.format_exception(e)
				print cand, " did not have scores generated."
				failures+=1
				continue
				
				################### Derivatives of profile #############################################################################
				
			try:
				
				# Calculates the derivatives of the profile.
				dy = ph.derivative_y(list(p))
				#print "Data in dy:\n",dy
				
			except Exception as e: # catch *all* exceptions
				print "Error computing derivative of profile using ProfileHelper.py (line 190 in test_22.py):\n\t", sys.exc_info()[0]
				print utils.format_exception(e)
				print cand, " did not have scores generated."
				failures+=1
				continue
				
				################### Histogram of derivatives ###########################################################################
				
			try:
				
				# This is a deprecated call of a numpy library. The 'new' parameter is not
				# included in newer versions, but I haven't removed it here since the Jodrell
				# servers may be using an older version of numpy/scipy. Instead just uncomment or
				# comment as appropriate to use the original call. Here I believe that 60
				# is the number of bins used in the histogram.
				#histogram_dy = histogram(dy,60,new=True)
				#derivative_bins = ph.freedman_diaconis_rule(dy)
				#print "Bins for derivative histogram: ",derivative_bins
				
				histogram_dy = histogram(dy,histogramBins) # Calculates a histogram of the derivative dy.
				
				# Performs a gaussian fit on the derivative histogram.
				gaussianFitToDerivativeHistogram = ph.fit_gaussian(histogram_dy[1],histogram_dy[0])
				derivativeHistogram_sigma, derivativeHistogram_expect, derivativeHistogram_maximum = gaussianFitToDerivativeHistogram[0]
				
				if(debug==True):
					print "\n\tGaussian fit to Derivative Histogram details: " 
					print "\tSigma of derivative histogram = " , derivativeHistogram_sigma
					print "\tMu of derivative histogram = "    , derivativeHistogram_expect
					print "\tMax of derivative histogram = "   , derivativeHistogram_maximum
				
				# View histogram - for debugging only... uncomment matlibplot import at top if needed.
				hist, bins = histogram(dy,histogramBins) # Calculates a histogram of the derivative.
				center = (bins[:-1] + bins[1:]) / 2
				plt.bar(center, hist, align='center')
				plt.title("Histogram of derivative dy")
				plt.show()
				#raw_input("Press Enter to continue...")
				
			except Exception as e: # catch *all* exceptions
				print "Error computing histogram of derivatives (lines 210-214 in test_22.py):\n\t", sys.exc_info()[0]
				print utils.format_exception(e)
				print cand, " did not have scores generated."
				failures+=1
				continue
				
				################### Histogram of profile ##############################################################################
				
			try:
				
				# See above about deprectated call to understand why this is commented out.
				#histogram_profile = histogram(p,60,new=True) # Calculates a histogram of the profile.
				
				#profile_bins = ph.freedman_diaconis_rule(p)
				#print "Bins for profile histogram: ",profile_bins
				
				histogram_profile = histogram(p,histogramBins) # Calculates a histogram of the profile data.
				
				# Performs a gaussian fit on the profile histogram.
				gaussianFitToProfileHistogram = ph.fit_gaussian(histogram_profile[1],histogram_profile[0])
				profileHistogram_sigma, profileHistogram_expect, profileHistogram_maximum = gaussianFitToProfileHistogram[0]
				
				if(debug==True):
					print "\n\tGaussian fit to Profile Histogram details: " 
					print "\tSigma of profile histogram = " , profileHistogram_sigma
					print "\tMu of profile histogram = "    , profileHistogram_expect
					print "\tMax of profile histogram = "   , profileHistogram_maximum
					
				# View histogram - for debugging only... uncomment matlibplot import at top if needed.
				hist, bins = histogram(p,histogramBins) # Calculates a histogram of the profile.
				center = (bins[:-1] + bins[1:]) / 2
				plt.bar(center, hist, align='center')
				plt.title("Histogram of profile")
				plt.show()
				#raw_input("Press Enter to continue...")
				
			except Exception as e: # catch *all* exceptions
				print "Error computing histogram of profile (line 243 in test_22.py):\n\t", sys.exc_info()[0]
				print utils.format_exception(e)
				print cand, " did not have scores generated."
				failures+=1
				continue
				
				#################### Gaussian fit of profile histogram with fixed expect ##############################################

			try:
				# Here gf refers to Gaussian fit.
				gf_ProfileHistogram_fixed_Expect = ph.fit_gaussian_fixed_with_bins(histogram_profile[1],histogram_profile[0],histogramBins)	# Performs a gaussian fit with fixed expectation value on the profile histogram.m.
				gf_ProfileHistogram_fixed_sigma, gf_ProfileHistogram_fixed_maximum = gf_ProfileHistogram_fixed_Expect[0]
				gf_ProfileHistogram_fixed_fwhm = gf_ProfileHistogram_fixed_Expect[1]
				gf_ProfileHistogram_fixed_chi  = gf_ProfileHistogram_fixed_Expect[2] 
				gf_ProfileHistogram_fixed_xmax = gf_ProfileHistogram_fixed_Expect[4]
				
				if(debug==True):
					print "\n\tGaussian fits to Profile Historgram with fixed Mu details:" 
					print "\tSigma of Gaussian fit to Profile Historgram = "         , gf_ProfileHistogram_fixed_sigma
					print "\tMax of Gaussian fit to Profile Historgram = "           , gf_ProfileHistogram_fixed_maximum
					print "\tFWHM of Gaussian fit to Profile Historgram = "          , gf_ProfileHistogram_fixed_fwhm
					print "\tChi-squared of Gaussian fit to Profile Historgram = "   , gf_ProfileHistogram_fixed_chi
					print "\txmax of Gaussian fit to Profile Historgram = "          , gf_ProfileHistogram_fixed_xmax
					
			except Exception as e: # catch *all* exceptions
				print "Error computing gaussian fit of profile histogram with fixed expect (lines 274-278 in test_22.py):\n\t", sys.exc_info()[0]
				print utils.format_exception(e)
				print cand, " did not have scores generated."
				failures+=1
				continue
					
				#################### Distance fit to fixed ############################################################################"
			try:
				dexp_fix = abs(gf_ProfileHistogram_fixed_xmax - profileHistogram_expect)	  # Score 5.
				amp_fix =  abs( gf_ProfileHistogram_fixed_maximum / profileHistogram_maximum) # Score 6.
				dexp = abs(derivativeHistogram_expect - profileHistogram_expect)              # Score 7.
				
				# Add scores.
				scores.append(float(dexp_fix)) # Score 5. Distance between expectation values of Gaussian and fixed Gaussian fits to profile histogram.
				scores.append(float(amp_fix))  # Score 6. Ratio of the maximum values of Gaussian and fixed Gaussian fits to profile histogram.
				scores.append(float(dexp))     # Score 7. Distance between expectation values of derivative histogram and profile histogram.
				
				if(debug==True):
					print "\nScore 5. Distance between expectation values of Gaussian and fixed Gaussian fits to profile histogram = ", dexp_fix
					print "Score 6. Ratio of the maximum values of Gaussian and fixed Gaussian fits to profile histogram = ",amp_fix
					print "Score 7. Distance between expectation values of derivative histogram and profile histogram. = ",dexp
					
			except Exception as e: # catch *all* exceptions
				print "Error computing scores 5-7, 'distance fit to fixed' (lines 297-304 in test_22.py):\n\t", sys.exc_info()[0]
				print utils.format_exception(e)
				print cand, " did not have scores generated."
				failures+=1
				continue
			
				#################### T1 - 1-Peak fit on profile #######################################################################		
			try:
				minbg = min(profileHistogram_expect,p.mean()) # Estimate background.
				
				tempProfile = []

				if minbg > 0.:					  
										
					for i in range(len(p)):				
						newy = p[i] - minbg + p.std()		  # Substract background from profile
						if newy < 0.:					      # and store the new profile in list temp
							newy = 0.
												
						tempProfile.append(newy)
				else:							
					tempProfile = p					  
				
				# Here gf refers to Gaussian fit
				gf_profile_result = ph.fit_gaussian_t1(tempProfile)
				gf_profile_fwhm, gf_profile_chi = gf_profile_result[1], gf_profile_result[2]
				
				# Add scores.
				scores.append(float(gf_profile_fwhm)) # Score 8. Full-width-half-maximum (FWHM) of Gaussian fit to pulse profile. 
				scores.append(float(gf_profile_chi))  # Score 9. Chi squared value from Gaussian fit to pulse profile.
		
				if(debug==True):
					print "\nScore 8. Full-width-half-maximum (FWHM) of Gaussian fit to pulse profile = ", gf_profile_fwhm
					print "Score 9. Chi squared value from Gaussian fit to pulse profile = ",gf_profile_chi
				
			except Exception as e: # catch *all* exceptions
				print "Error computing scores 8-9, 't1 peak fit on profile' (lines 320-341 in test_22.py):\n\t", sys.exc_info()[0]
				print utils.format_exception(e)
				print cand, " did not have scores generated."
				failures+=1
				continue
			
				#################### T2 - Double Gaussian fit on profile #######################################################################
			try:
				# dgf means double Gaussian fit
				dgf_profile_result = ph.fit_doubleGaussian_t2(p) # Double gaussian fit arount the maximum of the profile.
				dgf_profile_fwhm1  = dgf_profile_result[1]
				dgf_profile_chi    = dgf_profile_result[2]
				dgf_profile_fwhm2  = dgf_profile_result[6]
				
				# Here p.std() is the standard deviation of the profile.
				# gf is Gaussian fit, dgf is double Gaussian fit.
				gf_dgf_diff = dgf_profile_result[3] - (gf_profile_result[3] + minbg - p.std())	# Differences of gaussian fits t1 and t2.
				gf_dgf_std  = float(abs(gf_dgf_diff.std()))			                            # Standard deviation of differences.
				
				if gf_dgf_std < 3.:
					dgf_fwhm = gf_profile_fwhm
				else:
					dgf_fwhm = float(min(dgf_profile_fwhm1 , dgf_profile_fwhm2))
					
				# Add scores.
				scores.append(float(dgf_fwhm))         # Score 10. Smallest FWHM of double-Gaussian fit to pulse profile. 
				scores.append(float(dgf_profile_chi))  # Score 11. Chi squared value from double Gaussian fit to pulse profile.
		
				if(debug==True):
					print "\nScore 10. Smallest FWHM of double-Gaussian fit to pulse profile = ", dgf_fwhm
					print "Score 11. Chi squared value from double Gaussian fit to pulse profile = ", dgf_profile_chi
				
			except Exception as e: # catch *all* exceptions
				print "Error computing scores 10-11, 'double Gaussian fit on profile' (line 357-370 in test_22.py):\n\t", sys.exc_info()[0]
				print utils.format_exception(e)
				print cand, " did not have scores generated."
				failures+=1
				continue
			
				#################### DM curve analysis ################################################################################
			
			try:
				
				dm_fit = ph.dm_curve_fit_and_candidate_params(xmldata)
				bestPeriod = float(dm_fit[7])				       # Score 12. Best period.
				bestSNR    = float(dm_fit[8])			           # Score 13. Best S/N value.
				bestDM = float(dm_fit[9])				           # Score 14. Best DM value.
				bestPulseWidth = float(dm_fit[10])			       # Score 15. Best pulse width.
				peak = float(dm_fit[11])			               # Score 16. SNR / SQRT( (P-W)/W ).
				diff_fitting_factor = float(abs(1-dm_fit[0][1]))   # Score 17. Difference between fitting factor, Prop, and 1.
				diff_DM_to_optimsied_DM = float(abs(dm_fit[0][2])) # Score 18. Difference between best DM value and optimised DM value from fit, mod(DMfit - DMbest).
				chiSqDMFit = float(dm_fit[2])				       # Score 19. Chi squared value from DM curve fit.
				
				# Add scores.
				scores.append(bestPeriod)              
				scores.append(bestSNR)
				scores.append(bestDM)
				scores.append(bestPulseWidth)
				scores.append(peak)
				scores.append(diff_fitting_factor)
				scores.append(diff_DM_to_optimsied_DM)
				scores.append(chiSqDMFit)
		
				if(debug==True):
					print "\nScore 12. Best period = " , bestPeriod
					print "Score 13. Best S/N value = " , bestSNR
					print "Score 14. Best DM value = " , bestDM
					print "Score 15. Best pulse width = " , bestPulseWidth
					print "Score 16. SNR / SQRT( (P-W)/W ) = " , peak
					print "Score 17. Difference between fitting factor, Prop, and 1 = " , diff_fitting_factor
					print "Score 18. Difference between best DM value and optimised DM value from fit, mod(DMfit - DMbest) = " , diff_DM_to_optimsied_DM
					print "Score 19. Chi squared value from DM curve fit = " , chiSqDMFit
				
			except Exception as e: # catch *all* exceptions
				print "Error computing scores 12-19, DM curve analysis (line 391-399 in test_22.py):\n\t", sys.exc_info()[0]
				print utils.format_exception(e)
				print cand, " did not have scores generated."
				failures+=1
				continue
			
				#################### Subband scores ####################################################################################

			try:
				subband_scores = ph.getSubband_scores(xmldata)
				RMS = float(subband_scores[0])       # Score 20. RMS of peak positions in all sub-bands.
				mean_corr = float(subband_scores[1]) # Score 21. Average correlation coefficient for each pair of sub-bands.
				
				# Add scores.
				scores.append(RMS)
				scores.append(mean_corr)
		
				if(debug==True):
					print "\nScore 20. RMS of peak positions in all sub-bands = " , RMS
					print "Score 21. Average correlation coefficient for each pair of sub-bands = " , mean_corr
					
			except Exception as e: # catch *all* exceptions
				print "Error computing scores 20-21 (line 431-433 in test_22.py):\n\t", sys.exc_info()[0]
				print utils.format_exception(e)
				print cand, " did not have scores generated."
				failures+=1
				continue
			
				#################### Correlation of profile with subbands #############################################################

			try:
				# Calculate the correlation of profile with all subbands.
				correlation = ph.getProfileCorr(xmldata,p,"Bands")
				
				# Now calculate integral of correlation coefficients.
				correlation_integral = 0
				for i in range( len( correlation ) ):
					correlation_integral += correlation[i]
				
				# Add scores.
				scores.append(correlation_integral)        # Score 22. Sum of correlation coefficients between sub-bands and profile.
		
				if(debug==True):
					print "\nScore 22. Sum of correlation coefficients between sub-bands and profile = " , correlation_integral
					

			except Exception as e: # catch *all* exceptions
				print "Error computing score 22, correlation of profile with sub-bands (line 454-459 in test_22.py):\n\t", sys.exc_info()[0]
				print utils.format_exception(e)
				print cand, " did not have scores generated."
				failures+=1
				continue
				
				########################### Write out data ####################################################################################
			try:
				for i in range(len(scores)):
					output.write(str(scores[i])+",")	
			except Exception as e:
				print "Error writing out candidate scores (line 477-478 in test_22.py):\n\t", sys.exc_info()[0]
				print utils.format_exception(e)
				print cand, " did not have scores generated."
				failures+=1
				continue
			
			successes+=1
		
		print "\nCandidates processed:\t",candidatesProcessed
		print "Sucesses:\t", successes
		print "Failures:\t", failures
		print "Done."
		
if __name__ == '__main__':
	test_22().main()