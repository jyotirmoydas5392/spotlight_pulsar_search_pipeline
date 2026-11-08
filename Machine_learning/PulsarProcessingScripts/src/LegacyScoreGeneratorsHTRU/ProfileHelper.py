"""
This code runs on python 2.4 or later.

Based on source code provided by Sam Bates, Dan Thornton, Jenny Green, and Ben Stappers.

Re-implementation of the operations.py code original used with the test_22.py file. This
code doesn't try to change the functionality of the original file, but simply tries to 
clean up the code making it more maintainbale.

By Rob Lyon <robert.lyon@cs.man.ac.uk>

+-----------------------------------------------------------------------------------------+
+                       PLEASE RECORD ANY MODIFICATIONS YOU MAKE BELOW                    +
+-----------------------------------------------------------------------------------------+
+ Revision |   Author    | Description                                       |    DATE    +
+-----------------------------------------------------------------------------------------+

 Revision:1    Rob Lyon    Initial version of the re-written code.            07/01/2014 

 Revision:2    Rob Lyon     Changes made to fit_sine(self,yData,maxima):      29/01/2014
 
                            a) Added MatLibPlot import, now plots sine curve 
                               fitted to the pulse profile.
                            b) Added an additional input to the __residuals
                               and __evaluate functions, the amplitude of
                               the peak and the background (fixed values).
                            c) Fixed the amplitude in the fit_sine function
                               so it now fixes amp=max(yData)-min(yData)/2.
                               This was done so that score 1 would not try
                               to fit just any sine curve, but one with the
                               max peak amplitude of the profile. Also fixed
                               the background value to the mean profile 
                               intensity.
                            d) Altered the parameters received by the call
                               to scipy.optimize.leastsq() in fit_sine()
                               function. Rather than just returning the
                               parameters of the fit, we now obtain the
                               covarience matrix too along with other variables.
                               This enables the R-squared value and other stats
                               to be computed.
                            e) Now produces plot of sine fit to pulse proifle
                               in fit_sine(self,yData,maxima). Also computes
                               the R-squared value for the fit and other stats
                               i.e. the standard error and total error.
                            f) Placed original fit_sine(self,yData,maxima) under
                               new method name: fit_sine_original(), although
                               this code has the addition of matlibplot calls.
                               
Revision:3    Rob Lyon      Changes made to def fit_sine_sqr(self,yData,maxima): 29/01/2014 

                            a) Added an additional input to the __residuals
                               and __evaluate functions, the amplitude of
                               the peak (fixed value).
                            b) Fixed the amplitude in the fit_sine function
                               so it now fixes amp=max(yData)-min(yData)/2.
                               This was done so that score 1 would not try
                               to fit just any sine curve, but one with the
                               max peak amplitude of the profile. Also fixed
                               the background value to the mean profile 
                               intensity.
                            c) Altered the parameters received by the call
                               to scipy.optimize.leastsq() in fit_sine_sqr()
                               function. Rather than just returning the
                               parameters of the fit, we now obtain the
                               covarience matrix too along with other variables.
                               This enables the R-squared value and other stats
                               to be computed.
                            d) Now produces plot of sine squared fit to pulse proifle
                               in fit_sine_sqr(self,yData,maxima). Also computes
                               the R-squared value for the fit and other stats
                               i.e. the standard error and total error.
                            e) Placed original fit_sine_sqr(self,yData,maxima) under
                               new method name: fit_sine_sqr_original(), although
                               this code has the addition of matlibplot calls.

Revision:4    Rob Lyon      Added method that calculates number of bins needed  30/01/2014 
                            in a historgram, code provided by Sam Bates; uses
                            the freedman diaconis rule. Cheers Sam.
                            Look for def freedman_diaconis_rule(self,data)
                            
Revision:5    Rob Lyon      Created new version of the method:                  30/01/2014
                            def fit_gaussian_fixed_with_bins(self,xData,yData)
                            Changed the method so that it now uses the specific
                            number of bins for the histogram data passed in to
                            the method.

Revision:6    Rob Lyon      Changed Chi-squared calculation.                    30/01/2014
                            
                            a) In fit_sine(self,yData,maxima) changed chi
                            squared calculation (removed a restriction). 
                            
                            b) In fit_sine_sqr(self,yData,maxima) changed chi
                            squared calculation (removed a restriction).
                            
                            c) Added chisq /= len(yData) to the function:
                               fit_gaussian(self,xData,yData).
                               
                            d) In fit_gaussian_fixed(self,xData,yData) changed
                               chi-squared calculation (again removed a
                               restriction).
                            
Revision:7    Rob Lyon      Added background terms to __residuals and           30/01/2014
                            __evvaluate in fit_sine_sqr() function.
                            
Revision:8    Rob Lyon      Now take the absolute value of sigma,               30/01/2014
                            in def fit_gaussian_with_bg(self,xData,yData),
                            such that:

                            a) In __residuals(x, paras) we have:
                                err = y - ( abs(maximum) * exp( (-((x - mu) / abs(sigma) )**2) / 2) + (bg) )
                            
                            b) In __evaluate(x, paras) we have:
                                return ( abs(maximum) * exp( (-((x - mu) / abs(sigma) )**2) / 2) + (bg) )
                                
                            This change was to prevent errors caused by sigma
                            obtaining negative values.
                             




+-----------------------------------------------------------------------------------------+

NOTE: You can go directly to a revision by searching the text below i.e. search for "Revision:2b"

"""

# Python 2.4 imports.
import Common
from numpy import argmax
from numpy import array
from numpy import ceil
from numpy import corrcoef
from numpy import delete
from numpy import exp
from numpy import log
from numpy import mean
from numpy import pi
from numpy import sin
from numpy import sqrt

from scipy.optimize import leastsq
from scipy import std
from scipy import stats

import matplotlib.pyplot as plt # Revision:1

# ******************************************************************************************
#
# CLASS DEFINITION
#
# ******************************************************************************************

class ProfileHelper(Common.Common):
    """
    Provides utility functions used when computing scores. Replaces the
    makeprofile.py file.
    
    """
    
    # ******************************************************************************************
    #
    # Constructor.
    #
    # ******************************************************************************************
    
    def __init__(self,debugFlag):
        Common.Common.__init__(self, debugFlag)
        
    # ******************************************************************************************
    #
    # Functions.
    #
    # ******************************************************************************************
       
    def wellFormed_phcx(self,xmldata):
        """
        Tests the xml data loaded from a phcx file for well-formedness, and invalid values.
        To understand the code here its best to take a look at a phcx xml file, to see the
        underlying struture. Alternatively I've generated a xml schema file which summarises
        the structure (should be in same folder as this file) called: phcx.xsd.xml .
        
        Parameters:
        xmldata    -    the xml data read in from the phcx file.
        
        Returns:
        True if the xml data is well formed and valid, else false.
        """
        
        # Read out data blocks.
        profile_block = xmldata.getElementsByTagName('Profile')
        subband_block = xmldata.getElementsByTagName('SubBands')
        datablock_block = xmldata.getElementsByTagName('DataBlock')
        
        # Test length of data in blocks. These should be equal to 2, since there
        # are two profile blocks, two sub-band blocks and two data blocks in the
        # xml file.
        if ( len(profile_block) == len(subband_block) == len(datablock_block) == 2 ):
            
            # There are two sections in the XML file:
            #<Section name='FFT'>...</Section>
            #<Section name='FFT-pdmpd'>...</Section>
            #
            # The first section (index=0) contains the raw FFT data, the second (index=1)
            # contains data that has been period and DM searched using a separate tool.
            # Mike Keith should know more about this tool called "pdmpd". Here
            # data from both these sections is extracted to determine its length.
            
            # From <Section name='FFT'>...</Section>
            subband_points_fft = subband_block[0].childNodes[0].data
            datablock_points_fft = datablock_block[0].childNodes[0].data
            
            # From <Section name='FFT-pdmpd'>...</Section>
            profile_points_opt = profile_block[1].childNodes[0].data
            subband_points_opt = subband_block[1].childNodes[0].data
            datablock_points_opt = datablock_block[1].childNodes[0].data
            
            # Note sure if the checks here are valid, i.e. if there are 99 profile points is that bad?
            if ( len(profile_points_opt)>100) & (len(subband_points_opt)>1000) & (len(subband_points_fft)>1000) & (len(datablock_points_opt)>1000 ):
                
                subband_bins = int(subband_block[1].getAttribute("nBins"))
                subband_subbands = int(subband_block[1].getAttribute("nSub"))
                dmindex = list(xmldata.getElementsByTagName('DmIndex')[1].childNodes[0].data)
                
                # Stored here so call to len() made only once.
                lengthDMIndex = len(dmindex) # This is the DM index from the <Section name='FFT'>...</Section> part of the xml file.
                
                if (subband_bins == 128) & (subband_subbands == 16) & (lengthDMIndex > 100):
                    
                    # Now check for NaN values.
                    bestWidth = float(xmldata.getElementsByTagName('Width')[1].childNodes[0].data)
                    bestSNR = float(xmldata.getElementsByTagName('Snr')[1].childNodes[0].data)
                    bestDM = float(xmldata.getElementsByTagName('Dm')[1].childNodes[0].data)
                    bestBaryPeriod = float(xmldata.getElementsByTagName('BaryPeriod')[1].childNodes[0].data)
                    
                    if (bestWidth != "nan") & (bestSNR != "nan") & (bestDM != "nan") & (bestBaryPeriod != "nan"):
                        return True
                    else:
                        print "Phcx check 4 failed, NaN's present (KILLER NANS!): Line 99 of ProfileHelper.py"
                        
                        # Extra debugging info for anybody encoutnering errors.
                        if (bestWidth != "nan") :
                            print "\"Width\" value found in <Section name='FFT-pdmpd'>...</> of phcx file is NaN."
                        if (bestSNR != "nan") :
                            print "\"Snr\" value found in <Section name='FFT-pdmpd'>...</> of phcx file is NaN."
                        if (bestDM != "nan"):
                            print "\"Dm\" value found in <Section name='FFT-pdmpd'>...</> of phcx file is NaN."
                        if (bestBaryPeriod != "nan"):
                            print "\"BaryPeriod\" value found in <Section name='FFT-pdmpd'>...</> of phcx file is NaN."
                            
                        return False
                else:
                    print "Phcx check 3 failed, wrong number of bins, sub-bands etc: Line 92 of ProfileHelper.py"
                    
                    # Extra debugging info for anybody encoutnering errors.
                    if(subband_bins!=128):
                        print "Number of sub-band bins != 128 there are instead ",subband_bins, "."
                    if(subband_subbands!=16):
                        print "Number of sub-bands != 16 there are instead ",subband_subbands, "."
                    if(lengthDMIndex<100):
                        print "Number of DM indexes < 100 there are instead ",lengthDMIndex, "."
                        
                    return False
            else:
                print "Phcx check 2 failed, not enough profile points, sub-band points etc: Line 86 of ProfileHelper.py"
                print "Points in <Section name='FFT'>...</>"
                print "\tSub-band points: ",len(subband_points_fft)," Data block points: ", len(datablock_points_fft)
                print "Points in <Section name='FFT-pdmpd'>...</>"
                print "\tProfile points: ",len(profile_points_opt)," Sub-band points: ",len(subband_points_opt)," Data block points: ", len(datablock_points_opt)
                return False
        else:
            print "Phcx check 1 failed, profile, sub-band and datablocks of unequal size: Line 79 of ProfileHelper.py"
            return False
    
    # ******************************************************************************************
          
    def getprofile(self,xmldata,profileIndex):
        """
        Returns a list of 128 integer data points represeting a pulse profile.
        Takes two parameters: the xml data and the profile index to use. 
        The xml data contains two distinct profile sections (i.e. there are two <Profile>...</Profile> 
        sections in the file) which are indexed. The first section with profileIndex = 0 pretains to a
        profile obtained after the FFT, the second, profileIndex = 1, to a profile that has been period
        and DM searched using PDMPD.
        
        Parameters:
        xmldata    -    the xml data read in from the phcx file.
        profileIndex    -    index of the <Profile/> tag to read in the xml data.
        
        Returns:
        A list data type containing 128 integer data points.
        """
        # First obtain desired block of xml data.
        block = xmldata.getElementsByTagName('Profile')
        
        # Get raw hexadecimal data from the block
        points = block[profileIndex].childNodes[0].data
        
        # The format of the hexadecimal data is 02X, i.e. hexadecimal value with 2 digits.
        decimal_profile = []
        index = 0 # The index at which hexadecimal conversion will be performed.
        
        while index < len(points):
            if points[index] != "\n":
                try:
                    #ADDED
                    hex_value = points[index:index+2]
                    #print "Hex value:\t", hex_value
                    decimal_profile.append(int(hex_value,16)) # now the profile (shape, unscaled) is stored in dec_value
                    #print "Decimal value:\t",int(hex_value,16)
                    index = index+2 # Skip two characters to next hexadecimal number since format is 02X.
                except ValueError:
                    if points[index] =="\t":# There is a tab at the end of the xml data. So break the loop normally here.
                        break
                    else: # Unexpected error, report to user. 
                        print "Unexpected value error obtaining profile data: Lines 82-87 of ProfileHelper.py ."
                        break
            else:
                index = index+1
        return decimal_profile
    
    # ******************************************************************************************
          
    def fitSineAndSineSquaredCureveToProfile(self,profile):
        """
        Fits a sine and sine-squared curve to the candidate profile.
        
        Parameters:
        profile    -    a numpy.ndarray containing profile data.
        
        Returns:
        Parameters of the fittings.
        
        """
        profile_mean = profile.mean()
        profile_std = profile.std() # Note this is over n, not n-1.
        profile_max = profile.max()
        profile_min = profile.min()
        
        #print "mean:\t" , profile_mean
        #print "min:\t" , profile_min
        #print "max:\t" , profile_max
        #print "std:\t" , profile_std

        sumOverResiduals = 0
        
        # Calculate sum over residuals.
        for i in range( len(profile) ):
            sumOverResiduals += (abs( profile_max - profile_min ) / 2.)-profile[i]
        #print "Sum Over Residuals:\t",sumOverResiduals
        
        # Subtract background from profile. This is a type of feature scaling or
        # normalisation of the data. I'm not sure why the standard score isn't calculated
        # here, i.e. (x-mean) / standard deviation , but there must be a good reason. If
        # you would like to use the standard score, just uncomment/comment as needed.
        #p = (profile - profile_mean) / profile_std
        normalisedProfile = profile - profile_mean - profile_std
        normalisedProfileLength = len(normalisedProfile)
        
        for i in range( normalisedProfileLength ):
            if normalisedProfile[i] < 0:
                normalisedProfile[i] = 0
        
        #print "Profile after normalisation:\n",normalisedProfile
        
        # Find peaks in the normalised profile.
        # This code works by looking at small blocks of the normalised profile
        # for maximum values. The array indexes of the maximum values in the
        # normalised profile are then stored in 'peakIndexes'. The 'newProfile'
        # variable contains only those blocks from the normalised profile that
        # contain peaks. For example, a profile as follows:
        #
        #    index:      0   1   2    3   4   5   6   7   8   9   10  11   12  13  14  15
        # 
        #    profile = [ 0 , 0 , 5 , 10 , 5 , 0 , 0 , 0 , 0 , 0 , 0 , 15 , 0 , 0 , 0 , 0]
        #
        # would give:
        #
        #    peakIndexes = [3 , 11]
        #    newProfile  = [0 , 0 , 5 , 10 , 5 , 0 , 0 , 15 , 0 , 0 , 0 , 0]
        #                   |                        |   |                |
        #                   -------------------------    -----------------
        #                                |                        |
        #                                v                        v
        #                             Block 1                 Block 2
        #
        # Each block contains four zeros. So in this code a peak appears to be defined
        # as the maximum value occurring in a block of the normalised profile separted
        # by 4 bins with a normalised intensity of zero. Note that a block containing
        # intensities of only zero will be ignored. In this example the data in indexes
        # 7-10 was ignored.
        #
        # Note: I'm not sure why four zeroes was chosen, but I certainly won't change it!
        #       Changing it would give very different results.
    
        tempBinIndexes, tempBinValues, peakIndexes, newProfile = [],[],[],[] # 4 new array variables.
        zeroCounter = 0
        
        for i in range( normalisedProfileLength ):
            
            # If intensity at index i is not equal to zero, there is some signal.
            # This is not necessarily a peak.
            if normalisedProfile[i] != 0:
                tempBinValues.append(normalisedProfile[i])
                tempBinIndexes.append(i)
            
            # If four zeroes encountered, increment the counter.
            # This will cause the final else statement to be executed
            # if the next data item is another zero.    
            elif zeroCounter < 4:
                tempBinValues.append(normalisedProfile[i])
                tempBinIndexes.append(i)
                zeroCounter += 1
                
            else:              
                if max(tempBinValues) != 0:# If there is a peak...
                    peakIndexes.append(tempBinIndexes[argmax(tempBinValues)])
                    newProfile += list(tempBinValues)
                
                # Reset for next iteration.
                tempBinIndexes,tempBinValues = [],[]
                zeroCounter = 0
        
        # If there are leftover bins not processed in the loop above...
        if (tempBinValues != []):
            if (max(tempBinValues) != 0):# If there is a peak...
                peakIndexes.append(tempBinIndexes[argmax(tempBinValues)])
                newProfile += list(tempBinValues)# Add to the new profile.
        
        # The newProfile array will contain zero's at the start and end. This is
        # baecause on line 303 is 4 zeros haven't been seen, then they will be added
        # to the newProfile array. Just in case you wonder where the zeroes are coming
        # from.
        #print "New Profile:\n",newProfile         
        # Locate and count maxima.
        maxima = len(peakIndexes)
        
        #print "Maxima:\t" , maxima
        #print "Peak indexes:\t" , peakIndexes 
        
        # Calculate difference between maxima. This code simply subtracts
        # the peaks in the peakIndexes array at the indexes between 1 to n, from 
        # the peak values in the same array at indexes at 0 to (n-1).
        #
        # i.e. if peakIndexes = [ 1 , 2 , 3 , 4 , 5 , 6] , then:
        #    
        #    peakIndexs from 1 to n are   [ 2 , 3 , 4 , 5 , 6]
        #    peakIndexs from 0 to n-1 are [ 1 , 2 , 3 , 4 , 5]
        #
        #    So diff is given by,
        #    diff = [ (2-1) , (3-2) , (4-3) , (5-4) , (6-5) ]
        #         = [ 1 , 1 , 1 , 1 , 1 ]
        if maxima > 0:
            diff = delete(peakIndexes,0) - delete(peakIndexes,maxima-1)
        else:
            diff = []
        
        # Delete zeros in newProfile array. Does not delete all zero's however.
        # It leaves a single zero inbetween each block with data.
        finalProfile = []
        zeroCounter , i = 0,0
        while i < len(newProfile):
            if newProfile[i] != 0:
                finalProfile.append(newProfile[i])
                zeroCounter = 0
                
            elif zeroCounter < 1:
                finalProfile.append(newProfile[i])
                zeroCounter += 1
                
            i += 1 # Increment loop variable.
        
        #print "Final Profile for Sine fitting:\n",finalProfile
        
        # Perform fits to profile.
        chisq_profile_sine_fit = self.fit_sine(profile,ceil(1.5*maxima)) # Fit sine curve to raw profile.
        chisq_finalProfile_sine_sqr_fit = self.fit_sine_sqr(finalProfile,maxima) # Fit sine curve to amended profile.
        
        return chisq_profile_sine_fit , chisq_finalProfile_sine_sqr_fit , float(len (diff)) , sumOverResiduals
    
    # ******************************************************************************************
    
    def fit_sine(self,yData,maxima):
        """
        Fits a sine curve to data and returns the chi-squared value of the fit.
        
        Parameters:
        yData    -    a numpy.ndarray containing the data to fit the curve to (y-axis data).
        maxima  -    the number of maxima in the data.
        
        Returns:
        The chi-squared value of the fit.
        
        """
        
        # Obtain parameters for fitting.
        xData = array(range(len(yData)))
        amplitude = abs( max(yData) - min(yData) ) / 2.
        frequency = float( maxima / (len(yData) - 1.) )
        # The background terms decides where the middle of the sine curve will be,
        # i.e. smaller moves the curve down the y-axis, higher moves the curve up the
        # y-axis.
        background = abs( max(yData) - min(yData) ) / 2.
        
        # Calculates the residuals.
        def __residuals(paras, x, y,amp,bg): # Revision:2b
            # amp = the amplitude
            # f = the frequency
            # pi = Good old pi or 3.14159... mmmm pi.
            # phi = the phase.
            # bg = the mean of the data, center amplitude.
            # err = error.
            
            # Remembmer that here x and y are the data, such that,
            # x = bin number
            # y = intensity in bin number x
            f, phi = paras

            err = y - (abs(amp) * sin( 2 * pi * f * x + phi) + abs(bg))
            return err
        
        # Evaluates the function.
        def __evaluate(x, paras,amp,bg): # Revision:2b
            # Same variables as above.
            f, phi = paras
            return abs(amp) * sin( 2 * pi * f * x + phi) + abs(bg)
        
        if yData[0] == background:
            phi0 = 0
        elif yData[0] < background:
            phi0 = -1 / (4 * frequency)
        elif yData[0] > background:
            phi0 = +1 / (4 * frequency)
            
        # Perform sine fit.
        parameters = (frequency,phi0)
        # This call uses full-output=True flag so that we can compute the R2 and other stats.
        # This makes it easier to validate and debug the resulting fit in other tools like 
        # Matlab. The original code is left below, just uncomment and remove new code if necessary (1).
        #leastSquaresParameters = leastsq(__residuals, parameters, args=(xData,yData,amplitude),full_output=True)
        #fit = __evaluate(xData, leastSquaresParameters[0],amplitude)
        leastSquaresParameters,cov,infodict,mesg,ier = leastsq(__residuals, parameters, args=(xData,yData,amplitude,background),full_output=True) # Revision:2d
        fit = __evaluate(xData, leastSquaresParameters,amplitude,background) # Revision:2c
        
        # Chi-squared fit. Revision:6a
        chisq = 0
        for i in range(len(yData)):
            #if yData[i] >= 5.: # Not sure why this restriction is here. Removed for Revision:6a
            chisq += (yData[i]-fit[i])**2
        
        chisq /= len(yData)
        
        # Note leastSquaresParameters[0] contains the parameters of the fit obtained
        # by the least squares optimise call, [frequency,phi0,background].
        #print "Least squares parameters:\n", leastSquaresParameters[0]# This is used if the area below (1) above is uncommented.
        #print "Least squares parameters Full:\n", leastSquaresParameters
        #print "Chi Squared:\n", chisq*pow(float(maxima),4)/100000000.  
        #print "fit:\n", fit
        
        # Revision:2e
        # This section should be commented out when testing is completed.
        ssErr = (infodict['fvec']**2).sum() # 'fvec' is an array of residuals. 
        ssTot = ((yData-yData.mean())**2).sum()
        rsquared = 1-(ssErr/ssTot )
        
        print "\n\tSine fit to Pulse profile statistics:"
        print "\tStandard Error: ", ssErr
        print "\tTotal Error: ", ssTot
        print "\tR-Squared: ", rsquared
        print "\tAmplitude: ",amplitude
        print "\tFrequency: ",str(leastSquaresParameters[0])
        print "\tPhi: ",str(leastSquaresParameters[1])
        print "\tBackground: ",background
        plt.plot(xData,yData,'o', xData, __evaluate(xData, leastSquaresParameters,amplitude,background))
        plt.title("Sine fit to Profile")
        plt.show()
        
        #return leastSquaresParameters[0], chisq*pow(maxima,4)/100000000., fit, xData, yData
        # I've commented out the return statement above, as only the chi-squared value is used.
        # By not returning the extra items, the memory they use will be freed up when this
        # function terminates, reducing memory overhead.
        #return chisq*pow(float(maxima),4)/100000000.
        return chisq
    
    # ******************************************************************************************
    
    #Revision:2f
    def fit_sine_original(self,yData,maxima):
        """
        Fits a sine curve to data and returns the chi-squared value of the fit.
        
        Parameters:
        yData    -    a numpy.ndarray containing the data to fit the curve to (y-axis data).
        maxima  -    the number of maxima in the data.
        
        Returns:
        The chi-squared value of the fit.
        
        """
        
        # Calculates the residuals.
        def __residuals(paras, x, y):
            # a = the amplitude
            # f = the frequency
            # pi = Good old pi or 3.14159... mmmm pi.
            # phi = the phase.
            # bg = the mean of the data, center amplitude.
            # err = error.
            
            # Remembmer that here x and y are the data, such that,
            # x = bin number
            # y = intensity in bin number x
            a, f, phi, bg = paras

            err = y - (abs(a) * sin( 2 * pi * f * x + phi) + abs(bg))
            return err
        
        # Evaluates the function.
        def __evaluate(x, paras):
            # Same variables as above.
            a, f, phi, bg = paras
            return abs(a) * sin( 2 * pi * f * x + phi) + abs(bg)
        
        # Obtain parameters for fitting.
        xData = array(range(len(yData)))
        amplitude = abs( max(yData) - min(yData) ) / 2.
        frequency = float( maxima / (len(yData) - 1.) )
        background = mean(yData)
        
        if yData[0] == background:
            phi0 = 0
        elif yData[0] < background:
            phi0 = -1 / (4 * frequency)
        elif yData[0] > background:
            phi0 = +1 / (4 * frequency)
            
        # Perform sine fit.
        parameters = (amplitude,frequency,phi0,background)
        leastSquaresParameters,cov,infodict,mesg,ier = leastsq(__residuals, parameters, args=(xData,yData),full_output=True)
        fit = __evaluate(xData, leastSquaresParameters)
        
        # Chi-squared fit.
        chisq = 0
        for i in range(len(yData)):
            if yData[i] >= 5.: # Not sure why this restriction is here.
                chisq += (yData[i]-fit[i])**2
        
        #print "Least squares parameters:\n", leastSquaresParameters[0]
        #print "Chi Squared:\n", chisq*pow(float(maxima),4)/100000000.  
        #print "fit:\n", fit
        
        ssErr = (infodict['fvec']**2).sum() # 'fvec' is an array of residuals. 
        ssTot = ((yData-yData.mean())**2).sum()
        rsquared = 1-(ssErr/ssTot )
        
        print "\n\tSine fit to Pulse profile statistics:"
        print "\tStandard Error: ", ssErr
        print "\tTotal Error: ", ssTot
        print "\tR-Squared: ", rsquared
        print "\tAmplitude: ",amplitude
        print "\tFrequency: ",str(leastSquaresParameters[0])
        print "\tPhi: ",str(leastSquaresParameters[1])
        print "\tBackground: ",str(leastSquaresParameters[2])
        plt.plot(xData,yData,'o', xData, __evaluate(xData, leastSquaresParameters))
        plt.title("Sine fit to Profile")
        plt.show()
        
        #return leastSquaresParameters[0], chisq*pow(maxima,4)/100000000., fit, xData, yData
        # I've commented out return statement above, as only the chi-squared value is used.
        # By not returning the extra items, the memory they use will be freed up when this
        # function terminates, reducing memory overhead.
        return chisq*pow(float(maxima),4)/100000000.
    
    # ******************************************************************************************
    
    def fit_sine_sqr(self,yData,maxima):
        """
        Fits a sine-squared curve to data and returns the chi-squared value of the fit.
        
        Parameters:
        yData    -    a numpy.ndarray containing the data to fit the curve to (y-axis data).
        maxima  -    the number of maxima in the data.
        
        Returns:
        The chi-squared value of the fit.
        
        """
        
        # Calculates the residuals.
        def __residuals(paras, x, y,amp,bg): # Revision:3a
            # a = the amplitude
            # f = the frequency
            # pi = Good old pi or 3.14159... mmmm pi.
            # phi = the phase.
            # err = error.
            # bg = background term
            
            # Remembmer that here x and y are the data, such that,
            # x = bin number.
            # y = intensity in bin number x.
            f, phi = paras

            err = y - (abs(amp) * pow ( sin ( 2 * pi * f * x + phi),2)) + abs(bg)
            return err
        
        # Evaluates the function.
        def __evaluate(x, paras,amp,bg): # Revision:3a
            # Same variables as above.
            f, phi = paras
            return abs(amp) * pow ( sin ( 2 * pi * f * x + phi),2) + abs(bg)
        
        # Obtain parameters for fitting.
        xData = array(range(len(yData)))
        #amplitude = max(yData)
        amplitude = abs( max(yData) - min(yData) ) / 2. # Revision:3b
        frequency = float( maxima / (len(yData) - 1.) / 2. )
        background = abs( max(yData) - min(yData) ) / 2. # Revision:7
        
        if yData[0] == 0:
            phi0 = 0
        else:
            phi0 = -1 / (4 * frequency)
            
        # Perform sine fit.
        parameters = (frequency,phi0)
        # Revision:3c
        leastSquaresParameters,cov,infodict,mesg,ier = leastsq(__residuals, parameters, args=(xData,yData,amplitude,background),full_output=True)
        fit = __evaluate(xData, leastSquaresParameters,amplitude,background)
        
        # Chi-squared fit. Revision:6b
        chisq = 0
        for i in range(len(yData)):
            #if yData[i] >= 5.: # Not sure why this restriction is here. Removed for Revision:6b
            chisq += (yData[i]-fit[i])**2
        
        chisq /= len(yData)
        
        #print "Least squares parameters:\n", leastSquaresParameters[0]
        #print "Chi Squared:\n", chisq / pow(float(maxima),4)  
        #print "fit:\n", fit
        
        # Revision:3d
        ssErr = (infodict['fvec']**2).sum() # 'fvec' is an array of residuals. 
        ssTot = ((yData-mean(yData))**2).sum()
        rsquared = 1-(ssErr/ssTot )
        
        print "\n\tSine Squared fit to Pulse profile statistics:"
        print "\tStandard Error: ", ssErr
        print "\tTotal Error: ", ssTot
        print "\tR-Squared: ", rsquared
        print "\tAmplitude: ",amplitude
        print "\tFrequency: ",str(leastSquaresParameters[0])
        print "\tPhi: ",str(leastSquaresParameters[1])
        plt.plot(xData,yData,'o', xData, __evaluate(xData, leastSquaresParameters,amplitude,background))
        plt.title("Sine Squared fit to Profile")
        plt.show()
        
        #return leastSquaresParameters[0], chisq / pow(float(maxima),4), fit, xData, yData
        # I've commented out return statement above, as only the chi-squared value is used.
        # By not returning the extra items, the memory they use will be freed up when this
        # function terminates, reducing memory overhead.
        return chisq
    
    # ******************************************************************************************
    
    # Revision:3e
    def fit_sine_sqr_original(self,yData,maxima):
        """
        Fits a sine-squared curve to data and returns the chi-squared value of the fit.
        
        Parameters:
        yData    -    a numpy.ndarray containing the data to fit the curve to (y-axis data).
        maxima  -    the number of maxima in the data.
        
        Returns:
        The chi-squared value of the fit.
        
        """
        
        # Calculates the residuals.
        def __residuals(paras, x, y):
            # a = the amplitude
            # f = the frequency
            # pi = Good old pi or 3.14159... mmmm pi.
            # phi = the phase.
            # err = error.
            
            # Remembmer that here x and y are the data, such that,
            # x = bin number.
            # y = intensity in bin number x.
            a, f, phi = paras

            err = y - (abs(a) * pow ( sin ( 2 * pi * f * x + phi),2))
            return err
        
        # Evaluates the function.
        def __evaluate(x, paras):
            # Same variables as above.
            a, f, phi = paras
            return abs(a) * pow ( sin ( 2 * pi * f * x + phi),2)
        
        # Obtain parameters for fitting.
        xData = array(range(len(yData)))
        amplitude = max(yData)
        frequency = float( maxima / (len(yData) - 1.) / 2. )
        
        if yData[0] == 0:
            phi0 = 0
        else:
            phi0 = -1 / (4 * frequency)
            
        # Perform sine fit.
        parameters = (amplitude,frequency,phi0)
        leastSquaresParameters,cov,infodict,mesg,ier = leastsq(__residuals, parameters, args=(xData,yData),full_output=True)
        fit = __evaluate(xData, leastSquaresParameters)
        
        # Chi-squared fit.
        chisq = 0
        for i in range(len(yData)):
            if yData[i] >= 5.: # Not sure why this restriction is here.
                chisq += (yData[i]-fit[i])**2
        
        #print "Least squares parameters:\n", leastSquaresParameters[0]
        #print "Chi Squared:\n", chisq / pow(float(maxima),4)  
        #print "fit:\n", fit
        
        ssErr = (infodict['fvec']**2).sum() # 'fvec' is an array of residuals. 
        ssTot = ((yData-mean(yData))**2).sum()
        rsquared = 1-(ssErr/ssTot )
        
        print "\n\tSine Squared fit to Pulse profile statistics:"
        print "\tStandard Error: ", ssErr
        print "\tTotal Error: ", ssTot
        print "\tR-Squared: ", rsquared
        print "\tAmplitude: ",str(leastSquaresParameters[0])
        print "\tFrequency: ",str(leastSquaresParameters[1])
        print "\tPhi: ",str(leastSquaresParameters[2])
        plt.plot(xData,yData,'o', xData, __evaluate(xData, leastSquaresParameters))
        plt.title("Sine Squared fit to Profile")
        plt.show()
        
        #return leastSquaresParameters[0], chisq / pow(float(maxima),4), fit, xData, yData
        # I've commented out return statement above, as only the chi-squared value is used.
        # By not returning the extra items, the memory they use will be freed up when this
        # function terminates, reducing memory overhead.
        return chisq / pow(float(maxima),4)
    
    # ******************************************************************************************
    
    def derivative_y(self,yData):
        """
        Obtains the derivative for the y data points by simply perfroming,
        dy = y[i] - y[i+1] .
        
        Parameters:
        yData    -    a numpy.ndarray containing data (y-axis data).
        
        Returns:
        The changes in y, dy, for each point in yData as an array.
        
        """
        dy = []
        dataPoints = len(yData)-1 # Since there are n data points, with only n-1 line segments joining them.
        for i in range(dataPoints):
            dy.append(yData[i] - yData[i+1])
        return dy
    
    # ******************************************************************************************
    
    def fit_gaussian(self,xData,yData):
        """
        Fits a Gaussian to the supplied data. This should be histrogram data,
        that is the details of the bins (xData) and the frequencies (yData).
        
        Parameters:
        xData    -    a numpy.ndarray containing data (x-axis data).
        yData    -    a numpy.ndarray containing data (y-axis data).
        
        Returns:
        The parameters of the fit, one array and three other variables.
        
            leastSquaresParameters - array containing optimum three values for:
                                        * sigma
                                        * expect
                                        * maximum
            fwhm - the full width half maximum of the Gaussian.
            chisq - the chi-squared value of the fit.
            fit - the fit.
        
        """
        
        #print "xData (LENGTH=",len(xData),"):\n", xData
        #print "yData (LENGTH=",len(yData),"):\n", yData
        
        # Calculates the residuals.
        def __residuals(paras, x, y):
            # sigma = the standard deviation.
            # mu = the mean aka the expectation of the distribution.
            # maximum = .
            
            # Remembmer that here x and y are the data, such that,
            # x = bin number.
            # y = intensity in bin number x.
            sigma, mu, maximum = paras
            err = y - ( abs(maximum) * exp( (-((x - mu) / sigma )**2) / 2))
            return err
        
        # Evaluates the function.
        def __evaluate(x, paras):
            # Same variables as above.
            sigma, mu, maximum = paras
            return ( abs(maximum) * exp( (-((x - mu) / sigma )**2) / 2))
        
        # Reverses the order of the list entries.
        def __mirror(_list):
            reversedList = []
            listLength = len(_list)
            for i in range( listLength ):
                reversedList.append(_list[abs(i - listLength + 1)])
            return reversedList
        
        if xData == []:
            xData = range(len(yData))
        
        # Set up variables required to perfrom fit.
        _exit,counter = 0,0
        indexOfLargestValue_xAxis = argmax(yData) # First index of largest value along x-axis (highest frequency).
        expect = xData[indexOfLargestValue_xAxis]
        sigma = std(yData)
        maximum = max(yData)
        meansq = mean(yData)**2
        temp = yData
        
        #print "Index of largest value on x-axis:\t", indexOfLargestValue_xAxis
        #print "expect:\t", expect
        #print "sigma:\t", sigma
        #print "maximum:\t", maximum
        #print "meansq:\t", meansq
        
        # We are chopping off some the x-axis data here. This is because this function
        # is running on histogram data, i.e bin positions and frequncies. The xData array
        # holds details of the bins, yData the frequencies. So if the length of xData is n,
        # than the length of yData must be n-1.
        #     
        # For example, if the yData has been split across 6 bins then if we had:
        #
        #    xData = [ 0 , 10 , 20 , 30 , 40 , 50 ] # Bins
        #    yData = [ 1 , 2 , 5 , 1 , 0] # Frequencies
        # 
        # So here the last data point in xData is removed.
        if len(xData) == len(yData)+1:
            xData = xData[0:-1] # Chopping off last data point.
        xDatalength = len(xData)
        
        # Here check if maximum frequency is on the border. If it is then the data
        # is reversed. This code appears to throw data away, since when reversing
        # the data, the part not being reversed is discarded. Is this acceptable?
        #
        # For example, if we have data as follows:
        #
        #     yData = [10 , 0 , 3 , 1 , 1 , 1 , 0 , 1 , 3 , 0]
        #
        # then,
        #    
        #    indexOfLargestValue_xAxis = 0
        #
        # So,
        #
        #    cut = ceil( len(yData) / 2) = 5
        #    part1 = [10 , 0 , 3 , 1 , 1]
        #    part2 = [1 , 1 , 3 , 0 , 10]
        #
        # then yData is set to:
        #
        #    yData = part2+ part1 = [1 , 1 , 3 , 0 , 10 , 10 , 0 , 3 , 1 , 1]
        #
        # Obviously this isn't the data we started with, so is this a bug?
        #
        # BUG: If there are two bins with an equal MAXIMUM frequency, one of those *Could* be discarded here.
        #      This is because the indexOfLargestValue_xAxis variable from above, is obtained from
        #      the first bin with the maximum frequency, but there could be multiple bins with the
        #      maximum frequency in the histogram. So if,
        #      
        #      a) there is a max value in the first or last bin which we label b;
        #      b) 1 or more bins have the same frequency as bin b;
        #      c) those 1 or more bins which share the same frequency as b are not in the same half of the data;
        #
        #      Then the other bins with the the shared max frequency will be discarded.
        part1,part2 = [],[]
        nearBorder = False
        if (indexOfLargestValue_xAxis == 0):# If the max value is at the begining.
            cut = ceil(len(yData)/2)        # Find midpoint.
            part1 = yData[:cut]             # Isolate first half of data.
            part2 = __mirror(part1)         # Reverse the order of the first half of data.
            yData = list(part2)+list(part1) # Data equals reversed data + first half of data.
            nearBorder = True
            
        elif (indexOfLargestValue_xAxis == xDatalength-1):# If the max value is at the end.
            cut = ceil(len(yData)/2)                     # Find midpoint.
            part1 = yData[cut:]                          # Isolate second half of data.
            part2 = __mirror(part1)                      # Reverse the order of the second half of data.
            yData = list(part1)+list(part2)              # Data equals second half of data + reversed data.
            nearBorder = True
        
        # Perform the gaussian fit.       
        while _exit == 0:
            
            parameters = [sigma, expect, maximum]
            leastSquaresParameters = leastsq(__residuals, parameters, args=(xData,yData))
            
            if nearBorder == True:
                leastSquaresParameters[0][1] -= xData[cut]
                yData = temp
                
            fwhm = abs(2 * sqrt(2 * log(2)) * leastSquaresParameters[0][0])
            fit = __evaluate(xData, leastSquaresParameters[0])
            
            # Compute Chi-squared value for fit.
            chisq = 0
            for i in range(xDatalength):
                chisq += (yData[i] - fit[i])**2
            
            chisq /= len(yData)# Revision:6c
            
            if (chisq > meansq * xDatalength) & (leastSquaresParameters[0][0] < 0.2 * xDatalength) & (nearBorder==False):
                
                counter += 1
                temp = delete(temp,indexOfLargestValue_xAxis)
                pos = argmax(temp)
                expect = xData[pos+counter]
                if counter > 5:
                    _exit += 1
            else:
                _exit += 1
        
        # I've commented this return statement out to avoid returning all the data (xData and yData),
        # since this data was passed in to the method in the first place.               
        #return leastSquaresParameters[0], fwhm, chisq, fit, xData, yData
        return leastSquaresParameters[0], fwhm, chisq, fit
    
    # ******************************************************************************************
    
    def fit_gaussian_t1(self,yData):
        """
        Fits a Gaussian to the supplied data.
        
        Parameters:
        yData    -    a numpy.ndarray containing data (y-axis data).
        
        Returns:
        
        An object containing:
        
        the parameters of the fit, one array and three other variables.
        
            leastSquaresParameters - array containing optimum three values for:
                                        * sigma
                                        * expect
                                        * maximum
            fwhm - the full width half maximum of the Gaussian.
            chisq - the chi-squared value of the fit.
            fit - the fit.
            params - the parmeters of the fit.
        
        """
        
        xData,part1,part2 = [],[],[]
        yDatalength = len(yData)
        xData = range(yDatalength)
        xmax = argmax(yData) # Finds index of max value in yData.
        
        # Check if maximum is near borders of the interval.
        nearBorder = False
        if (xmax < 15) or (xmax >= 112):   # If index of max value is near begining or end.
            cut = int(ceil(yDatalength/2)) # Obtain midpoint.
            part1 = yData[:cut]            # Part 1 contains 1st half of data.
            part2 = yData[cut:]            # Part 2 contains 2nd half of data.
            yData = list(part2)+list(part1)# Swap the parts around. This is done differently in the function fit_gaussian(self,xData,yData) in this file.
            nearBorder = True
            
        # Perform gaussian fit.
        result = self.fit_gaussian_with_bg(xData,yData)
        
        if nearBorder == True:
            result[0][1] = result[0][1]+cut
        
        
        return result
    
    # ******************************************************************************************
    
    def fit_gaussian_with_bg(self,xData,yData):
        """
        Fits a Gaussian to the supplied data.
        
        Parameters:
        yData    -    a numpy.ndarray containing data (y-axis data).
        
        Returns:
        The parameters of the fit, one array and three other variables.
        
            leastSquaresParameters - array containing optimum three values for:
                                        * sigma
                                        * expect
                                        * maximum
            fwhm - the full width half maximum of the Gaussian.
            chisq - the chi-squared value of the fit.
            fit - the fit.
            params - the parmeters of the fit.
        
        """
        
        #print "xData (LENGTH=",len(xData),"):\n", xData
        #print "yData (LENGTH=",len(yData),"):\n", yData
        
        # Calculates the residuals.
        def __residuals(paras, x, y):
            # sigma = the standard deviation.
            # mu = the mean aka the expectation of the distribution.
            # maximum = .
            # bg = background term.
            
            sigma, mu, maximum, bg = paras
            err = y - ( abs(maximum) * exp( (-((x - mu) / abs(sigma) )**2) / 2) + (bg) ) # Revision:8a
            return err
        
        # Evaluates the function.
        def __evaluate(x, paras):
            # Same variables as above.
            sigma, mu, maximum, bg = paras
            return ( abs(maximum) * exp( (-((x - mu) / abs(sigma) )**2) / 2) + (bg) ) # Revision:8b
        
        ###### perform gaussian fit ######
        if xData == []:
            xData = range(len(yData))
        
        expect = argmax(yData)
        maximum = yData[expect]
        sigma = std(yData)
        bg = 1. 
        #mean(ydata) # Not sure why this was commented out?
        
        parameters = [sigma, expect, maximum, bg]
        leastSquaresParameters = leastsq(__residuals, parameters, args=(xData,yData))
        fwhm = abs(2 * sqrt(2 * log(2)) * leastSquaresParameters[0][0])
        fit = __evaluate(xData, leastSquaresParameters[0])
        
        print "\tSigma chosen: ",leastSquaresParameters[0][0]
        print "\tMu chosen: ",leastSquaresParameters[0][1]
        print "\tMaximum chosen: ",leastSquaresParameters[0][2]
        print "\tbg chosen: ",leastSquaresParameters[0][3]
        
        chisq = 0
        for i in range(len(yData)):
            chisq += (yData[i]-fit[i])**2
        
        # I've commented this return statement out to avoid returning all the data.
        # Since all this data was passed in to the method in the first place.               
        #return leastSquaresParameters[0], fwhm, chisq/len(yData), fit, xData, yData, parameters    
        return leastSquaresParameters[0], fwhm, chisq/len(yData), fit, parameters

    
    # ******************************************************************************************
    
    def fit_gaussian_fixed(self,xData,yData):
        """
        Fits a Gaussian to the supplied data under the constraint
        that the expectation value is fixed.
        
        Parameters:
        xData    -    a numpy.ndarray containing data (x-axis data).
        yData    -    a numpy.ndarray containing data (y-axis data).
        
        Returns:
        The parameters of the fit, one array and four other variables.
        
            leastSquaresParameters - array containing optimum three values for:
                                        * sigma
                                        * expect
                                        * maximum
            fwhm - the full width half maximum of the Gaussian.
            chisq - the chi-squared value of the fit.
            fit - the fit.
            xmax - the max expectation value.
        
        """
        
        #print "xData (LENGTH=",len(xData),"):\n", xData
        #print "yData (LENGTH=",len(yData),"):\n", yData
        
        # Calculates the residuals.
        def __residuals(paras, x, y, xmax):
            # sigma = the standard deviation.
            
            # Remembmer that here x and y are the data, such that,
            # x = bin number.
            # y = intensity in bin number x.
            sigma, maximum = paras
            err = y - ( abs(maximum) * exp( (-((x - xmax) / sigma )**2) / 2))
            return err
        
        # Evaluates the function.
        def __evaluate(x, paras, xmax):
            # Same variables as above.
            sigma, maximum = paras
            return ( abs(maximum) * exp( (-((x - xmax) / sigma )**2) / 2))
        
        if xData == []:
            xData = range(len(yData))
        if len(xData) == len(yData)+1:
            xData = xData[0:-1]
        
        # Set up variables required to perfrom fit.
        sigma = std(yData)
        maximum = max(yData)
        
        # But where does this arbitrary 29 come from? Is this just choosing a
        # random starting point for the least squares optimisation?
        # Actually I beleive that since the histograms generated in test_22.py
        # use 60 bins, this is simply setting xmax to the value of the centre
        # bin as the fixed maximum value.
        xmax = xData[29] 
        
        """
                       ______              ______
                       /___   \___\ || /___/   ___\
                      //\]/\ ___  \\||//  ___ /\[/\\
                      \\/[\//  _)   \/   (_  \\/]\//
                       \___/ _/   o    o   \_ \___/
                           _/                \_
                          //'VvvvvvvvvvvvvvvV'\\
                         ( \.'^^^^^^^^^^^^^^'./ )
                          \____   . .. .   ____/
               ________        \ . .''. . /        ________
              /______  \________)________(________/ _______\
             /|       \ \                        / /       |\
            (\|____   / /                        \ \   ____|/)
            (\_____>- \/                          \/ -<_____/)
            (\_____>-  |                          |  -<_____/)
            (\_____>-                                -<_____/)
             \_____>-                                -<_____/
              |                                            |
              |        Bob the coding alien says...        |
              |                                            |
              |        Please explain the meaning of       |
              |        literal variables, otherwise        |
              |        I become confused, and a little     |
              |        sad inside.                         |
              |                                            |
              |____________________________________________|
                       /       )          (       \
                      /       /            \       \
                     / / / /\ \            / /\ \ \ \
                    ( ( ( ( (  )          (  ) ) ) ) )
                    'v'v'v'v'(_)          (_)'v'v'v'v'
                              \)          (/
        """
        
        #print "sigma:\t", sigma
        #print "maximum:\t", maximum
        #print "xmax:\t", xmax
        
        # perform fit ######
        parameters = [sigma, maximum]
        leastSquaresParameters = leastsq(__residuals, parameters, args=(xData,yData,xmax))
        fwhm = abs(2 * sqrt( 2 * log(2) ) * leastSquaresParameters[0][0])
        fit = __evaluate(xData, leastSquaresParameters[0],xmax)
        
        # Chi-squared fit. Revision:6d
        chisq = 0
        for i in range(len(yData)):
            #if yData[i] >= 1.: # Not sure why this restriction is here. Removed for Revision:6d
            chisq += (yData[i]-fit[i])**2
        
        chisq /= len(yData)
        
        return leastSquaresParameters[0], fwhm, chisq, fit, xmax
    
    # ******************************************************************************************
    
    # Revision:5
    def fit_gaussian_fixed_with_bins(self,xData,yData,bins):
        """
        Fits a Gaussian to the supplied data under the constraint
        that the expectation value is fixed.
        
        Parameters:
        xData    -    a numpy.ndarray containing data (x-axis data).
        yData    -    a numpy.ndarray containing data (y-axis data).
        bins     -    the number of bins in the profile histogram.
        
        Returns:
        The parameters of the fit, one array and four other variables.
        
            leastSquaresParameters - array containing optimum three values for:
                                        * sigma
                                        * expect
                                        * maximum
            fwhm - the full width half maximum of the Gaussian.
            chisq - the chi-squared value of the fit.
            fit - the fit.
            xmax - the max expectation value.
        
        """
        
        #print "xData (LENGTH=",len(xData),"):\n", xData
        #print "yData (LENGTH=",len(yData),"):\n", yData
        
        # Calculates the residuals.
        def __residuals(paras, x, y, xmax):
            # sigma = the standard deviation.
            
            # Remembmer that here x and y are the data, such that,
            # x = bin number.
            # y = intensity in bin number x.
            sigma, maximum = paras
            err = y - ( abs(maximum) * exp( (-((x - xmax) / sigma )**2) / 2))
            return err
        
        # Evaluates the function.
        def __evaluate(x, paras, xmax):
            # Same variables as above.
            sigma, maximum = paras
            return ( abs(maximum) * exp( (-((x - xmax) / sigma )**2) / 2))
        
        if xData == []:
            xData = range(len(yData))
        if len(xData) == len(yData)+1:
            xData = xData[0:-1]
        
        # Set up variables required to perfrom fit.
        sigma = std(yData)
        maximum = max(yData)
        
        print "\tWhat we get: ", str(int(bins/2)-1)
        xmax = xData[int(bins/2)-1] # Made change here to ensure we start with centre bin.
        
        print "\tsigma: ", sigma
        print "\tmaximum: ", maximum
        print "\txmax: ", xmax
        
        # perform fit ######
        parameters = [sigma, maximum]
        leastSquaresParameters = leastsq(__residuals, parameters, args=(xData,yData,xmax))
        fwhm = abs(2 * sqrt( 2 * log(2) ) * leastSquaresParameters[0][0])
        fit = __evaluate(xData, leastSquaresParameters[0],xmax)
        
        # Chi-squared fit. Revision:5
        chisq = 0
        for i in range(len(yData)):
            #if yData[i] >= 1.: # Not sure why this restriction is here. Removed for Revision:5
            chisq += (yData[i]-fit[i])**2
        
        chisq /= len(yData)
        
        return leastSquaresParameters[0], fwhm, chisq, fit, xmax
    
    # ******************************************************************************************
    
    def fit_doubleGaussian_t2(self,yData):
        """
        Fits a double Gaussian to the supplied data.
        
        Parameters:
        yData    -    a numpy.ndarray containing data (y-axis data).
        
        Returns:
        
        An object containing the parameters of the fit.
        
        """
        
        part1,part2 = [],[]
        yDatalength = len(yData)
        xmax = argmax(yData) # Finds index of max value in yData.
        
        # Check if maximum is near borders of the interval.
        nearBorder = False
        if (xmax < 15) or (xmax >= 112):   # If index of max value is near begining or end.
            cut = int(ceil(yDatalength/2)) # Obtain midpoint.
            part1 = yData[:cut]            # Part 1 contains 1st half of data.
            part2 = yData[cut:]            # Part 2 contains 2nd half of data.
            yData = list(part2)+list(part1)# Swap the parts around. This is done differently in the function fit_gaussian(self,xData,yData) in this file.
            nearBorder = True
            
        # Perform gaussian fit.
        result = self.fit_doubleGaussian(yData)
        
        if nearBorder == True:
            result[0][1] = result[0][1]+cut
            result[0][5] = result[0][5]+cut
        
        
        return result
    
    # ******************************************************************************************
    
    def fit_doubleGaussian(self,yData):
        """
        Fits a double Gaussian to the supplied data.
        
        Parameters:
        yData    -    a numpy.ndarray containing data (y-axis data).
        
        Returns:
        
        An object containing the parameters of the fit.
        
        """
        
        # I think this code needs another cleaning pass, as its still hard
        # to understand in places.
        
        #print "xData (LENGTH=",len(xData),"):\n", xData
        #print "yData (LENGTH=",len(yData),"):\n", yData
        
        # Calculates the residuals.
        def __residuals(paras, x, y):
            # sigma = the standard deviation.
            # mu = the mean aka the expectation of the distribution.
            # maximum = .
            # bg = background term.
            
            sigma, mu, maximum, bg = paras
            err = y - ( abs(maximum) * exp( (-((x - mu) / sigma )**2) / 2) + abs(bg) )
            return err
        
        # Evaluates the function.
        def __evaluate(x, paras):
            # Same variables as above.
            sigma, mu, maximum, bg = paras
            return ( abs(maximum) * exp( (-((x - mu) / sigma )**2) / 2) + abs(bg) )
        
        xData = range(len(yData))
        pos = argmax(yData) # indexOfLargestValue_xAxis
        newx,newy = xData,yData
        tolerance,limit = 0,5
        
        # Delete first peak.
        newx = delete(newx,pos)
        newy = delete(newy,pos)
        
        # I haven't understood how this part of the code works yet!
        # Once I've debugged it I will try and explain...
        i = 1   
        while i < len(yData):
            if ((pos-i) > 0) & ((pos+i) < len(yData)):
                if (yData[pos-i] < yData[pos-i+1]) & (yData[pos+i] < yData[pos+i-1]):
                    newx = delete(newx,pos-i)
                    newx = delete(newx,pos-i)
                    newy = delete(newy,pos-i)
                    newy = delete(newy,pos-i)
                elif (yData[pos-i]  >= yData[pos-i+1]) or (yData[pos+i] >= yData[pos+i-1]) & (tolerance < limit):
                    newx = delete(newx,pos-i)
                    newx = delete(newx,pos-i)
                    newy = delete(newy,pos-i)
                    newy = delete(newy,pos-i)
                    tolerance += 1
                else:
                    break
            elif ((pos-i) < 0):
                if (yData[pos+i] < yData[pos+i-1]):
                    newx = delete(newx,pos-i+1)
                    newy = delete(newy,pos-i+1)
                elif (yData[pos+i] >= yData[pos+i-1]) & (tolerance < limit):
                    newx = delete(newx,pos-i+1)
                    newy = delete(newy,pos-i+1)
                    tolerance += 1
                else:
                    break
            
            elif ((pos+i) > len(yData)):
                if (yData[pos-i] < yData[pos-i+1]):
                    newx = delete(newx,pos-i+1)
                    newy = delete(newy,pos-i+1)
                elif (yData[pos-i]  >= yData[pos-i+1]) & (tolerance < limit):
                    newx = delete(newx,pos-i)
                    newy = delete(newy,pos-i)
                    tolerance += 1
                else:
                    break
                
            i += 1 # Increment counter.
        
        counter = 0
        debugCounter=0
        while counter < 8:
            # New gaussian fit.
            debugCounter+=1
            npos = argmax(newy)
            nexpect = newx[npos]
            nsigma = std(newy)
            nmaximum = max(newy)
            nbg = mean(newy)
            
            np0 = [nsigma, nexpect, nmaximum, nbg]
            plsq = leastsq(__residuals, np0, args=(newx,newy))
            nfwhm = abs(2 * sqrt(2*log(2)) * plsq[0][0])
            nfit = __evaluate(newx, plsq[0])
            
            nchisq = 0
            for i in range(len(newy)):
                nchisq += (newy[i]-nfit[i])**2/len(newy)
                #print "Entered My for loop: ", debugCounter+i
                
            # Substraction to data.
            newy = []
            for i in range(len(yData)):
                evaly = __evaluate(xData[i],plsq[0])
                if (evaly <= yData[i]):
                    newy.append(yData[i]-evaly+plsq[0][3])
                elif (evaly > yData[i]) & (xData[i] > (plsq[0][2]-(1.5*nfwhm)/2)) & (xData[i] < (plsq[0][2]+(1.5*nfwhm)/2)):
                    newy.append(plsq[0][3])
                else:
                    newy.append(yData[i])
                    
                newx = range(len(newy))
                    
            counter += 1
            if counter == 7:
                store_p2 = plsq[0]
            elif counter == 8:
                store_p1 = plsq[0]
        
        # Perform final gaussian fit.
        p = list(store_p1) + list(store_p2)
        
        # Data arriving here not the same.
        finalfit = self.fit_gaussian_double_with_bg(yData,array(p))
        
        fit1 = __evaluate(xData,store_p1)
        fit2 = __evaluate(xData,store_p2)
        combifit = fit1+fit2-store_p1[3]-store_p2[3]+(store_p1[3]+store_p2[3])/2
        combi_chisq = 0
        
        for i in range(len(yData)):
            if combifit[i] >= 1.:
                combi_chisq += (yData[i]-combifit[i])**2/len(yData)
                
        combi_fwhm1 = abs(2 * sqrt(2*log(2)) * p[0])
        combi_fwhm2 = abs(2 * sqrt(2*log(2)) * p[4])
        
        if (finalfit[2] <= combi_chisq):
            return finalfit
        else:
            return [p,combi_fwhm2,combi_chisq,combifit,xData,yData,combi_fwhm1]
    
    # ******************************************************************************************
    
    def fit_gaussian_double_with_bg(self,yData,p0):
        """
        Fits a double Gaussian to the supplied data with background.
        
        Parameters:
        yData    -    a numpy.ndarray containing data (y-axis data).
        
        Returns:
        
        An array object containing the parameters of the fit = [sigma1, mu1, maximum1, bg1, sigma2, mu2, maximum2, bg2].
        The fwhm of the first Gaussian.
        The chi-squared value of the first Gaussian fit.
        The double gaussian fit parameters.
        The xData.
        The yData yData.
        The fwhm of the second Gaussian.
        
        """

        # Calculates the residuals.
        def __residuals(paras, x, y):
            # sigma = the standard deviation.
            # mu = the mean aka the expectation of the distribution.
            # maximum = .
            # bg = background term.
            
            sigma1, mu1, maximum1, bg1, sigma2, mu2, maximum2, bg2 = paras
            err = y - ((abs(maximum1) * exp((-((x - mu1) / abs(sigma1))**2) / 2)) +
                       (abs(maximum2) * exp((-((x - mu2) / abs(sigma2))**2) / 2)) + (abs(bg1) + abs(bg2))/2)
            return err
        
        # Evaluates the function.
        def __evaluate(x, paras):
            # Same variables as above.
            sigma1, mu1, maximum1, bg1, sigma2, mu2, maximum2, bg2 = paras
            return ((abs(maximum1) * exp((-((x - mu1) / abs(sigma1))**2) / 2)) +
                       (abs(maximum2) * exp((-((x - mu2) / abs(sigma2))**2) / 2)) + (abs(bg1) + abs(bg2))/2)
            
        xData = range(len(yData))
        
        # Perform gaussian fit.
        leastSquaresParameters = leastsq(__residuals, p0, args=(xData,yData))
        fwhm1 = abs(2 * sqrt(2*log(2)) * leastSquaresParameters[0][0])
        fwhm2 = abs(2 * sqrt(2*log(2)) * leastSquaresParameters[0][4])
        fit = __evaluate(xData, leastSquaresParameters[0])

        chisq = 0
        for i in range(len(yData)):
            if fit[i] >= 1.:
                chisq += (yData[i]-fit[i])**2/len(yData)
                
        return leastSquaresParameters[0], fwhm1, chisq, fit, xData, yData, fwhm2
    
    # ******************************************************************************************
    
    def dm_curve_fit_and_candidate_params(self,xmldata):
        """
        Fits a curve to the DM data and obtains the candidates parameters.
        
        Parameters:
        xmldata    -    the xmal data obtained from the candidate phcx file.
        
        Returns:
        
        Twelve parameters of the curve fit:
        
            The least squares parameters, an array of three items:
                Amplitude at index 0, Prop at index 1 ,and Shift at index 2.
            The Chi-squared fit
            Chi_theo 
            theo
            The fit objects containing parameters.
            The xdata.
            The ydata
            The period.
            The SNR
            The DM.
            The width of the curve.
            The peak of the curve.
        
        """

        # Calculates the residuals.
        def __residuals(paras, x, y):     
            Amp,Prop,Shift = paras
            weff = sqrt(wint + pow(Prop*kdm*abs((dm + Shift)-x)*df/pow(f,3),2))
            SNR  = Amp*sqrt((period-weff)/weff)
            err  = y - SNR
            return err
        
        # Evaluates the function.
        def __evaluate(x, paras):
            Amp,Prop,Shift = paras
            weff = sqrt(wint + pow(Prop*kdm*abs((dm + Shift)-x)*df/pow(f,3),2))
            SNR  = Amp*sqrt((period-weff)/weff)
            return SNR
        
        # get best values for candidate.
        snr = float(xmldata.getElementsByTagName('Snr')[1].childNodes[0].data)
        dm = float(xmldata.getElementsByTagName('Dm')[1].childNodes[0].data)
        bary_period = float(xmldata.getElementsByTagName('BaryPeriod')[1].childNodes[0].data)
        width = float(xmldata.getElementsByTagName('Width')[1].childNodes[0].data)
        
        # Extract DM curve.
        dm_curve_all = array(self.getDM_FFT(xmldata,1))
        curve = self.dm_curve(dm_curve_all)
        yData = curve[0]
        length_all = len(dm_curve_all)
        length = len(yData)
        
        # Extract x-scale for DM curve.
        read_data = list(xmldata.getElementsByTagName('DmIndex')[1].childNodes[0].data)
        dm_index,temp = [],''
        for i in range(len(read_data)):
            if (read_data[i] != "\n"):
                temp += (read_data[i])
            else:
                dm_index.append(temp)
                temp = ''
                
        # Get start and end DM value and calculate step width.
        dm_start,dm_end = float(dm_index[1]),float(dm_index[len(dm_index)-1])
        dm_step = abs(dm_start-dm_end)/length_all
        
        # SNR and pulse parameters.
        period = bary_period*1000.
        wint = (width*period)**2
        kdm = 8.3*10**6
        df = 400
        f = 1374
        
        peak = snr/sqrt((period-sqrt(wint))/sqrt(wint))
        
        # Scale x-data.
        xData = []
        for i in range(length):
            xData.append(dm_start+curve[1][i]*dm_step)    
        xData = array(xData)
        
        # Calculate theoretic dm-curve from best values.
        help = []
        for i in range(length):
            weff = sqrt(wint + pow(kdm*abs(dm-xData[i])*df/pow(f,3),2))
            SNR = sqrt((period-weff)/weff)
            help.append(float(SNR))
            
        theo = (255./max(help))*array(help)
        
        # Start parameter for fit.
        Amp = (255./max(help))
        Prop,Shift  = 1,0
        p0 = (Amp,Prop,Shift)
        plsq = leastsq(__residuals, p0, args=(xData,yData))
        fit = __evaluate(xData, plsq[0])
        
        # Chi square calculation.
        chi_fit,chi_theo = 0,0
        for i in range(length):
            if fit[i] >= 1.:
                chi_fit  += (yData[i]-fit[i])**2
                chi_theo += (yData[i]-theo[i])**2
                
        chi_fit  =  chi_fit/length
        chi_theo = chi_theo/length
        
        return plsq[0], chi_fit, chi_theo, theo, fit, xData, yData, period, snr, dm, width, peak
    
    # ******************************************************************************************
    
    def dm_curve(self,data):
        """
        Extracts the DM curve from the DM data block in the phcx file.
        
        Parameters:
        data    -    a numpy.ndarray containing the DM data.
        
        Returns:
        
        An array describing the curve.
        
        """
        
        result,x,temp = [],[],[]
        for i in range(len(data)):
            if (i+1)%128 == 0:
                result.append(max(temp))
                x.append(i - 128)
                temp = []
            else:
                temp.append(data[i])
        
        return array(result),array(x)
        
    # ******************************************************************************************
    
    def getDM_FFT(self,xmldata,n):
        """
        Extracts the DM curve from the DM data block in the phcx file.
        
        Parameters:
        xmldata    -    a numpy.ndarray containing the DM data in decimal format.
        n          -    the section of the xml file to find the DM data within. This
                        is required sine there are two DM sections in the phcx file.
        
        Returns:
        
        An array containing the  DM curve data in decimal format.
        
        """
        
        # Extract data.
        dec_value = []
        block = xmldata.getElementsByTagName('DataBlock') # gets all of the bits with the title 'section'.
        points = block[n].childNodes[0].data
        
        # Transform data from hexadecimal to decimal values.
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
        
    # ******************************************************************************************
 
    def getSubband_scores(self,xmldata):
        """
        Computes sub-band scores for a candidate. The function is based on
        a C-script by Aristeidis Noutsos.
        
        Parameters:
        xmldata    -    a numpy.ndarray containing the DM data in decimal format.
        
        Returns:
        
        The root mean squared (RMS) of peak positions in all sub-bands.
        The average correlation coefficient for each pair of sub-bands.            
        
        """
        
        # Grab data from xml file.
        block_bands = xmldata.getElementsByTagName("SubBands")
        frequency = block_bands[1].childNodes[0].data
        prof_bins = int(block_bands[1].getAttribute("nBins"))
        band_subbands = int(block_bands[1].getAttribute("nSub"))
        subbands = self.hexToDec(frequency, band_subbands, prof_bins)
        bestWidth = float(xmldata.getElementsByTagName('Width')[1].childNodes[0].data)
        
        width_bins = int(ceil(bestWidth*prof_bins))
        subband_sums = []
        
        # CALCULATE THE AMPLITUDES FOR EACH SUBBAND USING A BOX-CAR EQUAL TO THE PULSE WIDTH.
        
        for i in range(band_subbands):
            
            sums_vec = []
            
            for j in range(prof_bins-width_bins+1):
                sum = 0
                for b in range(width_bins):
                    sum += subbands[i][j+b]
                sums_vec.append(sum)
            
            subband_sums.append(sums_vec)
            
        # FIND THE MAXIMA OF THE AMPLITUDES FOR EACH SUBBAND.
        
        max_bins, max_sums = [], []
        for i in range(len(subband_sums)):
            max_sum = -10000.0
            for j in range(len(subband_sums[i])):
                if (subband_sums[i][j]>max_sum):
                    max_sum = subband_sums[i][j]
                    max_bin = j+width_bins/2
                    
            max_bins.append(float(max_bin))
            max_sums.append(max_sum)
            
        med = array(max_bins).mean()
        
        # CHECK HOW CLOSE TO EACH OTHER ARE THE POSITIONS OF THE MAXIMA.
        
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
        
        #  Linear correlation.
        # Correlates the amplitudes across the pulse between subbands pairs.
        
        m = 0
        sum = 0.0
        for i in range( len(subband_sums) ):
            k = i+1
            while k < len(subband_sums):
                # A RuntimeWarning is raised here when subband_sums[i]
                # and subband_sums[k] are completely empty. The code appears
                #subband_sums[i] to execute normally despite the error.
                cc = corrcoef(subband_sums[i], subband_sums[k])[0][1]
                if str(cc)=="nan":
                    k += 1
                else:
                    sum += cc
                    k += 1
                    m += 1 
        
        # AVERAGE CORRELATION COEFFICIENT ACROSS ALL SUBBAND PAIRS.
        mean_corr = sum/float(m)
        
        # RMS SCATTER OF THE MAXIMA NORMALISED TO THE PULSE WIDTH.
        rms = stdev/float(width_bins)
        
        return rms, mean_corr
    
    # ******************************************************************************************
 
    def hexToDec(self,listData,nsub,nbin):
        """
        Converts hexadecimal data to decimal data.
        
        Parameters:
        list    -    a numpy.ndarray containing the DM data in hexadecimal format.
        nsub    -    number of sub-bands.
        nbin    -    number of bins.
        
        Returns:
        
        A list with the data in decimal format.            
        
        """
        x,y = 0,0
        newlist = []
        while x < len(listData):
            if listData[x] != "\n":
                try:
                    hexValue = listData[x:x+2]
                    newlist.append(int(hexValue,16))
                    x += 2
                    y += 1
                except ValueError:
                    break
            else:
                x += 1
                
        a = array(newlist).reshape(nsub,nbin)
        return a
    
    # ******************************************************************************************
 
    def getProfileCorr(self,xmldata,p,section):
        """
        Calculates the correlation of the profile with the subbands, -integrals.
        
        Parameters:
        xmldata    -    a numpy.ndarray containing the DM data in hexadecimal format.
        p          -    the profile data.
        section    -    the section of the phcx xml file to look in, should be 'Bands'.
        
        Returns:
        
        A list with the correlation data in decimal format.            
        
        """
        
        block_bands = xmldata.getElementsByTagName('Sub'+section)
        frequency = block_bands[1].childNodes[0].data
        nbin_bands = int(block_bands[1].getAttribute("nBins"))
        nsub_bands = int(block_bands[1].getAttribute("nSub"))
        allbands = self.hexToDec(frequency, nsub_bands, nbin_bands)
        
        corrlist = []
        for j in range(nsub_bands):
            coef = abs(corrcoef(allbands[j],p))
            if coef[0][1] > 0.0055:
                corrlist.append(coef[0][1])
                
        return array(corrlist)

            
    # ******************************************************************************************
    
    #Revision:4
    def freedman_diaconis_rule(self,data):
        """
        Calculate number of bins to use in histogram according to this rule.
        
        Parameters:
        data    -    a numpy.ndarray containing the data for which a histogram is to be computed.
        
        Returns:
        
        The 'optimal' number of bins for the histogram.   
        """
        # interquartile range, Q3-Q1....
        iqr = stats.scoreatpercentile(data, 75) - stats.scoreatpercentile(data, 25)
        binwidth = 2 * iqr * pow(len(data), -0.3333333)
        
        # calculate n bins
        rnge = max(data) - min(data)
        nbins = ceil(rnge/binwidth)
        
        print "\tIQR: ",iqr
        print "\tBin Width: ",binwidth
        print "\tRange: ",rnge
        print "\tNumber of bins: ", nbins
        return int(nbins)
    
    # ******************************************************************************************