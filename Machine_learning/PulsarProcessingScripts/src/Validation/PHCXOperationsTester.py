"""
Contains unit test functions for the ProfileOperations.PY and PHCXFile.py scripts.
These tests are not complete, and will need to be finished at a later date - don't
have time right now!


This code runs on python 2.4 or later.
By Rob Lyon <robert.lyon@cs.man.ac.uk>

+-----------------------------------------------------------------------------------------+
+                       PLEASE RECORD ANY MODIFICATIONS YOU MAKE BELOW                    +
+-----------------------------------------------------------------------------------------+
+ Revision |   Author    | Description                                       |    DATE    +
+-----------------------------------------------------------------------------------------+

 Revision:0    Rob Lyon    Initial version of the the code.                    13/02/2014



+-----------------------------------------------------------------------------------------+

NOTE: You can go directly to a revision by searching the text below i.e. search for "Revision:2b"

"""

import sys,os

# Scipy/numpy imports.
from numpy import random
from numpy import array
from numpy import histogram
from numpy import pi
from numpy import sin
import numpy as np
import scipy.optimize as optimize
import scipy.fftpack as fftpack
from scipy.optimize import leastsq

# Custom file imports.
from Utilities import Utilities

import matplotlib.pyplot as plt

# Update path to code to test.
sys.path.append(os.path.abspath(os.path.dirname(__file__) + '/' + '../src'))

from CandidateScoreGenerators import ProfileOperations

# ****************************************************************************************************
#
# CLASS DEFINITION
#
# ****************************************************************************************************
    
class PHCXTester(Utilities):
    """
    Unit test code.
    """
        
    # ****************************************************************************************************
    #
    # Functions.
    #
    # ****************************************************************************************************
    
    def __init__(self,debugFlag):
        Utilities.__init__(self,debugFlag)
        self.phcx=ProfileOperations.ProfileOperations(debugFlag)
    
    # ****************************************************************************************************
    #
    # Sinusoid Fittings
    #
    # ****************************************************************************************************
    
    def testSinusoidFittings(self,profile):
        """
        Tests the code that performs the sinusoid fitting on the profile.
        """
        
        # Generate some random data.
        mu, sigma = 0, 0.1 # mean and standard deviation
        samples=random.normal(mu, sigma, 128)
        yData=[]
        xData=[]
        
        min_ = min(samples)
        max_ = max(samples)
        
        newMin =0
        newMax =255
        
        counter = 0
        for x in samples:
            yData.append(self.scale(x,min_,max_,newMin,newMax))
            xData.append(counter)
            counter+=1
        
        # Plot the randomly generated data.
        plt.plot(xData,yData)
        plt.title("Randomly Generated Data")
        plt.show()
        
        # Produce histogram of randomly generated data.
        histogramBins=self.phcx.freedmanDiaconisRule(yData)
        print "Suggested Histogram bins:",histogramBins
        hist, bins = histogram(yData,histogramBins) # Calculates a histogram of the profile.
        center = (bins[:-1] + bins[1:]) / 2
        plt.bar(center, hist, align='center')
        plt.title("Histogram of Randomly Generated Data")
        plt.show()
        
        
        # Perform a fitting to the random data, check the scores generated.
        try:
            sin_fit = self.phcx.getSinusoidFittings(array(yData))
            
            if(self.debug==True):
                print "\nScore 1. Chi-Squared value for sine fit to raw profile = ",sin_fit[0]
                print "Score 2. Chi-Squared value for sine-squared fit to amended profile = ",sin_fit[1]
                print "Score 3. Difference between maxima = ",sin_fit[2]
                print "Score 4. Sum over residuals = ",sin_fit[3]
        
        except Exception as e: # catch *all* exceptions
            print "Error computing scores 1-4 (Sinusoid Fitting) \n\t", sys.exc_info()[0]
            print self.format_exception(e)
            raise Exception("Sinusoid fitting exception")
    
    
    
        
    # ****************************************************************************************************
    
    def scale(self,x,min_,max_,newMin,newMax):
        """
        Re-scales a data value occurring in the range min and max, the
        a new data range given by newMin and newMax.
        
        Parameter:
        x        -    the data value to rescale.
        min_     -    the minimum value of the original data range for x.
        max_     -    the maximum value of the original data range for x.
        newMin   -    the minimum value of the new data range for x.
        newMax   -    the maximum value of the new data range for x.
        
        Returns:
        A new array with the data scaled to within the range [newMin,newMax].
        """
        
        x = (newMin * (1-( (x-min_) /( max_-min_ )))) + (newMax * ( (x-min_) /( max_-min_ ) ))
        return x
    
    # ****************************************************************************************************
    #
    # Gaussian Fittings
    #
    # ****************************************************************************************************
    
    def testGaussianFittings(self,profile):
        raise NotImplementedError("Please Implement this method")
    
    def testGaussian(self,xData,yData):
        raise NotImplementedError("Please Implement this method")
    
    def testGaussianFixedWidthBins(self,xData,yData,bins):
        raise NotImplementedError("Please Implement this method")
    
    def testGaussianWithBackground(self,xData,yData):
        raise NotImplementedError("Please Implement this method")
    
    def testGaussianT1(self,yData):
        raise NotImplementedError("Please Implement this method")
    
    def testDoubleGaussianT2(self,yData):
        raise NotImplementedError("Please Implement this method")
    
    def testDoubleGaussian(self,yData):
        raise NotImplementedError("Please Implement this method")
    
    def testDoubleGaussianWithBackground(self,yData,p0):
        raise NotImplementedError("Please Implement this method")
    
    # ****************************************************************************************************