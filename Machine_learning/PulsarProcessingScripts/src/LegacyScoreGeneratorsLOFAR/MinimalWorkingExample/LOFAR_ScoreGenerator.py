"""
Script which generates scores for pulsar candidates. These scores are used as the
input features for machine learning classification algorithms. In total 22 scores
are generated from each individual candidate. Each score summarises a candidate
in some way.
  
This code runs on python 2.4 or later.

Used to generate candidate scores for LOFAR pfd files. 

Rob Lyon <robert.lyon@cs.man.ac.uk>

+-----------------------------------------------------------------------------------------+
+                       PLEASE RECORD ANY MODIFICATIONS YOU MAKE BELOW                    +
+-----------------------------------------------------------------------------------------+
+ Revision |   Author    | Description                                       |    DATE    +
+-----------------------------------------------------------------------------------------+

 Revision:0    Rob Lyon    Initial version of the re-written code.            06/02/2014
 
"""

# Numpy Imports:
from numpy import mean

# Command Line processing Imports:
from optparse import OptionParser

# Standard library Imports:
import glob,sys

# Custom file Imports:
import Utilities
import PFD# @UnusedImport - Ignore this comment, used to tell IDE not to show warning.

# ******************************
#
# CLASS DEFINITION
#
# ******************************

class LOFAR_ScoreGenerator:
    """                
    Generates 22 scores that describe the key features of pulsar candidate, from the
    candidate's own phcx file. The scores generated are as follows:
    
    Score number    Description of score                                                                                Group
        1            Chi squared value from fitting since curve to pulse profile.                                    Sinusoid Fitting
        2            Chi squared value from fitting sine-squared curve to pulse profile.                             Sinusoid Fitting
        
        3            Number of peaks the program identifies in the pulse profile - 1.                                Pulse Profile Tests
        4            Sum over residuals.                                                                             Pulse Profile Tests
        
        5            Distance between expectation values of Gaussian and fixed Gaussian fits to profile histogram.   Gaussian Fitting
        6            Ratio of the maximum values of Gaussian and fixed Gaussian fits to profile histogram.           Gaussian Fitting
        7            Distance between expectation values of derivative histogram and profile histogram.              Gaussian Fitting    
        8            Full-width-half-maximum (FWHM) of Gaussian fit to pulse profile.                                Gaussian Fitting
        9            Chi squared value from Gaussian fit to pulse profile.                                           Gaussian Fitting
        10           Smallest FWHM of double-Gaussian fit to pulse profile.                                          Gaussian Fitting
        11           Chi squared value from double Gaussian fit to pulse profile.                                    Gaussian Fitting
        
        12           Best period.                                                                                    Candidate Parameters
        13           Best SNR value.                                                                                 Candidate Parameters
        14           Best DM value.                                                                                  Candidate Parameters
        15           Best pulse width (original reported as Duty cycle (pulse width / period)).                      Candidate Parameters
        
        16           SNR / SQRT( (P-W)/W ).                                                                          Dispersion Measure (DM) Curve Fitting
        17           Difference between fitting factor, Prop, and 1.                                                 Dispersion Measure (DM) Curve Fitting
        18           Difference between best DM value and optimised DM value from fit, mod(DMfit - DMbest).          Dispersion Measure (DM) Curve Fitting
        19           Chi squared value from DM curve fit.                                                            Dispersion Measure (DM) Curve Fitting
        
        20           RMS of peak positions in all sub-bands.                                                         Sub-band Scores
        21           Average correlation coefficient for each pair of sub-bands.                                     Sub-band Scores
        22           Sum of correlation coefficients between sub-bands and profile.                                  Sub-band Scores
        
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
        
        # Python 2.4 argument processing.
        parser = OptionParser()

        # REQUIRED ARGUMENTS
        # None.
        
        # OPTIONAL ARGUMENTS
        parser.add_option("-v", action="store_true", dest="verbose",help='Verbose debugging flag (optional).',default=False)

        (args,options) = parser.parse_args()# @UnusedVariable : Comment for IDE to ignore warning.
        
        # Update variables with command line parameters.
        self.debug = args.verbose
        
        print "\n***********************************"
        print "| Executing score generation code |"
        print "***********************************"
        
        # Helper files.
        utils = Utilities.Utilities(True)
        
        # Variables used to store stats on candidate scores generated,
        # and failure rate etc.
        candidatesProcessed = 0;
        successes = 0;
        failures = 0;
        
        # Sets the number of bins used when creating the profile histogram.
        histogramBins = 60 # @UnusedVariable - this silences IDE warnings 
        
        for cand in glob.glob('*.pfd'):
            
            candidatesProcessed+=1
            
            print "Processing candidate:\t" , cand
            output = open(cand + ".dat", 'w') # @UnusedVariable - this silences IDE warnings # Create path to output file where scores will be written.
            
            scores = [] # @UnusedVariable - this silences IDE warnings # stores the scores generated for each candidate.
            
            try:
                ########## Read raw profile data ##########
                
                candidate = PFD.PFD(True,cand)
                
                profile = candidate.plot_sumprof()
                profile = profile/mean(profile)
                scores = candidate.test_22(profile)
                #print scores
                
                attributes = len(scores)
                
                for i in range(attributes):
                    if (i+1) == attributes:
                        output.write(str(scores[i]))
                    else:
                        output.write(str(scores[i])+",")
                        
                output.close()
                
            except Exception as e: # Catch *all* exceptions.
                print "Error reading profile data using ProfileHelper.py :\n\t", sys.exc_info()[0]
                print utils.format_exception(e)
                print cand, " did not have scores generated."
                failures+=1
                continue
            
            successes+=1
        
        print "\nCandidates processed:\t",candidatesProcessed
        print "Successes:\t", successes
        print "Failures:\t", failures
        print "Done."
        
if __name__ == '__main__':
    LOFAR_ScoreGenerator().main()