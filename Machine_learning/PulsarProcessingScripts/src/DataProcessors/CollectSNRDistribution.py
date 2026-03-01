"""
    ==========================================================================================
    | Used to collect SNR, DM and Period statistics for HTRU data,                           |
    | on a per observation basis. The script will output a file containing                   |
    | per observation statistics and per beam statistics.                                    |
    |                                                                                        |
    | This script expects on data files which are in CSV format containing an                |
    | individual candidate on each line where we have:                                       |
    |                                                                                        |
    | candidate path, snr, dm, period                                                        |
    |                                                                                        |
    | This script will output two files in CSV format containing statistics describing       |
    | individual observations and beams. File one will provide statistics per observation.   |
    | This will be output in the following format                                            |
    |                                                                                        |
    | day, month, year, hour, minute, min snr, max snr, mean snr, median snr, Q1 snr, Q3 snr,|
    | IQR snr, range snr, var snr, stdev snr, sum snr, count snr, min dm, max dm,...,        |
    | count dm, min period, max period, ..., count period                                    |
    |                                                                                        | 
    | For file one then there are 60 columns of data, where the following stats are computed |
    | for snr, dm and period:                                                                |
    |                                                                                        |
    | min, max, mean, median, Q1, Q3, IQR, range, var, stdev, sum, skew, kurtosis,           |
    | pearson's r with data 1, pearson's r with data 2, count                                |
    |                                                                                        |
    | Here count is simply the number of candidates found for an observation. From this you  |
    | could recompute the average yourself doing avg = sum/count, this is for error checking |
    | purposes only.                                                                         |
    |                                                                                        |
    | Pearson's r with data 1 computes the correlation coefficient between variables.        |
    | For instance for SNR, then Pearson's r with data 1 would be the correlation with dm,   |
    | and Pearson's r with data 2 the correlation with period. In full we have two           |
    | two variables, the correlation coefficient and a p-value (estimated).                  |
    |                                                                                        |
    |  -------------------------------------------------                                     |
    | |        | Pearson's data 1  | Pearson's data 2  |                                     |
    | |  Stat  |----------------------------------------                                     |
    | |        |  Stat  |  Column  |  Stat  |  Column  |                                     |
    | |--------|--------|----------|--------|----------|                                     |
    | |   snr  |   dm   | 19 | 20  | period | 21 | 22  |                                     |
    | |--------|--------|----------|--------|----------|                                     |
    | |   dm   |   snr  | 37 | 38  | period | 39 | 40  |                                     |
    | |--------|--------|----------|--------|----------|                                     |
    | | period |   snr  | 55 | 56  |   dm   | 57 | 58  |                                     |
    | |------------------------------------------------|                                     |
    |                           ^                   ^                                        |
    |                           |                   |                                        |
    |                           --------------------                                         |
    |                                     |                                                  |
    |                                     v                                                  |
    |                                 P-values                                               |
    |                                                                                        |
    |                                                                                        |
    | * Column indexing starts at 1.                                                         |
    |                                                                                        |
    | The second file has almost the same format, except the stats are computed per beam     |
    | and the final column contains the beam number (61 columns in total).                   |
    |                                                                                        |
    | Since the Parkes multibeam receiver has 13 beams, and for each observation the         |
    | initial data processing stored 100 of the best candidates per beam,                    |
    | then there should be 1300 candidates per observation. We try to compute                |
    | statistics per beam and per observation independent of this 1300 figure                |
    | however, since some observations may have less or perhaps slightly more                |
    | candidates (some may have been deleted, gotten lost or corrupted?).                    |
    |                                                                                        |
    | Thus we have to parse the candidate paths and determine if they belong                 |
    | to the same beam, and the same observation, using string parsing approaches.           |
    |                                                                                        |
    | The program is designed to process all the candidates from a single observation        |
    | month together. For example 2008_11.csv should contain all the candidates obtained     |
    | during November 2008. In total there are 26 such csv files (please contact me          |
    | if you need them, or produce the files yourself from /local/scratch/cands) describing  |
    | over ~11,200,000 candidates. Here I process a directory containing all 26 files.       |
    |                                                                                        |
    | NOTES:                                                                                 |
    |                                                                                        |
    | Column indexes for output data...                                                      |
    |                                                                                        |
    |  1.  day          2. month       3. year        4. hour          5. min                |
    |                                                                                        |
    |  6.  min snr      7. max snr     8. mean snr    9. median snr   10. Q1 snr             |
    | 11. Q3 snr       12. IQR snr    13. range snr  14. var snr      15. stdev snr          |
    | 16. sum snr      17. skew snr   18. kurt snr   19. p1 snr       20. p1 snr p-value     |
    | 21. p2 snr       22. p2 snr p-value            23. count snr                           |
    |                                                                                        |
    | 24. min dm       25.  max dm    26. mean dm    27. median dm    28. Q1 dm              |
    | 29. Q3 dm        30. IQR dm     31. range dm   32. var dm       33. stdev dm           |
    | 34. sum dm       35. skew dm    36. kurt dm    37. p1 dm        38. p1 dm p-value      |
    | 39. p2 dm        40. p2 dm p-value             41. count dm                            |
    |                                                                                        |
    |                                                                                        |
    | 42. min p0       43.  max p0    44. mean p0    45. median p0    46. Q1 p0              |
    | 47. Q3 p0        48. IQR p0     49. range p0   50. var p0       51. stdev p0           |
    | 52. sum p0       53. skew p0    54. kurt p0    55. p1 p0        56. p1 p0 p-value      |
    | 57. p2 p0        58. p2 p0 p-value             59. count p0     60. Date               |
    |                                                                                        |
    | 61. beam number * only in beam file.                                                   |
    |                                                                                        |
    | Rob Lyon <robert.lyon@cs.man.ac.uk>                                                    |
    ==========================================================================================
 
"""

# Standard library Imports:
import datetime, sys, os
from optparse import OptionParser
from Utilities import Utilities
from CollectStatsObject import CollectStatsObject

# ******************************
#
# CLASS DEFINITION
#
# ******************************

class CollectSNRDistribution(Utilities):
    """                
    
    """
    
    # ******************************
    #
    # MAIN METHOD AND ENTRY POINT.
    #
    # ******************************
    def __init__(self,debugFlag,):
        """
        
        """
        Utilities.__init__(self,debugFlag) 
        
    def main(self,argv=None):
        """
        Main entry point for the Application.
    
        """
        
        print "\n*****************************"
        print "| CollectSNRDistribution.py |"
        print "|---------------------------|"
        print "| Version 1.0               |"
        print "| robert.lyon@cs.man.ac.uk  |"
        print "****************************\n"
        print(__doc__)
        
        # Python 2.4 argument processing.
        parser = OptionParser()

        # REQUIRED ARGUMENTS
        parser.add_option('-i', action="store", dest="path_collect",type="string",help='The path to the directory containing candidate files by observation month (required).',default="")

        # OPTIONAL ARGUMENTS
        # None.

        (args,options) = parser.parse_args()
        
        # Variables used by this class.
        self.dirPath = args.path_collect
        
        # Counters for the number of candidates processed.
        self.candidatesProcessed = 0
        self.candsWithNoBeam     = 0
        # Used only while testing outside of terminal.
        #directory = "/local/scratch/cands"
        #dataFile = "/tmp/snr_dm_period.csv"
        
        directory  = self.dirPath
        obsFile    = directory + "/" + "Observation_SNR_Dist_Data.txt"
        beamFile   = directory + "/" + "Beam_SNR_Dist_Data.txt"
        
        # By giving the user an indication of how long this
        # program takes to run, they can estimate how long it
        # will take to process varying numbers of candidates.
        start = datetime.datetime.now()
        
        statsObs = CollectStatsObject(self.debug)
        statsBeam = CollectStatsObject(self.debug)
        
        self.outputFileHeaders(obsFile, beamFile)
        
        # Search the supplied directory recursively.    
        for root, directories, files in os.walk(directory):
            
            for f in files:
                
                # If the file found isn't some form of csv, then ignore it.
                if(not f.endswith('.csv')):
                    continue
                
                filePath = os.path.join(root, f)
                print "Processing file: ", filePath
                
                
                _file = open(filePath)
                content = _file.readlines()
                _file.close()
        
                if(len(content)>0):
                    
                    for line in content:
                        
                        components = line.split(",")
                        
                        length = len(components)
                        
                        if(length < 4):
                            continue
                        
                        candidatePath = components[0]
                        snr           = components[1]
                        dm            = components[2]
                        period        = components[3]
                        
                        # We process data per observation and per beam separately.
                        # So I use two objects to manage the data for each.
                        
                        statsObs.computeObservationTime(candidatePath)
                        statsBeam.computeObservationTime(candidatePath)
                        
                        # If this next candidate is part of the same observation as the last...
                        if(statsObs.partOfSameObservation(candidatePath)):
                            # Then update the stats for this obeservation.
                            statsObs.SNR_update(snr,dm,period)
                            self.candidatesProcessed+=1
                        else:
                            # Else this candidate is from a new observation, so save and clear
                            # the data in the stats object, and begin storing the stats again.
                            statsObs.saveAndClearSNR(obsFile)
                            statsObs.computeObservationTime(candidatePath)
                            statsObs.SNR_update(snr,dm,period)
                            self.candidatesProcessed+=1
                        
                        # If this next candidate is part of the same observation AND beam as the last...    
                        if(statsBeam.partOfSameObservation(candidatePath) and statsBeam.partOfSameBeam(candidatePath)==1):
                            # Then update the stats for this obeservation and beam.
                            statsBeam.SNR_update(snr,dm,period)
                        elif(statsBeam.partOfSameObservation(candidatePath) and statsBeam.partOfSameBeam(candidatePath)==-1):
                            # Treat candidates with no beam number as part of this current beam,
                            # provided the current beam is not itself without a number.
                            if(statsBeam.beam>0):
                                statsBeam.SNR_update(snr,dm,period)
                                
                            self.candsWithNoBeam +=1
                        else:
                            # Else this candidate is from a new observation or beam, so save and clear
                            # the data in the stats object, and begin storing the stats again.
                            statsBeam.saveAndClearSNR(beamFile)
                            statsBeam.computeObservationTime(candidatePath)
                            statsBeam.SNR_update(snr,dm,period)
                    
                    # The two if statements here are catching the last set of data points
                    # in the file - without this if, these would be ignored, since stats aren't
                    # written to the output files until triggered by the arrival of a new set of
                    # beam / obs data.    
                    if(statsBeam.saved == False):
                        statsBeam.saveAndClearSNR(beamFile)
                    
                    if(statsObs.saved == False):
                        statsObs.saveAndClearSNR(obsFile)                
        
        end = datetime.datetime.now()
            
        print "Candidates Processed:      ", self.candidatesProcessed
        print "Candidates no beam number: ", self.candsWithNoBeam
        print "Execution time:            ", str(end - start)    
        print "Done processing directory  ", self.dirPath
    
    # **************************************************************************************************** 
    
    def outputFileHeaders(self,obsPath,beamPath):
        
        header_1 = "Day,Month,Year,Hour,Min"                                                             
        
        for x in range(0, 1001):
            header_1+= "," + str(x)
            
        #self.appendToFile(obsPath,header_1  +"\n")
        #self.appendToFile(beamPath,header_1 +"\n")                                                                 
                                                                                                                                             
    # ****************************************************************************************************
         
if __name__ == '__main__':
    CollectSNRDistribution(True).main()