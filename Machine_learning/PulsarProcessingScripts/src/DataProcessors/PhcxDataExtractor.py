"""
Rob Lyon <robert.lyon@cs.man.ac.uk>
 
"""

# Standard library Imports:
import datetime, gzip, sys, os
from optparse import OptionParser

# XML processing Imports:
from xml.dom import minidom
from Utilities import Utilities

# ******************************
#
# CLASS DEFINITION
#
# ******************************

class PhcxDataExtractor(Utilities):
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
        
        print "\n****************************"
        print "|                          |"
        print "|--------------------------|"
        print "| Version 1.0              |"
        print "| robert.lyon@cs.man.ac.uk |"
        print "***************************\n"
        
        # Python 2.4 argument processing.
        parser = OptionParser()

        # REQUIRED ARGUMENTS
        parser.add_option('-d', action="store", dest="path_collect",type="string",help='The path to the directory containing candidates (required).',default="")
        parser.add_option('-o', action="store", dest="path_output",type="string",help='The path to an output file where the patterns can be written to (required).',default="")

        # OPTIONAL ARGUMENTS
        # None.

        (args,options) = parser.parse_args()
        
        # Variables used by this class.
        self.dirPath = args.path_collect
        self.output = args.path_output
        
        # Counters for the number of candidates collected.
        self.candidatesCollected = 0
        
        #directory = "/local/scratch/cands"
        #dataFile = "/tmp/snr_dm_period.csv"
        
        directory = self.dirPath
        dataFile = self.output
        
        # By giving the user an indication of how long this
        # program takes to run, they can estimate how long it
        # will take to convert varying numbers of candidates.
        start = datetime.datetime.now()
            
        # Search the supplied directory recursively.    
        for root, directories, files in os.walk(directory):
            
            for file in files:
                
                # If the file found isn't some form of candidate file, then ignore it.
                if(not file.endswith('.phcx.gz')):
                    continue
                
                candidateFile = os.path.join(root, file)
                # Read data directly from phcx file.
                cand = gzip.open(candidateFile,'rb')
                c = minidom.parse(cand) # strip off xml data
                cand.close()
                    
                snr = float(c.getElementsByTagName('Snr')[1].childNodes[0].data)
                dm = float(c.getElementsByTagName('Dm')[1].childNodes[0].data)
                period = float(c.getElementsByTagName('BaryPeriod')[1].childNodes[0].data) * 1000
                #width = float(c.getElementsByTagName('Width')[1].childNodes[0].data)
                    
                self.appendToFile(dataFile, str(candidateFile)+","+str(snr) + "," +str(dm) + "," + str(period) + "\n")
                self.candidatesCollected+=1
        
        end = datetime.datetime.now()
            
        print "Candidates processed: ", self.candidatesCollected
        print "Execution time: ", str(end - start)
            
        print "Done processing ", self.dirPath
    
    # **************************************************************************************************** 
      
if __name__ == '__main__':
    PhcxDataExtractor(True).main()