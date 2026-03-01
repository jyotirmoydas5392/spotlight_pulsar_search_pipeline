"""
Collects specified pulsar candidate PHCX files.

This application runs on python 2.4 or later.

By Rob Lyon <robert.lyon@cs.man.ac.uk>

Simple usage example:

+----------------------------------------------------------------------------------+
+                   PLEASE RECORD ANY MODIFICATIONS YOU MAKE BELOW                 +
+----------------------------------------------------------------------------------+
+ N |   Author    | Description                                       |    DATE    +
+----------------------------------------------------------------------------------+
+ 1 | Rob Lyon    | Initial version of the code.                      | 05/11/2013 +
+----------------------------------------------------------------------------------+
+                                                                                  +
+----------------------------------------------------------------------------------+

"""

# Python 2.4 imports.
import os, shutil
from optparse import OptionParser

# ******************************
#
# CLASS DEFINITION
#
# ******************************

class CandidateDataCollector:
    """
    Collects candidates specified in a supplied file.
    
    """
    
    # ******************************
    #
    # MAIN METHOD AND ENTRY POINT.
    #
    # ******************************

    def main(self,argv=None):
        """
        Main entry point for the Application. Processes command line
        input and begins converting the candidates to PNG's.
    
        """
    
        # Python 2.4 argument processing.
        parser = OptionParser()

        # REQUIRED ARGUMENTS
        parser.add_option('-c', action="store", dest="path_candidate",type="string",help='The path to the file containing paths to candidates (required).',default="")

        # OPTIONAL ARGUMENTS
        # None.

        (args,options) = parser.parse_args()
        
        # Variables used by this class.
        self.candidatePath = args.path_candidate
    
        if os.path.isfile(self.candidatePath):
            print "The supplied file path is valid."           
            
            cands = self.loadCands(self.candidatePath)
            
            for candPath in cands:
                try:
                    self.copy(candPath)
                except:
                    print "Error processing:", candPath
        
        else:
            print "Command line parameters invalid.\nBe sure that the specified files exist."
        
        print "Done."
        
    # ******************************
    #
    # VIEW CANDIDATE AS PNG.
    #
    # ******************************
      
    def copy(self,candPath):
        """
        Converts the candidate by executing a simple terminal command.
        
        """
        
        #print "Copying candidate at: ",candPath
        
        # this gets the path to the directory this application is executing in.
        copyDir = os.path.realpath(__file__).replace(__file__,"")
        
        prefix="/lustre/projects/p002_swin/processing/periodicity/23Aug2013_medlat/"
        tempFileName=candPath.replace(prefix,"")
        tempFileName=tempFileName.replace("/","_")
        destPath = copyDir + tempFileName
        
        # Copy Raw candidate data to current working directory:
        shutil.copyfile(candPath,destPath)
    
    # ******************************
    #
    # Filter Functions.
    #
    # ******************************

    def loadCands(self,filePath):
        """
        Method that reads candidate paths in from a file.
        
        """
        cands = []
        
        if(self.fileExists(filePath)==True):
            
            file = open(filePath,'rU') # Read only access
    
            for line in file.readlines():
                    cands.append(line.replace("\n",""))
        
            file.close()
                
        return cands
        print "Loaded Candidate Paths Successfully"
        
    def appendToFile(self,path,text):
        """
        Appends the provided text to the file at the specified path.
        
        """
        
        destinationFile = open(path,'a')
        destinationFile.write(str(text))
        destinationFile.close()
    
    def fileExists(self,path):
        """
        Checks a file exists, returns true if it does, else false.
        
        """
        
        try:
            fh = open(path)
            fh.close()
            return True
        except IOError:
            return False 
    
if __name__ == '__main__':
    CandidateDataCollector().main()