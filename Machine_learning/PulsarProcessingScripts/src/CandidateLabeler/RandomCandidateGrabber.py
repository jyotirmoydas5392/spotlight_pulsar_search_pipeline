"""
Randomly selects pulsar candidates stored on a network drive and collects the data
of those selected. Also obtains the PNG image describing the candidate using the pulsar
hunter tool. In particular this application executes terminal commands that use the
ph-view-phcx application (found at /packages/pulsar/soft/pulsarhunter-new/scripts/ph-view-phcx).


The data will be collected in whichever folder this application is executed in.

This application runs on python 2.4 or later.

By Rob Lyon <robert.lyon@cs.man.ac.uk>

Simple usage example:

    python RandomCandidateGrabber -n 1250
    
    This will collect 1250 candidates.

+-----------------------------------------------------------------------------------------+
+                       PLEASE RECORD ANY MODIFICATIONS YOU MAKE BELOW                    +
+-----------------------------------------------------------------------------------------+
+ Revision |   Author    | Description                                       |    DATE    +
+-----------------------------------------------------------------------------------------+

 Revision:0    Rob Lyon    Initial version of code.                            05/11/2013

"""

# Python 2.4 imports.
import os, shutil
from optparse import OptionParser
from random import choice

# ****************************************************************************************************
#
# CLASS DEFINITION
#
# ****************************************************************************************************

class RandomCandidateGrabber:
    """
    Randomly selects pulsar candidates stored on a network drive. Obtains their PNG,
    raw phcx file and .dat file.
    
    """
    
    # ****************************************************************************************************
    #
    # MAIN METHOD AND ENTRY POINT.
    #
    # ****************************************************************************************************

    def main(self,argv=None):
        """
        Main entry point for the Application. Processes command line
        input and begins collecting the candidate data.
    
        """
    
        # Python 2.4 argument processing.
        parser = OptionParser()

        # REQUIRED ARGUMENTS
        parser.add_option('-n', action="store", dest="cands",type="int",help='The number of candidates to grab (required).',default=1000)

        # OPTIONAL ARGUMENTS
        # None.

        (args,options) = parser.parse_args()  # @UnusedVariable
        
        # Variables used by this class.
        self.samples = args.cands
        
        if(self.samples > 0):
            
            for n in range(1, self.samples+1):# @UnusedVariable
                
                candPath = self.getRandomPath()
                print "Cand path:",candPath
                self.toPNG(candPath)
        
        else:
            print "Command line parameters invalid.\nBe sure that the specified files exist."
        
        print "Done."
        
    # ****************************************************************************************************
    #
    # VIEW CANDIDATE AS PNG.
    #
    # ****************************************************************************************************
      
    def toPNG(self,candPath):
        """
        Converts the candidate by executing a simple terminal command.
        
        Parameters:
        
        candPath    -    the path to the candidate for which the PNG should be generated.
        
        Returns:
        N/A
        """
        
        # Firstly we make sure that some environment variables
        # that ph-view-phcx needs are set.
        os.system("setenv JAVA_HOME /usr/java/latest")
        os.system("setenv PULSARHUNTER_HOME /packages/pulsar/soft/pulsarhunter-new")
        
        # For each candidate we've stored in the dictionary,
        # run the ph-view-phcx tool on it.
        
        print "Converting candidate at: ",candPath
        
        # destination path for ML scores being copied.
        destScores = os.getcwd()+"/"+ os.path.basename(candPath)+".dat"
        
        # destination path for raw profile data being copied.
        destProfile = os.getcwd()+"/"+ os.path.basename(candPath)
        
        # Copy Raw candidate data to current working directory:
        shutil.copyfile(candPath+".dat",destScores)
        shutil.copyfile(candPath,destProfile)
        
        # Actually run the program that converts the candidates to PNG.
        os.system("/packages/pulsar/soft/pulsarhunter-new/scripts/ph-view-phcx " + candPath + " --imageoutput")
    
    # ****************************************************************************************************
    #
    # Selection Functions.
    #
    # ****************************************************************************************************

    def getRandomPath(self):
        """
        Method that constructs a random file path to a candidate.
        
        Parameters:
        N/A
        
        Returns:
        
        A random path to a candidate file.
        """
        topLevelDirs = [ "/local/scratch/cands/2008-11","/local/scratch/cands/2008-12","/local/scratch/cands/2009-01",\
                        "/local/scratch/cands/2009-03","/local/scratch/cands/2009-04","/local/scratch/cands/2009-05",\
                        "/local/scratch/cands/2009-06","/local/scratch/cands/2009-07","/local/scratch/cands/2009-08",\
                        "/local/scratch/cands/2010-12","/local/scratch/cands/2009-09","/local/scratch/cands/2009-10",\
                        "/local/scratch/cands/2009-11","/local/scratch/cands/2009-12","/local/scratch/cands/2010-01",\
                        "/local/scratch/cands/2010-02","/local/scratch/cands/2010-03","/local/scratch/cands/2010-04",\
                        "/local/scratch/cands/2010-05","/local/scratch/cands/2010-06","/local/scratch/cands/2010-07",\
                        "/local/scratch/cands/2010-08","/local/scratch/cands/2010-09","/local/scratch/cands/2010-11"]
        
        root = choice(topLevelDirs)
        
        obsFolder = self.pickRandomDir(root)
        beamFolder = self.pickRandomDir(root+"/"+ obsFolder)
        pickedFile = self.pickRandomCandidateFile(root+"/"+ obsFolder+"/" + beamFolder)
        
        return root+"/"+ obsFolder+"/" + beamFolder + "/" + pickedFile
    
    # ****************************************************************************************************
    
    def pickRandomDir(self,directory):
        """
        Picks a random sub-directory within an existing directory.
        
        Parameters:
        
        directory    -    the folder from which a random sub-directory will be chosen.
        
        Returns:
        The name of the chosen random sub-directory.
        """
        dirs=[]
        for x in os.listdir(directory):
            if os.path.isfile(x): pass
            else: 
                dir_=str(x)
                if("."  not in dir_):
                    dirs.append(x)

        return choice(dirs)
    
    # ****************************************************************************************************
        
    def pickRandomCandidateFile(self,directory):
        """
        Picks a random candidate file file within an existing directory.
        
        Parameters:
        
        directory    -    the folder from which a random file will be chosen.
        
        Returns:
        The name of the file chosen.
        """
        files=[]
        for x in os.listdir(directory):
            file_=str(x)
            if(".gz" in file_ and ".dat" not in file_):
                files.append(file_)
        
        return choice(files)
    
    # ****************************************************************************************************
            
    def appendToFile(self,path,text):
        """
        Appends the provided text to the file at the specified path.
        
        Parameters:
        
        path    -    the path to the file to append the text to.
        text    -    the text to append.
        
        Returns:
        N/A
        """
        
        destinationFile = open(path,'a')
        destinationFile.write(str(text))
        destinationFile.close()
    
    # ****************************************************************************************************
    
    def fileExists(self,path):
        """
        Checks a file exists, returns true if it does, else false.
        
        Parameters:
        
        path    -    the path to the file to check the existence of.
        
        Returns:
        True if the file exists, else false.
        
        """
        
        try:
            fh = open(path)
            fh.close()
            return True
        except IOError:
            return False 
        
    # ****************************************************************************************************
    
if __name__ == '__main__':
    RandomCandidateGrabber().main()
