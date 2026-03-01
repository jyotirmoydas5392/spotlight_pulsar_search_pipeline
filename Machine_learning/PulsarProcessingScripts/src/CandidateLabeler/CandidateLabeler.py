"""
Labels pulsar candidates based on a viewing of their PNG representation. This script
is mainly used to help produce labeled data for machine learning algorithms. For example
if the RandomCandidateGrabber.py script is used to collect a number of candidates, then
this script can be used to label them.

It takes approximately 1-10 seconds to label a candidate, so it can take some time to
label many. The application works as follows:

    1. Look for .gz files in the directory specified by the user. Each .gz file is a candidate
       and should be accompanied by a corresponding .dat and .png file.
    2. Request a label for each each candidate from the user.
    3. Once the user decides a label, obtain the candidate scores from the .dat file.
    4. Write the scores to the output file.
    5. Copy the .gz file to a specified user directory, delete the original .gz file.
    6. Copy the .png file to a specified user directory, delete the original .png file.
    7. Delete the original .dat file since it is no longer needed - ScoreGenerator.py
       can be used to generate new and improved .dat files.
       
    Steps 5 and 6 above make it easier to maintain the labeled data we obtain, since
    we can collect all labeled .gz files in a single directory, ditto for the image files.
    We don't throw either of these files away, since the .gz files can be used to generate
    new .dat files, and the png files can be used to understand the scores generated.


This application runs on python 2.4 or later.

By Rob Lyon <robert.lyon@cs.man.ac.uk>

Simple usage example:

    python CandidateLabeler -o Data.csv
    
    Will begin labeling candidates in the same directory as the script, will write the results to the file Data.csv.

+-----------------------------------------------------------------------------------------+
+                       PLEASE RECORD ANY MODIFICATIONS YOU MAKE BELOW                    +
+-----------------------------------------------------------------------------------------+
+ Revision |   Author    | Description                                       |    DATE    +
+-----------------------------------------------------------------------------------------+
 Revision:0    Rob Lyon    Initial version of the code.                        14/02/2014


"""

# Python 2.4 imports.
import datetime, math, os, shutil
from optparse import OptionParser
from PIL import Image  # @UnresolvedImport - Ignore this comment, simply stops my IDE complaining.
import matplotlib.pyplot as plt
import matplotlib.image as mpimg

# ******************************
#
# CLASS DEFINITION
#
# ******************************

class CandidateLabeler:
    """
    Labels candidates using user input.
    
    """
    
    # ******************************
    #
    # MAIN METHOD AND ENTRY POINT.
    #
    # ******************************

    def main(self,argv=None):
        """
        Main entry point for the Application. Processes command line
        input and begins labeling the candidates.
    
        """
    
        # Python 2.4 argument processing.
        parser = OptionParser()

        # REQUIRED ARGUMENTS
        parser.add_option('-o', action="store", dest="path_output",type="string",help='The path to the output file where results will be written to (required).',default="")
        
        # OPTIONAL ARGUMENTS
        parser.add_option('-i', action="store", dest="image_output",type="string",help='The path to the directory where phcx images will be stored (optional).',default="")
        parser.add_option('-g', action="store", dest="gz_output",type="string",help='The path to the directory where raw candidate data will be stored (optional).',default="")
        parser.add_option('-f', action="store", dest="path_png",type="string",help='The path to the folder containing PNG candidates to label (optional).',default="")
        parser.add_option('--width', action="store", dest="plot_width",type="float",help='The width of the viewing plot (default 10)',default=10)
        parser.add_option('--height', action="store", dest="plot_height",type="float",help='The height of the viewing plot (default 10)',default=8)

        (args,options) = parser.parse_args()# @UnusedVariable
        
        print "\n************************************"
        print "|      Candidate Labeler Code      |"
        print "************************************"
        
        # Variables used by this class.
        self.path = args.path_png      # Path to the directory containing PNGs.
        self.output = args.path_output # Path to the directory where results should be written.
        self.gzDir=args.gz_output      # Path to where raw data will be stored post labeling.
        self.imageDir=args.image_output# Path to where png image data will be stored post labeling.
        
        if(not os.path.isdir(self.path)):
            self.path=os.path.dirname(os.path.realpath(__file__))
            
        if(not os.path.isdir(self.gzDir)):
            # Set to default - this must be changed on your system!
            self.gzDir="/Users/rob/Documents/Aptana/PulsarProcessingScripts/lib/newphcx"
            
        if(not os.path.isdir(self.imageDir)):
            self.imageDir=self.gzDir+"/"+"Images"
            
        print "Output Path: ",self.output
        print "Candidate Directory: ",self.path
        
        self.POSITIVE_FLAG = 1;          # The flag used to indicate a positive classification.
        self.NEGATIVE_FLAG = 0;          # The flag used to indicate a negative classification.
        self.positive = 0;               # Counts the number of positive labeled examples.
        self.negative = 0;               # Counts the number of negative labeled examples.

        
        self.width = math.ceil(args.plot_width)   # The width of the image viewing panel.
        self.height = math.ceil(args.plot_height) # The height of the image viewing panel.
        self.totalLabelled = 1                    # The total number of candidates labeled.
        
        # This is a boolean flag that if set to true, will copy each classified
        # image to a sub folder. In other words, those PNGs classified positive
        # will be copied to a POSITIVE sub folder, the negatives to a NEGATIVE
        # sub folder and the same for positives.
        self.exitEarly = False
    
        if os.path.isdir(self.path):
            print "The supplied paths are valid."           
            print "Attempting to label candidates..."
            
            # Check output files exists, else create them.
            if not self.fileExists(self.output):
                # Now create clean empty file.
                tmpFile = open(self.output, 'w')
                tmpFile.write('')
                tmpFile.close()
            
            start = datetime.datetime.now()
            self.viewCandidates(self.path)
            end = datetime.datetime.now()
            
            print "\n\nTotal Candidates Labelled : ", str(self.totalLabelled-1)
            print "Total Labelled Positive   : ", self.positive
            print "Total Labelled Negative   : ", self.negative
            print "Time taken: ", str(end - start)
            
        
        else:
            print "Command line parameters invalid.\nBe sure that the specified files and directories exist."
        
        print "Done."

    # ******************************
    #
    # Filter Functions.
    #
    # ******************************
    
    def viewCandidates(self, directoryPath):
        """
        Searches a directory for ".png" files, representing candidates,
        and allows them to be labeled.
        
        """
        
        print "Looking in specified directory for candidates."
        
        images = []
        # Search the supplied directory recursively.    
        for root, directories, files in os.walk(directoryPath):  # @UnusedVariable
            
            for file_ in files:
                
                if file_.endswith('.png'):
                    
                    images.append(os.path.join(root, file_))
                    
        print "Candidate images found: ",str(len(images))
        
        selection = raw_input('Continue to label all these? (Y/N)\n')
        
        if selection.startswith("y") or selection.startswith("Y"):
            
            print "Proceeding to label..."
            
            count = 1
            for img in images:
                
                self.getUserLabel(img)
                
                # Check if user wants to stop labeling part way through.
                if(self.exitEarly):
                    print "Exiting early..."
                    return 0
            count += 1
            
        elif selection.startswith("n") or selection.startswith("N"):
            print "Cancel labeling..."
            return 0
        else:
            print "Invalid response, canceling."
    
    # ******************************
    # 
    # ******************************
                       
    def getUserLabel(self,pngPath):
        """
        Request that the user label a candidate.
        
        """
        
        #print "\nRequesting user label for: ", pngPath, "\n"
        
        fig=plt.figure(figsize=(self.width,self.height))# @UnusedVariable
        plt.ion() 
             
        candidateImage = mpimg.imread(pngPath)
        plt.imshow(candidateImage, aspect='auto')
        plt.show()
        
        # Wait for user input. If an invalid input
        # is provided, then prompt again.
        selection = ""
        while 'p' not in selection and 'n' not in selection and 'm' not in selection and 'x' not in selection:
            selection = raw_input('Type p for known pulsar, n for not pulsar (x for exit):\n')
         
        if 'p' in selection:
            self.positive+=1
            self.addToTrainingSet(pngPath,self.POSITIVE_FLAG)
        elif 'n' in selection:
            self.negative+=1
            self.addToTrainingSet(pngPath,self.NEGATIVE_FLAG)
        elif 'x' in selection:
            print "Exiting"
            self.exitEarly = True
            
        plt.clf()
        plt.close()
        
        # Some nice formatting for the command line.
        print "\n+----------------------------------------+\n"
        
        
    # ******************************
    #
    # UTILITY FUNCTIONS.
    #
    # ******************************
        
    def addToTrainingSet(self,imgPath,classification):
        """
        Adds a user classified candidate to a data file.
        
        Parameters:
        
        imgPath           -    the path to the candidates png file.
        classification    -    the classification assigned to the candidate.
        
        Returns:
        N/A
        
        """
        
        datFile = imgPath.replace(".png",".dat")
        
        if(classification == self.POSITIVE_FLAG):
            label = "POSITIVE"
        else:
            label = "NEGATIVE"
        
        gzFile=str(os.path.basename(datFile)).replace(".dat", "")
        gzSource=str(datFile).replace(".dat", "")
        gzDestination=self.gzDir+"/"+gzFile
        imageDestination=self.imageDir+"/"+gzFile+".png"
        
        print "Checking if: ",gzSource, " is at: ",gzDestination
        print "Checking if: ",imgPath, " is at: ",imageDestination
        if(not self.fileExists(gzDestination)):# If the file hasn't been previously processed.
                
            if(self.fileExists(datFile)):
                    
                # Construct the input pattern.
                fileName = os.path.basename(datFile)
                scores = self.getScoresForCandidate(datFile)
    
                entry = str(fileName) + "," + label + ",?,?," + scores+"\n"  
                    
                self.appendToFile(self.output,entry)
                
                # Copy Raw candidate data to destination directory.
                shutil.copyfile(gzSource,gzDestination)
                shutil.copyfile(imgPath,imageDestination)
                
                os.remove(gzSource)
                os.remove(imgPath)
                os.remove(datFile)
            else:
                print "ERROR: .dat file does not exist: "+datFile
        else:
            print "Processed previously: "+datFile
    
    # ******************************
    # 
    # ******************************
            
    def getScoresForCandidate(self,scoreFile):
        """
        Gets the scores from a score file as a string value.
        
        """
        
        contents = open(scoreFile,'rU').read()
        
        if(contents is not None):
            return str(contents).replace(" ", ",")
        else:
            return None
        
        
    # ******************************
    # 
    # ******************************
           
    def appendToFile(self,path,text):
        """
        Appends the provided text to the file at the specified path.
        
        """
        destinationFile = open(path,'a')
        destinationFile.write(str(text))
        destinationFile.close()
        self.totalLabelled +=1
        
    # ******************************
    #
    # FILE TYPE CHECKS.
    #
    # ******************************
    
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
    CandidateLabeler().main()