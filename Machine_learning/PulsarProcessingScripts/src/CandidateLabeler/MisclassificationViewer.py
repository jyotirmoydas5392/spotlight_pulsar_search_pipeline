"""
Views pulsar candidates classified by the AutomatedTreeTester.jar application.

By Rob Lyon <robert.lyon@cs.man.ac.uk>


+-----------------------------------------------------------------------------------------+
+                       PLEASE RECORD ANY MODIFICATIONS YOU MAKE BELOW                    +
+-----------------------------------------------------------------------------------------+
+ Revision |   Author    | Description                                       |    DATE    +
+-----------------------------------------------------------------------------------------+
 Revision:0    Rob Lyon    Initial version of the code.                        19/02/2014


"""

# Python 2.4 imports.
import datetime, sys, os, math
from optparse import OptionParser
from PIL import Image  # @UnresolvedImport - Ignore this comment, simply stops my IDE complaining.
import matplotlib.pyplot as plt
import matplotlib.image as mpimg

# ******************************
#
# CLASS DEFINITION
#
# ******************************

class MisclassificationViewer:
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
        input and begins to view the candidates.
    
        """
    
        # Python 2.4 argument processing.
        parser = OptionParser()

        # REQUIRED ARGUMENTS
        parser.add_option('-m', action="store", dest="path_missclassifications",type="string",help='The path to the file containing details of the misclassifications.',default="")
        
        # OPTIONAL ARGUMENTS
        parser.add_option('--width', action="store", dest="plot_width",type="float",help='The width of the viewing plot (default 10)',default=10)
        parser.add_option('--height', action="store", dest="plot_height",type="float",help='The height of the viewing plot (default 10)',default=8)

        (args,options) = parser.parse_args()# @UnusedVariable
        
        print "\n**************************************"
        print "|      Misclassification Viewer      |"
        print "**************************************"
        
        # Variables used by this class.
        self.path = args.path_missclassifications # Path to the file containing details of misclassifications.
        
        if(not os.path.isfile(self.path)):
            print "Input file path invalid, exiting."
            sys.exit()
            
        print "Input file: ",self.path
        
        self.width = math.ceil(args.plot_width)   # The width of the image viewing panel.
        self.height = math.ceil(args.plot_height) # The height of the image viewing panel.
    
        print "The supplied path is valid."           
        print "Attempting to view mislcassified candidates..."
        
        start = datetime.datetime.now()
        self.viewCandidates(self.path)
        end = datetime.datetime.now()
        print "Time taken: ", str(end - start)
        
        print "Done."

    # ******************************
    #
    # Filter Functions.
    #
    # ******************************
    
    def viewCandidates(self, path):
        """
        Searches a directory for ".png" files, representing candidates,
        and allows them to be labeled.
        
        """
        
        print "Reading in misclassifications."
        
        if(self.fileExists(path)==True):
            
            f = open(path,',"rU"') # Read only access
            
            # Process each candidate in the file.
            for line in f.readlines():

                contents=line.split(",")
                pngPath = contents[0]
                scores = contents[1:23]
                label = contents[23]
                missclassification = contents[24]

                # Now format information for user
                scoresFormatted=""
                count =1
                for s in scores:
                    scoresFormatted+="Score " + str(count) + " : " + str(s)+"\n"
                    count+=1
                
                cand=str(pngPath)
                cand=cand[cand.rfind('/')+1:]
                cand=cand.replace(".png", "")
                
                if(label=="1"):
                    label="POSITIVE"
                else:
                    label="NEGATIVE"
                    
                if("FP" in missclassification):
                    missclassification="False positive (predicted positive, actual negative)"
                else:
                    missclassification="False negative (predicted negative, actual positive)"
                
                print "\nCandidate: ",cand
                print scoresFormatted
                print "Label: ",label
                print "Why misclassified: ", missclassification, "\n"
                self.view(pngPath)
                             
                
            f.close()
    
    # ******************************
    # 
    # ******************************
                       
    def view(self,pngPath):
        """
        Views the candidate at the supplied path.
        
        Parameters:
        
        pngPath    -    the path to the image file representing a candidate.
        
        Returns:
        
        N/A
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
        while 'n' not in selection and 'x' not in selection:
            selection = raw_input('Type n for next pulsar (x for exit):\n')
         
        if 'n' in selection:
            print "Loading next."
        elif 'x' in selection:
            print "Exiting."
            sys.exit()
            
        plt.clf()
        plt.close()
        
        # Some nice formatting for the command line.
        print "\n+----------------------------------------+\n"
            
    # ******************************
    #
    # UTILITY FUNCTIONS.
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
    MisclassificationViewer().main()