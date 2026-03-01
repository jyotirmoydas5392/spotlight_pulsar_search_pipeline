"""
Executes the unit test code.

Rob Lyon <robert.lyon@cs.man.ac.uk>

+-----------------------------------------------------------------------------------------+
+                       PLEASE RECORD ANY MODIFICATIONS YOU MAKE BELOW                    +
+-----------------------------------------------------------------------------------------+
+ Revision |   Author    | Description                                       |    DATE    +
+-----------------------------------------------------------------------------------------+
 Revision:0    Rob Lyon    Initial version of the code                         03/02/2014

"""

# Standard library Imports:

# Custom file Imports:
import Utilities
from PHCXOperationsTester import PHCXTester

# ******************************
#
# CLASS DEFINITION
#
# ******************************

class UnitTester(Utilities.Utilities):
    """                
    Runs the unit tests.
    
    """
    
    # ******************************
    #
    # Constructor.
    #
    # ******************************
    
    def __init__(self,debugFlag):
        
        Utilities.Utilities.__init__(self, debugFlag)
        self.epsilon = 0.000005 # Used during score comparison.
        
    # ******************************
    #
    # MAIN METHOD AND ENTRY POINT.
    #
    # ******************************

    def main(self,argv=None):
        """
        Main entry point for the Application. Processes command line
        input and begins creating the scores.
        
        Parameters:
        
        originalScoresFile    -    The path to the file containing data on the original value
                                   of the scores.
        """
        
        print "\n*************************************"
        print "| Unit testing score generation code |"
        print "*************************************"
        
        print "Unit Tests...."
        phcxTester = PHCXTester(self.debug)
        phcxTester.testSinusoidFittings(None)
        print "Done."
        
        
if __name__ == '__main__':
    UnitTester(True).main()