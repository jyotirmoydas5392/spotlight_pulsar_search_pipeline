"""
This code runs on python 2.4 or later.

Based on source code provided by Sam Bates, Dan Thornton, Jenny Green, and Ben Stappers.

Re-implementation of the makeprofile.py code original used with the test_22.py file. This
code doesn't try to change the functionality of the original file, but simply tries to 
clean up the code making it more maintainbale.

By Rob Lyon <robert.lyon@cs.man.ac.uk>

+----------------------------------------------------------------------------------+
+                   PLEASE RECORD ANY MODIFICATIONS YOU MAKE BELOW                 +
+----------------------------------------------------------------------------------+
+ N |   Author    | Description                                       |    DATE    +
+----------------------------------------------------------------------------------+
+ 1 | Rob Lyon    | Initial version of the code.                      | 10/12/2013 +
+----------------------------------------------------------------------------------+
+                                                                                  +
+----------------------------------------------------------------------------------+

"""

# Python 2.4 imports.
import Common
import traceback
import sys

# ******************************
#
# CLASS DEFINITION
#
# ******************************

class Utilities(Common.Common):
    """
    Provides utility functions used when computing scores. Replaces the
    makeprofile.py file.
    
    """
    
    # ******************************
    #
    # Constructor.
    #
    # ******************************
    
    def __init__(self,debugFlag):
        Common.Common.__init__(self, debugFlag)
        
    # ******************************
    #
    # Functions.
    #
    # ******************************
    
    def test(self):
        print "Test worked."
    
    # ******************************************************************************************
        
    def listOut(self,name,xList,yList,candidateFilename):
        """
        Writes the contents of two lists to the specified file path.
        
        Parameters:
        name    -    the name used to describe the data being output.
        xList    -    the first list to write out.
        yList    -    the second list to write out (required).
        candidateFilename    -    the name of the candidate the data belongs to.
        
        Returns:
        N/A
        """
        
        # First check if either list is empty. If a list is empty, set its
        # length to the length of the non empty list. If BOTH are empty report
        # error and return.
        
        if xList == []:
            xList = range(len(yList)) 
        elif yList == []:
            yList = range(len(xList))
        
        # After error checking we can proceed...
        
        # If no candidate file name has been presented, then just use the name variable.
        # I've left this code like this (rather than doing stronger error checking or
        # finding the actual candidate name) to make sure I do not introduce bugs elsewhere.
        if candidateFilename == "":
            output = open(name+".data","w")
            for i in range(len(yList)):
                output.write(str(xList[i])+"\t"+str(yList[i])+"\n")
            output.close
        else:
            output = open(name+candidateFilename[:30]+".data","w")
            for i in range(len(yList)):
                output.write(str(xList[i])+"\t"+str(yList[i])+"\n")
            output.close
            
    # ******************************************************************************************
            
    def format_exception(self,e):
        """
        Formats error messages.
        
        Parameters:
        e    -    the exception.
        
        Returns:
        Tthe formatted exception string.
        """
        exception_list = traceback.format_stack()
        exception_list = exception_list[:-2]
        exception_list.extend(traceback.format_tb(sys.exc_info()[2]))
        exception_list.extend(traceback.format_exception_only(sys.exc_info()[0], sys.exc_info()[1]))
        
        exception_str = "Traceback (most recent call last):\n"
        exception_str += "".join(exception_list)
        
        # Removing the last \n
        exception_str = exception_str[:-1]
        
        return exception_str
    
    # ******************************************************************************************
            