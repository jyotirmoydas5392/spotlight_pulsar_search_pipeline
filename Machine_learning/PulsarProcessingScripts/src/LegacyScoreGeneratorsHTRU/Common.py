"""
This code runs on python 2.4 or later.

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
# N/A

# ******************************
#
# CLASS DEFINITION
#
# ******************************

class Common(object):
    """
    Contains commonly used functions for file IO etc. These may not be used right
    now, but could be useful in the future. This class is also used as a base object 
    in a class heirarchy, supporting the ScoreUtilities and ProfileHelper classes.
    I've made the code object-orientated in this way to make the code more maintainable
    and easier to modularise. 
    
    """
    
    # ******************************
    #
    # Constructor.
    #
    # ******************************
    
    def __init__(self,debugFlag):
        self.debug = debugFlag
    
    # ******************************
    #
    # Functions.
    #
    # ******************************
    
    def appendToFile(self,path,text):
        """
        Appends the provided text to the file at the specified path.
        
        Parameters:
        path    -    the path to the file to append text to.
        text    -    the text to append to the file.
        
        Returns:
        N/A
        """
        
        destinationFile = open(path,'a')
        destinationFile.write(str(text))
        destinationFile.close()
    
    # ******************************
    
    def fileExists(self,path):
        """
        Checks a file exists, returns true if it does, else false.
        
        Parameters:
        path    -    the path to the file to look for.
        
        Returns:
        True if the file exists, else false.
        """
        
        try:
            fh = open(path)
            fh.close()
            return True
        except IOError:
            return False