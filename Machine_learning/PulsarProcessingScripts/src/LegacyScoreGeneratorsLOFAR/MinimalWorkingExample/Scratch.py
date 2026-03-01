"""
Experimental script.

Rob Lyon <robert.lyon@cs.man.ac.uk>

+-----------------------------------------------------------------------------------------+
+                       PLEASE RECORD ANY MODIFICATIONS YOU MAKE BELOW                    +
+-----------------------------------------------------------------------------------------+
+ Revision |   Author    | Description                                       |    DATE    +
+-----------------------------------------------------------------------------------------+

 Revision:0    Rob Lyon    Initial version of the re-written code.            06/02/2014
 
"""


# Command Line processing Imports:
from optparse import OptionParser

# Standard library Imports:
import sys
import struct

# ******************************
#
# CLASS DEFINITION
#
# ******************************

class Scratch:
    """                
    Used to execute small chunks of experimental code.
    
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
        self.debug=True
        
        print "\n***********************************"
        print "|       Executing Scratch         |"
        print "***********************************\n"
        
        """
        infile = open("/Users/rob/Documents/Aptana/PulsarProcessingScripts/lib/LOFAR_Candidate_Examples/L83017_SAP2_BEAM9_DM96.39_Z0_ACCEL_Cand_4.pfd", "rb")
        
        data = infile.read(5*4)
        print "Data:\n" , data
        
        swapchar = '<' # this is little-endian
        testswap = struct.unpack(swapchar+"i"*5, data)
        
        print "Testswap:\n", testswap
        
        (self.numdms, self.numperiods, self.numpdots, self.nsub, self.npart) =  struct.unpack(swapchar+"i"*5, data)
        
        (self.proflen, self.numchan, self.pstep, self.pdstep, self.dmstep, self.ndmfact, self.npfact) = struct.unpack(swapchar+"i"*7, infile.read(7*4))
        
        self.filenm = infile.read(struct.unpack(swapchar+"i", infile.read(4))[0])
        self.candnm = infile.read(struct.unpack(swapchar+"i", infile.read(4))[0])
        self.telescope = infile.read(struct.unpack(swapchar+"i", infile.read(4))[0])
        self.pgdev = infile.read(struct.unpack(swapchar+"i", infile.read(4))[0])
        
        print "numdms:\t",self.numdms
        print "numperiods:\t",self.numperiods
        print "numpdots:\t",self.numpdots
        print "nsub:\t",self.nsub
        print "npart:\t",self.npart
        
        print "proflen:\t",self.proflen
        print "numchan:\t",self.numchan
        print "pstep:\t",self.pstep
        print "pdstep:\t",self.pdstep
        print "dmstep:\t",self.dmstep
        print "ndmfact:\t",self.ndmfact
        print "npfact:\t",self.npfact
        
        print "filenm:\t",self.filenm
        print "candnm:\t",self.candnm
        print "telescope:\t",self.telescope
        print "pgdev:\t",self.pgdev"""
        
        msg1="Message 1: "
        msg2=" Message 2: "
        msg3=" Message 3: "
        value1=98.323
        value2=["34","35","36"]
        value3=[22,33,44,55]
        
        self.debugMutiple(msg1,value1,msg2,value2,msg3,value3)
        print "Done.\n"
        
    def debugMutiple(self,*parameters):
        """
        Writes a debug statement out if the debug flag is set to true.
        
        Parameters:
        message    -    the string message to write out
        parameter  -    an accompanying parameter to write out.
        
        Returns:
        N/A
        """
        
        if(self.debug):
            
            output =""
            for p in parameters:
                output+=str(p)
                
            print output
       
        
if __name__ == '__main__':
    Scratch().main()