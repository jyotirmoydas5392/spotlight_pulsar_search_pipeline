"""
This code runs on python 2.4 or later.

Rob Lyon <robert.lyon@cs.man.ac.uk>

+-----------------------------------------------------------------------------------------+
+                       PLEASE RECORD ANY MODIFICATIONS YOU MAKE BELOW                    +
+-----------------------------------------------------------------------------------------+
+ Revision |   Author    | Description                                       |    DATE    +
+-----------------------------------------------------------------------------------------+

"""
# Numpy Imports:
from numpy import array
from numpy import mean
from numpy import sqrt
from scipy.optimize import leastsq

# XML processing Imports:
from xml.dom import minidom

import matplotlib.pyplot as plt # Revision:1

# Standard library Imports:
import gzip,glob,sys,traceback,os

# ******************************
#
# CLASS DEFINITION
#
# ******************************

class ExtractProfile:
    
    # ******************************************************************************************
    
    def getDM_FFT(self,xmldata,section):
        """
        Extracts the DM curve from the DM data block in the phcx file.
        
        Parameters:
        xmldata    -    a numpy.ndarray containing the DM data in decimal format.
        section    -    the section of the xml file to find the DM data within. This
                        is required sine there are two DM sections in the phcx file.
        
        Returns:
        
        An array containing the DM curve data in decimal format.
        
        """
        
        # Extract data.
        dec_value = []
        block = xmldata.getElementsByTagName('DataBlock') # gets all of the bits with the title 'section'.
        points = block[section].childNodes[0].data
        
        # Transform data from hexadecimal to decimal values.
        x,y=0,0
        while x < len(points):
            if points[x] != "\n":
                try:
                    hex_value = points[x:x+2]
                    dec_value.append(int(hex_value,16)) # now the profile (shape, unscaled) is stored in dec_value
                    x = x+2
                    y = y+1
                except ValueError:
                    break
            else:
                x = x+1
                
        return dec_value
    
    # ******************************************************************************************
    
    def dm_curve(self,data):
        """
        Extracts the DM curve from the DM data block in the phcx file.
        
        Parameters:
        data    -    a numpy.ndarray containing the DM data.
        
        Returns:
        
        An array describing the curve.
        
        """
        
        result,x,temp = [],[],[]
        for i in range(len(data)):
            if (i+1)%128 == 0:
                result.append(max(temp))
                x.append(i - 128)
                temp = []
            else:
                temp.append(data[i])
        
        return array(result),array(x)
        
    # ******************************************************************************************
    
    def getDMCurve(self,data,section):
        """
        Returns a list of integer data points representing the candidate DM curve.
        
        Parameters:
        data    -    the xml data read in from the phcx file.
        section    -    A phcx file specific variable, used to identify the section of the xml data to read.
                        Value should be 1 for standard PHCX and 0 for SUPERB PHCX files.
        
        Returns:
        A list data type containing data points.
        
        """
        
        # Calculates the residuals.
        def __residuals(paras, x, y):     
            Amp,Prop,Shift = paras
            weff = sqrt(wint + pow(Prop*kdm*abs((self.dm + Shift)-x)*df/pow(f,3),2))
            SNR  = Amp*sqrt((self.period-weff)/weff)
            err  = y - SNR
            return err
        
        # Evaluates the function.
        def __evaluate(x, paras):
            Amp,Prop,Shift = paras
            weff = sqrt(wint + pow(Prop*kdm*abs((self.dm + Shift)-x)*df/pow(f,3),2))
            SNR  = Amp*sqrt((self.period-weff)/weff)
            return SNR
        
        self.debug = False
        self.snr = float(data.getElementsByTagName('Snr')[section].childNodes[0].data)
        self.dm = float(data.getElementsByTagName('Dm')[section].childNodes[0].data)
        self.period = float(data.getElementsByTagName('BaryPeriod')[section].childNodes[0].data) * 1000
        self.width = float(data.getElementsByTagName('Width')[section].childNodes[0].data)
        
        # Extract DM curve.
        dm_curve_all = array(self.getDM_FFT(data,section))
        curve = self.dm_curve(dm_curve_all)
        yData = curve[0]
        length_all = len(dm_curve_all)
        length = len(yData)
        
        # Extract x-scale for DM curve.
        read_data = list(data.getElementsByTagName('DmIndex')[section].childNodes[0].data)
        dm_index,temp = [],''
        for i in range(len(read_data)):
            if (read_data[i] != "\n"):
                temp += (read_data[i])
            else:
                dm_index.append(temp)
                temp = ''
                
        # Get start and end DM value and calculate step width.
        dm_start,dm_end = float(dm_index[1]),float(dm_index[len(dm_index)-1])
        dm_step = abs(dm_start-dm_end)/length_all
        
        # SNR and pulse parameters.
        wint = (self.width * self.period)**2
        kdm = 8.3*10**6
        df = 400
        f = 1374
        
        peak = self.snr/sqrt((self.period-sqrt(wint))/sqrt(wint))
        
        # Scale x-data.
        xData = []
        for i in range(length):
            xData.append(dm_start+curve[1][i]*dm_step)    
        xData = array(xData)
        
        # Calculate theoretic dm-curve from best values.
        _help = []
        for i in range(length):
            weff = sqrt(wint + pow(kdm*abs(self.dm-xData[i])*df/pow(f,3),2))
            SNR = sqrt((self.period-weff)/weff)
            _help.append(float(SNR))
            
        theo = (255./max(_help))*array(_help)
        
        # Start parameter for fit.
        Amp = (255./max(_help))
        Prop,Shift  = 1,0
        p0 = (Amp,Prop,Shift)
        plsq = leastsq(__residuals, p0, args=(xData,yData))
        fit = __evaluate(xData, plsq[0])
        
        if(self.debug):
            plt.plot(xData,fit,xData,yData,xData,theo)
            plt.title("DM Curve, theoretical curve and fit.")
            plt.legend( ('Fit to DM', 'DM', 'Theoretical') )
            plt.show()
            
        # Chi square calculation.
        chi_fit,chi_theo = 0,0
        for i in range(length):
            if fit[i] >= 1.:
                chi_fit  += (yData[i]-fit[i])**2
                chi_theo += (yData[i]-theo[i])**2
                
        chi_fit  =  chi_fit/length
        chi_theo = chi_theo/length
        
        diffBetweenFittingFactor = abs(1-plsq[0][1])
        diffBetweenBestAndOptimisedDM = plsq[0][2]
        
        return yData
    
    # ******************************************************************************************
          
    def getprofile(self,xmldata,profileIndex):
        """
        Returns a list of 128 integer data points representing a pulse profile.
        
        Parameters:
        xmldata    -    the xml data read in from the phcx file.
        profileIndex    -    index of the <Profile/> tag to read in the xml data.
        
        Returns:
        A list data type containing 128 integer data points.
        """
        # First obtain desired block of xml data.
        block = xmldata.getElementsByTagName('Profile')
        
        # Get raw hexadecimal data from the block
        points = block[profileIndex].childNodes[0].data
        
        # The format of the hexadecimal data is 02X, i.e. hexadecimal value with 2 digits.
        decimal_profile = []
        index = 0 # The index at which hexadecimal conversion will be performed.
        
        while index < len(points):
            if points[index] != "\n":
                try:
                    #ADDED
                    hex_value = points[index:index+2]
                    #print "Hex value:\t", hex_value
                    decimal_profile.append(int(hex_value,16)) # now the profile (shape, unscaled) is stored in dec_value
                    #print "Decimal value:\t",int(hex_value,16)
                    index = index+2 # Skip two characters to next hexadecimal number since format is 02X.
                except ValueError:
                    if points[index] =="\t":# There is a tab at the end of the xml data. So break the loop normally here.
                        break
                    else: # Unexpected error, report to user. 
                        print "Unexpected value error obtaining profile data."
                        break
            else:
                index = index+1
        return decimal_profile
    
    # ******************************************************************************************
          
    def getSubbands(self,xmldata,profileIndex):
        """
        Returns sub-band data.
        
        Parameters:
        xmldata    -    the xml data read in from the phcx file.
        profileIndex    -    index of the <Profile/> tag to read in the xml data.
        
        Returns:
        A list data type containing 128 integer data points.
        """
        
        block_bands = xmldata.getElementsByTagName('SubBands')
        frequency = block_bands[1].childNodes[0].data
        nbin_bands = int(block_bands[1].getAttribute("nBins"))
        nsub_bands = int(block_bands[1].getAttribute("nSub"))
        allbands = self.hexToDec(frequency, nsub_bands, nbin_bands)
        
        # OK so the allbands variable contains data of size 16x128
        data = array(allbands)
        # Post processing of data, convert to a single vector of data.
        sum_= [0] * 128 # empty array.
        for i in range(0, len(data)):
            
            #print "ROW:",i,"\n"
            row = array(data[i])
            #print row
            
            mean_ = mean(row)
            
            #print "Mean: ", mean_
            row = row - mean_
            #print "normalised row:\n",row
            sum_+=row
            
        #print "Summed data...\n"
        for i in range(0, len(sum_)):
            if sum_[i]<0:
                sum_[i]=0.0
                
        #print sum        
        #print "Returning sub-band data..."    
        return sum_
    
    # ******************************************************************************************
          
    def getSubInts(self,xmldata,profileIndex):
        """
        Returns sub integration data.
        
        Parameters:
        xmldata    -    the xml data read in from the phcx file.
        profileIndex    -    index of the <Profile/> tag to read in the xml data.
        
        Returns:
        A list data type containing 128 integer data points.
        """
        block_bands = xmldata.getElementsByTagName('SubIntegrations')
        frequency = block_bands[1].childNodes[0].data
        nbin_bands = int(block_bands[1].getAttribute("nBins"))
        nsub_bands = int(block_bands[1].getAttribute("nSub"))
        allInts = self.hexToDec(frequency, nsub_bands, nbin_bands)
        
        # OK so the allbands variable contains data of size 32x128
        data = array(allInts)
        # Post processing of data, convert to a single vector of data.
        sum_= [0] * 128 # empty array.
        for i in range(0, len(data)):
            
            #print "ROW:",i,"\n"
            row = array(data[i])
            #print row
            
            mean_ = mean(row)
            
            #print "Mean: ", mean_
            row = row - mean_
            #print "normalised row:\n",row
            sum_+=row
            
        #print "Summed data...\n"
        for i in range(0, len(sum_)):
            if sum_[i]<0:
                sum_[i]=0.0
                
        #print sum        
        #print "Returning sub-band data..."    
        return sum_
    
    
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
        
        print "\n*****************"
        print "| Executing code |"
        print "*****************"
        
        root="/Users/rob/Dropbox/ARFF/Pulsar/SCORE_DATA/NEW_SCORES/"
        
        # Files that store the data obtained.
        profileDataFile=root+"ProfileDataFile.txt"
        subbandDataFile=root+"SubBandDataFile.txt"
        subintDataFile=root+"SubIntDataFile.txt"
        dmDataFile=root+"DMDataFile.txt"
        
        # First we obtain details on which class each candidate belongs to.
        # This information is stored in two files, one containing a list
        # of positive candidates, the other the negatives.
        positiveCandidateFile=root+"PositivePHCXCandidates_1637.csv"
        negativeCandidateFile=root+"NegativePHCXCandidates_8896.csv"
        
        positives = []
        negatives = []
        
        # Here we read the .dat file which contains the scores. The scores
        # are all on a single line of the file, separated by a single space.
        if(self.fileExists(positiveCandidateFile)==True):
            
            f = open(positiveCandidateFile,'rU') # Read only access
    
            for line in f.readlines():
                positives.append(line.replace("\n",""))
        
            f.close()
            
        if(self.fileExists(negativeCandidateFile)==True):
            
            f = open(negativeCandidateFile,'rU') # Read only access
    
            for line in f.readlines():
                negatives.append(line.replace("\n",""))
        
            f.close()
            
        # Variables used to store stats on candidates viewed.
        candidatesProcessed = 0;
        successes = 0;
        failures = 0;
        positiveCount=0
        negativeCount=0
        
        
        for cand in glob.glob('*.phcx.gz'):
            
            candidatesProcessed+=1
            
            if(cand in positives):
                label=1
                positiveCount+=1
            else:
                label=0
                negativeCount+=1
            
            print "Processing candidate:\t" , cand , " LABEL: ",label 
            
            data = gzip.open(cand,'rb')
            xmldata = minidom.parse(data) # strip off xml data
            data.close()
            
            try:
                
                profileIndex = 1
                
                # ******************************
                #         Profile data
                # ******************************
    
                #profileData = self.getprofile(xmldata,profileIndex)
                
                # Simply convert list to an array data type (actually a numpy.ndarray):
                #p = array(profileData)
                
                #self.appendToFile(profileDataFile, str(cand)+",")
                #for intensity in p:
                    #self.appendToFile(profileDataFile, str(intensity)+",")
                
                #self.appendToFile(profileDataFile,str(label))
                #self.appendToFile(profileDataFile,"\n")
                
                # ******************************
                #         Sub-band data
                # ******************************
                
                #subbsandData = self.getSubbands(xmldata,profileIndex)
                
                # Simply convert list to an array data type (actually a numpy.ndarray):
                #p = array(subbsandData)
                
                #self.appendToFile(subbandDataFile, str(cand)+",")
                #for intensity in p:
                    #self.appendToFile(subbandDataFile, str(intensity)+",")
                    
                #self.appendToFile(subbandDataFile,str(label))
                #self.appendToFile(subbandDataFile,"\n")
                
                # ******************************
                #         Sub-int data
                # ******************************
                
                #subintData = self.getSubInts(xmldata,profileIndex)
                
                # Simply convert list to an array data type (actually a numpy.ndarray):
                #p = array(subintData)
                
                #self.appendToFile(subintDataFile, str(cand)+",")
                #for intensity in p:
                    #self.appendToFile(subintDataFile, str(intensity)+",")
                    
                #self.appendToFile(subintDataFile,str(label))
                #self.appendToFile(subintDataFile,"\n")
                
                # ******************************
                #         DM Curve data
                # ******************************
                
                dmData = self.getDMCurve(xmldata,1)
                
                # Simply convert list to an array data type (actually a numpy.ndarray):
                p = array(dmData)
                
                self.appendToFile(dmDataFile, str(cand)+",")
                for intensity in p:
                    self.appendToFile(dmDataFile, str(intensity)+",")
                    
                self.appendToFile(dmDataFile,str(label))
                self.appendToFile(dmDataFile,"\n")
                
                
            except Exception as e: # catch *all* exceptions
                print "Error reading profile data:\n\t", sys.exc_info()[0]
                print self.format_exception(e)
                print cand, " did not have scores generated."
                failures+=1
                continue
            
            successes+=1
            
        
        print "\nCandidates processed:\t",candidatesProcessed
        print "Successes:\t", successes
        print "Failures:\t", failures
        print "Positive count:\t", positiveCount
        print "Negative count:\t", negativeCount
        print "Done."
        
    # ******************************
    #
    # UTILITY METHODS
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
    
    # ******************************************************************************************
    
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
    
    # ******************************************************************************************
    
    def dirExists(self,path):
        """
        Checks a directory exists, returns true if it does, else false.
        
        Parameters:
        path    -    the path to the directory to look for.
        
        Returns:
        True if the file exists, else false.
        """
        
        try:
            if(os.path.isdir(path)):
                return True
            else:
                return False
        except IOError:
            return False
    
    # ******************************************************************************************
            
    def format_exception(self,e):
        """
        Formats error messages.
        
        Parameters:
        e    -    the exception.
        
        Returns:
        
        The formatted exception string.
        """
        exception_list = traceback.format_stack()
        exception_list = exception_list[:-2]
        exception_list.extend(traceback.format_tb(sys.exc_info()[2]))
        exception_list.extend(traceback.format_exception_only(sys.exc_info()[0], sys.exc_info()[1]))
        
        exception_str = "\nTraceback (most recent call last):\n"
        exception_str += "".join(exception_list)
        
        # Removing the last \n
        exception_str = exception_str[:-1]
        
        return exception_str
    
    # ******************************************************************************************
    
    def hexToDec(self,listData,nsub,nbin):
        """
        Converts hexadecimal data to decimal data.
        
        Parameters:
        list    -    a numpy.ndarray containing the DM data in hexadecimal format.
        nsub    -    number of sub-bands.
        nbin    -    number of bins.
        
        Returns:
        
        A list with the data in decimal format.            
        
        """
        x,y = 0,0
        newlist = []
        while x < len(listData):
            if listData[x] != "\n":
                try:
                    hexValue = listData[x:x+2]
                    newlist.append(int(hexValue,16))
                    x += 2
                    y += 1
                except ValueError:
                    break
            else:
                x += 1
                
        a = array(newlist).reshape(nsub,nbin)
        return a
    
    # ******************************************************************************************
    
if __name__ == '__main__':
    ExtractProfile().main()