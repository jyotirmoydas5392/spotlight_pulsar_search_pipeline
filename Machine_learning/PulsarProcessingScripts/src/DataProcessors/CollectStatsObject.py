"""
    ==========================================================================================
    | Used to store SNR, DM and Period statistics for HTRU data.                             |
    | Maintains the following statistics for snr, dm and period:                             |
    |                                                                                        |
    | min, max, mean, median, Q1, Q3, IQR, range, var, stdev, sum, count                     |
    |                                                                                        |
    | Here count is simply the number of candidates found for an observation. From this you  |
    | could recompute the average yourself doing avg = sum/count, this is for error checking |
    | purposes only.                                                                         |
    |                                                                                        |
    | NOTES:                                                                                 |
    |                                                                                        |
    | Column indexes for output data...                                                      |
    |                                                                                        |
    |  1.  day          2. month       3. year        4. hour          5. min                |
    |                                                                                        |
    |  6.  min snr      7. max snr     8. mean snr    9. median snr   10. Q1 snr             |
    | 11. Q3 snr       12. IQR snr    13. range snr  14. var snr      15. stdev snr          |
    | 16. sum snr      17. skew snr   18. kurt snr   19. p1 snr       20. p1 snr p-value     |
    | 21. p2 snr       22. p2 snr p-value            23. count snr                           |
    |                                                                                        |
    | 24. min dm       25.  max dm    26. mean dm    27. median dm    28. Q1 dm              |
    | 29. Q3 dm        30. IQR dm     31. range dm   32. var dm       33. stdev dm           |
    | 34. sum dm       35. skew dm    36. kurt dm    37. p1 dm        38. p1 dm p-value      |
    | 39. p2 dm        40. p2 dm p-value             41. count dm                            |
    |                                                                                        |
    |                                                                                        |
    | 42. min p0       43.  max p0    44. mean p0    45. median p0    46. Q1 p0              |
    | 47. Q3 p0        48. IQR p0     49. range p0   50. var p0       51. stdev p0           |
    | 52. sum p0       53. skew p0    54. kurt p0    55. p1 p0        56. p1 p0 p-value      |
    | 57. p2 p0        58. p2 p0 p-value             59. count dm     60. Date               |
    |                                                                                        |
    | 61. beam number * only in beam file.                                                   |
    |                                                                                        |
    | Rob Lyon <robert.lyon@cs.man.ac.uk>                                                    |
    ==========================================================================================
 
"""

# Standard library Imports:
import sys
import numpy as np
from math import floor
from scipy.stats import skew
from scipy.stats import kurtosis
from scipy.stats.stats import pearsonr
from datetime import datetime
from Utilities import Utilities

# ******************************
#
# CLASS DEFINITION
#
# ******************************

class CollectStatsObject(Utilities):
    """                
    
    """
    
    # ******************************
    #
    # Constructor.
    #
    # ******************************
    def __init__(self,debugFlag,):
        """
        
        """
        Utilities.__init__(self,debugFlag)
        
        # Here we set up the dictionaries to store all
        # the stats we need.
        self.snrDictionary = {}
        self.dmDictionary  = {}
        self.p0Dictionary  = {}
        
        self.snr = []
        self.dm  = []
        self.p0  = []
        
        self.buildVariables()
        
        self.day   = 0
        self.month = 0
        self.year  = 0
        self.hour  = 0
        self.min   = 0
        self.sec   = 0
        
        self.beam  = -1
        
        self.date  = None
        self.saved = False
        
        self.snr_dist = [0]*1001
        
    # ******************************
    #
    # Functions.
    #
    # ******************************
    
    def computeObservationTime(self,candidatePath):
        """
        """
        
        # Check if the date and time is known...
        if (self.day == 0 and self.month == 0 and self.year == 0):
            
            # We are not sure of this objects observation time.
            # Thus it must be calculated. Here is an example of a
            # candidate file path that contains this information:
            #
            #/local/scratch/cands/2008-11/2008-11-24-06:37:34/06/2008-11-24-06:37:34.06.fil_sigproc_076.phcx.gz
            #
            # We need to extract this information from the string.
            
            attempt_1 = candidatePath.replace("/local/scratch/cands/","")
            # Ok now we should have something resembling:
            # 2008-11/2008-11-24-06:37:34/06/2008-11-24-06:37:34.06.fil_sigproc_076.phcx.gz
            
            lastOccuranceOfSlash = attempt_1.rfind("/")
            attempt_2 = attempt_1[lastOccuranceOfSlash+1:len(attempt_1)]
            # Ok now we should have something resembling:
            # 2008-11-24-06:37:34.06.fil_sigproc_076.phcx.gz
            
            attempt_3 = attempt_2.replace(".phcx.gz","")
            # Ok now we should have something resembling:
            # 2008-11-24-06:37:34.06.fil_sigproc_076
            
            lastOccuranceOfDot = attempt_3.rfind(".")
            attempt_4 = attempt_3[0:lastOccuranceOfDot+1]
            # Ok now we should have something resembling:
            # 2008-11-24-06:37:34.06.
            
            attempt_5 = attempt_4.replace(".","-")
            # Ok now we should have something resembling:
            # 2008-11-24-06:37:34-06-
            
            attempt_6 = attempt_5.replace(":","-")
            # Ok now we should have something resembling:
            # 2008-11-24-06-37-34-06-
            
            components = attempt_6.split("-")
            
            # Ok so now we should have the following in the components variable:
            # 
            #     0     1    2    3    4    5    6    7
            # [ [2008],[11],[24],[06],[37],[34],[06],[] ]
            
            self.year  = int(components[0])
            self.month = int(components[1])
            self.day   = int(components[2])
            self.hour  = int(components[3])
            self.min   = int(components[4])
            self.sec   = int(components[5])
            
            self.date = datetime(self.year, self.month, self.day, self.hour, self.min, self.sec)
            
            try:
                self.beam  = int(components[6])
            except:
                self.beam = 0
        else:
            # We know the observation time of this object, so do nothing.
            pass
    
    # ****************************************************************************************************
    
    def computeStats(self):
        """
        """
        
        snr = np.array(self.snr)
        dm  = np.array(self.dm)
        p0  = np.array(self.p0)
        
        self.snrDictionary['min']        = np.min(snr)
        self.snrDictionary['max']        = np.max(snr)
        self.snrDictionary['mean']       = np.mean(snr)
        self.snrDictionary['median']     = np.median(snr)
        self.snrDictionary['Q1']         = np.percentile(snr, 25)
        self.snrDictionary['Q3']         = np.percentile(snr, 75)
        self.snrDictionary['range']      = self.snrDictionary.get('max') - self.snrDictionary.get('min')
        self.snrDictionary['iqr']        = self.snrDictionary.get('Q3') - self.snrDictionary.get('Q1')
        self.snrDictionary['var']        = np.var(snr)
        self.snrDictionary['stdev']      = np.std(snr)
        self.snrDictionary['skew']       = skew(snr)
        self.snrDictionary['kurtosis']   = kurtosis(snr)
        self.snrDictionary['pearson_dm'],self.snrDictionary['pearson_dm_p'] = pearsonr(snr, dm)
        self.snrDictionary['pearson_p0'],self.snrDictionary['pearson_p0_p'] = pearsonr(snr, p0)
        
        self.dmDictionary['min']        = np.min(dm)
        self.dmDictionary['max']        = np.max(dm)
        self.dmDictionary['mean']       = np.mean(dm)
        self.dmDictionary['median']     = np.median(dm)
        self.dmDictionary['Q1']         = np.percentile(dm, 25)
        self.dmDictionary['Q3']         = np.percentile(dm, 75)
        self.dmDictionary['range']      = self.dmDictionary.get('max') - self.dmDictionary.get('min')
        self.dmDictionary['iqr']        = self.dmDictionary.get('Q3') - self.dmDictionary.get('Q1')
        self.dmDictionary['var']        = np.var(dm)
        self.dmDictionary['stdev']      = np.std(dm)
        self.dmDictionary['skew']       = skew(dm)
        self.dmDictionary['kurtosis']   = kurtosis(dm)
        self.dmDictionary['pearson_snr'], self.dmDictionary['pearson_snr_p'] = pearsonr(dm, snr)
        self.dmDictionary['pearson_p0'] , self.dmDictionary['pearson_p0_p'] = pearsonr(dm, p0)
        
        self.p0Dictionary['min']        = np.min(p0)
        self.p0Dictionary['max']        = np.max(p0)
        self.p0Dictionary['mean']       = np.mean(p0)
        self.p0Dictionary['median']     = np.median(p0)
        self.p0Dictionary['Q1']         = np.percentile(p0, 25)
        self.p0Dictionary['Q3']         = np.percentile(p0, 75)
        self.p0Dictionary['range']      = self.p0Dictionary.get('max') - self.p0Dictionary.get('min')
        self.p0Dictionary['iqr']        = self.p0Dictionary.get('Q3') - self.p0Dictionary.get('Q1')
        self.p0Dictionary['var']        = np.var(p0)
        self.p0Dictionary['stdev']      = np.std(p0)
        self.p0Dictionary['skew']       = skew(p0)
        self.p0Dictionary['kurtosis']   = kurtosis(p0)
        self.p0Dictionary['pearson_snr'],self.p0Dictionary['pearson_snr_p'] = pearsonr(p0, snr)
        self.p0Dictionary['pearson_dm'] ,self.p0Dictionary['pearson_dm_p'] = pearsonr(p0, dm)
        
    # ****************************************************************************************************
            
    def partOfSameObservation(self,candidatePath):
        """
        """
        # We are not sure of this objects observation time.
        # Thus it must be calculated. Here is an example of a
        # candidate file path that contains this information:
        #
        #/local/scratch/cands/2008-11/2008-11-24-06:37:34/06/2008-11-24-06:37:34.06.fil_sigproc_076.phcx.gz
        #
        # We need to extract this information from the string.
        
        attempt_1 = candidatePath.replace("/local/scratch/cands/","")
        # Ok now we should have something resembling:
        # 2008-11/2008-11-24-06:37:34/06/2008-11-24-06:37:34.06.fil_sigproc_076.phcx.gz
        
        lastOccuranceOfSlash = attempt_1.rfind("/")
        attempt_2 = attempt_1[lastOccuranceOfSlash+1:len(attempt_1)]
        # Ok now we should have something resembling:
        # 2008-11-24-06:37:34.06.fil_sigproc_076.phcx.gz
        
        attempt_3 = attempt_2.replace(".phcx.gz","")
        # Ok now we should have something resembling:
        # 2008-11-24-06:37:34.06.fil_sigproc_076
        
        lastOccuranceOfDot = attempt_3.rfind(".")
        attempt_4 = attempt_3[0:lastOccuranceOfDot+1]
        # Ok now we should have something resembling:
        # 2008-11-24-06:37:34.06.
        
        attempt_5 = attempt_4.replace(".","-")
        # Ok now we should have something resembling:
        # 2008-11-24-06:37:34-06-
        
        attempt_6 = attempt_5.replace(":","-")
        # Ok now we should have something resembling:
        # 2008-11-24-06-37-34-06-
        
        components = attempt_6.split("-")
        
        # Ok so now we should have the following in the components variable:
        # 
        #     0     1    2    3    4    5    6    7
        # [ [2008],[11],[24],[06],[37],[34],[06],[] ]
        
        year  = int(components[0])
        month = int(components[1])
        day   = int(components[2])
        hour  = int(components[3])
        min   = int(components[4])
        sec   = int(components[5])
        
        if (self.year == year and self.month == month and self.day == day and
            self.hour == hour and self.min   == min   and self.sec == sec):
            return True
        else:
            return False
        
    # ****************************************************************************************************
        
    def partOfSameBeam(self,candidatePath):
        """
        """
        
        # We are not sure of this objects observation time.
        # Thus it must be calculated. Here is an example of a
        # candidate file path that contains this information:
        #
        #/local/scratch/cands/2008-11/2008-11-24-06:37:34/06/2008-11-24-06:37:34.06.fil_sigproc_076.phcx.gz
        #
        # We need to extract this information from the string.
        
        attempt_1 = candidatePath.replace("/local/scratch/cands/","")
        # Ok now we should have something resembling:
        # 2008-11/2008-11-24-06:37:34/06/2008-11-24-06:37:34.06.fil_sigproc_076.phcx.gz
        
        lastOccuranceOfSlash = attempt_1.rfind("/")
        attempt_2 = attempt_1[lastOccuranceOfSlash+1:len(attempt_1)]
        # Ok now we should have something resembling:
        # 2008-11-24-06:37:34.06.fil_sigproc_076.phcx.gz
        
        attempt_3 = attempt_2.replace(".phcx.gz","")
        # Ok now we should have something resembling:
        # 2008-11-24-06:37:34.06.fil_sigproc_076
        
        lastOccuranceOfDot = attempt_3.rfind(".")
        attempt_4 = attempt_3[0:lastOccuranceOfDot+1]
        # Ok now we should have something resembling:
        # 2008-11-24-06:37:34.06.
        
        attempt_5 = attempt_4.replace(".","-")
        # Ok now we should have something resembling:
        # 2008-11-24-06:37:34-06-
        
        attempt_6 = attempt_5.replace(":","-")
        # Ok now we should have something resembling:
        # 2008-11-24-06-37-34-06-
        
        components = attempt_6.split("-")
        
        # Ok so now we should have the following in the components variable:
        # 
        #     0     1    2    3    4    5    6    7
        # [ [2008],[11],[24],[06],[37],[34],[06],[] ]
        
        year  = int(components[0])
        month = int(components[1])
        day   = int(components[2])
        hour  = int(components[3])
        min   = int(components[4])
        sec   = int(components[5])
        beam  = 0
        
        try:
            beam  = int(components[6])
        except:
            return -1
        
        if (self.year == year and self.month == month and self.day == day and
            self.hour == hour and self.min   == min   and self.sec == sec and self.beam == beam):
            return 1
        else:
            return 0
            
    # ****************************************************************************************************
    
    def update(self,s,d,p):
        """
        """
        
        snr    = float(s)
        dm     = float(d)
        period = float(p)
        
        self.snr.append(snr)
        self.dm.append(dm)
        self.p0.append(period)
        
        current_snr_sum   = self.snrDictionary.get('sum')
        current_snr_count = self.snrDictionary.get('count')
        self.snrDictionary['sum']   = float(current_snr_sum + snr)
        self.snrDictionary['count'] = current_snr_count+1
        
        current_dm_sum   = self.dmDictionary.get('sum')
        current_dm_count = self.dmDictionary.get('count')
        self.dmDictionary['sum']   = float(current_dm_sum + dm)
        self.dmDictionary['count'] = current_dm_count+1
        
        current_p0_sum   = self.p0Dictionary.get('sum')
        current_p0_count = self.p0Dictionary.get('count')
        self.p0Dictionary['sum']   = float(current_p0_sum + period)
        self.p0Dictionary['count'] = current_p0_count+1
        
    def SNR_update(self,s,d,p):
        """
        """
        
        # This basic records SNR frequencies, for SNR 0-1000.
        snr    = float(s)
        dm     = float(d)
        period = float(p)

        value = int(floor(snr))
        
        if(value <=1000):
            self.snr_dist[value]+=1
        else:
            self.snr_dist[1000]+=1
        
    # ******************************
    #
    # Output and variable updates.
    #
    # ******************************
    
    
    def toStringBeam(self):
        """
        """
        
        var_1   = str(self.day) +","+ str(self.month)+","+ str(self.year)+","+ str(self.hour)+","+ str(self.min) + ","
        
        var_snr = str(self.snrDictionary.get('min')) +","+ str(self.snrDictionary.get('max'))          +","+ str(self.snrDictionary.get('mean'))         +","+\
        str(self.snrDictionary.get('median'))        +","+ str(self.snrDictionary.get('Q1'))           +","+ str(self.snrDictionary.get('Q3'))           +","+\
        str(self.snrDictionary.get('iqr'))           +","+ str(self.snrDictionary.get('range'))        +","+ str(self.snrDictionary.get('var'))          +","+\
        str(self.snrDictionary.get('stdev'))         +","+ str(self.snrDictionary.get('sum'))          +","+ str(self.snrDictionary.get('skew'))         +","+\
        str(self.snrDictionary.get('kurtosis'))      +","+ str(self.snrDictionary.get('pearson_dm'))   +","+ str(self.snrDictionary.get('pearson_dm_p')) +","+\
        str(self.snrDictionary.get('pearson_p0'))    +","+ str(self.snrDictionary.get('pearson_p0_p')) +","+ str(self.snrDictionary.get('count'))+","
        
        var_dm = str(self.dmDictionary.get('min'))  +","+ str(self.dmDictionary.get('max'))          +","+ str(self.dmDictionary.get('mean'))         +","+\
        str(self.dmDictionary.get('median'))        +","+ str(self.dmDictionary.get('Q1'))           +","+ str(self.dmDictionary.get('Q3'))           +","+\
        str(self.dmDictionary.get('iqr'))           +","+ str(self.dmDictionary.get('range'))        +","+ str(self.dmDictionary.get('var'))          +","+\
        str(self.dmDictionary.get('stdev'))         +","+ str(self.dmDictionary.get('sum'))          +","+ str(self.dmDictionary.get('skew'))         +","+\
        str(self.dmDictionary.get('kurtosis'))      +","+ str(self.dmDictionary.get('pearson_snr'))   +","+ str(self.dmDictionary.get('pearson_snr_p')) +","+\
        str(self.dmDictionary.get('pearson_p0'))    +","+ str(self.dmDictionary.get('pearson_p0_p')) +","+ str(self.dmDictionary.get('count'))+","
        
        var_p0 = str(self.p0Dictionary.get('min'))  +","+ str(self.p0Dictionary.get('max'))          +","+ str(self.p0Dictionary.get('mean'))         +","+\
        str(self.p0Dictionary.get('median'))        +","+ str(self.p0Dictionary.get('Q1'))           +","+ str(self.p0Dictionary.get('Q3'))           +","+\
        str(self.p0Dictionary.get('iqr'))           +","+ str(self.p0Dictionary.get('range'))        +","+ str(self.p0Dictionary.get('var'))          +","+\
        str(self.p0Dictionary.get('stdev'))         +","+ str(self.p0Dictionary.get('sum'))          +","+ str(self.p0Dictionary.get('skew'))         +","+\
        str(self.p0Dictionary.get('kurtosis'))      +","+ str(self.p0Dictionary.get('pearson_snr'))   +","+ str(self.p0Dictionary.get('pearson_snr_p')) +","+\
        str(self.p0Dictionary.get('pearson_dm'))    +","+ str(self.p0Dictionary.get('pearson_dm_p')) +","+ str(self.p0Dictionary.get('count'))+","
        
        return var_1 + var_snr + var_dm + var_p0 + str(self.beam) + "," + self.date.strftime("%d/%m/%Y %I:%M:%S") +"\n"
    
    # ****************************************************************************************************
    
    def toStringObs(self):
        """
        """
        var_1   = str(self.day) +","+ str(self.month)+","+ str(self.year)+","+ str(self.hour)+","+ str(self.min) + ","
        
        var_snr = str(self.snrDictionary.get('min')) +","+ str(self.snrDictionary.get('max'))          +","+ str(self.snrDictionary.get('mean'))         +","+\
        str(self.snrDictionary.get('median'))        +","+ str(self.snrDictionary.get('Q1'))           +","+ str(self.snrDictionary.get('Q3'))           +","+\
        str(self.snrDictionary.get('iqr'))           +","+ str(self.snrDictionary.get('range'))        +","+ str(self.snrDictionary.get('var'))          +","+\
        str(self.snrDictionary.get('stdev'))         +","+ str(self.snrDictionary.get('sum'))          +","+ str(self.snrDictionary.get('skew'))         +","+\
        str(self.snrDictionary.get('kurtosis'))      +","+ str(self.snrDictionary.get('pearson_dm'))   +","+ str(self.snrDictionary.get('pearson_dm_p')) +","+\
        str(self.snrDictionary.get('pearson_p0'))    +","+ str(self.snrDictionary.get('pearson_p0_p')) +","+ str(self.snrDictionary.get('count'))+","
        
        var_dm = str(self.dmDictionary.get('min'))  +","+ str(self.dmDictionary.get('max'))          +","+ str(self.dmDictionary.get('mean'))         +","+\
        str(self.dmDictionary.get('median'))        +","+ str(self.dmDictionary.get('Q1'))           +","+ str(self.dmDictionary.get('Q3'))           +","+\
        str(self.dmDictionary.get('iqr'))           +","+ str(self.dmDictionary.get('range'))        +","+ str(self.dmDictionary.get('var'))          +","+\
        str(self.dmDictionary.get('stdev'))         +","+ str(self.dmDictionary.get('sum'))          +","+ str(self.dmDictionary.get('skew'))         +","+\
        str(self.dmDictionary.get('kurtosis'))      +","+ str(self.dmDictionary.get('pearson_snr'))   +","+ str(self.dmDictionary.get('pearson_snr_p')) +","+\
        str(self.dmDictionary.get('pearson_p0'))    +","+ str(self.dmDictionary.get('pearson_p0_p')) +","+ str(self.dmDictionary.get('count'))+","
        
        var_p0 = str(self.p0Dictionary.get('min'))  +","+ str(self.p0Dictionary.get('max'))          +","+ str(self.p0Dictionary.get('mean'))         +","+\
        str(self.p0Dictionary.get('median'))        +","+ str(self.p0Dictionary.get('Q1'))           +","+ str(self.p0Dictionary.get('Q3'))           +","+\
        str(self.p0Dictionary.get('iqr'))           +","+ str(self.p0Dictionary.get('range'))        +","+ str(self.p0Dictionary.get('var'))          +","+\
        str(self.p0Dictionary.get('stdev'))         +","+ str(self.p0Dictionary.get('sum'))          +","+ str(self.p0Dictionary.get('skew'))         +","+\
        str(self.p0Dictionary.get('kurtosis'))      +","+ str(self.p0Dictionary.get('pearson_snr'))   +","+ str(self.p0Dictionary.get('pearson_snr_p')) +","+\
        str(self.p0Dictionary.get('pearson_dm'))    +","+ str(self.p0Dictionary.get('pearson_dm_p')) +","+ str(self.p0Dictionary.get('count'))
        
        return var_1 + var_snr + var_dm + var_p0 + "," + self.date.strftime("%d/%m/%Y %I:%M:%S") +"\n"
    
    # ****************************************************************************************************
    
    def toStringSNR(self):
        """
        """
        var_1   = str(self.day) +","+ str(self.month)+","+ str(self.year)+","+ str(self.hour)+","+ str(self.min)
        
        for value in self.snr_dist:
            var_1 += "," + str(value)  
        
        return var_1 +"\n"
            
    # ****************************************************************************************************
    
    def saveBeam(self,path):
        """
        """
        
        if(self.beam != 0):
            self.appendToFile(path, self.toStringBeam())
            self.saved = True
        
    # ****************************************************************************************************
    
    def saveObs(self,path):
        """
        """
        if(self.beam != 0):
            self.appendToFile(path, self.toStringObs())
            self.saved = True
    
    # ****************************************************************************************************
    
    def saveSNR(self,path):
        """
        """
        if(self.beam != 0):
            self.appendToFile(path, self.toStringSNR())
            self.saved = True
            
    # ****************************************************************************************************
    
    def saveAndClearBeam(self,path):
        """
        """
        
        self.computeStats()
        self.saveBeam(path)
        self.snrDictionary = {}
        self.dmDictionary  = {}
        self.p0Dictionary  = {}
        
        self.snr = []
        self.dm  = []
        self.p0  = []
        
        self.buildVariables()
        
        self.day   = 0
        self.month = 0
        self.year  = 0
        self.hour  = 0
        self.min   = 0
        self.sec   = 0
        
        self.beam  = -1
        self.date  = None
        self.saved = False
        self.snr_dist = [0]*1001
    
    # ****************************************************************************************************
        
    def saveAndClearObs(self,path):
        """
        """
        
        self.computeStats()
        self.saveObs(path)
        self.snrDictionary = {}
        self.dmDictionary  = {}
        self.p0Dictionary  = {}
        
        self.snr = []
        self.dm  = []
        self.p0  = []
        
        self.buildVariables()
        
        self.day   = 0
        self.month = 0
        self.year  = 0
        self.hour  = 0
        self.min   = 0
        self.sec   = 0
        
        self.beam  = -1
        self.date  = None
        self.saved = False
        self.snr_dist = [0]*1001
    
    # ****************************************************************************************************
        
    def saveAndClearSNR(self,path):
        """
        """
        
        self.saveSNR(path)
        self.snrDictionary = {}
        self.dmDictionary  = {}
        self.p0Dictionary  = {}
        
        self.snr = []
        self.dm  = []
        self.p0  = []
        
        self.buildVariables()
        
        self.day   = 0
        self.month = 0
        self.year  = 0
        self.hour  = 0
        self.min   = 0
        self.sec   = 0
        
        self.beam  = -1
        self.date  = None
        self.saved = False
        self.snr_dist = [0]*1001
            
    # ****************************************************************************************************
    
    def buildVariables(self):
        """
        """
        self.snrDictionary['min']        = sys.float_info.max
        self.snrDictionary['max']        = sys.float_info.min
        self.snrDictionary['mean']       = 0.0
        self.snrDictionary['median']     = 0.0
        self.snrDictionary['Q1']         = 0.0
        self.snrDictionary['Q3']         = 0.0
        self.snrDictionary['range']      = 0.0
        self.snrDictionary['iqr']      = 0.0
        self.snrDictionary['var']        = 0.0
        self.snrDictionary['stdev']      = 0.0
        self.snrDictionary['sum']        = 0.0
        self.snrDictionary['skew']       = 0.0
        self.snrDictionary['kurtosis']   = 0.0
        self.snrDictionary['pearson_dm'] = 0.0
        self.snrDictionary['pearson_p0'] = 0.0
        self.snrDictionary['pearson_dm_p'] = 0.0
        self.snrDictionary['pearson_p0_p'] = 0.0
        self.snrDictionary['count']      = 0.0
        
        self.dmDictionary['min']         = sys.float_info.max
        self.dmDictionary['max']         = sys.float_info.min
        self.dmDictionary['mean']        = 0.0
        self.dmDictionary['median']      = 0.0
        self.dmDictionary['Q1']          = 0.0
        self.dmDictionary['Q3']          = 0.0
        self.dmDictionary['range']       = 0.0
        self.dmDictionary['iqr']       = 0.0
        self.dmDictionary['var']         = 0.0
        self.dmDictionary['stdev']       = 0.0
        self.dmDictionary['sum']         = 0.0
        self.dmDictionary['skew']        = 0.0
        self.dmDictionary['kurtosis']    = 0.0
        self.dmDictionary['pearson_snr'] = 0.0
        self.dmDictionary['pearson_p0']  = 0.0
        self.dmDictionary['pearson_snr_p'] = 0.0
        self.dmDictionary['pearson_p0_p']  = 0.0
        self.dmDictionary['count']       = 0.0
        
        self.p0Dictionary['min']         = sys.float_info.max
        self.p0Dictionary['max']         = sys.float_info.min
        self.p0Dictionary['mean']        = 0.0
        self.p0Dictionary['median']      = 0.0
        self.p0Dictionary['Q1']          = 0.0
        self.p0Dictionary['Q3']          = 0.0
        self.p0Dictionary['range']       = 0.0
        self.p0Dictionary['iqr']       = 0.0
        self.p0Dictionary['var']         = 0.0
        self.p0Dictionary['stdev']       = 0.0
        self.p0Dictionary['sum']         = 0.0
        self.p0Dictionary['skew']        = 0.0
        self.p0Dictionary['kurtosis']    = 0.0
        self.p0Dictionary['pearson_snr'] = 0.0
        self.p0Dictionary['pearson_dm']  = 0.0
        self.p0Dictionary['pearson_snr_p'] = 0.0
        self.p0Dictionary['pearson_dm_p']  = 0.0
        self.p0Dictionary['count']       = 0.0
        
    # ****************************************************************************************************
    