"""
By Rob Lyon <robert.lyon@cs.man.ac.uk>

+-----------------------------------------------------------------------------------------+
+                       PLEASE RECORD ANY MODIFICATIONS YOU MAKE BELOW                    +
+-----------------------------------------------------------------------------------------+
+ Revision |   Author    | Description                                       |    DATE    +
+-----------------------------------------------------------------------------------------+

 Revision:0    Rob Lyon    Initial version of the code.                        04/02/2014
 
 
"""

# ******************************
#
# CLASS DEFINITION
#
# ******************************

class ScoreWatcher:
    """
    Used to watch and record changes in candidate scores observed during validation.
    Thus if changes are made to the code in ScoreGenerators.py (or other supporting
    files; and the validation script is executed, this will monitor how these changes
    impact the scores. For instance it will monitor if score 5 values increase or
    decrease in response to a change - though it is up to the user to interpret if
    these changes are good or bad.
    
    """
    
    # ******************************
    #
    # INIT FUNCTION
    #
    # ******************************
    def __init__(self,s=-1,e=0.000005):
        """
        Expects an integer input parameter which is the score to be watched.
        Also accepts an epsilon value, which determines how large the difference
        between old and new score must be, before we consider a change to have taken
        place.
        
        """
        self.score=s
        
        # These keep the increase, decrease and unchanged counts for a score.
        # For instance [5,2,0] indicates the score increased 5 times, decreased 2
        # times and stayed the same twice. 
        self.positives=[0,0,0]
        
        # The stats variables on the hand keep track of the min, max and average
        # values observed in this case for the positive class.
        self.positiveStats=[float("infinity"),float("-infinity"),0.0]
        
        # The delta variables keep track of the min, max and average
        # changes in score value, in this case for the positive class.
        self.positiveDeltas=[float("infinity"),float("-infinity"),0.0]
        
        self.positiveDeltaSum=0.0   # The sum of changes in score values seen.
        self.positiveSum=0.0        # The sum of score values seen.
        self.positiveCount=0        # The count of positive instances seen.
        
        # Similar variables to those described above provided for the negative class.
        self.negatives=[0,0,0]
        self.negativeStats=[float("infinity"),float("-infinity"),0.0]
        self.negativeDeltas=[float("infinity"),float("-infinity"),0.0]
        self.negativeDeltaSum=0.0
        self.negativeSum=0.0
        self.negativeCount=0
        
        # Stores string messages for notable candidates. Important messages
        # should be added to this list to ensure they will be written out
        # and brought to the users' attention.
        self.messages=[]
        self.epsilon=e
        
    # ******************************
    #
    # UTILITY FUNCTIONS.
    #
    # ******************************
    
    def update(self,originalValue,newValue,label):
        """
        Updates statistics which records the changes when there has been 
        an increase or decrease in the score value for a candidate.
        
        Parameters:
        originalValue    -    the original float value for the score.
        newValue         -    the new float value computed for the score.
        label            -    the string class label of the candidate which
                              has the score newValue. This label is
                              either POSITIVE or NEGATIVE.
        
        Returns:
        
        N/A
        
        """
        
        # First deal with positively labeled candidates.
        if(label=="POSITIVE"):
            
            # Increment counters and sums.
            self.positiveCount+=1
            self.positiveSum+=newValue
            
            # Now check minimum and max values
            if(self.isEqual(newValue, self.positiveStats[0], self.epsilon) == -1):
                self.positiveStats[0]=newValue
            
            if(self.isEqual(newValue, self.positiveStats[1], self.epsilon) == 1):
                self.positiveStats[1]=newValue
            
            self.positiveStats[2]=self.positiveSum / self.positiveCount
            
            # Now we compute the deltas...
            # The delta variables keep track of the min, max and average
            diff = abs(originalValue-newValue)
            self.positiveDeltaSum+=diff
            
            # Now check minimum and max delta values
            if(self.isEqual(diff,self.positiveDeltas[0],self.epsilon) == -1):
                self.positiveDeltas[0]=diff
            
            if(self.isEqual(diff,self.positiveDeltas[1],self.epsilon) == 1):
                self.positiveDeltas[1]=diff
            
            self.positiveDeltas[2]=self.positiveDeltaSum / self.positiveCount
            
            # Now update positive stats, i.e. increased, decreased and unchanged.
            if(self.isEqual(newValue,originalValue,self.epsilon) == -1):
                
                self.positives[0]+=1
                
            elif(self.isEqual(newValue,originalValue,self.epsilon) == 1):
                
                self.positives[1]+=1
                
            else:
                
                self.positives[2]+=1
        
        # Now deal with negatively labeled candidates.
        elif(label=="NEGATIVE"):
            
            # Increment counters and sums.
            self.negativeCount+=1
            self.negativeSum+=newValue
            
            # Now check minimum and max values
            if(self.isEqual(newValue, self.negativeStats[0], self.epsilon) == -1):
                self.negativeStats[0]=newValue
            
            if(self.isEqual(newValue, self.negativeStats[1], self.epsilon) == 1):
                self.negativeStats[1]=newValue
            
            self.negativeStats[2]=self.negativeSum / self.negativeCount
            
            # Now we compute the deltas...
            # The delta variables keep track of the min, max and average
            diff = abs(originalValue-newValue)
            self.negativeDeltaSum+=diff
            
            # Now check minimum and max delta values
            if(self.isEqual(diff,self.negativeDeltas[0],self.epsilon) == -1):
                self.negativeDeltas[0]=diff
            
            if(self.isEqual(diff,self.negativeDeltas[1],self.epsilon) == 1):
                self.negativeDeltas[1]=diff
            
            self.negativeDeltas[2]=self.negativeDeltaSum / self.negativeCount
            
            # Now update negative stats, i.e. increased, decreased and unchanged.
            if(self.isEqual(newValue,originalValue,self.epsilon) == -1):
                
                self.negatives[0]+=1
                
            elif(self.isEqual(newValue,originalValue,self.epsilon) == 1):
                
                self.negatives[1]+=1
                
            else:
                
                self.negatives[2]+=1
    
    # ******************************
    # 
    # ******************************
    
    def addMessage(self,msg):
        """
        Adds a notable message to this object, which should be brought to the
        users attention when this object is written to string output.
        
        Parameters:
        msg    -    the string message. Should typically indicate how a
                    score value has changed for a particular instance.
        
        Returns:
        
        N/A
        
        """
        
        self.messages.append(str(msg))
    
    # ******************************
    # 
    # ******************************
    
    def isEqual(self,a,b,epsln):
        """
        Used to compare two floats for equality. This code has to cope with some
        extreme possibilities, i.e. the comparison of two floats which are arbitrarily
        small or large.
        
        Parameters:
        a        -    the first floating point number.
        b        -    the second floating point number.
        epsln    -    the allowable error.
        
        Returns:
        
        A value of -1 if a < b, a value greater than 1 if a > b, else
        zero is returned.
        
        """
        
        # There are two possibilities - both numbers may have exponents,
        # neither may have exponents, or a combination may occur. We need
        # a valid way to compare numbers with these possibilities which fits
        # ALL scenarios. The decision here (right or wrong!) is to avoid
        # wasting time on the perfect solution, and just allow the user to
        # specify an epsilon value they are happy with. In this case we 
        # are assuming a change to the score smaller than epsilon is 
        # effectively meaningless. 
        
        if( abs(a - b) > epsln):
            if( a < b):
                return -1
            else:
                return 1 
        else:
            return 0
            
    # ******************************
    # 
    # ******************************
    
    def getOptimalScoreDescription(self,s):
        """
        Returns a string which describes the optimal value for a score. For
        example if we want score 4 to achieve higher values, then this function
        will return "higher" for score 4. These optimal values are somewhat
        subjective, and in some cases there is no optimal value. For instance,
        is it better or worse to have a lower or higher DM, SNR or period?
        
        Parameters:
        s    -    the integer score number between 1 and 22 inclusive.
        
        Returns:
        
        The string describing the optimal value for this score, either "Higher",
        "Lower" or "?".
        
        """
        
        if(s==1):
            return "Higher"
        elif(s==2):
            return "Higher"
        elif(s==3):
            return "Lower"
        elif(s==4):
            return "Higher"
        elif(s==5):
            return "Higher"
        elif(s==6):
            return "Lower"
        elif(s==7):
            return "Higher"
        elif(s==9):
            return "Lower"
        elif(s==11):
            return "Lower"
        elif(s==13):
            return "Higher"
        elif(s==14):
            return "Higher"
        elif(s==18):
            return "Lower"
        elif(s==19):
            return "Lower"
        elif(s==20):
            return "Lower"
        elif(s==21):
            return "Higher"
        elif(s==22):
            return "Higher"
        else:
            return "?"
           
    # ******************************
    # 
    # ******************************
    
    def __str__(self):
        """
        Overridden method that provides a neater string representation
        of this class. This is useful when writing these objects to a file
        or the terminal.
        
        """
        
        header="\nScore " + str(self.score) + " results ("+self.getOptimalScoreDescription(self.score) + " value preferred) :"+"\n"
        
        positives="\tPositives"+"\n"
        
        positives1=positives + "\tDecreased: "     + str(self.positives[0])      + "\tIncreased: " + str(self.positives[1])      + "\tUnchanged: "     + str(self.positives[2]) + "\n"
        positives2=positives1 + "\tMin: "           + str(self.positiveStats[0])  + "\tMax: "       + str(self.positiveStats[1])  + "\tAverage: "       + str(self.positiveStats[2]) +"\n"
        positives3=positives2 + "\tDelta Min: "     + str(self.positiveDeltas[0]) + "\tDelta Max: " + str(self.positiveDeltas[1]) + "\tDelta Average: " + str(self.positiveDeltas[2]) +"\n"
        positives4=positives3 + "\tPositives seen: "+ str(self.positiveCount)     + "\n\n"
        
        negatives="\tNegatives"+"\n"
        negatives1=negatives + "\tDecreased: "     + str(self.negatives[0])      + "\tIncreased: " + str(self.negatives[1])      + "\tUnchanged: "     + str(self.negatives[2]) + "\n"
        negatives2=negatives1 + "\tMin: "           + str(self.negativeStats[0])  + "\tMax: "       + str(self.negativeStats[1])  + "\tAverage: "       + str(self.negativeStats[2]) +"\n"
        negatives3=negatives2 + "\tDelta Min: "     + str(self.negativeDeltas[0]) + "\tDelta Max: " + str(self.negativeDeltas[1]) + "\tDelta Average: " + str(self.negativeDeltas[2]) +"\n"
        negatives4=negatives3 + "\tNegatives seen: "+ str(self.negativeCount)     + "\n\n"
        
        msgs = '\n'.join(self.messages)
        
        description = header + positives4 + negatives4 + msgs
        return description
    