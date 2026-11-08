This lib folder contains:

HTRU_Candidate_Examples				- A directory containing pulsar candidates obtained during the High Time Resolution Universe Survey.
									  These candidates are stored in phcx.gz files, containing xml data.

LOFAR_Candidate_Examples			- A directory containing pulsar candidates obtained during the LOFAR Survey.
									 These candidates are stored in pfd files.

HTRU_Candidate_OriginalScores.csv	- A meta data file containing details of the candidates used during testing/validation of HTRU
									candidate scores. This file should have a very specific format. It should contain the unique name
									for each candidate, its true class label (i.e. if a candidate is a definite pulsar its true class
									label would be "POSITIVE", else its label is "NEGATIVE". The file should also contain details
									of why each candidate is unique, i.e. if one of its scores is unusual which could mean it is
									the highest or the lowest of all those observed.
									
									Format:
									
									< Candidate File Name (no path) > , < True label > , < Unusual Score > , < Why score is unusual> , < Score 1 > , ... , < Score n>
									
									Example of the format (if only 5 scores):
        
                                                                    						Why unusual   Score 4 
                                                                        						  |             |
                                                                        						  |   Score 2   |
                                                                        						  |      |      |
                                                                        						  v      v      v
         							2008-05-11-05:24:10.01.fil_sigproc_001.phcx.gz.dat,NEGATIVE,1,MAX,100,4,56,0.01,99
         							2008-06-12-06:33:19.03.fil_sigproc_002.phcx.gz.dat,POSITIVE,1,MIN,0.1,5,59,0.03,88
         							2009-07-13-07:44:54.06.fil_sigproc_005.phcx.gz.dat,POSITIVE,3,MIN,33 ,6,33,0.06,78
         							2009-08-14-08:16:23.05.fil_sigproc_006.phcx.gz.dat,NEGATIVE,4,MAX,37 ,7,89,2000,56
         							2010-09-15-09:20:35.02.fil_sigproc_009.phcx.gz.dat,NEGATIVE,5,MIN,56 ,8,76,1.34,50
         							2010-10-16-10:03:31.01.fil_sigproc_004.phcx.gz.dat,NEGATIVE,2,MAX,76 ,9,50,4.01,55
                                    						   ^                           ^    ^      ^    ^        ^
                                    						   |                           |    |      |    |        |
                                						   Filename                      Label  |  Score 1  |     Score 5   
                                                                     							|           |        
                                                              							 Unusual Score   Score 3     

LOFAR_Candidate_OriginalScores.csv - Same as above but for LOFAR candidates.

HTRU_AND_LOFAR_OriginalScores.csv  - Contains both the LOFAR and HTRU candidates.

MASTER_10000_Candidates.csv 	   - A much larger set of candidates in the same format as above.

OriginalLabelledData.arff 		   - The original scores in WEKA format for analysis (machine learning toolkit).

OriginalLabelledDataNegatives.arff - The original negative scores in WEKA format for analysis (machine learning toolkit).

OriginalLabelledDataPositives.arff - The original positive scores in WEKA format for analysis (machine learning toolkit).

NewLabelledData.arff 			   - The scores output by the latest version of the code in WEKA format for analysis
					                 (machine learning toolkit).

NewLabelledDataNegatives.arff      - The new negative scores in WEKA format for analysis (machine learning toolkit).

NewLabelledDataPositives.arff      - The new positive scores in WEKA format for analysis (machine learning toolkit).

Misclassifications.csv			   - This files contains those instances which are known to be misclassified when using
									 the new version of the scores.

Misclassifications.arff			   - This files contains those instances which are known to be misclassified when using
									 the new version of the scores, but in WEKA format.

AutomatedTreeTester.jar	-	A java application that tests the effectiveness of the new scores. It perfroms 10 fold
							cross validation using a decision tree, and the data stored in NewLabelledData.arff. This
							application then outputs a list of misclassified instances to a file. Usage example:
							
							-jar AutomatedTreeTester.jar -tNewLabelledData.arff -oMisclassifications.csv -i<PATH TO LIB DIR>
							
phcx.xsd.xml	-	An xml schema for the phcx files.


NOTE: 

If you can't find the files above in the code downloaded from github, thats because I do not
have permission to make them publically available. LOFAR data belongs to the LOFAR consortium,
and HTRU data belongs to the ATNF.

Rob.
