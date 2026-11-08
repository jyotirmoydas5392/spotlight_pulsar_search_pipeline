"""
Automates the execution of the PhcxDataExtractor.py application.

This is required as there are 11 million candidates at /local/scratch/cands.
Trying to obtain them all in one go is not sensible, and time consuming. 

By Rob Lyon <robert.lyon@cs.man.ac.uk>

"""

import subprocess

# ******************************
#
# MAIN METHOD AND ENTRY POINT.
#
# ******************************

def main(argv=None):
    """
    Main entry point for the Application. 
    
    """
    print "Automating..."  
    
    # These are the commands required to search each folder. I've also recorded
    # the results obtained after each run here, simply to give you an idea of how long
    # these batches take to run / the number of matches obtained.
    
    # BATCH 1 - Total Candidates = 1,286,200    Total Runtime = 48 minutes.
    
    # Folder: 2008-11    Candidates = 414,600    Runtime = 20 minutes.
    cmd1 = ['python', 'PhcxExperimenter.py', '-d', '/local/scratch/cands/2008-11','-o','/tmp/2008_11.txt']
    
    # Folder: 2008-11-22-18:57:06    Candidates = 0 (1,300 present)     Total Runtime = 0 minutes (Score files are missing).
    cmd2 = ['python', 'PhcxExperimenter.py', '-d', '/local/scratch/cands/2008-11-22-18:57:06','-o','/tmp/2008_11_22_18_57_06.txt']
    
    # Folder: 2008-12    Candidates = 123,200     Total Runtime = 5 minutes.
    cmd3 = ['python', 'PhcxExperimenter.py', '-d', '/local/scratch/cands/2008-12','-o','/tmp/2008_12.txt']
    
    # Folder: 2009-01    Candidates = 378,000    Total Runtime = 12 minutes.
    cmd4 = ['python', 'PhcxExperimenter.py', '-d', '/local/scratch/cands/2009-01','-o','/tmp/2009_01.txt']
    
    # Folder: 2009-03    Candidates = 370,400    Total Runtime = 11 minutes.
    cmd5 = ['python', 'PhcxExperimenter.py', '-d', '/local/scratch/cands/2009-03','-o','/tmp/2009_03.txt']
    
    # BATCH 2 - Total Candidates = 3,234,619    Total Runtime = 1 hour 24 minutes.
    
    # Folder: 2009-04    Candidates = 393,400    Total Runtime = 11 minutes.
    cmd6 = ['python', 'PhcxExperimenter.py', '-d', '/local/scratch/cands/2009-04','-o','/tmp/2009_04.txt']
    
    # Folder: 2009-05    Candidates = 536,500    Total Runtime = 16 minutes.
    cmd7 = ['python', 'PhcxExperimenter.py', '-d', '/local/scratch/cands/2009-05','-o','/tmp/2009_05.txt']
    
    # Folder: 2009-06    Candidates = 777,600    Total Runtime = 25 minutes.
    cmd8 = ['python', 'PhcxExperimenter.py', '-d', '/local/scratch/cands/2009-06','-o','/tmp/2009_06.txt']
    
    # Folder: 2009-07    Candidates = 3,900     Total Runtime = 5 seconds.
    cmd9 = ['python', 'PhcxExperimenter.py', '-d', '/local/scratch/cands/2009-07','-o','/tmp/2009_07.txt']
    
    # Folder: 2009-08    Candidates = 1,523,219    Total Runtime = 31 minutes.
    cmd10 = ['python', 'PhcxExperimenter.py', '-d', '/local/scratch/cands/2009-08','-o','/tmp/2009_08.txt']
    
    # BATCH 3 - Total Candidates = 2,412,574    Total Runtime =  1 hour.
    
    # Folder: 2009-08_2    Candidates = 218,118    Total Runtime = 4 minutes.
    cmd11 = ['python', 'PhcxExperimenter.py', '-d', '/local/scratch/cands/2009-08_2','-o','/tmp/2009_08_2.txt']
    
    # Folder: 2009-09    Candidates = 651,049    Total Runtime = 15 minutes.
    cmd12 = ['python', 'PhcxExperimenter.py', '-d', '/local/scratch/cands/2009-09','-o','/tmp/2009_09.txt']
    
    # Folder: 2009-10    Candidates = 225,100    Total Runtime = 6 minutes.
    cmd13 = ['python', 'PhcxExperimenter.py', '-d', '/local/scratch/cands/2009-10','-o','/tmp/2009_10.txt']
    
    # Folder: 2009-11    Candidates = 286,300    Total Runtime = 7 minutes.
    cmd14 = ['python', 'PhcxExperimenter.py', '-d', '/local/scratch/cands/2009-11','-o','/tmp/2009_11.txt']
    
    # Folder: 2009-12    Candidates = 1,032,007    Total Runtime = 28 minutes.
    cmd15 = ['python', 'PhcxExperimenter.py', '-d', '/local/scratch/cands/2009-12','-o','/tmp/2009_12.txt']
    
    # BATCH 4 - Total Candidates = 2,894,376    Total Runtime = 1 hours 31 minutes.
    
    # Folder: 2010-01    Candidates = 983,600    Total Runtime = 30 minutes.
    cmd16 = ['python', 'PhcxExperimenter.py', '-d', '/local/scratch/cands/2010-01','-o','/tmp/2010_01.txt']
    
    # Folder: 2010-02    Candidates = 5,200    Total Runtime = 17 seconds.
    cmd17 = ['python', 'PhcxExperimenter.py', '-d', '/local/scratch/cands/2010-02','-o','/tmp/2010_02.txt']
    
    # Folder: 2010-03    Candidates = 343,000    Total Runtime = 13 minutes.
    cmd18 = ['python', 'PhcxExperimenter.py', '-d', '/local/scratch/cands/2010-03','-o','/tmp/2010_03.txt']
    
    # Folder: 2010-04    Candidates = 450,826    Total Runtime = 14 minutes.
    cmd19 = ['python', 'PhcxExperimenter.py', '-d', '/local/scratch/cands/2010-04','-o','/tmp/2010_04.txt']
    
    # Folder: 2010-05    Candidates = 1,111,750    Total Runtime = 33 minutes.
    cmd20 = ['python', 'PhcxExperimenter.py', '-d', '/local/scratch/cands/2010-05','-o','/tmp/2010_05.txt']
    
    # BATCH 5 - Total Candidates = 1,395,102    Total Runtime = 1 hour 3 minutes.
    
    # Folder: 2010-06    Candidates = 298,167    Total Runtime = 13 minutes.
    cmd21 = ['python', 'PhcxExperimenter.py', '-d', '/local/scratch/cands/2010-06','-o','/tmp/2010_06.txt']
    
    # Folder: 2010-07    Candidates = 3,900    Total Runtime = 10 seconds.
    cmd22 = ['python', 'PhcxExperimenter.py', '-d', '/local/scratch/cands/2010-07','-o','/tmp/2010_07.txt']
    
    # Folder: 2010-08    Candidates = 16,900    Total Runtime = 1 minute.
    cmd23 = ['python', 'PhcxExperimenter.py', '-d', '/local/scratch/cands/2010-08','-o','/tmp/2010_08.txt']
    
    # Folder: 2010-09    Candidates = 41,600    Total Runtime = 3 minutes.
    cmd24 = ['python', 'PhcxExperimenter.py', '-d', '/local/scratch/cands/2010-09','-o','/tmp/2010_09.txt']
    
    # Folder: 2010-11    Candidates = 94,100    Total Runtime = 4 minutes.
    cmd25 = ['python', 'PhcxExperimenter.py', '-d', '/local/scratch/cands/2010-11','-o','/tmp/2010_11.txt']
    
    # Folder: 2010-12    Candidates = 940,435    Total Runtime = 41 minutes.
    cmd26 = ['python', 'PhcxExperimenter.py', '-d', '/local/scratch/cands/2010-12','-o','/tmp/2010_12.txt']
    
    # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
    # TOTAL CANDIDATES: 11,222,871   Total Runtime = hours minutes.                               #
    # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
    
    # SIMPLY COMMENT OUT THOSE BATCHES YOU DO NOT WISH TO RUN:
    
    # Batch 1
    
    print "Automator run 1 executing ....\n" 
    p1 = subprocess.Popen(cmd1)
    #p1.wait()
    
    print "\nAutomator run 2 executing ....\n" 
    p2 = subprocess.Popen(cmd2)
    #p2.wait()
    
    print "\nAutomator run 3 executing ....\n" 
    p3 = subprocess.Popen(cmd3)
    #p3.wait()
    
    print "\nAutomator run 4 executing ....\n" 
    p4 = subprocess.Popen(cmd4)
    #p4.wait()
    
    print "Automator run 5 executing ....\n" 
    p5 = subprocess.Popen(cmd5)
    p5.wait()
    
    
    
    # Batch 2
    print "\nAutomator run 6 executing ....\n" 
    p6 = subprocess.Popen(cmd6)
    #p6.wait()
    
    print "\nAutomator run 7 executing ....\n" 
    p7 = subprocess.Popen(cmd7)
    #p7.wait()
    
    print "\nAutomator run 8 executing ....\n" 
    p8 = subprocess.Popen(cmd8)
    #p8.wait()
    
    print "Automator run 9 executing ....\n" 
    p9 = subprocess.Popen(cmd9)
    #p9.wait()
    
    print "\nAutomator run 10 executing ....\n" 
    p10 = subprocess.Popen(cmd10)
    p10.wait()
    
    
    # Batch 3

    print "\nAutomator run 11 executing ....\n" 
    p11 = subprocess.Popen(cmd11)
    #p11.wait()
    
    print "\nAutomator run 12 executing ....\n" 
    p12 = subprocess.Popen(cmd12)
    #p12.wait()
    
    print "Automator run 13 executing ....\n" 
    p13 = subprocess.Popen(cmd13)
    #p13.wait()
    
    print "\nAutomator run 14 executing ....\n" 
    p14 = subprocess.Popen(cmd14)
    #p14.wait()
    
    print "\nAutomator run 15 executing ....\n" 
    p15 = subprocess.Popen(cmd15)
    p15.wait()

    
    # Batch 4

    print "\nAutomator run 16 executing ....\n" 
    p16 = subprocess.Popen(cmd16)
    #p16.wait()

    print "Automator run 17 executing ....\n" 
    p17 = subprocess.Popen(cmd17)
    #p17.wait()
    
    print "\nAutomator run 18 executing ....\n" 
    p18 = subprocess.Popen(cmd18)
    #p18.wait()
    
    print "\nAutomator run 19 executing ....\n" 
    p19 = subprocess.Popen(cmd19)
    #p19.wait()
    
    print "\nAutomator run 20 executing ....\n" 
    p20 = subprocess.Popen(cmd20)
    p20.wait()
    
    
    # Batch 5
    
    print "Automator run 21 executing ....\n" 
    p21 = subprocess.Popen(cmd21)
    #p21.wait()
    
    print "\nAutomator run 22 executing ....\n" 
    p22 = subprocess.Popen(cmd22)
    #p22.wait()
    
    print "\nAutomator run 23 executing ....\n" 
    p23 = subprocess.Popen(cmd23)
    #p23.wait()
    
    print "\nAutomator run 24 executing ....\n" 
    p24 = subprocess.Popen(cmd24)
    #p24.wait()

    print "Automator run 25 executing ....\n" 
    p25 = subprocess.Popen(cmd25)
    #p25.wait()
    
    print "\nAutomator run 26 executing ....\n" 
    p26 = subprocess.Popen(cmd26)
    p26.wait()
    
    
        
    print "Automator completed."  
    
if __name__ == '__main__':
    main()