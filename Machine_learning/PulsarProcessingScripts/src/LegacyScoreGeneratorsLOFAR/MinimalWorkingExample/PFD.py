"""
This code runs on python 2.4 or later. Represents a lofar PFD file.

By Rob Lyon <robert.lyon@cs.man.ac.uk>

+-----------------------------------------------------------------------------------------+
+                       PLEASE RECORD ANY MODIFICATIONS YOU MAKE BELOW                    +
+-----------------------------------------------------------------------------------------+
+ Revision |   Author    | Description                                       |    DATE    +
+-----------------------------------------------------------------------------------------+

 Revision:0    Rob Lyon    Initial version of the re-written code.            06/02/2014
 



+-----------------------------------------------------------------------------------------+

NOTE: You can go directly to a revision by searching the text below i.e. search for "Revision:2b"

"""

# Python 2.4 imports.
import Utilities
import polycos
import infodata
import struct

from numpy import array
from numpy import asarray
from numpy import histogram
from numpy import concatenate
from numpy import floor
from numpy import fabs
from numpy import fmod
from numpy import fromfile
from numpy import reshape
from numpy import float64
from numpy import arange
from numpy import add
from numpy import shape
from numpy import zeros

import operations_new as op
import psr_utils


# ******************************************************************************************
#
# CLASS DEFINITION
#
# ******************************************************************************************

class PFD(Utilities.Utilities):
    """
    Provides utility functions used when computing scores. 
    
    """
    
    # ******************************************************************************************
    #
    # Constructor.
    #
    # ******************************************************************************************
    
    def __init__(self, debugFlag,filename):
        
        Utilities.Utilities.__init__(self, debugFlag)
        self.pfd_filename = str(filename)
        #print "PFD Candidate: ",self.pfd_filename
        infile = open(filename, "rb")
            
        swapchar = '<' # this is little-endian
        data = infile.read(5*4)
        testswap = struct.unpack(swapchar+"i"*5, data)
        # This is a hack to try and test the endianness of the data.
        # None of the 5 values should be a large positive number.
        
        if (fabs(asarray(testswap))).max() > 100000:
            swapchar = '>' # this is big-endian
        (self.numdms, self.numperiods, self.numpdots, self.nsub, self.npart) = \
                      struct.unpack(swapchar+"i"*5, data)
                      
        (self.proflen, self.numchan, self.pstep, self.pdstep, self.dmstep, \
         self.ndmfact, self.npfact) = struct.unpack(swapchar+"i"*7, infile.read(7*4))
         
        self.filenm = infile.read(struct.unpack(swapchar+"i", infile.read(4))[0])
        self.candnm = infile.read(struct.unpack(swapchar+"i", infile.read(4))[0])
        self.telescope = infile.read(struct.unpack(swapchar+"i", infile.read(4))[0])
        self.pgdev = infile.read(struct.unpack(swapchar+"i", infile.read(4))[0])
        
        test = infile.read(16)
        has_posn = 1
        for ii in range(16):
            if test[ii] not in '0123456789:.-\0':
                has_posn = 0
                break
        if has_posn:
            self.rastr = test[:test.find('\0')]
            test = infile.read(16)
            self.decstr = test[:test.find('\0')]
            (self.dt, self.startT) = struct.unpack(swapchar+"dd", infile.read(2*8))
        else:
            self.rastr = "Unknown"
            self.decstr = "Unknown"
            (self.dt, self.startT) = struct.unpack(swapchar+"dd", test)
            
        (self.endT, self.tepoch, self.bepoch, self.avgvoverc, self.lofreq, \
         self.chan_wid, self.bestdm) = struct.unpack(swapchar+"d"*7, infile.read(7*8))
        
                    
        (self.topo_pow, tmp) = struct.unpack(swapchar+"f"*2, infile.read(2*4))
        (self.topo_p1, self.topo_p2, self.topo_p3) = struct.unpack(swapchar+"d"*3, \
                                                                   infile.read(3*8))
        (self.bary_pow, tmp) = struct.unpack(swapchar+"f"*2, infile.read(2*4))
        (self.bary_p1, self.bary_p2, self.bary_p3) = struct.unpack(swapchar+"d"*3, \
                                                                   infile.read(3*8))
        (self.fold_pow, tmp) = struct.unpack(swapchar+"f"*2, infile.read(2*4))
        (self.fold_p1, self.fold_p2, self.fold_p3) = struct.unpack(swapchar+"d"*3, \
                                                                   infile.read(3*8))
        (self.orb_p, self.orb_e, self.orb_x, self.orb_w, self.orb_t, self.orb_pd, \
         self.orb_wd) = struct.unpack(swapchar+"d"*7, infile.read(7*8))
        self.dms = asarray(struct.unpack(swapchar+"d"*self.numdms, \
                                             infile.read(self.numdms*8)))
        if self.numdms==1:
            self.dms = self.dms[0]
        self.periods = asarray(struct.unpack(swapchar+"d"*self.numperiods, \
                                                 infile.read(self.numperiods*8)))
        self.pdots = asarray(struct.unpack(swapchar+"d"*self.numpdots, \
                                               infile.read(self.numpdots*8)))
        self.numprofs = self.nsub*self.npart
        if (swapchar=='<'):  # little endian
            self.profs = zeros((self.npart, self.nsub, self.proflen), dtype='d')
            for ii in range(self.npart):
                for jj in range(self.nsub):
                    self.profs[ii,jj,:] = fromfile(infile, float64, self.proflen)
        else:
            self.profs = asarray(struct.unpack(swapchar+"d"*self.numprofs*self.proflen, \
                                                   infile.read(self.numprofs*self.proflen*8)))
            self.profs = reshape(self.profs, (self.npart, self.nsub, self.proflen))
        if (self.numchan==1):
            print "Numchan=1"
            try:
                idata = infodata.infodata(self.filenm[:self.filenm.rfind('.')]+".inf")
                if idata.waveband=="Radio":
                    self.bestdm = idata.DM
                    self.numchan = idata.numchan
                else: # i.e. for events
                    self.bestdm = 0.0
                    self.numchan = 1
            except IOError:
                print "Warning!  Can't open the .inf file for "+filename+"!"
        self.binspersec = self.fold_p1*self.proflen
        self.chanpersub = self.numchan/self.nsub
        self.subdeltafreq = self.chan_wid*self.chanpersub
        self.hifreq = self.lofreq + (self.numchan-1)*self.chan_wid
        self.losubfreq = self.lofreq + self.subdeltafreq - self.chan_wid
        self.subfreqs = arange(self.nsub, dtype='d')*self.subdeltafreq + \
                        self.losubfreq
        self.subdelays_bins = zeros(self.nsub, dtype='d')
        self.killed_subbands = []
        self.killed_intervals = []
        self.pts_per_fold = []
        # Note: a foldstats struct is read in as a group of 7 doubles
        # the correspond to, in order: 
        #    numdata, data_avg, data_var, numprof, prof_avg, prof_var, redchi
        self.stats = zeros((self.npart, self.nsub, 7), dtype='d')
        for ii in range(self.npart):
            currentstats = self.stats[ii]
            for jj in range(self.nsub):
                if (swapchar=='<'):  # little endian
                    currentstats[jj] = fromfile(infile, float64, 7)
                else:
                    currentstats[jj] = asarray(struct.unpack(swapchar+"d"*7, \
                                                                 infile.read(7*8)))
            self.pts_per_fold.append(self.stats[ii][0][0])  # numdata from foldstats
        self.start_secs = add.accumulate([0]+self.pts_per_fold[:-1])*self.dt
        self.pts_per_fold = asarray(self.pts_per_fold)
        self.mid_secs = self.start_secs + 0.5*self.dt*self.pts_per_fold
        if (not self.tepoch==0.0):
            self.start_topo_MJDs = self.start_secs/86400.0 + self.tepoch
            self.mid_topo_MJDs = self.mid_secs/86400.0 + self.tepoch
        if (not self.bepoch==0.0):
            self.start_bary_MJDs = self.start_secs/86400.0 + self.bepoch
            self.mid_bary_MJDs = self.mid_secs/86400.0 + self.bepoch
        self.Nfolded = add.reduce(self.pts_per_fold)
        self.T = self.Nfolded*self.dt
        self.avgprof = (self.profs/self.proflen).sum()
        self.varprof = self.calc_varprof()
        infile.close()
        self.barysubfreqs = None
        if self.avgvoverc==0:
            if self.candnm.startswith("PSR_"):
                print "Arrived"
                # If this doesn't work, we should try to use the barycentering calcs
                # in the presto module.
                try:
                    self.polycos = polycos(self.candnm[4:],
                                                   filenm=self.pfd_filename+".polycos")
                    midMJD = self.tepoch + 0.5*self.T/86400.0
                    self.avgvoverc = self.polycos.get_voverc(int(midMJD), midMJD-int(midMJD))
                    #sys.stderr.write("Approximate Doppler velocity (in c) is:  %.4g\n"%self.avgvoverc)
                    # Make the Doppler correction
                    self.barysubfreqs = self.subfreqs*(1.0+self.avgvoverc)
                except IOError:
                    self.polycos = 0
        if self.barysubfreqs is None:
            self.barysubfreqs = self.subfreqs
        
    # ******************************************************************************************
    #
    # Functions.
    #
    # ******************************************************************************************
       
    
    def getDutyCycle(self, profile):
        """
        Returns the duty cycle, which is the percentage of one period
        in which a signal is active.
        
        Parameters:
        
        profile    -    the raw profile data.
        
        Returns:
        
        The duty cycle.
        """
        print "getDutyCycle"
        profile = profile - min(profile)
        max_profile = max(profile)
        peak = profile.argmax() # Finds the index of the largest value across the x-axis.
        i=2
        j=1
        k=1
        l=2

        check = 1

        while i <= len(profile)-peak:
            one = profile[peak:peak+i]
            i+=1
            for value in one:               
                if value <= max_profile/2:
                    check = 0
            if check == 1:
                j+=1
                
        test = 1

        while l <= peak:
            two = profile[peak-l:peak]
            l+=1
            for number in two:
                if number <= max_profile/2:
                    test = 0
            if test == 1:
                k+=1
        
        duty_cycle = float(j)/(len(profile))

        return duty_cycle
    
    # ******************************************************************************************
          
    def plot_sumprof(self, device='/xwin'):
        """
        Plot the dedispersed and summed profile.
        """
        print "plot_sumprof"
        if not self.__dict__.has_key('subdelays'):
            print "NEED TO DEDISPERSE"
            self.dedisperse()
            
        normprof = self.sumprof - min(self.sumprof)

        return normprof
    
    # ******************************************************************************************
    
    def dedisperse(self, DM=None, interp=0):
        """
        Rotate (internally) the profiles so that they are de-dispersed
        at a dispersion measure of DM.  Use FFT-based interpolation if
        'interp' is non-zero (NOTE: It is off by default!).
        """
        print "dedisperse"
        if DM is None:
            DM = self.bestdm
        # Note:  Since TEMPO pler corrects observing frequencies, for
        #        TOAs, at least, we need to de-disperse using topocentric
        #        observing frequencies.
        self.subdelays = psr_utils.delay_from_DM(DM, self.subfreqs)
        self.hifreqdelay = self.subdelays[-1]
        self.subdelays = self.subdelays-self.hifreqdelay
        delaybins = self.subdelays*self.binspersec - self.subdelays_bins
        if interp:
            print "interp"
            new_subdelays_bins = delaybins
            for ii in range(self.npart):
                for jj in range(self.nsub):
                    tmp_prof = self.profs[ii,jj,:]
                    self.profs[ii,jj] = psr_utils.fft_rotate(tmp_prof, delaybins[jj])
            # Note: Since the rotation process slightly changes the values of the
            # profs, we need to re-calculate the average profile value
            self.avgprof = (self.profs/self.proflen).sum()
        else:
            print "not interp"
            new_subdelays_bins = floor(delaybins+0.5)
            for ii in range(self.nsub):
                rotbins = int(new_subdelays_bins[ii])%self.proflen
                if rotbins:  # i.e. if not zero
                    subdata = self.profs[:,ii,:]
                    self.profs[:,ii] = concatenate((subdata[:,rotbins:],
                                                        subdata[:,:rotbins]), 1)
        self.subdelays_bins += new_subdelays_bins
        self.sumprof = self.profs.sum(0).sum(0)
        if fabs((self.sumprof/self.proflen).sum() - self.avgprof) > 1.0:
            print "self.avgprof is not the correct value!"
            
    # ******************************************************************************************
    
    def calc_varprof(self):
        """
        calc_varprof(self):
            This function calculates the summed profile variance of the
                current pfd file.  Killed profiles are ignored.
        """
        print "calc_varprof"
        varprof = 0.0
        for part in range(self.npart):
            if part in self.killed_intervals: continue
            for sub in range(self.nsub):
                if sub in self.killed_subbands: continue
                varprof += self.stats[part][sub][5] # foldstats prof_var
        return varprof
    
    # ******************************************************************************************
    
    def plot_chi2_vs_DM(self, loDM, hiDM, N=100, interp=0, device='/xwin'):
        """
        plot_chi2_vs_DM(self, loDM, hiDM, N=100, interp=0, device='/xwin'):
             Plot (and return) an array showing the reduced-chi^2 versus
                DM (N DMs spanning loDM-hiDM).  Use sinc_interpolation
                if 'interp' is non-zero.
        """
        print "plot_chi2_vs_DM"
        # Sum the profiles in time
        sumprofs = self.profs.sum(0)
        if not interp:
            profs = sumprofs
        else:
            profs = zeros(shape(sumprofs), dtype='d')
        DMs = psr_utils.span(loDM, hiDM, N)
        chis = zeros(N, dtype='f')
        subdelays_bins = self.subdelays_bins.copy()
        for ii, DM in enumerate(DMs):
            subdelays = psr_utils.delay_from_DM(DM, self.barysubfreqs)
            hifreqdelay = subdelays[-1]
            subdelays = subdelays - hifreqdelay
            delaybins = subdelays*self.binspersec - subdelays_bins
            if interp:
                interp_factor = 16
                for jj in range(self.nsub):
                    profs[jj] = psr_utils.interp_rotate(sumprofs[jj], delaybins[jj],
                                                        zoomfact=interp_factor)
                # Note: Since the interpolation process slightly changes the values of the
                # profs, we need to re-calculate the average profile value
                avgprof = (profs/self.proflen).sum()
            else:
                new_subdelays_bins = floor(delaybins+0.5)
                for jj in range(self.nsub):
                    profs[jj] = psr_utils.rotate(profs[jj], int(new_subdelays_bins[jj]))
                subdelays_bins += new_subdelays_bins
                avgprof = self.avgprof
            sumprof = profs.sum(0)
            chis[ii] = self.calc_redchi2(prof=sumprof, avg=avgprof)

        return (chis, DMs)
    
    # ******************************************************************************************
    
    def plot_subbands(self, phasebins='All', device='/xwin'):
        """
        plot_subbands(self, phasebins='All', device='/xwin'):
            Plot the interval-summed profiles vs subband.  Restrict
                the bins in the plot to the (low:high) slice defined
                by the phasebins option if it is a tuple (low,high)
                instead of the string 'All'. 
        """
        
        print "Plot_subbands"
        if not self.__dict__.has_key('subdelays'):
            print "Dedispersing first..."
            self.dedisperse()
        if phasebins is not 'All':
            lo, hi = phasebins
            profs = self.profs[:,:,lo:hi].sum(0)
        else:
            lo, hi = 0.0, self.proflen
            profs = self.profs.sum(0)
        lof = self.lofreq - 0.5*self.chan_wid
        hif = lof + self.chan_wid*self.numchan
        return profs
    
    # ******************************************************************************************
    
    def plot_intervals(self, phasebins='All', device='/xwin'):
        """
        plot_intervals(self, phasebins='All', device='/xwin'):
            Plot the subband-summed profiles vs time.  Restrict
                the bins in the plot to the (low:high) slice defined
                by the phasebins option if it is a tuple (low,high)
                instead of the string 'All'. 
        """
        print "plot_intervals"
        if not self.__dict__.has_key('subdelays'):
            print "Dedispersing first..."
            self.dedisperse()
        if phasebins is not 'All':
            lo, hi = phasebins
            profs = self.profs[:,:,lo:hi].sum(1)
        else:
            lo, hi = 0.0, self.proflen
            profs = self.profs.sum(1)

        return profs
    
    # ******************************************************************************************
    
    # ******************************************************************************************
    
    def calc_redchi2(self, prof=None, avg=None, var=None):
        """
        calc_redchi2(self, prof=None, avg=None, var=None):
            Return the calculated reduced-chi^2 of the current summed profile.
        """
        print "calc_redchi2"
        if not self.__dict__.has_key('subdelays'):
            print "Dedispersing first..."
            self.dedisperse()
        if prof is None:  prof = self.sumprof
        if avg is None:  avg = self.avgprof
        if var is None:  var = self.varprof
        return ((prof-avg)**2.0/var).sum()/(len(prof)-1.0)
    
    # ******************************************************************************************
    
    def test_22(self, profile):
        
        swap, names = [], []  
        # "swap" is a store for all values of interest whereas "names"
        # stores their names; all stored values can be sorted and put out in a file
        values = []  # "values" is the store for the neural net (NN) scores for each candidate
        
        #  02 - read profile data #
        width = self.getDutyCycle(profile)
        p = array(profile)
        
        # 03 - sine test on profile data #
        sin_fit = op.t3(p, width)  # performs sine and sine squared fits on profile
        values.append(float(sin_fit[0][1]))  # NN score 1: chi squared for sine fit
        values.append(float(sin_fit[1][1]))  # NN score 2: chi squared for sine squared fit
        values.append(float(sin_fit[2]))  # NN score 3: length of list of the maxima differences
        values.append(float(sin_fit[3]))  # NN score 4: sum over residuals
        
        # 04 - derivative of profile #
        dy = op.derivative(list(p))  # calculates the derivative of the profile
        
        # 05 - histogram of derivative #
        hist_d = histogram(dy, 60)  # calculates a histogram of the derivative
        
        # 06 - histogram of profile #
        hist_p = histogram(p, 60)  # calculates a histogram of the profile
        
        # 07 - gaussian fit of derivative histogram #
        fit_d = op.fit_gaussian(hist_d[1], hist_d[0])  # performs a gaussian fit on the derivative histogram
        d_sigma, d_expect, d_maximum = fit_d[0]
        
        # 08 - gaussian fit of profile histogram #
        fit_p = op.fit_gaussian(hist_p[1], hist_p[0])  # performs a gaussian fit on the profile histogram
        p_sigma, p_expect, p_maximum = fit_p[0]
        
        # 09 - gaussian fit of profile histogram with fixed expect #
        fit_p_f = op.fit_gaussian_fixed(hist_p[1], hist_p[0])  # performs a gaussian fit with fixed expectation value on the profile histogram
        p_sigma_f, p_maximum_f = fit_p_f[0]
        p_chi_f, p_fwhm_f, p_xmax_f = fit_p_f[2], fit_p_f[1], fit_p_f[4]
        
        # 10 - distance fit to fixed #
        dexp_fix = abs(p_xmax_f - p_expect)    
        amp_fix = abs(p_maximum_f / p_maximum)
        values.append(float(dexp_fix))  # NN score 5: distance fit to fixed expectation value for profile histogram
        values.append(float(amp_fix))  # NN score 6: ratio of fixed to fit maximum for profile histogram
        
        # 11 - distance of the gaussfit expectation values #
        dexp = abs(d_expect - p_expect)    
        values.append(float(dexp))  # NN score 7: distance of expectation values of profile and derivative histogram
        
        # 12 - T1 - 1-Peak fit on profile #
        minbg = min(p_expect, p.mean())  # estimate background
        if minbg > 0.:
            temp = []
            for i in range(len(p)):
                newy = p[i] - minbg + p.std()  # substract background from profile
                if newy < 0.:  # and store the new profile in list temp
                    newy = 0.  #
                    temp.append(newy)
        else:
            temp = p
        
        t1_result = op.t1(temp)  # gaussian fit around the maximum of the profile
        t1_fwhm, t1_chi2 = t1_result[1], t1_result[2]
        values.append(float(t1_fwhm))  # NN score 8: fwhm of gaussian fit 
        values.append(float(t1_chi2))  # NN score 9: chi squared of gaussian fit
    
        # 13 - T2 - 2-Peak fit on profile #
    
        t2_result = op.t2(p)  # double gaussian fit arount the maximum of the profile
        t2_fwhm1, t2_chi2, t2_fwhm2 = t2_result[1], t2_result[2], t2_result[6]
        
        try:
            t1t2_diff = t2_result[3] - (t1_result[3] + minbg - p.std())  # differences of gaussian fits t1 and t2
            t1t2_std = float(abs(t1t2_diff.std()))  # standard deviation of differences
        except ValueError:
            print 'Invalid value!'
            t1t2_std=0.0
            
    
        if t1t2_std < 3.:
            t2_fwhm = t1_fwhm
        else:
            t2_fwhm = float(min(t2_fwhm1, t2_fwhm2))
        
        values.append(float(t2_fwhm))  # NN score 10: fwhm of gaussian fit for two peaks as NN score
        values.append(float(t2_chi2))  # NN score 11: chi squared of gaussian fit for two peaks as NN score
    
        # 14 - dm curve analysis #
        lodm = self.dms[0]
        hidm = self.dms[-1]
        y_values, dm_index = self.plot_chi2_vs_DM(lodm, hidm)
    
        ###### optimized DM curve fit ######
        dm_sqr_fit = op.dm_curve_fit(self.calc_redchi2(), self.bestdm, self.bary_p1, width, dm_index, y_values)  # calculate theoretical DM-curve and fit of observed DM-curve

        ###### NN-scores ########
        values.append(float(dm_sqr_fit[7]))  # NN score 12: best period
        values.append(float(dm_sqr_fit[8]))  # NN score 13: best S/N value
        values.append(float(dm_sqr_fit[9]))  # NN score 14: best DM value
        values.append(float(dm_sqr_fit[10]))  # NN score 15: best pulse width
        values.append(float(dm_sqr_fit[11]))  # NN score 16: theoretical peak
        values.append(float(abs(1 - dm_sqr_fit[0][1])))  # NN score 17: deviation of the proportional factor of DM-fit from 1
        values.append(float(abs(dm_sqr_fit[0][2])))  # NN score 18: deviation of the fitted DM-value from the best DM-value off phcx data
        values.append(float(dm_sqr_fit[2]))  # NN score 19: chi squared of theoretical DM-curve and observed data
    
        # 15 - subband score #
        subbands = self.plot_subbands()
        subints = self.plot_intervals()
        prof_bins = self.proflen
        sb_score = op.subband_score(subbands, prof_bins, self.nsub, width)    
    
        values.append(float(sb_score[0]))  # NN score 20: RMS scatter of the maxima
        values.append(float(sb_score[1]))  # NN score 21: mean correlation of the amplitude pairs
    
        # 16 - correlation of profile with subbands #
        cs = op.profile_corr(subbands, prof_bins, self.nsub, p)  # calculate the correlation of profile with all subbands
        #    cs_av = cs.mean()    # mean correlation coefficient 
    
        cs_int = 0
        for i in range(len(cs)):  # calculate integral of correlation coefficients
            cs_int += cs[i]  #########
        
        # values.append(float(cs_av))# NN score: mean correlation coefficient 
        values.append(float(cs_int))  # NN score 22: integral of correlation coefficients
    
        #print values
    
        # reduced chi^2 vs period #
        # period_chi2_fit = op.period_fit(self.calc_redchi2(), self.bestdm, self.bary_p1, width, dm_list, y_numbers)
    
        return values
    
    # ******************************************************************************************
