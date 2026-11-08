#!/share/apps/Python2.6/bin/python


import numpy as Num
import struct
import sys, psr_utils, copy, random
from types import StringType, FloatType, IntType
import bestprof
import pylab



import os,sys
import gzip
from numpy import *
from scipy import *
from xml.dom import minidom, EMPTY_NAMESPACE
import makeprofile as mk
import operations_new as op
import glob
import scipy.stats


class pfd:

    def __init__(self, filename):
        self.pfd_filename = filename
        infile = open(filename, "rb")
        # See if the .bestprof file is around
        try:
            self.bestprof = bestprof(filename+".bestprof")
        except IOError:
            self.bestprof = 0
        swapchar = '<' # this is little-endian
        data = infile.read(5*4)
        testswap = struct.unpack(swapchar+"i"*5, data)
        # This is a hack to try and test the endianness of the data.
        # None of the 5 values should be a large positive number.
        if (Num.fabs(Num.asarray(testswap))).max() > 100000:
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
        # The following "fixes" (we think) the observing frequency of the Spigot
        # based on tests done by Ingrid on 0737 (comparing it to GASP)
        # The same sorts of corrections should be made to WAPP data as well...
        # The tepoch corrections are empirically determined timing corrections
        # Note that epoch is only double precision and so the floating
        # point accuracy is ~1 us!
        if self.telescope=='GBT':
            if Num.fabs(Num.fmod(self.dt, 8.192e-05) < 1e-12) and \
               ("spigot" in filename.lower() or "guppi" not in filename.lower()):
                if self.chan_wid==800.0/1024: # Spigot 800 MHz mode 2
                    self.lofreq -= 0.5 * self.chan_wid
                    # original values
                    #if self.tepoch > 0.0: self.tepoch += 0.039334/86400.0
                    #if self.bestprof: self.bestprof.epochf += 0.039334/86400.0
                    # values measured with 1713+0747 wrt BCPM2 on 13 Sept 2007
                    if self.tepoch > 0.0: self.tepoch += 0.039365/86400.0
                    if self.bestprof: self.bestprof.epochf += 0.039365/86400.0
                elif self.chan_wid==800.0/2048:
                    self.lofreq -= 0.5 * self.chan_wid 
                    if self.tepoch < 53700.0:  # Spigot 800 MHz mode 16 (downsampled)
                        if self.tepoch > 0.0: self.tepoch += 0.039352/86400.0
                        if self.bestprof: self.bestprof.epochf += 0.039352/86400.0
                    else:  # Spigot 800 MHz mode 14 
                        # values measured with 1713+0747 wrt BCPM2 on 13 Sept 2007
                        if self.tepoch > 0.0: self.tepoch += 0.039365/86400.0
                        if self.bestprof: self.bestprof.epochf += 0.039365/86400.0
                elif self.chan_wid==50.0/1024 or self.chan_wid==50.0/2048: # Spigot 50 MHz modes
                    self.lofreq += 0.5 * self.chan_wid
                    # Note: the offset has _not_ been measured for the 2048-lag mode
                    if self.tepoch > 0.0: self.tepoch += 0.039450/86400.0
                    if self.bestprof: self.bestprof.epochf += 0.039450/86400.0
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
        self.dms = Num.asarray(struct.unpack(swapchar+"d"*self.numdms, \
                                             infile.read(self.numdms*8)))
        if self.numdms==1:
            self.dms = self.dms[0]
        self.periods = Num.asarray(struct.unpack(swapchar+"d"*self.numperiods, \
                                                 infile.read(self.numperiods*8)))
        self.pdots = Num.asarray(struct.unpack(swapchar+"d"*self.numpdots, \
                                               infile.read(self.numpdots*8)))
        self.numprofs = self.nsub*self.npart
        if (swapchar=='<'):  # little endian
            self.profs = Num.zeros((self.npart, self.nsub, self.proflen), dtype='d')
            for ii in range(self.npart):
                for jj in range(self.nsub):
                    self.profs[ii,jj,:] = Num.fromfile(infile, Num.float64, self.proflen)
        else:
            self.profs = Num.asarray(struct.unpack(swapchar+"d"*self.numprofs*self.proflen, \
                                                   infile.read(self.numprofs*self.proflen*8)))
            self.profs = Num.reshape(self.profs, (self.npart, self.nsub, self.proflen))
        if (self.numchan==1):
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
        self.subfreqs = Num.arange(self.nsub, dtype='d')*self.subdeltafreq + \
                        self.losubfreq
        self.subdelays_bins = Num.zeros(self.nsub, dtype='d')
        self.killed_subbands = []
        self.killed_intervals = []
        self.pts_per_fold = []
        # Note: a foldstats struct is read in as a group of 7 doubles
        # the correspond to, in order: 
        #    numdata, data_avg, data_var, numprof, prof_avg, prof_var, redchi
        self.stats = Num.zeros((self.npart, self.nsub, 7), dtype='d')
        for ii in range(self.npart):
            currentstats = self.stats[ii]
            for jj in range(self.nsub):
                if (swapchar=='<'):  # little endian
                    currentstats[jj] = Num.fromfile(infile, Num.float64, 7)
                else:
                    currentstats[jj] = Num.asarray(struct.unpack(swapchar+"d"*7, \
                                                                 infile.read(7*8)))
            self.pts_per_fold.append(self.stats[ii][0][0])  # numdata from foldstats
        self.start_secs = Num.add.accumulate([0]+self.pts_per_fold[:-1])*self.dt
        self.pts_per_fold = Num.asarray(self.pts_per_fold)
        self.mid_secs = self.start_secs + 0.5*self.dt*self.pts_per_fold
        if (not self.tepoch==0.0):
            self.start_topo_MJDs = self.start_secs/86400.0 + self.tepoch
            self.mid_topo_MJDs = self.mid_secs/86400.0 + self.tepoch
        if (not self.bepoch==0.0):
            self.start_bary_MJDs = self.start_secs/86400.0 + self.bepoch
            self.mid_bary_MJDs = self.mid_secs/86400.0 + self.bepoch
        self.Nfolded = Num.add.reduce(self.pts_per_fold)
        self.T = self.Nfolded*self.dt
        self.avgprof = (self.profs/self.proflen).sum()
        self.varprof = self.calc_varprof()
        infile.close()
        self.barysubfreqs = None
        if self.avgvoverc==0:
            if self.candnm.startswith("PSR_"):
                # If this doesn't work, we should try to use the barycentering calcs
                # in the presto module.
                try:
                    self.polycos = polycos.polycos(self.candnm[4:],
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

    def __str__(self):
        out = ""
        for k, v in self.__dict__.items():
            if k[:2]!="__":
                if type(self.__dict__[k]) is StringType:
                    out += "%10s = '%s'\n" % (k, v)
                elif type(self.__dict__[k]) is IntType:
                    out += "%10s = %d\n" % (k, v)
                elif type(self.__dict__[k]) is FloatType:
                    out += "%10s = %-20.15g\n" % (k, v)
        return out

    def dedisperse(self, DM=None, interp=0):
        """
        dedisperse(DM=self.bestdm, interp=0):
            Rotate (internally) the profiles so that they are de-dispersed
                at a dispersion measure of DM.  Use FFT-based interpolation if
                'interp' is non-zero (NOTE: It is off by default!).
        """
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
            new_subdelays_bins = delaybins
            for ii in range(self.npart):
                for jj in range(self.nsub):
                    tmp_prof = self.profs[ii,jj,:]
                    self.profs[ii,jj] = psr_utils.fft_rotate(tmp_prof, delaybins[jj])
            # Note: Since the rotation process slightly changes the values of the
            # profs, we need to re-calculate the average profile value
            self.avgprof = (self.profs/self.proflen).sum()
        else:
            new_subdelays_bins = Num.floor(delaybins+0.5)
            for ii in range(self.nsub):
                rotbins = int(new_subdelays_bins[ii])%self.proflen
                if rotbins:  # i.e. if not zero
                    subdata = self.profs[:,ii,:]
                    self.profs[:,ii] = Num.concatenate((subdata[:,rotbins:],
                                                        subdata[:,:rotbins]), 1)
        self.subdelays_bins += new_subdelays_bins
        self.sumprof = self.profs.sum(0).sum(0)
        if Num.fabs((self.sumprof/self.proflen).sum() - self.avgprof) > 1.0:
            print "self.avgprof is not the correct value!"

    def combine_profs(self, new_npart, new_nsub):
        """
        combine_profs(self, new_npart, new_nsub):
            Combine intervals and/or subbands together and return a new
                array of profiles.
        """
        if (self.npart % new_npart):
            print "Warning!  The new number of intervals (%d) is not a" % new_npart
            print "          divisor of the original number of intervals (%d)!"  % self.npart
            print "Doing nothing."
            return None
        if (self.nsub % new_nsub):
            print "Warning!  The new number of subbands (%d) is not a" % new_nsub
            print "          divisor of the original number of subbands (%d)!"  % self.nsub
            print "Doing nothing."
            return None

        dp = self.npart/new_npart
        ds = self.nsub/new_nsub

        newprofs = Num.zeros((new_npart, new_nsub, self.proflen), 'd')
        for ii in range(new_npart):
            # Combine the subbands if required
            if (self.nsub > 1):
                for jj in range(new_nsub):
                    subprofs = Num.add.reduce(self.profs[:,jj*ds:(jj+1)*ds], 1)
                    # Combine the time intervals
                    newprofs[ii][jj] = Num.add.reduce(subprofs[ii*dp:(ii+1)*dp])
            else:
                newprofs[ii][0] = Num.add.reduce(self.profs[ii*dp:(ii+1)*dp,0])
        return newprofs

    def kill_intervals(self, intervals):
        """
        kill_intervals(intervals):
            Set all the subintervals (internally) from the list of
                subintervals to all zeros, effectively 'killing' them.
        """
        for part in intervals:
            self.profs[part,:,:] *= 0.0
            self.killed_intervals.append(part)
        # Update the stats
        self.avgprof = (self.profs/self.proflen).sum()
        self.varprof = self.calc_varprof()

    def kill_subbands(self, subbands):
        """
        kill_subbands(subbands):
            Set all the profiles (internally) from the list of
                subbands to all zeros, effectively 'killing' them.
        """
        for sub in subbands:
            self.profs[:,sub,:] *= 0.0
            self.killed_subbands.append(sub)
        # Update the stats
        self.avgprof = (self.profs/self.proflen).sum()
        self.varprof = self.calc_varprof()

    def plot_sumprof(self, device='/xwin'):
        """
        plot_sumprof(self, device='/xwin'):
            Plot the dedispersed and summed profile.
        """
        if not self.__dict__.has_key('subdelays'):
            print "Dedispersing first..."
            self.dedisperse()
        normprof = self.sumprof - min(self.sumprof)
        #normprof /= max(normprof)
        #normprof /=
        #print scipy.stats.mode(normprof, axis=None)
        #Pgplot.plotxy(normprof, labx="Phase Bins", laby="Normalized Flux",
                      #device=device)

   #def greyscale(self, array2d, **kwargs):
        #"""
        #greyscale(array2d, **kwargs):
            #Plot a 2D array as a greyscale image using the same scalings
                #as in prepfold.
        #"""
        # Use the same scaling as in prepfold_plot.c
        #global_max = Num.maximum.reduce(Num.maximum.reduce(array2d))
        #min_parts = Num.minimum.reduce(array2d, 1)
        #array2d = (array2d-min_parts[:,Num.newaxis])/global_max
        #Pgplot.plot2d(array2d, image='antigrey', **kwargs)
        return normprof


    def plot_intervals(self, phasebins='All', device='/xwin'):
        """
        plot_intervals(self, phasebins='All', device='/xwin'):
            Plot the subband-summed profiles vs time.  Restrict
                the bins in the plot to the (low:high) slice defined
                by the phasebins option if it is a tuple (low,high)
                instead of the string 'All'. 
        """
        if not self.__dict__.has_key('subdelays'):
            print "Dedispersing first..."
            self.dedisperse()
        if phasebins is not 'All':
            lo, hi = phasebins
            profs = self.profs[:,:,lo:hi].sum(1)
        else:
            lo, hi = 0.0, self.proflen
            profs = self.profs.sum(1)
        #self.greyscale(profs, rangex=[lo, hi], rangey=[0.0, self.npart],
                       #labx="Phase Bins", labx2="Pulse Phase", laby="Time Intervals",
                       #rangex2=Num.asarray([lo, hi])*1.0/self.proflen,
                       #laby2="Time (s)", rangey2=[0.0, self.T], 
                       #device=device)
        return profs

    def plot_subbands(self, phasebins='All', device='/xwin'):
        """
        plot_subbands(self, phasebins='All', device='/xwin'):
            Plot the interval-summed profiles vs subband.  Restrict
                the bins in the plot to the (low:high) slice defined
                by the phasebins option if it is a tuple (low,high)
                instead of the string 'All'. 
        """
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
        #self.greyscale(profs, rangex=[lo, hi], rangey=[0.0, self.nsub],
                      #labx="Phase Bins", labx2="Pulse Phase", laby="Subbands",
                      #rangex2=Num.asarray([lo, hi])*1.0/self.proflen,
                      #laby2="Frequency (MHz)", rangey2=[lof, hif],
                      #device=device)
        return profs





    def calc_varprof(self):
        """
        calc_varprof(self):
            This function calculates the summed profile variance of the
                current pfd file.  Killed profiles are ignored.
        """
        varprof = 0.0
        for part in range(self.npart):
            if part in self.killed_intervals: continue
            for sub in range(self.nsub):
                if sub in self.killed_subbands: continue
                varprof += self.stats[part][sub][5] # foldstats prof_var
        return varprof

    def calc_redchi2(self, prof=None, avg=None, var=None):
        """
        calc_redchi2(self, prof=None, avg=None, var=None):
            Return the calculated reduced-chi^2 of the current summed profile.
        """
        if not self.__dict__.has_key('subdelays'):
            print "Dedispersing first..."
            self.dedisperse()
        if prof is None:  prof = self.sumprof
        if avg is None:  avg = self.avgprof
        if var is None:  var = self.varprof
        return ((prof-avg)**2.0/var).sum()/(len(prof)-1.0)

    def plot_chi2_vs_DM(self, loDM, hiDM, N=100, interp=0, device='/xwin'):
        """
        plot_chi2_vs_DM(self, loDM, hiDM, N=100, interp=0, device='/xwin'):
             Plot (and return) an array showing the reduced-chi^2 versus
                DM (N DMs spanning loDM-hiDM).  Use sinc_interpolation
                if 'interp' is non-zero.
        """
        # Sum the profiles in time
        sumprofs = self.profs.sum(0)
        if not interp:
            profs = sumprofs
        else:
            profs = Num.zeros(Num.shape(sumprofs), dtype='d')
        DMs = psr_utils.span(loDM, hiDM, N)
        chis = Num.zeros(N, dtype='f')
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
                new_subdelays_bins = Num.floor(delaybins+0.5)
                for jj in range(self.nsub):
                    profs[jj] = psr_utils.rotate(profs[jj], int(new_subdelays_bins[jj]))
                subdelays_bins += new_subdelays_bins
                avgprof = self.avgprof
            sumprof = profs.sum(0)
            chis[ii] = self.calc_redchi2(prof=sumprof, avg=avgprof)
        # Now plot it
        ##x = range(len(chis))
        #pylab.plot(DMs,chis,'k-')
        #pylab.show()

        #Pgplot.plotxy(chis, DMs, labx="DM", laby="Reduced-\gx\u2\d", device=device)
        return (chis, DMs)

    def plot_chi2_vs_sub(self, device='/xwin'):
        """
        plot_chi2_vs_sub(self, device='/xwin'):
            Plot (and return) an array showing the reduced-chi^2 versus
                the subband number.
        """
        # Sum the profiles in each subband
        profs = self.profs.sum(0)
        # Compute the averages and variances for the subbands
        avgs = profs.sum(1)/self.proflen
        vars = []
        for sub in range(self.nsub):
            var = 0.0
            if sub in self.killed_subbands:
                vars.append(var)
                continue
            for part in range(self.npart):
                if part in self.killed_intervals:
                    continue
                var += self.stats[part][sub][5] # foldstats prof_var
            vars.append(var)
        chis = Num.zeros(self.nsub, dtype='f')
        for ii in range(self.nsub):
            chis[ii] = self.calc_redchi2(prof=profs[ii], avg=avgs[ii], var=vars[ii])
        # Now plot it
        #Pgplot.plotxy(chis, labx="Subband Number", laby="Reduced-\gx\u2\d",
            #          rangey=[0.0, max(chis)*1.1], device=device)
        return chis

    def estimate_offsignal_redchi2(self):
        """
        estimate_offsignal_redchi2():
            Estimate the reduced-chi^2 off of the signal based on randomly shifting
                and summing all of the component profiles.  
        """
        numtrials = 20
        redchi2s = []
        for count in range(numtrials):
            prof = Num.zeros(self.proflen, dtype='d')
            for ii in range(self.npart):
                for jj in range(self.nsub):
                    tmpprof = copy.copy(self.profs[ii][jj])
                    prof += psr_utils.rotate(tmpprof, random.randrange(0,self.proflen))
            redchi2s.append(self.calc_redchi2(prof=prof))
        return psr_utils.mean(redchi2s)

    def adjust_fold_frequency(self, phasebins, profs=None, shiftsubs=False):
        """
        adjust_fold_frequency(phasebins, profs=None, shiftsubs=False):
            Linearly shift the intervals by phasebins over the course of
                the observation in order to change the apparent folding
                frequency.  Return a 2D array containing the de-dispersed
                profiles as a function of time (i.e. shape = (npart, proflen)),
				and the reduced chi^2 of the resulting summed profile.
                If profs is not None, then use profs instead of self.profs.
				If shiftsubs is not False, then actually correct the subbands
				instead of a 2D projection of them.
        """
        if not self.__dict__.has_key('subdelays'):
            print "Dedispersing first..."
            self.dedisperse()
        if shiftsubs:
            print "Shifting all the subbands..."
            if profs is None:
                profs = self.profs
            for ii in range(self.npart):
                bins_to_shift = int(round(float(ii)/self.npart * phasebins))
                for jj in range(self.nsub):
                    profs[ii,jj] = psr_utils.rotate(profs[ii,jj], bins_to_shift)
            redchi = self.calc_redchi2(prof=profs.sum(0).sum(0))
        else:
            print "Shifting just the projected intervals (not individual subbands)..."
            if profs is None:
                profs = self.profs.sum(1)
            for ii in range(self.npart):
                bins_to_shift = int(round(float(ii)/self.npart * phasebins))
                profs[ii] = psr_utils.rotate(profs[ii], bins_to_shift)
            redchi = self.calc_redchi2(prof=profs.sum(0))
        print "New reduced-chi^2 =", redchi
        return profs, redchi

    def dynamic_spectra(self, onbins, combineints=1, combinechans=1,
                        calibrate=True, plot=True, device='/xwin'):
        """
        dynamic_spectra(onbins, combineints=1, combinechans=1,
                        calibrate=True, plot=True, device='/xwin'):
            Return (and plot) the dynamic spectrum (DS) resulting
                from the folds in the .pfd assuming that the pulsar
                is 'on' during the bins specified in 'onbins' and
                off elsewhere (ON-OFF).  If calibrate is True, the
                DS will be (ON-OFF)/OFF.  combineints and combinechans
                describe how many adjacent intervals or frequency
                channels will be combined when making the DS.
        """
        # Determine the indices of the off-pulse region
        indices = Num.arange(self.proflen)
        Num.put(indices, Num.asarray(onbins), -1)
        offbins = Num.compress(indices >= 0, Num.arange(self.proflen))
        numon = len(onbins)
        numoff = len(offbins)
        # De-disperse if required first
        if not self.__dict__.has_key('subdelays'):
            print "Dedispersing first..."
            self.dedisperse()
        # The following is the average offpulse level
        offpulse = Num.sum(Num.take(self.profs, offbins, 2), 2)/float(numoff)
        # The following is the average onpulse level
        onpulse  = Num.sum(Num.take(self.profs,  onbins, 2), 2)/float(numon)
        # Now make the DS
        self.DS = onpulse - offpulse
        self.DSnpart = self.npart
        self.DSstart_secs = self.start_secs
        self.DSintdt = self.DSstart_secs[1] - self.DSstart_secs[0]
        self.DSnsub = self.nsub
        self.DSsubfreqs = self.subfreqs
        self.DSsubdeltafreq = self.subdeltafreq
        if (calibrate):
            # Protect against division by zero
            offpulse[offpulse==0.0] = 1.0
            self.DS /= offpulse
        # Combine intervals if required
        if (combineints > 1):
            # First chop off any extra intervals
            if (self.npart % combineints):
                self.DSnpart = (self.npart/combineints) * combineints
                self.DS = self.DS[:self.DSnpart,:]
            # Now reshape and add the neighboring intervals
            self.DS = Num.reshape(self.DS, (self.DSnpart/combineints,
                                            combineints, self.DSnsub))
            print Num.shape(self.DS)
            self.DS = Num.sum(self.DS, 1)
            self.DSstart_secs = self.DSstart_secs[::combineints]
            self.DSintdt *= combineints
            self.DSnpart /= combineints
        # Combine channels if required
        if (combinechans > 1):
            # First chop off any extra channels
            if (self.nsub % combinechans):
                self.DSnsub = (self.nsub/combinechans) * combinechans
                self.DS = self.DS[:,:self.DSnsub]
            # Now reshape and add the neighboring intervals
            self.DS = Num.reshape(self.DS, (self.DSnpart,
                                            self.DSnsub/combinechans, combinechans))
            self.DS = Num.sum(self.DS, 2)
            self.DSsubfreqs = psr_utils.running_avg(self.subfreqs[:self.DSnsub], combinechans)
            self.DSsubdeltafreq *= combinechans
            self.DSnsub /= combinechans
        print "DS shape = ", Num.shape(self.DS)
        # Plot it if required
        if plot:
            lof = self.subfreqs[0]-0.5*self.DSsubdeltafreq
            hif = self.subfreqs[-1]+0.5*self.DSsubdeltafreq
            lot = 0.0
            hit = self.DSstart_secs[-1] + self.DSintdt
            self.greyscale(self.DS, rangex=[lof, hif], rangey=[lot, hit],
                           labx="Frequency (MHz)", labx2="Subband Number",
                           laby="Time (s)", laby2="Interval Number",
                           rangex2=[0, self.DSnsub], rangey2=[0, self.DSnpart], 
                           device=device)
        return self.DS





    def calc_width(self, profile):

        profile = profile - min(profile)
        max_profile = max(profile)
        peak = profile.argmax()
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
        #print duty_cycle
        return duty_cycle













    def plot_chisqs(scores):
	###### read in pulsar data ######
        #print scores
	pulsars = getdict("pulsars.dat")

	numbers = len(pulsars)
	keys   = pulsars.keys()
	values = pulsars.values()
        chis1 = []
        chis2 = []




	strlen = len(scores[keys[0]])

	for cand in keys:

           chis1.append(scores[cand][0])
           chis2.append(scores[cand][1])
	   
      #  pylab.plot(chis1, chis2)
       # pylab.show()












    
               
        


    def test_22(self, profile):
    
                        
                
                        swap,names = [],[]			# "swap" is a store for all values of interest whereas "names" stores their names; all stored values can be sorted and put out in a file
	         	values = []				# "values" is the store for the neural net (NN) scores for each candidate
		

	################### 02 - read profile data #################################################################################"
		#	'''
			#profile = mk.getprofile(xmldata,1)
   
                        #profile = self.sumprof # extracts profile data from phcx file
                        width = tp.calc_width(profile)
                       	p = array(profile)

                       
                        #print p

		#	op.listout("profile_",[],p,cand)		# output of profile data in file
		#	'''

		################### 03 - sine test on profile data #############


		#	op.listout("profile_",[],p,cand)		# output of profile data in file
		#	'''

		################### 03 - sine test on profile data ##########################################################################"
		#	'''
			sin_fit = op.t3(p,width)				# performs sine and sine squared fits on profile

                       

                        # print sin_fit[0][1]		# NN score: chi squared for sine fit
                        #			print sin_fit[1][1]		# NN score: chi squared for sine squared fit
                        #			print sin_fit[2]		# NN score: length of list of the maxima differences
                        #                        print sin_fit[3]               # NN score: sum over residual
                        
                        values.append(float(sin_fit[0][1]))		# NN score: chi squared for sine fit
                        values.append(float(sin_fit[1][1]))		# NN score: chi squared for sine squared fit
             
			values.append(float(sin_fit[2]))		# NN score: length of list of the maxima differences
			values.append(float(sin_fit[3]))		# NN score: sum over residuals

		


		################### 04 - derivative of profile #############################################################################"
		#	'''
			dy = op.derivative(list(p))			# calculates the derivative of the profile
		#	op.listout("derivative_",[],dy,cand)		# output of derivative in file
		#	'''
                        #pylab.plot(range(len(dy)), dy)
                        #pylab.show()
		################### 05 - histogram of derivative ###########################################################################"
		#	'''
			hist_d = histogram(dy,60)		# calculates a histogram of the derivative
                #	op.listout("his_d_",hist_d[1],hist_d[0],cand)	# output of histogram of the derivative in file
		#	'''

                        
                

		################### 06 - histogram of profile ##############################################################################"
		#	'''
			hist_p = histogram(p,60)		# calculates a histogram of the profile
                                                
		#	op.listout("his_p_",hist_p[1],hist_p[0],cand)	# output of histogram of the profile in file
		#	'''

		#################### 07 - gaussian fit of derivative histogram #############################################################"
		#	'''
			fit_d = op.fit_gaussian(hist_d[1],hist_d[0])		# performs a gaussian fit on the derivative histogram
			d_sigma, d_expect, d_maximum = fit_d[0]		
		#	d_chi, d_fwhm = fit_d[2],fit_d[1]
		#	d_fit = fit_d[3]

                        

                

	
		#################### 08 - gaussian fit of profile histogram ################################################################"
		#	'''
                        fit_p = op.fit_gaussian(hist_p[1],hist_p[0])		# performs a gaussian fit on the profile histogram
			p_sigma, p_expect, p_maximum = fit_p[0]

                       # print fit_p[0]
		#	p_chi, p_fwhm = fit_p[2],fit_p[1]
		#	p_fit = fit_p[3]


		#################### 09 - gaussian fit of profile histogram with fixed expect ##############################################"
		#	'''
			fit_p_f = op.fit_gaussian_fixed(hist_p[1],hist_p[0])	# performs a gaussian fit with fixed expectation value on the profile histogram
			p_sigma_f, p_maximum_f = fit_p_f[0]
			p_chi_f, p_fwhm_f, p_xmax_f = fit_p_f[2],fit_p_f[1],fit_p_f[4]
		#	p_fit_f = fit_p_f[3]


		#################### 10 - distance fit to fixed ############################################################################"
		#	'''
			dexp_fix = abs(p_xmax_f - p_expect)	
			amp_fix = abs(p_maximum_f/p_maximum)	
	
                        #print p_maximum_f, p_maximum
			values.append(float(dexp_fix))		# NN score: distance fit to fixed expectation value for profile histogram
			values.append(float(amp_fix))		# NN score: ratio of fixed to fit maximum for profile histogram
		#	'''

		#################### 11 - distance of the gaussfit expectation values ######################################################"
		#	'''
			dexp = abs(d_expect - p_expect)	
			
		

			values.append(float(dexp))		# NN score: distance of expectation values of profile and derivative histogram
		#	'''

		#################### 12 - T1 - 1-Peak fit on profile #######################################################################"		
		#	'''
			minbg = min(p_expect,p.mean())				# estimate background
			if minbg > 0.:					  #############
			   temp = []						#
			   for i in range(len(p)):				#
			      newy = p[i]-minbg+p.std()				# substract background from profile
			      if newy < 0.:					# and store the new profile in list temp
				 newy = 0.					#
			      temp.append(newy)					#
			else:							#
			   temp = p					  ##############

			t1_result = op.t1(temp)					# gaussian fit around the maximum of the profile
			t1_fwhm, t1_chi2 = t1_result[1], t1_result[2]

			values.append(float(t1_fwhm))				# NN score: fwhm of gaussian fit 
			values.append(float(t1_chi2))				# NN score: chi squared of gaussian fit
		

		#################### 13 - T2 - 2-Peak fit on profile #######################################################################"
		
		        t2_result = op.t2(p)					# double gaussian fit arount the maximum of the profile
			t2_fwhm1, t2_chi2, t2_fwhm2 = t2_result[1],t2_result[2],t2_result[6]

			t1t2_diff = t2_result[3] - (t1_result[3]+minbg-p.std())	# differences of gaussian fits t1 and t2
			t1t2_std  = float(abs(t1t2_diff.std()))			# standard deviation of differences
			if t1t2_std < 3.:
				t2_fwhm = t1_fwhm				
			else: 
				t2_fwhm = float(min(t2_fwhm1,t2_fwhm2))

	
			values.append(float(t2_fwhm))			# NN score: fwhm of gaussian fit for two peaks as NN score
			values.append(float(t2_chi2))			# NN score: chi squared of gaussian fit for two peaks as NN score
		#	'''			

		#################### 14 - dm curve analysis ################################################################################"
		#	'''		
			###### FFT DM curve analysis ######	
		#	dm_profile = array(mk.getdmfft(xmldata,0))			# get FFT-DM-profile from data file
		#	dm_norm = op.normalize(array(dm_profile))			# normalize DM-profile
		#	dm_nx, dm_ny = array(op.zero_del([],dm_norm))			# delete zeros from DM-profile

		#	if len(dm_ny) > 0:
		#	 dm_mean = dm_ny.mean()
		#	else:
		#	 dm_mean = 0.


                       
                         
                       
                        lodm = tp.dms[0]
                        hidm = tp.dms[-1]


                        y_values, dm_index = tp.plot_chi2_vs_DM(lodm, hidm)
#                        print y_values, len(y_values)
#                        print dm_index, len(dm_index)
                        #pylab.plot(dm_index, y_values)
                        #pylab.show()
                                                
			###### optimized DM curve fit ######
		#	dm_curve_all = array(mk.getdmfft(xmldata,1))			# get optimized FFT-DM-profile from data file
		#	dm_curve     = op.dm_curve(dm_curve_all)			# extract the DM-curve
                        #print tp.calc_redchi2()
			dm_sqr_fit   = op.dm_curve_fit(tp.calc_redchi2(), tp.bestdm, tp.bary_p1, width, dm_index, y_values)				# calculate theoretical DM-curve and fit of observed DM-curve

                 
			###### file output ######
		#	op.listout("dm_fft_",dm_nx,dm_ny,cand)				# output of FFT DM curve in file
		#	op.listout("dm_curve_",dm_sqr_fit[5],list(dm_curve[0]),cand)	# output proper DM curve in file	
		#	op.listout("dm_curve_all_",[],list(dm_curve_all),cand)		# output all DM data in file
		#	op.listout("dm_sqr_fit_theo_",dm_sqr_fit[5],dm_sqr_fit[3],cand) # output theoretical DM curve in file
		#	op.listout("dm_sqr_fit_",dm_sqr_fit[5],dm_sqr_fit[4],cand)	# output fit of DM curve in file


                # print dm_sqr_fit[7]				# NN score: best period
                #  print dm_sqr_fit[8]				# NN score: best S/N value
                # print dm_sqr_fit[9]				# NN score: best DM value
                # print dm_sqr_fit[10]				# NN score: best pulse width
                # print dm_sqr_fit[11]				# NN score: theoretical peak
                # print abs(1-dm_sqr_fit[0][1])			# NN score: deviation of the proportional factor of DM-fit from 1
                # print abs(dm_sqr_fit[0][2])			# NN score: deviation of the fitted DM-value from the best DM-value off phcx data
                # print dm_sqr_fit[2]


			###### NN-scores ########
                        values.append(float(dm_sqr_fit[7]))				# NN score: best period
			values.append(float(dm_sqr_fit[8]))				# NN score: best S/N value
                        values.append(float(dm_sqr_fit[9]))				# NN score: best DM value
			values.append(float(dm_sqr_fit[10]))				# NN score: best pulse width
			values.append(float(dm_sqr_fit[11]))				# NN score: theoretical peak
			values.append(float(abs(1-dm_sqr_fit[0][1])))			# NN score: deviation of the proportional factor of DM-fit from 1
			values.append(float(abs(dm_sqr_fit[0][2])))			# NN score: deviation of the fitted DM-value from the best DM-value off phcx data
			values.append(float(dm_sqr_fit[2]))				# NN score: chi squared of theoretical DM-curve and observed data
                        #if float(dm_sqr_fit[9]) <+ 0:
                        #breaker = 0/10
                        #	'''
                        
                    
		#################### 15 - subband score ####################################################################################"
		#	'''



                        subbands = tp.plot_subbands()
			print "Goodbye subbands"
                        subints = tp.plot_intervals()
			print "Goodbye subints"
                        prof_bins = tp.proflen
                        #print "sb = ", subbands[0]
                        #print "si = ", subints
                        #print "pb = ", prof_bins
                        #print "hello"
			sb_score = op.subband_score(subbands, prof_bins, tp.nsub, width)			
			print "After sb_score"
		
			values.append(float(sb_score[0]))			# NN score: RMS scatter of the maxima
			values.append(float(sb_score[1]))			# NN score: mean correlation of the amplitude pairs
		#	'''


		#################### 16 - correlation of profile with subbands #############################################################"
		#	'''
			print "At 16"
			cs = op.profile_corr(subbands, prof_bins, tp.nsub, p)			# calculate the correlation of profile with all subbands
		#	cs_av = cs.mean()					# mean correlation coefficient 

			cs_int = 0				   	   #########
			for i in range(len(cs)):		   	 	# calculate integral of correlation coefficients
			   cs_int += cs[i]				   #########

                        #print cs_int
		#	op.listout("corr_p-subband_",[],list(cs),cand)		# output of correlation coefficients in file

		#	values.append(float(cs_av))				# NN score: mean correlation coefficient 
			values.append(float(cs_int))				# NN score: integral of correlation coefficients
		#	'''

                      

                
                        print values
                        

    

#################### subbands cross profile ###########################################################################"
			'''
			sb = op.cross_profile(subbands, prof_bins, tp.nsub,'Bands',1)
			sb_mean,sb_max = sb.mean(),sb.max()

			swap.append(sb_mean),   	names.append("sb_mean   \t")		 
			swap.append(sb_max),    	names.append("sb_max    \t")		 

			op.listout("subbands_",[],list(sb),cand)

		#	values.append(float(sb_mean))
		#	values.append(float(sb_max))
		#	'''








                ###################### reduced chi^2 vs period ##################################################################

                      #  period_chi2_fit = op.period_fit(tp.calc_redchi2(), tp.bestdm, tp.bary_p1, width, dm_list, y_numbers)
                        


                        return values

		

    




    

if __name__ == "__main__":
#    import sys
    
    #testpfd = "/home/ransom/tmp_pfd/M5_52725_W234_PSR_1518+0204A.pfd"
    #testpfd = "/home/ransom/tmp_pfd/M13_52724_W234_PSR_1641+3627C.pfd"
        



    #testpfd = sys.argv[1]
    #tp = pfd(testpfd)
    #cand = testpfd
#    print tp.start_secs
##    print tp.mid_secs
#    print tp.start_topo_MJDs
##    print tp.mid_topo_MJDs
#    print tp.T
    
    #tp.kill_subbands([6,7,8,9,30,31,32,33])
    #tp.kill_intervals([2,3,4,5,6])

    #tp.plot_chi2_vs_sub()
    #(chis, DMs) = tp.plot_chi2_vs_DM(0.0, 50.0, 501, interp=1)
    #best_index = Num.argmax(chis)
    #print "Best DM = ", DMs[best_index]

#    (chis, DMs) = tp.plot_chi2_vs_DM(0.0, 50.0, 501)
#    best_index = Num.argmax(chis)
#    print "Best DM = ", DMs[best_index]
    
    #tp.dedisperse()
   # tp.plot_subbands()
   # tp.plot_sumprof()
   # print "DM =", tp.bestdm, "gives reduced chi^2 =", tp.calc_redchi2()

    #tp.dedisperse(27.0)
    #tp.plot_subbands()
    #tp.plot_sumprof()
    #print "DM = 27.0 gives reduced chi^2 =", tp.calc_redchi2()

    #tp.dedisperse(33.0)
    #tp.plot_subbands()
    #tp.plot_sumprof()
    #print "DM = 33.0 gives reduced chi^2 =", tp.calc_redchi2()

    #tp.plot_intervals()



#exit(1)






   chis1 = []
   #chis2 = []
   #per = []

   all,scores = {},{}
#output = open(".dat","w")
#### use the following 2 lines when runnig the script on several candidates in the current directory
##for cand in glob.glob('*.pfd'):
   for cand in glob.glob('*.pfd'):
                tp = pfd(cand)
#while 1:
#os.listdir(os.getcwd()):
	#if cand.endswith('.phcx.gz'):
		#output = open(cand + ".dat", 'w')
#### use the following 4 lines when running the script on just one candidate at a time
#if (len (sys.argv) < 2):
#        print "USAGE: ./test.py PHCXFILENAME"
#        sys.exit(1)
#cand = sys.argv[1]
#                tp.dedisperse()

		################### 01 - check format of phcx data file ####################################################################
		#phcx_test = mk.test_phcx(xmldata)	
		#if phcx_test != "OK":
		#	print cand, "failed phcx-file test and will be excluded from the analysis!!!"
		try:
                #if 1:
                    #tp.plot_chi2_vs_DM(0.0, 50.0, 501, interp=1)                    #tp.test_22
                    profile = tp.plot_sumprof()
                    profile = profile/mean(profile)
                    #x = range(len(profile))
                    #pylab.plot(x, profile)
                    #pylab.show()
                    values = tp.test_22(profile)
                    #print values

                    
                    #print tp.T
                    
                    #print tp.test_22

########################### complete data ####################################################################################"
		#	all[cand] = swap
                 
                    scores[cand]  = values
                    #print scores
                    output = open(cand + ".dat", 'w')
                    #output.write(cand + ", ")
                    attributes = len(values)
                    for i in range(len(values)):
                        
                        #print values[i],
                        if (i+1) == attributes:
                            output.write(str(values[i]))
                        else:
                            output.write(str(values[i])+", ")
                                
                                #output2 = open("not_pulsars", "a")
                                #output2.write(cand + " 0\n")
                                #output2.close()
                    #chis1.append(values[0])
                    #chis2.append(values[1])
                    #per.append(values[11])
                    #print values[10], values[11]
                    output.write("\n")
                    output.close()                
		except:
			print "fail:",cand
                        
              

		#################### output ###########################################################################################"
		#
		# generates sorted output files from the data stored in the "swap"-list for all candidates
		#
		#op.sort_out(all,names)
		#
		#
		#################### pattern file generator ##########################################################################"
		#
		# generates a pattern file for a neural network from a list of known pulsars and non-pulsars
		# to generate the pulsar list "pulsars.dat" execute "pulsar_test.py" within the same directory
		#
   #mk.gen_patternfile(scores)
   #mk.gen_patternfile_fann(scores)

   
   
   #tp.plot_chisqs(scores)
   #pylab.scatter(per, chis1)
   #pylab.show()
   #pylab.scatter(per, chis1)
   #pylab.show()
		#

                    
		#except:
		#	print "fail:",cand
                
