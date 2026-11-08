import numpy as Num
from scipy.special import i0
import numpy.fft as FFT

isintorlong = lambda x: type(x) == type(0) or type(x) == type(0L)

def delay_from_DM(DM, freq_emitted):
    """
    Return the delay in seconds caused by dispersion, given
    a Dispersion Measure (DM) in cm-3 pc, and the emitted
    frequency (freq_emitted) of the pulsar in MHz.
    """
    if (type(freq_emitted)==type(0.0)):
        if (freq_emitted > 0.0):
            return DM/(0.000241*freq_emitted*freq_emitted)
        else:
            return 0.0
    else:
        return Num.where(freq_emitted > 0.0,
                         DM/(0.000241*freq_emitted*freq_emitted), 0.0)

def fft_rotate(arr, bins):
    """
    fft_rotate(arr, bins):
        Return array 'arr' rotated by 'bins' places to the left.  The
            rotation is done in the Fourier domain using the Shift Theorem.
            'bins' can be fractional.  The resulting vector will have
            the same length as the oiginal.
    """
    arr = Num.asarray(arr)
    freqs = Num.arange(arr.size/2+1, dtype=Num.float)
    phasor = Num.exp(complex(0.0, (2.0*Num.pi)) * freqs * bins / float(arr.size))
    return Num.fft.irfft(phasor * Num.fft.rfft(arr))

def rotate(arr, bins):
    """
    rotate(arr, bins):
        Return an array rotated by 'bins' places to the left
    """
    bins = bins % len(arr)
    if bins==0:
        return arr
    else:
        return Num.concatenate((arr[bins:], arr[:bins]))
    
def interp_rotate(arr, bins, zoomfact=10):
    """
    interp_rotate(arr, bins, zoomfact=10):
        Return a sinc-interpolated array rotated by 'bins' places to the left.
            'bins' can be fractional and will be rounded to the closest
            whole-number of interpolated bins.  The resulting vector will
            have the same length as the oiginal.
    """
    newlen = len(arr)*zoomfact
    rotbins = int(Num.floor(bins*zoomfact+0.5)) % newlen
    newarr = periodic_interp(arr, zoomfact)  
    return rotate(newarr, rotbins)[::zoomfact]

def span(Min, Max, Number):
    """
    span(Min, Max, Number):
        Create a range of 'Num' floats given inclusive 'Min' and 'Max' values.
    """
    assert isintorlong(Number)
    if isintorlong(Min) and isintorlong(Max) and \
       (Max-Min) % (Number-1) != 0:
        Max = float(Max) # force floating points
    return Min+(Max-Min)*Num.arange(Number)/(Number-1)

def sinc(xs):
    """
    sinc(xs):
        Return the sinc function [i.e. sin(pi * xs)/(pi * xs)]
            for the values xs.
    """
    pxs = Num.pi*xs
    return Num.where(Num.fabs(pxs)<1e-3, 1.0-pxs*pxs/6.0, Num.sin(pxs)/pxs)

def kaiser_window(xs, halfwidth, alpha):
    """
    kaiser_window(xs, halfwidth, alpha):
        Return the kaiser window function for the values 'xs' when the
            the half-width of the window should be 'haldwidth' with
            the folloff parameter 'alpha'.  The following values are
            particularly interesting:

            alpha
            -----
            0           Rectangular Window
            5           Similar to Hamming window
            6           Similar to Hanning window
            8.6         Almost identical to the Blackman window 
    """
    win = i0(alpha*Num.sqrt(1.0-(xs/halfwidth)**2.0))/i0(alpha)
    return Num.where(Num.fabs(xs)<=halfwidth, win, 0.0)

def hanning_window(xs, halfwidth):
    """
    hanning_window(xs, halfwidth):
        Return the Hanning window of halfwidth 'halfwidth' evaluated at
            the values 'xs'.
    """
    win =  0.5 + 0.5*Num.cos(Num.pi*xs/halfwidth)
    return Num.where(Num.fabs(xs)<=halfwidth, win, 0.0)

def hamming_window(xs, halfwidth):
    """
    hamming_window(xs, halfwidth):
        Return the Hamming window of halfwidth 'halfwidth' evaluated at
            the values 'xs'.
    """
    win =  0.54 + 0.46*Num.cos(Num.pi*xs/halfwidth)
    return Num.where(Num.fabs(xs)<=halfwidth, win, 0.0)

def blackman_window(xs, halfwidth):
    """
    blackman_window(xs, halfwidth):
        Return the Blackman window of halfwidth 'halfwidth' evaluated at
            the values 'xs'.
    """
    rat = Num.pi*xs/halfwidth
    win =  0.42 + 0.5*Num.cos(rat) + 0.08*Num.cos(2.0*rat) 
    return Num.where(Num.fabs(xs)<=halfwidth, win, 0.0)

def rectangular_window(xs, halfwidth):
    """
    rectangular_window(xs, halfwidth):
        Return a rectangular window of halfwidth 'halfwidth' evaluated at
            the values 'xs'.
    """
    return Num.where(Num.fabs(xs)<=halfwidth, 1.0, 0.0)
    
_window_function = {"rectangular": rectangular_window,
                    "none": rectangular_window,
                    "hanning": hanning_window,
                    "hamming": hamming_window,
                    "blackman": blackman_window,
                    "kaiser": kaiser_window}

def periodic_interp(data, zoomfact, window='hanning', alpha=6.0):
    """
    periodic_interp(data, zoomfact, window='hanning', alpha=6.0):
        Return a periodic, windowed, sinc-interpolation of the data which
            is oversampled by a factor of 'zoomfact'.
    """
    zoomfact = int(zoomfact)
    if (zoomfact < 1):
        print "zoomfact must be >= 1."
        return 0.0
    elif zoomfact==1:
        return data
    newN = len(data)*zoomfact
    # Space out the data
    comb = Num.zeros((zoomfact, len(data)), dtype='d')
    comb[0] += data
    comb = Num.reshape(Num.transpose(comb), (newN,))
    # Compute the offsets
    xs = Num.zeros(newN, dtype='d')
    xs[:newN/2+1] = Num.arange(newN/2+1, dtype='d')/zoomfact
    xs[-newN/2:]  = xs[::-1][newN/2-1:-1]
    # Calculate the sinc times window for the kernel
    if window.lower()=="kaiser":
        win = _window_function[window](xs, len(data)/2, alpha)
    else:
        win = _window_function[window](xs, len(data)/2)
    kernel = win * sinc(xs)
    if (0):
        print "would have plotted."
    return FFT.irfft(FFT.rfft(kernel) * FFT.rfft(comb))
