import numpy as Num
from psr_constants import *

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
