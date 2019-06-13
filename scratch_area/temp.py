import os
import numpy as np
from sympy.physics.quantum import density

os.chdir('/home/curw/wrf_compare')

sat_file = '/home/curw/wrf_compare/mnt/disks/curwsl_nfs/sat/jaxa_sat_rf_2017-08-18_12:00.asc'
# wrf_file = '/home/curw/wrf_compare/mnt/disks/curwsl_nfs/output/wrf0/2017-08-17_00:00/1/wrf_inst_2017-08-18_12:00:00.asc'
wrf_file = '/home/curw/wrf_compare/mnt/disks/curwsl_nfs/output/wrf1/2017-08-17_00:00/1/wrf_inst_2017-08-18_12:00:00.asc'


# wrf_file = '/home/curw/wrf_compare/mnt/disks/curwsl_nfs/output/wrf2/2017-08-17_00:00/2/wrf_inst_2017-08-18_12:00:00.asc'
# wrf_file = '/home/curw/wrf_compare/mnt/disks/curwsl_nfs/output/wrf3/2017-08-17_00:00/1/wrf_inst_2017-08-18_12:00:00.asc'


def read_asc_file(path):
    meta = {}
    with open(path) as f:
        for i in range(6):
            line = next(f).split()
            meta[line[0]] = float(line[1])

    data = np.genfromtxt(path, skip_header=6)
    return data, meta


sat, sat_meta = read_asc_file(sat_file)
wrf, wrf_meta = read_asc_file(wrf_file)


def shrink_2D_array(data, new_shape, func=np.sum):
    cur_shape = np.shape(data)
    row_bins = np.round(np.arange(new_shape[0] + 1) * cur_shape[0] / new_shape[0]).astype(int)
    col_bins = np.round(np.arange(new_shape[1] + 1) * cur_shape[1] / new_shape[1]).astype(int)

    output = np.zeros(new_shape)
    for i in range(len(row_bins) - 1):
        for j in range(len(col_bins) - 1):
            output[i, j] = np.average(data[row_bins[i]:row_bins[i + 1], col_bins[j]:col_bins[j + 1]])

    return output


wrf_s = np.round(shrink_2D_array(wrf, (int(sat_meta['NROWS']), int(sat_meta['NCOLS']))), 3)

sat_s = np.round(sat, 3)

from scipy import ndimage

w_c = ndimage.measurements.center_of_mass(wrf_s)

s_c = ndimage.measurements.center_of_mass(sat_s)
import matplotlib.pyplot as plt

plt.imshow(wrf_s)
plt.imshow(sat_s)
plt.plot(s_c[1], s_c[0], 'bo')
plt.plot(w_c[1], w_c[0], 'ro')
plt.annotate

import scipy.signal

EPS = np.finfo(float).eps


def mutual_information_2d(x, y, sigma=1, normalized=False):
    """
    Computes (normalized) mutual information between two 1D variate from a
    joint histogram.
    Parameters
    ----------
    x : 1D array
        first variable
    y : 1D array
        second variable
    sigma: float
        sigma for Gaussian smoothing of the joint histogram
    Returns
    -------
    nmi: float
        the computed similariy measure
    """
    bins = (256, 256)

    jh = np.histogram2d(x, y, bins=bins)[0]

    # smooth the jh with a gaussian filter of given sigma
    ndimage.gaussian_filter(jh, sigma=sigma, mode='constant',
                            output=jh)

    # compute marginal histograms
    jh = jh + EPS
    sh = np.sum(jh)
    jh = jh / sh
    s1 = np.sum(jh, axis=0).reshape((-1, jh.shape[0]))
    s2 = np.sum(jh, axis=1).reshape((jh.shape[1], -1))

    # Normalised Mutual Information of:
    # Studholme,  jhill & jhawkes (1998).
    # "A normalized entropy measure of 3-D medical image alignment".
    # in Proc. Medical Imaging 1998, vol. 3338, San Diego, CA, pp. 132-143.
    if normalized:
        mi = ((np.sum(s1 * np.log(s1)) + np.sum(s2 * np.log(s2)))
              / np.sum(jh * np.log(jh))) - 1
    else:
        mi = (np.sum(jh * np.log(jh)) - np.sum(s1 * np.log(s1))
              - np.sum(s2 * np.log(s2)))

    return mi


import scipy

a = scipy.signal.correlate2d(sat_s, wrf_s, mode='same')
aa = ndimage.measurements.center_of_mass(a)


def round_partial(value, resolution):
    return np.round(value / float(resolution)) * resolution


plt.imshow(a)
plt.imshow(ndimage.gaussian_filter(sat_s, 1))

wrf_file = '/home/curw/wrf_compare/mnt/disks/curwsl_nfs/output/wrf1/2017-08-17_00:00/1/wrf_inst_2017-08-18_15:00:00.asc'
wrf, wrf_meta = read_asc_file(wrf_file)

# wrf[wrf<0.25] = 0
plt.figure(1)
plt.imshow(wrf)
plt.colorbar()

plt.figure(2)
plt.plot(wrf.flatten())

plt.figure(0)
wrf0 = wrf*(wrf>0.2)
plt.imshow(wrf0)
plt.colorbar()


plt.imshow(ndimage.median_filter(wrf, 3))

wrf_s = np.round(shrink_2D_array(wrf, (int(sat_meta['NROWS']), int(sat_meta['NCOLS']))), 3)
plt.hist(wrf_s, bins='auto')
plt.figure(1)
plt.imshow(wrf_s)
plt.colorbar()

plt.figure(0)
plt.imshow(sat_s)
plt.colorbar()

plt.figure(3)
plt.hist(wrf.flatten(), bins= 1000)
plt.legend()


wrf_sg = ndimage.gaussian_filter(wrf_s, 1)
plt.hist(wrf_sg, bins='auto')
plt.legend()

# wrf_s[wrf_s<0.5] = 0
plt.imshow(ndimage.gaussian_filter(wrf_s, 1))
plt.imshow(ndimage.median_filter(wrf_s, 3))
plt.colorbar()

plt.plot(s_c[1], s_c[0], 'bo')
plt.plot(w_c[1], w_c[0], 'ro')
plt.plot(aa[1], aa[0], 'yo')

b = np.corrcoef(sat_s.flatten(), wrf_s.flatten())
mutual_information_2d(sat_s.flatten(), wrf_s.flatten())

plt.imshow(shrink_2D_array(sat_s, (22, 13)))
plt.imshow(shrink_2D_array(wrf_s, (22, 13)))

plt.plot(sat_s.flatten())
plt.plot(wrf_s.flatten())
