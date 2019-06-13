#!/bin/python
import pandas as pd

xsec = pd.read_csv('/home/nira/Desktop/xsec/XNS-Colombo+Kolonnawa.txt', header=None, skip_blank_lines=False)
chainage = pd.read_csv('/home/nira/Desktop/xsec/crossection_output.txt', header=None,
                       names=['name', 'idx', 'chain', 'x', 'y'])
chainage['name'].str.upper()
stars = xsec[xsec[0].str.startswith('*********')].index.tolist()

with open('/home/nira/Desktop/xsec/xsec-xyz.txt', 'w') as outfile:
    for i in stars[0:len(stars) - 1]:
        name = xsec[0][i + 2].strip().upper()
        chain = float(xsec[0][i + 3])
        print('%d %s %f' % (i, name, chain))
        prof = xsec[0][i + 24].split()
        if prof[0].startswith('PROFILE'):
            profile_count = int(xsec[0][i + 24].split()[1])
            z = min([float(x.split()[1]) for x in xsec[0][i + 25:i + 25 + profile_count]])
            xy = chainage[(chainage['name'] == name) & (abs(chainage['chain'] - chain) < 0.0001)]
            if len(xy):
                outfile.write('%s, %f, %f, %f, %f\n' % (name, chain, xy['x'].values[0], xy['y'].values[0], z))
            else:
                print('Error %s %f' % (name, chain))
        else:
            print('Not PROFILE ' + prof[0])
