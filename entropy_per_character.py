#!/usr/bin/env python
'''
Calculate Shannon Entropy (min bits per byte-character)
original source: https://www.kennethghartman.com/calculate-file-entropy/
'''

__version__ = '0.1'
__description__ = 'Calculate Shannon Entropy for given file'

import sys
import math

def main():
    entropy()

def entropy():
    print('Opening file {}...'.format(sys.argv[1]))
    with open(sys.argv[1], 'rb') as f:
        byteArr = list(f.read())
    fileSize = len(byteArr)
    print
    print('File size in bytes: {:,d}'.format(fileSize))
    # calculate the frequency of each byte value in the file
    print('Calculating Shannon entropy of file. Please wait...')
    freqList = []
    for b in range(10,123):
        #print("type b is: "+ str(type(b)))
        ctr = 0
        for byte in byteArr:
            #print("type for byte is" + str(type(byte)) + "type b is: " + str(type(b)))
            if ord(byte) == b:
                ctr += 1
                #print("ctr is: " +str(ctr))
        print("for char: " +str(b) + " the freq is: "+ str(float(ctr) / fileSize))               
        freqList.append(float(ctr) / fileSize)
    # Shannon entropy
    ent = 0.0
    for freq in freqList:
        #print(freq)
        if freq > 0:
            ent = ent + freq * math.log(freq, 2)
            #print(freq)
    ent = -ent
    print('Shannon entropy: {}'.format(ent))
    print


if __name__== "__main__":
    main()
