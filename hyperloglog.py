import hashlib
from scipy import integrate
import math
import numpy as np
from pylab import *
import matplotlib.pyplot  as pyplot

def count_naive(file_name):

    count = {}

    for w in open(file_name).read().split():
        if w in count:
            count[w] += 1
        else:
            count[w] = 1

    distinct = 0
    for word, times in count.items():
        distinct += 1

    print file_name+"'s distinct words: ",distinct

    return distinct

def lowest_0_position(binary_string):
    for i in range(len(binary_string)-1,-1,-1):
        if binary_string[i]!='1':
            return len(binary_string)-i
    return 0

def count_hyper(file_name):

    count = [1 for x in range(425)]


    for w in open(file_name).read().split():
        index = int(hashlib.sha1(w).hexdigest(), 16) % 425
        hashcode = hashlib.md5(w).hexdigest()
        lowest_0 = lowest_0_position(bin(int(hashcode,16))[2:20])
        if lowest_0 > count[index]:
            count[index] = lowest_0


    func = lambda x: math.pow(math.log((x+2)/(x+1),2),424)
    result = integrate.quad(func, 0, np.inf)[0]

    Bm = float(1) / ( 424 *result )

    result = 0
    for i in range(425):
        result += math.pow(2, -1*count[i])

    n = Bm*424*424 / result

    print file_name+"'s distinct words: ",n

    return n


def main():
    d=count_naive("largefile")
    n=count_hyper("largefile")

    d1=count_naive("gutenberg_A")
    n1=count_hyper("gutenberg_A")

    d2=count_naive("gutenberg_B")
    n2=count_hyper("gutenberg_B")

    d2=count_naive("gutenberg_C")
    n2=count_hyper("gutenberg_C")

    d3=count_naive("gutenberg_D")
    n3=count_hyper("gutenberg_D")

    d4=count_naive("gutenberg")
    n4=count_hyper("gutenberg")

    xs = []
    xs.append(d1)
    xs.append(d2)
    xs.append(d3)
    xs.append(d4)

    ys = []
    ys.append( abs(n1-d1)/d1 )
    ys.append( abs(n2-d2)/d2 )
    ys.append( abs(n3-d3)/d3 )
    ys.append( abs(n4-d4)/d4 )

    pyplot.plot(xs, ys)
    pyplot.xscale('log')

    pyplot.show()


if __name__ == '__main__':
    main()