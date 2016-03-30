import sys, random, types, time, re, operator
import numpy as np
from collections import defaultdict


def em_algo(data, tolerance, max_iteration):
    N = len(data)
    D = len(data[0])
    print N,D
    iteration = 0

    K=10
    pi = np.random.rand(K)
    mu = np.random.rand(K, D)
    r = np.zeros([N, K])
    weight = np.zeros(K)

    while iteration <= max_iteration:
        print iteration
        # E-step
        for n in range(N):
            image = data[n]
            for k in range(K):
                weight[k] = pi[k]
                for d in range(D):
                    weight[k] *= mu[k][d]**image[d]+(1-mu[k][d])**(1-image[d])
            r[n,:] = weight/sum(weight)

        print "E step finish"

        # M-step
        nk = [ sum(r[:,i]) for i in range(K) ]

        new_mu = np.zeros([K,D])

        for k in range(0, K):
            mean = np.zeros(D)
            for n in range(0, N):
                mean += r[n, k] * data[n] 
            new_mu[k] = mean / nk[k]
        pi = nk / sum(nk) 

        print "M step finish"

        change = sum(sum(abs(new_mu-mu)))
        if change < tolerance:
            break
        else:
            mu = new_mu
            iteration += 1

    return [mu, pi, iteration, r]







def main():
    # read label and data file
    with open('t10k-labels-idx1-uft8.txt', 'r') as labelFile:
        labels = labelFile.read().splitlines()

    with open('t10k-images-idx3-uft8.txt', 'r') as dataFile:
        data = dataFile.read().splitlines()

    # cast each test data from string to numpy array

    data = data[:1000]
    points = []
    for vector in data:
        vector_temp = map( lambda s: 0 if int(s)==0 else 1 , filter( lambda s: s.isdigit()  , re.split(r'\t+', vector) ))
        vector_temp = np.array(vector_temp)
        points.append(vector_temp)
    data = points

    tolerance = 1e-5
    max_iteration = 1000

    [mu_est, pi_est, iterations,r] = em_algo(data, tolerance, max_iteration)

    output_labels =[]

    for scores in r:
        max = -1
        index = -1
        for i in range(10):
            if max < scores[i]:
                max =  scores[i]
                index = i
        output_labels.append(index)

    # for vector in data:
    #     label = find_cluster(vector, mu_est, pi_est)
    #     output_labels.append(label)


    with open('result_bmm', 'w+') as outputFile:
        for val in output_labels:
            outputFile.write(str(val)+"\n")



if __name__ == '__main__':
    main()