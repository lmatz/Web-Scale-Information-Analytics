import numpy as np
import scipy 
import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d import Axes3D
from sklearn import decomposition
from sklearn import datasets


def main():
    a = np.mat('2 2 5 7 0 3;0 7 0 0 10 6;8 0 7 8 0 8;6 2 4 5 0 7;0 8 0 0 7 7 ' )
    U, s, Vh = np.linalg.svd(a, full_matrices=False)
    # print "U: " , U
    U = U[:,:-3]
    print "U: " , U
    # print "s: " , s
    s = s[:2]
    print "s: " , s
    # print "V: " , V
    Vh = Vh[:-3,:]
    print "Vh: " , Vh
    new_a = np.dot(U, np.dot(np.diag(s), Vh))
    print "result: ",new_a

    # assert np.allclose(a, np.dot(U, np.dot(np.diag(s), Vh)))

    # s[2:] = 0
    # new_a = np.dot(U, np.dot(np.diag(s), Vh))
    # print(new_a)

    doc3 = a[2,:]
    print doc3

    new_coordinate_of_doc3 = np.dot(doc3, Vh.T)
    print "doc3: ",new_coordinate_of_doc3

    new_coordinate_of_all = np.dot(a,Vh.T)
    print "all: ",new_coordinate_of_all

    new_coordinate_of_all_for_word = np.dot(a.T,U)
    print "all_word: ",new_coordinate_of_all_for_word


    X = np.array(new_coordinate_of_all[:,0]).flatten().tolist()
    Y = np.array(new_coordinate_of_all[:,1]).flatten().tolist()

    X_ = np.array(new_coordinate_of_all_for_word[:,0]).flatten().tolist()
    Y_ = np.array(new_coordinate_of_all_for_word[:,0]).flatten().tolist()

    plt.plot(X,Y,'ro')
    # plt.plot(X_,Y_,'bo')
    plt.show()







if __name__ == '__main__':
    main()