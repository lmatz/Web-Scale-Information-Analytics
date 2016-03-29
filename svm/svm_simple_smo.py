import random
import numpy as np
import matplotlib.pyplot as plt

def load_data_set(file_name):
    data_mat = []
    label_mat = []
    with open(file_name,'r') as data_file:
        for line in data_file.readlines():
            line_arr = line.strip().split('\t')
            data_mat.append([float(line_arr[0]), float(line_arr[1])])
            label_mat.append(float(line_arr[2]))
    return data_mat, label_mat

# randomly choose an index j which is different from index i
def select_jrand(i, m):
    j=i
    while (j==i):
        j = int(random.uniform(0,m))
    return j

# if alpha is lower than L or greater than H
# we should clip it to satisfy the constraint
def clip_alpha(aj, H, L):
    if aj > H:
        aj = H
    if L > aj:
        aj = L
    return aj 

def smo_simple(data_matrix_input, class_labels, C, tolerance, max_iteration):
    data_matrix = np.mat(data_matrix_input)
    label_matrix = np.mat(class_labels).transpose()
    b = 0
    m, n = np.shape(data_matrix)
    alphas = np.mat(np.zeros((m,1)))
    iteration = 0
    while ( iteration<max_iteration ) :
        num_changed_alpha = 0
        for i in range(m):
            f_xi = float(np.multiply( alphas, label_matrix ).T * ( data_matrix * data_matrix[i,:].T )) + b
            e_xi = f_xi - float(label_matrix[i])
            if  ( (label_matrix[i] * e_xi < -tolerance) and (alphas[i] < C) )\
             or ( (label_matrix[i] * e_xi > tolerance) and (alphas[i] > 0)  )   :
                j = select_jrand(i, m)
                f_xj = float(np.multiply( alphas, label_matrix ).T * ( data_matrix * data_matrix[j,:].T )) + b
                e_xj = f_xj - float(label_matrix[j])

                alpha_i_old = alphas[i].copy()
                alpha_j_old = alphas[j].copy()

                if ( label_matrix[i] != label_matrix[j] ):
                    L = max(0, alphas[j] - alphas[i])
                    H = min(C, C + alphas[j] - alphas[i])
                if ( label_matrix[i] == label_matrix[j] ):
                    L = max(0, alphas[i] + alphas[j] - C)
                    H = min(C, alphas[i] + alphas[j])

                if L==H:
                    continue
                mu = 2 * data_matrix[i,:] * data_matrix[j,:].T - data_matrix[i,:] * data_matrix[i,:].T - data_matrix[j,:] * data_matrix[j,:].T
                if mu >= 0 :
                    continue
                alphas[j] = alphas[j] - label_matrix[j] * (e_xi - e_xj) / mu
                alphas[j] = clip_alpha(alphas[j], H, L)
                if abs( alpha_j_old - alphas[j] ) < 10**(-5):
                    continue
                alphas[i] = alphas[i] + label_matrix[i] * label_matrix[j] * ( alpha_j_old - alphas[j] )

                b1 = b - e_xi - label_matrix[i] * ( alphas[i] - alpha_i_old ) * ( data_matrix[i,:] * data_matrix[i,:].T )\
                    - label_matrix[j] * ( alphas[j] - alpha_j_old ) * ( data_matrix[i,:] * data_matrix[j,:].T )
                b2 = b - e_xj - label_matrix[i] * ( alphas[i] - alpha_i_old ) * ( data_matrix[i,:] * data_matrix[j,:].T )\
                    - label_matrix[j] * ( alphas[j] - alpha_j_old ) * ( data_matrix[j,:] * data_matrix[j,:].T )
                if alphas[i] < C and alphas[i] > 0 :
                    b = b1
                elif alphas[j] < C and alphas[j] > 0 :
                    b = b2
                else:
                    b = (b1 + b2)/2
                num_changed_alpha += 1 
                print "iter: %d i:%d, pairs changed %d" % (iteration,i,num_changed_alpha)
        if num_changed_alpha == 0:
            iteration += 1
        else:
            iteration = 0       
        print "iteration number: %d" % iteration
    w = np.mat(np.zeros((1,2)))
    for i in range(m):
        w += alphas[i] * label_matrix[i] * data_matrix[i,:]
    return b,alphas,w


def main():
    data_arr,label_arr = load_data_set('testSet.txt')
    b,alphas,w = smo_simple(data_arr, label_arr, 0.6, 0.001, 40)
    print "w: ",w
    print "b: ",b
    # print alphas[alphas>0]
    print ""
    # for i in range(100):
    #     if alphas[i]>0.0: 
    #         print data_arr[i],label_arr[i]

    data_arr_x_1 = [ data_arr[i][0] for i in range(len(data_arr)) if label_arr[i] == 1 ]
    data_arr_y_1 = [ data_arr[i][1] for i in range(len(data_arr)) if label_arr[i] == 1 ]

    data_arr_x_not_1 = [ data_arr[i][0] for i in range(len(data_arr)) if label_arr[i] != 1 ]
    data_arr_y_not_1 = [ data_arr[i][1] for i in range(len(data_arr)) if label_arr[i] != 1 ]


    plt.plot(data_arr_x_1,data_arr_y_1,'ro')
    plt.plot(data_arr_x_not_1,data_arr_y_not_1,'bo')

    x_range = np.arange(-6.0,12.0,0.1)
    y_range = np.arange(-6.0,6.0,0.1)
    X, Y = np.meshgrid(x_range, y_range)

    Z = w[0,0] * X + w[0,1] * Y + b

    plt.contour(X,Y,Z,[0]) 

    plt.show()


if __name__ == '__main__':
    main()
