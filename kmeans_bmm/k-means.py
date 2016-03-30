import sys, random, types, time, re, operator
import numpy as np
from collections import defaultdict


def calculate_distance(vector_1, vector_2):
    dist = np.linalg.norm(vector_1-vector_2)
    return dist


def find_the_cluster(index, data, cluster_center):
    distance = sys.maxint
    cluster_num = -1
    for clusterNum in range(0,10):
        current_distance = calculate_distance( cluster_center[clusterNum] ,data[index] )
        if distance > current_distance:
            distance = current_distance
            cluster_num = clusterNum
    return cluster_num


def calculate_new_center(data, cluster_center, label_for_each):
    new_cluster_center = []
    for now in range(0,10):
        center = np.zeros(784)
        num = 0
        for val in label_for_each[now]:
            num = num + 1
            center = center + data[val]
        center = center / num
        new_cluster_center.append(center)
    return new_cluster_center


# return: 1 number of points in this cluster
#         2 major label
#         3 number of correctly clusted points
#         4 accuracy = ratio of 3 and 1
def accuracy_each_cluster(point_index_list, labels):
    count_dict = {}
    num_total_points = 0
    for i in range(10):
        count_dict[str(i)] = 0
    for index in point_index_list:
        count_dict[ labels[index] ] += 1
        num_total_points += 1
    major_label = max(count_dict.iteritems(), key=operator.itemgetter(1))[0]
    num_correct_points = count_dict[major_label]
    return num_total_points, major_label, num_correct_points, float(num_correct_points) / num_total_points


def accuracy_entire():
    with open('t10k-labels-idx1-uft8.txt', 'r') as labelFile:
        labels = labelFile.read().splitlines()

    with open('result', 'r') as labelFile_2:
        labels_2 = labelFile_2.read().splitlines()

    size = len(labels)
    right = 0
    for i in range(len(labels)):
        if labels[i]==labels_2[i]:
            right = right + 1
    print str(size) + "  " + str(right) + "  " + str(float(right)/size)


def confusion_matrix(label_for_each, labels ):
    for key in label_for_each:
        label_for_each[key]


def main():
    # read label and data file
    with open('t10k-labels-idx1-uft8.txt', 'r') as labelFile:
        labels = labelFile.read().splitlines()

    with open('t10k-images-idx3-uft8.txt', 'r') as dataFile:
        data = dataFile.read().splitlines()

    label_for_each = defaultdict(list)

    # cast each test data from string to numpy array
    points = []
    for vector in data:
        vector_temp = [ int(s) for s in re.split(r'\t+', vector) if s.isdigit() ]
        vector_temp = np.array(vector_temp)
        points.append(vector_temp)
    data = points


    for index, item in enumerate(labels):
        label_for_each[item].append(index)

    # start_point records the first k points
    start_point = []

    # start by picking k, the number of clusters
    # pick one point at random
    random_number = random.randint(0,len(label_for_each['0'])-1)
    current_point = label_for_each['0'][random_number]
    start_point.append(current_point)

    # then k-1 other points, each as far away as possible from the previous points
    for now in range(1,10):
        print now
        min_distance_to_cluster = -sys.maxint
        for val in label_for_each[str(now)]:
            min_distance = min([ calculate_distance( data[val], data[start_point[past]] ) for past in range(0,now)])
            if min_distance_to_cluster < min_distance:
                min_distance_to_cluster = min_distance
                current_point = val
        start_point.append(current_point)
        print labels[current_point]


    print start_point

    cluster_center = [ data[index] for index in start_point ]

    output_label = []

    last_label_for_each = defaultdict(list)

    i = 0

    # for each point, place it in the cluster whose current centroid it is nearest
    while True:
        print i
        i += 1
        label_for_each = defaultdict(list)
        for index, item in enumerate(data):
            cluster_num = find_the_cluster(index, data, cluster_center)
            label_for_each[cluster_num].append(index)
        if i != 1:
                if cmp(label_for_each, last_label_for_each) == 0:
                    break
        last_label_for_each = label_for_each
        # after all points are assigned, update the locations of centroids of the k clusters
        cluster_center = calculate_new_center(data, cluster_center, label_for_each)


    label_for_each = defaultdict(list)
    for index, item in enumerate(data):
            # which cluster should this point be assigned to?
            cluster_num = find_the_cluster(index, data, cluster_center)
            label_for_each[cluster_num].append(index)
            output_label.append(cluster_num)



    with open('result', 'w+') as outputFile:
        for val in output_label:
            outputFile.write(str(val)+"\n")

    # calculate the statistics of each cluster
    for key in label_for_each:
        num_total_points, major_label, num_correct_points, accuracy = accuracy_each_cluster( label_for_each[key], labels )
        print str(key) + ":  " + str(num_total_points) + "  " + str(major_label) + "  " + str(num_correct_points) + "  " + str(accuracy)

    # calculate the statistics of entire data set
    accuracy_entire()



if __name__ == '__main__':
    main()