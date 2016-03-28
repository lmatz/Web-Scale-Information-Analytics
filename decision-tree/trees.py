import operator, json, copy, pickle
from math import log


def pretty_print(output_data, indent=0):
    for key, value in output_data.iteritems():
        print '\t'*indent + str(key)
        if isinstance(value, dict):
            pretty_print(value, indent+1)
        else:
            print '\t'*(indent+1) + str(value)

def majority_count(class_list):
    class_count = {}
    for vote in class_list:
        if vote not in class_count.keys():
            class_count[vote] = 0
        class_count[vote] += 1
        sorted_class_count = sorted(class_count.iteritems(), 
            key=operator.itemgetter(1), 
            reverse=True)
    return sorted_class_count[0][0]



def calculate_entropy(data_set):
    num_entries = len(data_set)
    label_counts = {}
    for feat_vec in data_set:
        current_label = feat_vec[-1]
        if current_label not in label_counts.keys():
            label_counts[current_label] = 0
        label_counts[current_label] += 1
    entropy = 0.0
    for key in label_counts:
        probability = float(label_counts[key]) / num_entries
        entropy -= probability * log(probability, 2)
    return entropy


def create_data_set():
    data_set = [[1, 1, 'yes'],[1, 1, 'yes'],[1, 0, 'no'],[0, 1, 'no'],[0, 1, 'no']]
    labels = ["no surfacing", "flippers"]
    return data_set, labels


def split_data_set(data_set, axis, value):
    ret_data_set = []
    for feat_vec in data_set:
        if feat_vec[axis] == value:
            reduced_feat_vec = feat_vec[:axis]
            reduced_feat_vec.extend( feat_vec[axis+1:] )
            ret_data_set.append(reduced_feat_vec)
    return ret_data_set


def choose_best_split(data_set):
    num_features = len(data_set[0])-1
    base_entropy = calculate_entropy(data_set)
    base_gain = 0.0
    best_feature = -1
    for i in range(num_features):
        feature_list = [ example[i] for example in data_set  ] 
        unique_values = set(feature_list)
        new_entropy = 0.0
        for value in unique_values:
            sub_data_set = split_data_set(data_set,i,value)
            probability = len(sub_data_set)/float(len(data_set))
            new_entropy += probability * calculate_entropy(sub_data_set)
        info_gain = base_entropy - new_entropy
        if (info_gain > base_gain):
            base_gain = info_gain
            best_feature = i
    return best_feature


def create_tree(data_set,labels):
    class_list = [ example[-1] for example in data_set ]
    # if all the labels are the same, then easily give the answer
    if class_list.count(class_list[0]) == len(class_list):
        return class_list[0]
    # if we have exhausted the features and there are still variable
    # options, then give the majority one as the answer
    if len(data_set[0]) == 1:
        return majority_count(class_list)
    # split as normal
    # choose the best split to have the maximum info gain
    best_feature = choose_best_split(data_set)
    # find the according label
    best_feature_label = labels[best_feature]
    # create the current tree node
    my_tree = {best_feature_label:{}}
    # delete the feature we chose
    del(labels[best_feature])
    # get all the values of the chosen feature
    feature_values = [ example[best_feature] for example in data_set]
    unique_values = set(feature_values)
    # according to the values, split the dataset to
    # recursively create the child node
    for value in unique_values:
        sub_labels = labels[:]
        sub_data_set = split_data_set(data_set, best_feature, value)
        my_tree[best_feature_label][value]= create_tree(sub_data_set, sub_labels)
    return my_tree


def classify(input_tree, feature_labels, test_vec):
    first_string = input_tree.keys()[0]
    second_dict = input_tree[first_string]
    feature_index = feature_labels.index(first_string)
    for key in second_dict.keys():
        if test_vec[feature_index] == key:
            if type(second_dict[key]).__name__=='dict':
                class_label = classify(second_dict[key], feature_labels, test_vec)
            else:
                class_label = second_dict[key]
    return class_label


def store_tree(input_tree, filename):
    with open(filename,'w') as output_file:
        pickle.dump(input_tree,output_file)


def load_tree(filename):
    with open(filename,'r') as input_file:
        return pickle.load(input_file)

def main():
    # myDat, labels = create_data_set()

    # feature_labels = copy.deepcopy(labels)

    # print myDat
    # print labels

    # # print calculate_entropy(myDat)
    # # print split_data_set(myDat,0,1)
    # # print split_data_set(myDat,0,0)
    # # print "best feature: ", choose_best_split(myDat)

    # my_tree = create_tree(myDat, labels)

    # print json.dumps(my_tree,indent=4)

    # print classify(my_tree, feature_labels, [1,0])

    # print classify(my_tree, feature_labels, [1,1])

    # store_tree(my_tree,"my_tree")

    # my_tree = load_tree("my_tree")
    
    # print my_tree


if __name__ == '__main__':
    main()