def merge_count(accumulated, current):
    return accumulated + 1

def combine_count(accumulated_1, accumulated_2):
    return accumulated_1 + accumulated_2

def merge_itemset(accumulated, current):
    accumulated.append(current)
    return accumulated

def combine_itemsetlist(accumulated_1, accumulated_2):
    accumulated_1.extend(accumulated_2)
    accumulated_1.sort()
    return accumulated_1

def generate_candidate_itemsets(base_itemsets, filtered_itemsets):
    candidate_itemsets = [
        tuple(filtered_itemsets[j] + [base_itemsets[i][0]])
        for i in range(len(base_itemsets))
        for j in range(len(filtered_itemsets))
        if base_itemsets[i][0] > filtered_itemsets[j][-1]
    ]
    return candidate_itemsets

def is_subset(child, parent):
    return all(elem in parent for elem in child)

def count_itemset(cancerpatients_expressed_genelist, candidate_itemsets, min_support):
    filtered_itemsets = []
    frequent_itemsets = []
    for itemset in candidate_itemsets:
        itemset_count = cancerpatients_expressed_genelist.filter(lambda x_y: is_subset(itemset, x_y[1])).map(lambda x_y: (itemset, 1)).aggregateByKey(0, merge_count, combine_count, 1).collect()
        if itemset_count[0][1] > min_support:
            filtered_itemsets.append(itemset)
            frequent_itemsets.extend(itemset_count)
    return filtered_itemsets, frequent_itemsets

if __name__ == '__main__':
    sc = SparkContext(appName="Frequent itemsets")
    patient_meta_data = sc.textFile("../data/PatientMetaData.txt")
    cancer_patients = patient_meta_data.filter(filter_by_cancertype).map(extract_cancerpatientid).collect()
    geo = sc.textFile('../data/GEO.txt')
    cancerpatients_expressed_geneid = geo.filter(lambda x: x.strip().split(",")[0] in cancer_patients and float(x.strip().split(",")[2]) >= 1250000).map(extract_patientid_geneid)

    cancerpatients_expressed_genelist = cancerpatients_expressed_geneid.aggregateByKey([], merge_geneid, combine_genelist, 1)
    num_of_cancerpatients = cancerpatients_expressed_genelist.count()
    min_support = int(num_of_cancerpatients * 0.3)
    expressed_cancergenelist = cancerpatients_expressed_geneid.map(lambda x_y: (1, x_y[1])).distinct().aggregateByKey([], merge_geneid, combine_genelist, 1).map(lambda x_y: [tuple([z]) for z in x_y[1]]).collect()[0]

    # finding individual frequent items (itemsets of size 1)
    frequent_itemsets = []
    base_itemsets = []
    filtered_itemsets, frequent_itemsets_to_add = count_itemset(cancerpatients_expressed_genelist, expressed_cancergenelist, min_support)
    base_itemsets = filtered_itemsets
    frequent_itemsets.extend(frequent_itemsets_to_add)
    candidate_itemsets = generate_candidate_itemsets(base_itemsets, base_itemsets)

    # iteratively generates candidate itemsets of length k+1 from frequent itemsets of length k, terminates when no further frequent itemsets are found
    while True:
        filtered_itemsets, frequent_itemsets_to_add = count_itemset(cancerpatients_expressed_genelist, candidate_itemsets, min_support)
        if not frequent_itemsets_to_add:
            break
        frequent_itemsets.extend(frequent_itemsets_to_add)
        candidate_itemsets = generate_candidate_itemsets(base_itemsets, filtered_itemsets)

    frequent_itemsets_rdd = sc.parallelize(frequent_itemsets).map(lambda x_y: (x_y[1], x_y[0])).aggregateByKey([], merge_itemset, combine_itemsetlist, 1).sortBy(lambda x: x[0], ascending=False)
    print(frequent_itemsets_rdd.collect())