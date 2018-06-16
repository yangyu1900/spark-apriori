from pyspark import SparkContext

def filter_by_cancertype(record):
    has_cancer = False
    cancer_list = ["breast-cancer", "prostate-cancer", "pancreatic-cancer", "leukemia", "lymphoma"]
    patientid, age, gender, postcode, diseases, drug_response = record.strip().split(",")
    for disease in diseases.strip().split(" "):
        if disease.strip() in cancer_list:
            has_cancer = True
    return has_cancer

def extract_cancerpatientid(record):
    patientid, age, gender, postcode, diseases, drug_response = record.strip().split(",")
    return patientid

def extract_patientid_geneid(record):
    patientid, geneid, expression_value = record.strip().split(",")
    return (patientid, geneid)

def merge_geneid(accumulated, current):
    accumulated.append(int(current))
    accumulated.sort()
    return accumulated

def combine_genelist(accumulated_1, accumulated_2):
    accumulated_1.extend(accumulated_2)
    accumulated_1.sort()
    return accumulated_1

def merge_count(accumulated, current):
    return accumulated+1

def combine_count(accumulated_1, accumulated_2):
    return accumulated_1+accumulated_2

def merge_itemset(accumulated, current):
    accumulated.append(current)
    return accumulated

def combine_itemsetlist(accumulated_1, accumulated_2):
    accumulated_1.extend(accumulated_2)
    accumulated_1.sort()
    return accumulated_1

def generate_candidate_itemsets(base_itemsets, filtered_itemsets):
    candidate_itemsets = [] 
    i = 0
    while i < len(base_itemsets):
        j = 0
        while j < len(filtered_itemsets):
            if base_itemsets[i][0] > filtered_itemsets[j][-1]:
                itemset = []
                itemset.extend(filtered_itemsets[j])
                itemset.append(base_itemsets[i][0])
                candidate_itemsets.append(tuple(itemset))
            j += 1
        i += 1
    return candidate_itemsets

def is_subset(child, parent):
    is_subset = True
    for elem in child:
        if elem not in parent:
            is_subset = False
    return is_subset

def count_itemset(cancerpatients_expressed_genelist, candidate_itemsets, min_support):
    filtered_itemsets = []
    frequent_itemsets = []
    for itemset in candidate_itemsets:
        itemset_count = cancerpatients_expressed_genelist.filter(lambda (x, y) : is_subset(itemset, y)).map(lambda (x, y) : (itemset, 1)).aggregateByKey(0, merge_count, combine_count, 1).collect()
        if itemset_count[0][1] > min_support:
            filtered_itemsets.append(itemset)
            frequent_itemsets.extend(itemset_count)
    return filtered_itemsets, frequent_itemsets

if __name__ == '__main__':
    sc = SparkContext(appName="Frequent itemsets")
    patient_meta_data = sc.textFile("./data/PatientMetaData.txt") 
    cancer_patients = patient_meta_data.filter(filter_by_cancertype).map(extract_cancerpatientid).collect()
    geo = sc.textFile('./data/GEO.txt')
    cancerpatients_expressed_geneid = geo.filter(lambda x : x.strip().split(",")[0] in cancer_patients and float(x.strip().split(",")[2]) >= 1250000).map(extract_patientid_geneid)

    cancerpatients_expressed_genelist =  cancerpatients_expressed_geneid.aggregateByKey([], merge_geneid, combine_genelist, 1)
    num_of_cancerpatients = cancerpatients_expressed_genelist.count()
    min_support = int(num_of_cancerpatients * 0.3)
    expressed_cancergenelist = cancerpatients_expressed_geneid.map(lambda (x, y) : (1, y)).distinct().aggregateByKey([], merge_geneid, combine_genelist, 1).map(lambda (x, y) : [tuple([z]) for z in y]).collect()[0]

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
        if 0 == len(frequent_itemsets_to_add):
            break
        frequent_itemsets.extend(frequent_itemsets_to_add)
        candidate_itemsets = generate_candidate_itemsets(base_itemsets, filtered_itemsets)

    frequent_itemsets_rdd = sc.parallelize(frequent_itemsets).map(lambda (x, y) : (y, x)).aggregateByKey([], merge_itemset, combine_itemsetlist, 1).sortBy(lambda x : x[0], ascending=False)
    print frequent_itemsets_rdd.collect()
    
