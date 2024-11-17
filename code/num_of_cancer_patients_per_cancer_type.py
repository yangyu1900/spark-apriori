from pyspark import SparkContext

def filter_by_expressed_gene(record):
    patientid, geneid, expression_value = record.strip().split(",")
    return geneid.strip() != 'geneid' and 42 == int(geneid.strip()) and float(expression_value) >= 1250000

def extract_expressed_patientid(record):
    return record.strip().split(",")[0]

def filter_and_flatmap_cancertype(record, expressed_patients_set):
    cancer_list = {"breast-cancer", "prostate-cancer", "pancreatic-cancer", "leukemia", "lymphoma"}
    patientid, age, gender, postcode, diseases, drug_response = record.strip().split(",")
    if patientid not in expressed_patients_set:
        return []
    result = []
    for disease in diseases.strip().split(" "):
        if disease.strip() in cancer_list:
            result.append((disease.strip(), patientid))
    return result

def merge_count(accumulated, current):
    return accumulated + 1

def combine_count(accumulated_1, accumulated_2):
    return accumulated_1 + accumulated_2

if __name__ == '__main__':
    sc = SparkContext(appName='Number of cancer patients with certain active genes per cancer type')
    geo = sc.textFile('./data/GEO.txt')
    expressed_patients = geo.filter(filter_by_expressed_gene).map(extract_expressed_patientid).collect()
    expressed_patients_set = set(expressed_patients)
    patient_meta_data = sc.textFile('./data/PatientMetaData.txt')
    expressed_patientid_cancertype = patient_meta_data.flatMap(lambda record: filter_and_flatmap_cancertype(record, expressed_patients_set))
    cancertype_count = expressed_patientid_cancertype.aggregateByKey(0, merge_count, combine_count).sortBy(lambda x: x[0]).sortBy(lambda x: x[1], ascending=False)
    print(cancertype_count.collect())