from pyspark import SparkContext

def filter_by_epressed_gene(record):
    patientid, geneid, expression_value = record.strip().split(",")
    return geneid.strip() != 'geneid' and 42 == int(geneid.strip()) and float(expression_value) >= 1250000

def extract_expressed_patientid(record):
    patientid, geneid, expression_value = record.strip().split(",")
    return patientid

def filter_by_cancertype(record):
    has_cancer = False
    cancer_list = ["breast-cancer", "prostate-cancer", "pancreatic-cancer", "leukemia", "lymphoma"]
    patientid, age, gender, postcode, diseases, drug_response = record.strip().split(",")
    for disease in diseases.strip().split(" "):
        if disease.strip() in cancer_list:
            has_cancer = True
    return has_cancer

def flatmap_cancertype(record):
    result = []
    cancer_list = ["breast-cancer", "prostate-cancer", "pancreatic-cancer", "leukemia", "lymphoma"]
    patientid, age, gender, postcode, diseases, drug_response = record.strip().split(",")
    for disease in diseases.strip().split(" "):
        if disease.strip() in cancer_list:
            result.append((disease, patientid))
    return result

def merge_count(accumulated, current):
    return accumulated+1

def combine_count(accumulated_1, accumulated_2):
    return accumulated_1+accumulated_2

if __name__ == '__main__':
    sc = SparkContext(appName='Number of cancer patients with certain active genes per cancer type')
    geo = sc.textFile('./data/GEO.txt')
    expressed_patients = geo.filter(filter_by_epressed_gene).map(extract_expressed_patientid).collect()
    patient_meta_data = sc.textFile('./data/PatientMetaData.txt')
    expressed_patientid_cancertype = patient_meta_data.filter(filter_by_cancertype).filter(lambda x : x.strip().split(",")[0] in expressed_patients).flatMap(flatmap_cancertype)
    cancertype_count = expressed_patientid_cancertype.aggregateByKey(0, merge_count, combine_count, 1).sortBy(lambda x : x[0]).sortBy(lambda x : x[1], ascending=False)
    print cancertype_count.collect()
 
