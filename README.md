# Cancer Patient Analysis
This project analyzes cancer patient data to determine the number of cancer patients with certain active genes per cancer type and to find frequent itemsets of expressed genes in cancer patients.

## Project Structure
code/
    frequent_itemsets.py
    num_of_cancer_patients_per_cancer_type.py
data/
    GeneMetaData.txt
    GEO.txt
    PatientMetaData.txt
requirements.txt

## Files
+ `code/num_of_cancer_patients_per_cancer_type.py`: Script to count the number of cancer patients with certain active genes per cancer type.
+ `code/frequent_itemsets.py`: Script to find frequent itemsets of expressed genes in cancer patients.
+ `data/GeneMetaData.txt`: Metadata about genes.
+ `data/GEO.txt`: Gene expression data.
+ `data/PatientMetaData.txt`: Metadata about patients.
+ `requirements.txt`: List of dependencies required to run the project.

## Setup
1. Install the required dependencies:
    > pip install -r requirements.txt

2. Ensure that the data files (GeneMetaData.txt, GEO.txt, PatientMetaData.txt) are placed in the data/ directory.

## Usage
### Counting Cancer Patients with Certain Active Genes per Cancer Type
Run the following command to execute the script:
>python code/num_of_cancer_patients_per_cancer_type.py

This script will:

1. Filter patients based on gene expression from GEO.txt.
2. Filter patient metadata from PatientMetaData.txt to find those with cancer.
3. Count the number of cancer patients with certain active genes per cancer type and print the results.

### Finding Frequent Itemsets of Expressed Genes in Cancer Patients
Run the following command to execute the script:
> python code/frequent_itemsets.py

This script will:

1. Filter cancer patients from PatientMetaData.txt.
2. Filter gene expression data from GEO.txt for these cancer patients.
3. Find frequent itemsets of expressed genes in cancer patients and print the results.

## License
This project is licensed under the MIT License. See the LICENSE file for details.