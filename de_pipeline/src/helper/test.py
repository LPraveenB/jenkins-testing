import numpy as np

input_paths = ["gs://extracted-bucket-dollar-tree/darshan/raw_data/2022-08-01-103447/success/FD_STORE_INV/FD_STORE_INV-20220801-2.csv","gs://extracted-bucket-dollar-tree/darshan/raw_data/2022-08-01-103447/success/FD_SALES/FD_SALES-20220801-2.csv","gs://extracted-bucket-dollar-tree/darshan/raw_data/2022-08-01-103447/success/FD_SALES/FD_SALES-20220801-2.csv"]
result_map = {}
for item in input_paths:
    # Split the value using "/"
    split_values = item.split("/")
    # Extract the value at index -2
    key = split_values[-2]
    # Add the item to the corresponding list in the dictionary
    print("***************",key)
    if key not in result_map:
        result_map[key] = []
    result_map[key].append(item)
print(result_map)
table_file_list = {}
resultList = [(key,value) for key, value in result_map.items()]
print(resultList[1][2])