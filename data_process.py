
def process(list):
    data = {}
    for one_line in list:
        key = one_line[0]
        if key in data.keys():
            data[key][1] += 1
            data[key][0] += float(one_line[4])
        else:
            new_list = [float(one_line[4]), 1]
            data[key] = new_list
    data_processed={}
    for k in data.keys():
        data_processed[k] = data[k][0]/data[k][1]
    return data_processed

