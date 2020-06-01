import json



def read_config_from_json():
    # Read The json file
    with open('config.json') as file:
        data = json.load(file)
    print('------------------------------------------')
    print('Task 1 The Total Items for each key ')
    # Run on the keys and calulate the size
    for key in data:
        print(key+ ' :', len(data[key]))
    print('------------------------------------------')
    print('Task 1 The items that are true ')
    # Run on eack dictinery of a key and find the ones that are true
    # We load the data to list and print each line
    # Print the list only there are true values
    for key in data:
        list = []
        for item in data[key]:
            if str(data[key][item]).lower() == 'true':
                list.append(item)
        if len(list) > 0:
            print(key + ' :')
            print('---------------------')
            for x in  list:
                print(x)
            print('---------------------')
if __name__ == '__main__':
    read_config_from_json()