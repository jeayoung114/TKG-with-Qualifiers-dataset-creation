import pandas as pd
from SPARQLWrapper import SPARQLWrapper, JSON
from tqdm import tqdm
import pandas as pd
import sys

input_file = sys.argv[1]
output_file = sys.argv[2]

with open("entity2id.txt") as f:
    data = f.read()
    
df = [i.split("\t") for i in data.split("\n")]


entity2id = dict()
id2entity = dict()

for line in df:
    try:
        i, j = line
        entity2id[i] = j
        id2entity[j] = i
    except:
        continue

with open("relation2id.txt") as f:
    data = f.read()
df = [i.split("\t") for i in data.split("\n")]    
relation2id = dict()
id2relation = dict()

for line in df:
    try:
        i, j = line
        relation2id[i] = j
        id2relation[j] = i
    except:
        continue
        
        
data = pd.read_csv(input_file, sep = "\t", header = None, skip_blank_lines=True)

data = data.rename({0 : "s", 1 : "p", 2 : "o", 3 : "t", 4 : 0}, axis = 1)



data["s"] = data["s"].apply(lambda x : id2entity[str(x)])
data["p"] = data["p"].apply(lambda x : id2relation[str(x)])
data["o"] = data["o"].apply(lambda x : id2entity[str(x)])
data.drop(0, axis = 1)


'''
SPARQL
'''

import pandas as pd
from SPARQLWrapper import SPARQLWrapper, JSON
from tqdm import tqdm
res_list = []
len_list = []
sparql = SPARQLWrapper("https://query.wikidata.org/sparql")

## key is not start time, end time, p itself, type, rank
f = open(output_file, 'w')
for idx, row in tqdm(data.iterrows()):
#     s, p, o, t, x, num, res = row
    s, p, o, t, x = row
    query = """
    SELECT DISTINCT ?k ?v
    WHERE
    {
      wd:""" + s + " p:" + p + " ?statement.\n" +\
      "?statement ps:" + p + " wd:" + o +".\n" +\
      "?statement ?k ?v.\n\n" +\
    "  Filter (?k != ps:" + p + ").\n  Filter (?k != rdf:type).\n  Filter (?k != wikibase:rank)." +\
    " Filter (?k != prov:wasDerivedFrom)\n"+\
    'SERVICE wikibase:label { bd:serviceParam wikibase:language "en" }\n}'
    
    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)
    
    results = sparql.query().convert()
#     res_list.append(results)
#     results_df = pd.json_normalize(results['results']['bindings'])
    kv_list = []
    for kv in results['results']['bindings']:
        if 'value' not in kv['k']['value']:
            
            key = kv['k']['value'].split("/")[-1]
            value = kv['v']['value']
            if 'entity' in value:
                value = value.split("/")[-1]
            
            kv_list.append(key)
            kv_list.append(value)
            
    string = s + "\t" + p + "\t" + o + "\t" + str(t)
    for i in kv_list:
        string += "\t"
        string += i
    string += '\n'
    f.write(string)
    print(string)

    




