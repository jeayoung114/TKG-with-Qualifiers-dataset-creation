from tqdm import tqdm
import numpy as np
import pandas as pd
from SPARQLWrapper import SPARQLWrapper, JSON
from tqdm import tqdm




def get_qnode_in_wiki(node):
    sparql = SPARQLWrapper("https://yago-knowledge.org/sparql/query")
    query_s = '''
    PREFIX owl: <http://www.w3.org/2002/07/owl#>
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    PREFIX yago: <http://yago-knowledge.org/resource/>
    PREFIX schema: <http://schema.org/>
    SELECT ?s WHERE {
      <http://yago-knowledge.org/resource/'''+ node +'> owl:sameAs ?s.' +\
    '''

      Filter contains(str(?s), 'wiki')

    } 
    LIMIT 1
    '''
    sparql.setQuery(query_s)
    sparql.setReturnFormat(JSON)
    results = sparql.query().convert()
    try:
        res = results['results']['bindings'][0]
        s = res["s"]["value"]
        sW = s.split("/")[-1]
    except:
        sW = None
        
    return sW



def get_predicate(s, o):
    sparql = SPARQLWrapper("https://query.wikidata.org/sparql")
    if s == None or o == None:
        return None, False
    

    query = '''
    PREFIX wd: <http://www.wikidata.org/entity/>
    PREFIX schema: <http://schema.org/>

    SELECT ?p
    WHERE 
    {
      wd:'''+ s + ' ?p wd:' + o +\
    '}'

    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)

    results = sparql.query().convert()
    
    try:
        predicate = results["results"]['bindings'][0]["p"]["value"].split("/")[-1]
        return predicate, True
    except:
        return None, False
    
    
    
def get_final_row(s, p, o, t, string):
    sparql = SPARQLWrapper("https://query.wikidata.org/sparql")
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
    
    kv_list = []
    for kv in results['results']['bindings']:
        if 'value' not in kv['k']['value']:
            
            key = kv['k']['value'].split("/")[-1]
            value = kv['v']['value']
            if 'entity' in value:
                value = value.split("/")[-1]
            
            kv_list.append(key)
            kv_list.append(value)
            

    for i in kv_list:
        string += "\t"
        string += i
    string += '\n'
        

    return string


def generate_output(output_file, data):
    with open(output_file, "w") as f:
        for row in tqdm(data):
            connected = False
            while connected == False:
                try:
                    node1 = row[0]
                    node2 = row[2]
                    predicate = row[1]
                    t = row[3]

                    node1 = idx2entity[node1]
                    node2 = idx2entity[node2]
                    predicate = idx2relation[predicate]  

                    ## get node1
                    if node1 not in qnode_mapping:
                        try:
                            node1_wiki = get_qnode_in_wiki(node1)
                        except:
                            node1_wiki = None
                        qnode_mapping[node1] = node1_wiki
                    else:
                        node1_wiki = qnode_mapping[node1]

                    ## get node2
                    if node2 not in qnode_mapping:
                        try:
                            node2_wiki = get_qnode_in_wiki(node2)
                        except:
                            node2_wiki = None
                        qnode_mapping[node2] = node2_wiki
                    else:
                        node2_wiki = qnode_mapping[node2]

                    ## get predicate if node1_wiki, node2_wiki exist
                    if node1_wiki and node2_wiki:
                        predicate_wiki, predicate_bool = get_predicate(node1_wiki, node2_wiki)

                    ## get qualifier if predicate exists
                    if predicate_bool and node1_wiki and node2_wiki:
                        # get q
                        front_string = str(node1) + "\t" + str(predicate) + "\t" + str(node2) + "\t" + str(t)
                        res_string = get_final_row(node1_wiki, predicate_wiki, node2_wiki, t, front_string)
                        f.write(res_string)
                    else:
                        f.write(str(node1) + "\t" + str(predicate) + "\t" + str(node2) + "\t" + str(t) + "\n")

                    connected = True
                except:
                    pass



with open('relation2id.txt', 'r') as f:
    relation = f.read()
    
relation2idx = dict()
idx2relation = dict()
for rel, idx in [i.split("\t") for i in relation.split("\n")][:-1]:
    relation2idx[rel[1:-1]] = idx
    idx2relation[idx] = rel[1:-1]
    
with open('entity2id.txt', 'r') as f:
    entity = f.read()
    
entity2idx = dict()
idx2entity = dict()
for ent, idx, start_date, end_date in [i.split("\t") for i in entity.split("\n")][:-1]:
    entity2idx[ent[1:-1]] = idx
    idx2entity[idx] = ent[1:-1]
    
with open('train.txt', 'r') as f:
    train = f.read()
    
train = [i.split("\t") for i in train.split("\n")][:-1]

with open('valid.txt', 'r') as f:
    valid = f.read()
    
valid = [i.split("\t") for i in valid.split("\n")][:-1]


with open('test.txt', 'r') as f:
    test = f.read()
    
test = [i.split("\t") for i in test.split("\n")][:-1]


qnode_mapping = dict()






generate_output("yago_q_train.txt", train)
generate_output("yago_q_valid.txt", valid)
generate_output("yago_q_test.txt", test)


with open("qnode_mapping.txt", 'w') as f:
    for key, val in qnode_mapping.items():
        if val != None:
            f.write(str(key) + "\t" + str(val) + "\n")
            
 

