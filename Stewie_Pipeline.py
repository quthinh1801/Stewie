import sqlite3
import pandas as pd
import networkx as nx
import itertools
import json
from itertools import chain

# Creating file path
dbfile = 'full.db'

# Create a SQL connection to our SQLite database
con = sqlite3.connect(dbfile)

# Reading all table names on .db file
model_types = pd.read_sql_query("SELECT * FROM model_types", con)
model_prerequisites = pd.read_sql_query("SELECT * FROM model_prerequisites", con)
model_types = model_types.set_index('id')

# Close connection
con.close()

# Switch positions of 2 columns (prereq and dependent)
model_prerequisites = model_prerequisites[['pipeline_name','prereq_model_type_id','dependent_model_type_id']]

# Get unique values in pipeline_name
pipeline_name = model_prerequisites['pipeline_name'].unique()

pipeline_group = []
pipeline_group_ids = []

# Segregating pipeline by pipeline (all relevant, directed model_id)
for p in pipeline_name:
    print(p)
    print('----------------------------')
    pipeline_subset = model_prerequisites[model_prerequisites['pipeline_name'] == p]
    pipeline_subset = pipeline_subset.drop(columns = ['pipeline_name'])
    pipeline_list = pipeline_subset.values.tolist()
    
    while pipeline_list != []: #Big(O) = nlogn
        test_subset = []
        group = [p for p in pipeline_list[0]]
        for t in pipeline_list:
            for e in t:
                if e in group:
                    group.extend(t)
            group = list(dict.fromkeys(group))
            if t[0] in group and t[1] in group:
                test_subset.append(t)
                pipeline_group.append(p)
                pipeline_group_ids.append(test_subset)
        print(test_subset)
        
        pipeline_list = [x for x in pipeline_list if x not in test_subset]
        
    print('')
      
# Convert the above data into dataframe
processed_pipeline = pd.DataFrame({
     'main_pipeline': pipeline_group,
     'pipeline_group_ids': pipeline_group_ids
    })

# Remove all duplicates
processed_pipeline = processed_pipeline.loc[processed_pipeline.astype(str).drop_duplicates().index]

# Convert list within list within list of model_id to tuple
for column in processed_pipeline['pipeline_group_ids']:
    for index, item in enumerate(column): 
        column[index] = tuple(item)

# Find all cycles in each pipeline
cycle_group = []
for group in processed_pipeline['pipeline_group_ids']:
    G = nx.DiGraph(group)
    invalid_models = list(itertools.chain.from_iterable(nx.recursive_simple_cycles(G)))
    cycle_group.append(invalid_models)
for c in cycle_group:
    cycle_group[cycle_group.index(c)] = list(set(c))
processed_pipeline['cycle_group'] = cycle_group

# Find all model_ids dependent on cycle
cycle_dependent = list(zip(processed_pipeline['pipeline_group_ids'].tolist(),processed_pipeline['cycle_group'].tolist()))
valid_model_group = []
invalid_model_group = []
for cd in cycle_dependent:
    if cd[1] == []: #no cycle within pipeline
        non_cycle_id = cd[0] #list of all tuples
        non_cycle_id = [i for sub in non_cycle_id for i in sub] #merge all tuples into 1 single list
        non_cycle_id = list(set(non_cycle_id)) #eliminate duplicates in list
        valid_model_group.append(non_cycle_id) 
        invalid_model_group.append([])
        
    else:
        cg = cd[0] #cg: cycle_group
        im = cd[1] #im: invalid_model cycle
        cycle_vertices = []
        # for c in cg:
        #     cg[cg.index(c)] = list(c)
        
        cg_element = list(set(list(chain(*cg))))
        while cg != []:
            
            #Remove vertices we know for sure fall in cycles
            for ele in cg:
                if ele[0] in im and ele[1] in im:
                    cycle_vertices.append(ele)
                    
            cg = [i for i in cg if i not in cycle_vertices]
            
            #Remove vertices connected with cycles by origin   
            for ele in cg:
                if ele[0] in im:
                    cycle_vertices.append(ele)
                    im.append(ele[1])
                    
            cg = [i for i in cg if i not in cycle_vertices]
            
            #Remove vertices that destination is an id within a cycle
            for ele in cg:
                if ele[1] in im:
                    cg.remove(ele)
                    
            #If there are/is still element(s) of invalid_models in all vertices of cg, the while loop continues
            for i in im:
                if i not in list(itertools.chain.from_iterable(cg)):
                    valid_model_group.append([i for i in cg_element if i not in im])
                    invalid_model_group.append(list(set(im)))
                    cg = []
                else:
                    cg = cg
                    
# Remove duplicates        
validity = pd.DataFrame({
    'valid_model_group': valid_model_group,
    'invalid_model_group':invalid_model_group
    }) 
validity = validity.loc[validity.astype(str).drop_duplicates().index]

# Add finding to processed_pipeline dataframe and trim away unnecessary columns
processed_pipeline['valid_models'] = validity['valid_model_group'].values.tolist()
processed_pipeline['invalid_models'] = validity['invalid_model_group'].values.tolist()
processed_pipeline = processed_pipeline.drop(columns = ['cycle_group', 'pipeline_group_ids']) #Irrelevant to final output
print(processed_pipeline)

# Lookup model_id to model_name in model_types table
vmg_name_group = []
for vmg in processed_pipeline['valid_models']:
    vmg_name = []
    if vmg == []:
        vmg_name.append('')     
    else:
        for v in vmg:
            v = model_types.loc[v, 'name']
            vmg_name.append(v)
    vmg_name_group.append(vmg_name)
            
ivmg_name_group = []
for ivmg in processed_pipeline['invalid_models']:
    ivmg_name = []
    if ivmg == []:
        ivmg_name.append('')     
    else:
        for iv in ivmg:
            iv = model_types.loc[iv, 'name']
            ivmg_name.append(iv)
    ivmg_name_group.append(ivmg_name)

# Add those model_names to process_pipeline table
processed_pipeline['vmg_name_group'] = vmg_name_group
processed_pipeline['ivmg_name_group'] = ivmg_name_group

# Drop valid_models and invalid_models (id) columns, keep their model_names
processed_pipeline = processed_pipeline.drop(columns = ['valid_models', 'invalid_models'])
processed_pipeline = processed_pipeline.rename(columns = {'vmg_name_group': 'valid_models',
                                                          'ivmg_name_group': 'invalid_models'})
print(processed_pipeline)

# JSON Output
process_pipeline_json = processed_pipeline.to_json(orient="records")
parsed = json.loads(process_pipeline_json)
with open('pipeline_json.txt', 'w') as jsonout:
    jsonout.write(process_pipeline_json)
    jsonout.close()

# print(json.dumps(parsed, indent = 4))  
