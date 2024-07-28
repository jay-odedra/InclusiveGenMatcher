import ROOT
from array import array
import json


def conjugate(keytuple,mapping):
    def create_zeros_tuple(t):
        if isinstance(t, tuple):  # Check if it's a tuple
            return tuple(create_zeros_tuple(sub) for sub in t)  # Recurse for each sub-tuple
        else:
            return 0  # Replace non-tuple elements with 0    
    
    zeros_tuple = create_zeros_tuple(keytuple)
    conjugate_tuple = create_zeros_tuple(keytuple)
    zeros_tuple = [list(item) for item in zeros_tuple]
    conjugate_tuple = [list(item) for item in conjugate_tuple]
    
    
    
    for i in range(len(keytuple)):
        for j in range(len(keytuple[i])):
            zeros_tuple[i][j] = mapping[str(int(keytuple[i][j]))][1]  
    for i in range(len(keytuple)):
        for j in range(len(keytuple[i])):
            if zeros_tuple[i][j] == False:
                conjugate_tuple[i][j] = int(keytuple[i][j])*-1
            else:
                conjugate_tuple[i][j] = int(keytuple[i][j])
    conjugate_tuple = tuple(map(tuple, conjugate_tuple))
    return conjugate_tuple
            
def remove_indices(list_to_modify, indices):
    indices_sorted = sorted(indices, reverse=True)
    for lst in list_to_modify:
        for index in indices_sorted:
            if index < len(lst):
                del lst[index]




file = ROOT.TFile.Open("/vols/cms/jo3717/Noahstest/InclusiveGenMatcher/outputjpsi/parrallel_full_1.root")
tree = file.Get("mytree")

print("total entries",tree.GetEntries())
totalEntries = tree.GetEntries()

ele1lin, ele1lingenidx, ele1linstatus  = [], [], []
ele2lin, ele2lingenidx, ele2linstatus  = [], [], []
kaonlin, kaonlingenidx, kaonlinstatus  = [], [], []

no_display_pdgid=[1,2,3,4,5,6,7,8,21,2212]

Decay_count ={}
with open('/vols/cms/jo3717/Noahstest/InclusiveGenMatcher/pdgid_dict.json', 'r') as filejson:
    mapping_dict = json.load(filejson)
for i in range(totalEntries):
    tree.GetEntry(i)
    if tree.Mll<300.2 and tree.Mll> -2.9 and tree.bdt_score>-2.0:    
        ele1lin.append(tree.GenPart_L1_pdgId)
        ele1lingenidx.append(tree.GenPart_L1_idx)
        ele1linstatus.append(tree.GenPart_L1_status)
        
        ele2lin.append(tree.GenPart_L2_pdgId)
        ele2lingenidx.append(tree.GenPart_L2_idx)
        ele2linstatus.append(tree.GenPart_L2_status)
        
        kaonlin.append(tree.GenPart_K_pdgId)
        kaonlingenidx.append(tree.GenPart_K_idx)
        kaonlinstatus.append(tree.GenPart_K_status)
        
        [ele1lin.append(x) for x in tree.GenPart_L1_lin_pdgId]
        [ele1lingenidx.append(x) for x in tree.GenPart_L1_lin_idx]
        [ele1linstatus.append(x) for x in tree.GenPart_L1_lin_status]
        
        [ele2lin.append(x) for x in tree.GenPart_L2_lin_pdgId]
        [ele2lingenidx.append(x) for x in tree.GenPart_L2_lin_idx]
        [ele2linstatus.append(x) for x in tree.GenPart_L2_lin_status]
        
        [kaonlin.append(x) for x in tree.GenPart_K_lin_pdgId]
        [kaonlingenidx.append(x) for x in tree.GenPart_K_lin_idx]
        [kaonlinstatus.append(x) for x in tree.GenPart_K_lin_status]
        
        #ele1lin = [ele1lin[i] for i in range(len(ele1lin) - 1) if ele1lin[i] != ele1lin[i + 1]] + [ele1lin[-1]]
        #ele2lin = [ele2lin[i] for i in range(len(ele2lin) - 1) if ele2lin[i] != ele2lin[i + 1]] + [ele2lin[-1]]
        #kaonlin = [kaonlin[i] for i in range(len(kaonlin) - 1) if kaonlin[i] != kaonlin[i + 1]] + [kaonlin[-1]]

        remove_indices([ele1lin, ele1lingenidx, ele1linstatus], [i for i, x in enumerate(ele1lin) if abs(x) in no_display_pdgid])
        remove_indices([ele2lin, ele2lingenidx, ele2linstatus], [i for i, x in enumerate(ele2lin) if abs(x) in no_display_pdgid])
        remove_indices([kaonlin, kaonlingenidx, kaonlinstatus], [i for i, x in enumerate(kaonlin) if abs(x) in no_display_pdgid])
        
        key = (tuple(ele1lin), tuple(ele2lin), tuple(kaonlin))
        conjugate_tuple = conjugate(key,mapping_dict)
        conjugate_list = [list(item) for item in conjugate_tuple]
        
        for i in range(len(conjugate_list)):
            for j, value in enumerate(conjugate_list[i]):
                value_str = str(int(value))
                if value_str in mapping_dict:
                    conjugate_list[i][j] = mapping_dict[value_str][0]
                else:
                    # Optional: handle missing keys
                    pass
            
        conjugate_key = tuple(map(tuple, conjugate_list))
        conjuate_key_list_ele1_ele2_inversion    = [list(item) for item in conjugate_key]
        ele1,ele2=0,1
        conjuate_key_list_ele1_ele2_inversion[ele1], conjuate_key_list_ele1_ele2_inversion[ele2] = conjuate_key_list_ele1_ele2_inversion[ele2], conjuate_key_list_ele1_ele2_inversion[ele1]
        conjugate_key_tuple_ele1_ele2_inversion = tuple(map(tuple, conjuate_key_list_ele1_ele2_inversion))
        
        
        pdgIdele1 = ele1lin[:]
        pdgIdele2 = ele2lin[:]
        pdgIdkaon = kaonlin[:]
        
        for i, value in enumerate(ele1lin):
            value_str = str(int(value))
            if value_str in mapping_dict:
                ele1lin[i] = mapping_dict[value_str][0]
            else:
                # Optional: handle missing keys
                pass
        for i, value in enumerate(ele2lin):
            value_str = str(int(value))
            if value_str in mapping_dict:
                ele2lin[i] = mapping_dict[value_str][0]
            else:
                # Optional: handle missing keys
                pass
        for i, value in enumerate(kaonlin):
            value_str = str(int(value))
            if value_str in mapping_dict:
                kaonlin[i] = mapping_dict[value_str][0]
            else:
                # Optional: handle missing keys
                pass
        
        electron1_list = [list(item) for item in zip(ele1lin, pdgIdele1, ele1lingenidx)]
        electron2_list = [list(item) for item in zip(ele2lin, pdgIdele2, ele2lingenidx)]
        kaon_list = [list(item) for item in zip(kaonlin, pdgIdkaon, kaonlingenidx)]
        
        electron1_list.reverse()
        electron2_list.reverse()
        kaon_list.reverse()
        
        key = (tuple(ele1lin), tuple(ele2lin), tuple(kaonlin))
        key_list_ele1_ele2_inversion    = [list(item) for item in key]
        ele1,ele2=0,1
        key_list_ele1_ele2_inversion[ele1], key_list_ele1_ele2_inversion[ele2] = key_list_ele1_ele2_inversion[ele2], key_list_ele1_ele2_inversion[ele1]
        key_tuple_ele1_ele2_inversion = tuple(map(tuple, key_list_ele1_ele2_inversion))
        counter_decay = (
            tuple(map(tuple, electron1_list)),
            tuple(map(tuple, electron2_list)),
            tuple(map(tuple, kaon_list)),
        )
        
        
        if key in Decay_count:
            correct_key = key
        elif conjugate_key in Decay_count:
            correct_key = conjugate_key
        elif conjugate_key_tuple_ele1_ele2_inversion in Decay_count:
            correct_key = conjugate_key_tuple_ele1_ele2_inversion
        elif key_tuple_ele1_ele2_inversion in Decay_count:
            correct_key = key_tuple_ele1_ele2_inversion
        else:
            correct_key = key
        #correct_key = key
            
        if correct_key in Decay_count:
            Decay_count[correct_key]['count'] += 1
            Decay_count[correct_key]['data'].append(counter_decay)
        else:
            Decay_count[correct_key] = {'data': [counter_decay], 'count': 1}

        ele1lin, ele2lin, kaonlin = [], [], []
        ele1lingenidx, ele2lingenidx, kaonlingenidx = [], [], []
        ele1linstatus, ele2linstatus, kaonlinstatus = [], [], []
        electron1_list = []
        electron2_list = []
        kaon_list = []
        indexes_to_removeel1 = []
        indexes_to_removeel2 = []
        indexes_to_removekaon = []
# Sort the Decay_count dictionary by the 'count' value of each entry
sorted_Decay_count = dict(sorted(Decay_count.items(), key=lambda item: item[1]['count'], reverse=True))

# Print out the sorted results
#print(sorted_Decay_count)

for key, value in sorted_Decay_count.items():
    print(f"Count: {value['count']}")
    print("Decay")
    print(f"Ele1: {value['data'][0][0]}")
    print(f"Ele2: {value['data'][0][1]}")
    print(f"Kaon: {value['data'][0][2]}")
