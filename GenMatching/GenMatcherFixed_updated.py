import ROOT
from array import array
from tqdm import tqdm
from multiprocessing import Process
import multiprocessing
from ROOT import vector
class Event:
    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)

    def print_details(self):
        # Example of printing a few attributes
        print(f"GenPart_pdgId: {self.GenPart_pdgId}, idx: {self.idx}, BToKEE_cand_idx: {self.BToKEE_cand_idx},GenPart_idx {self.GenPart_idx} , Status {self.GenPart_status}, GenPart_genPartIdxMother {self.GenPart_genPartIdxMother}")

def DeltaR(Eventobject,flag):
    if flag == "ele1":
        DRele1 = (Eventobject.L1eta - Eventobject.GenPart_eta)**2 + (Eventobject.L1phi - Eventobject.GenPart_phi)**2
        return DRele1
    elif flag == "ele2":
        DRele2 = (Eventobject.L2eta - Eventobject.GenPart_eta)**2 + (Eventobject.L2phi - Eventobject.GenPart_phi)**2
        return DRele2
    elif flag == "kaon":
        DRkaon = (Eventobject.Keta - Eventobject.GenPart_eta)**2 + (Eventobject.Kphi - Eventobject.GenPart_phi)**2
        return DRkaon
    else:
        return "No flag selected"

        
def ElectronMatching(listofobjects,ele1=False,ele2=False):
    #print("len object list", len(listofobjects))
    if ele1 == False and ele2 == False:
        return "No electrons selected"
    if ele1 == True and ele2 == False:
    #    print("Ele1 electron selected")
        electronflag = "ele1"
    elif ele1 == False and ele2 == True:
    #    print("Ele2 electron selected")
        electronflag = "ele2"
    ElectronID = None
    ElectronDR = 10000
    Electronobject = []
    
    for i in range(len(listofobjects)):
        if abs(listofobjects[i].GenPart_pdgId) == 11:
            DREvent = DeltaR(listofobjects[i],electronflag)
            if DREvent < ElectronDR:
                ElectronDR = DREvent
                ElectronID = i
    if ElectronID == None:
    #    print("No electron matched")
        return None , None
    #print("Electron matched")
    #listofobjects[ElectronID].print_details()
    Electronobject.append(listofobjects[ElectronID])
    listofobjects.pop(ElectronID)
    
    return Electronobject, listofobjects


def KaonMatching(listofobjects):
    kaonID = None
    kaonDR = 10000
    kaonobject = []
    
    for i in range(len(listofobjects)):
        DREvent = DeltaR(listofobjects[i],"kaon")
        if DREvent < kaonDR:
            kaonDR = DREvent
            kaonID = i
    if kaonID == None:
    #    print("No kaon matched")
        return None , None
    #print("Kaon matched")
    #listofobjects[kaonID].print_details()
    kaonobject.append(listofobjects[kaonID])
    listofobjects.pop(kaonID)
    
    return kaonobject, listofobjects

    
    
    
def MatchMother(listofobjects, id):
    matchedmother = []
    for obj in listofobjects:
        if obj.GenPart_idx == id:
            matchedmother.append(obj)
            return matchedmother

def FindMother(listofobjects,electron):
    electronmotheridx = electron[0].GenPart_genPartIdxMother
    #print(electronmotheridx)
    electronmother = MatchMother(listofobjects,electronmotheridx)
    return electronmother

def print_mother_lineage(objectlist, child, lineage_details=[], depth=0):
    if child is None or depth > 100:  # Assuming a maximum depth of 6 generations as in the original code
        return
    mother = FindMother(objectlist, child)
    if mother: #and mother[0].GenPart_status < 4:
        lineage_details.append(mother[0])
        #mother[0].print_details()  # Assuming mother is a list with the mother object at index 0
        print_mother_lineage(objectlist, mother,lineage_details, depth + 1)
    else:
        #print("No more mothers found.")
        return
        

def split_file_paths(file_paths, n_sections):
    num_cpus = multiprocessing.cpu_count()
    print("nsections : ",n_sections)
    """Split file paths into n_sections."""
    k, m = divmod(len(file_paths), n_sections)
    return [file_paths[i*k+min(i, m):(i+1)*k+min(i+1, m)] for i in range(n_sections)]



file_paths = [
#"/vols/cms/jo3717/Noahstest/InclusiveGenMatcher/measurement_Inclusive.root",
#"/vols/cms/jo3717/Noahstest/r3k-bdttools/outputbdt/bdtoutput27_06_24_inclusive_final_focusoninclusive_newnewnewnwen_03_07_24/measurement_Inclusive_part1.root",
#"/vols/cms/jo3717/Noahstest/r3k-bdttools/outputbdt/bdtoutput27_06_24_inclusive_final_focusoninclusive_newnewnewnwen_03_07_24/measurement_Inclusive_part2.root",
#"/vols/cms/jo3717/Noahstest/r3k-bdttools/outputbdt/bdtoutput27_06_24_inclusive_final_focusoninclusive_newnewnewnwen_03_07_24/measurement_Inclusive_part3.root",
#"/vols/cms/jo3717/Noahstest/r3k-bdttools/outputbdt/bdtoutput27_06_24_inclusive_final_focusoninclusive_newnewnewnwen_03_07_24/measurement_Inclusive_part4.root",
#"/vols/cms/jo3717/Noahstest/r3k-bdttools/outputbdt/bdtoutput27_06_24_inclusive_final_focusoninclusive_newnewnewnwen_03_07_24/measurement_Inclusive_part5.root",
#"/vols/cms/jo3717/Noahstest/r3k-bdttools/outputbdt/bdtoutput27_06_24_inclusive_final_focusoninclusive_newnewnewnwen_03_07_24/measurement_Inclusive_part6.root",
#"/vols/cms/jo3717/Noahstest/r3k-bdttools/outputbdt/bdtoutput27_06_24_inclusive_final_focusoninclusive_newnewnewnwen_03_07_24/measurement_Inclusive_part7.root",
#"/vols/cms/jo3717/Noahstest/r3k-bdttools/outputbdt/bdtoutput27_06_24_inclusive_final_focusoninclusive_newnewnewnwen_03_07_24/measurement_Inclusive_part8.root",
#"/vols/cms/jo3717/Noahstest/r3k-bdttools/outputbdt/bdtoutput27_06_24_inclusive_final_focusoninclusive_newnewnewnwen_03_07_24/measurement_Inclusive_part10.root",
#"/vols/cms/jo3717/Noahstest/r3k-bdttools/outputbdt/bdtoutput27_06_24_inclusive_final_focusoninclusive_newnewnewnwen_03_07_24/measurement_Inclusive_part11.root",
#"/vols/cms/jo3717/Noahstest/r3k-bdttools/outputbdt/bdtoutput27_06_24_inclusive_final_focusoninclusive_newnewnewnwen_03_07_24/measurement_Inclusive_part12.root",
#"/vols/cms/jo3717/Noahstest/r3k-bdttools/outputbdt/bdtoutput27_06_24_inclusive_final_focusoninclusive_newnewnewnwen_03_07_24/measurement_Inclusive_part13.root",
#"/vols/cms/jo3717/Noahstest/r3k-bdttools/outputbdt/bdtoutput27_06_24_inclusive_final_focusoninclusive_newnewnewnwen_03_07_24/measurement_Inclusive_part14.root",
#"/vols/cms/jo3717/Noahstest/r3k-bdttools/outputbdt/bdtoutput27_06_24_inclusive_final_focusoninclusive_newnewnewnwen_03_07_24/measurement_Inclusive_part15.root",
#"/vols/cms/jo3717/Noahstest/r3k-bdttools/outputbdt/bdtoutput27_06_24_inclusive_final_focusoninclusive_newnewnewnwen_03_07_24/measurement_Inclusive_part16.root",
#"/vols/cms/jo3717/Noahstest/r3k-bdttools/outputbdt/bdtoutput27_06_24_inclusive_final_focusoninclusive_newnewnewnwen_03_07_24/measurement_Inclusive_part17.root",
#"/vols/cms/jo3717/Noahstest/r3k-bdttools/outputbdt/bdtoutput27_06_24_inclusive_final_focusoninclusive_newnewnewnwen_03_07_24/measurement_Inclusive_part18.root",
#"/vols/cms/jo3717/Noahstest/r3k-bdttools/outputbdt/bdtoutput27_06_24_inclusive_final_focusoninclusive_newnewnewnwen_03_07_24/measurement_Inclusive_part19.root",
#"/vols/cms/jo3717/Noahstest/r3k-bdttools/outputbdt/bdtoutput27_06_24_inclusive_final_focusoninclusive_newnewnewnwen_03_07_24/measurement_Inclusive_part20.root",
#"/vols/cms/jo3717/Noahstest/r3k-bdttools/outputbdt/bdtoutput27_06_24_inclusive_final_focusoninclusive_newnewnewnwen_03_07_24/measurement_Inclusive_part21.root",
#"/vols/cms/jo3717/Noahstest/r3k-bdttools/outputbdt/bdtoutput27_06_24_inclusive_final_focusoninclusive_newnewnewnwen_03_07_24/measurement_Inclusive_part22.root",
#"/vols/cms/jo3717/Noahstest/r3k-bdttools/outputbdt/bdtoutput27_06_24_inclusive_final_focusoninclusive_newnewnewnwen_03_07_24/measurement_Inclusive_part23.root",
#"/vols/cms/jo3717/Noahstest/r3k-bdttools/outputbdt/bdtoutput27_06_24_inclusive_final_focusoninclusive_newnewnewnwen_03_07_24/measurement_Inclusive_part24.root",
#"/vols/cms/jo3717/Noahstest/r3k-bdttools/outputbdt/bdtoutput27_06_24_inclusive_final_focusoninclusive_newnewnewnwen_03_07_24/measurement_Inclusive_part25.root",
#"/vols/cms/jo3717/Noahstest/r3k-bdttools/outputbdt/bdtoutput27_06_24_inclusive_final_focusoninclusive_newnewnewnwen_03_07_24/measurement_Inclusive_part26.root",
#"/vols/cms/jo3717/Noahstest/r3k-bdttools/outputbdt/bdtoutput27_06_24_inclusive_final_focusoninclusive_newnewnewnwen_03_07_24/measurement_Inclusive_part27.root",
#"/vols/cms/jo3717/Noahstest/r3k-bdttools/outputbdt/bdtoutput27_06_24_inclusive_final_focusoninclusive_newnewnewnwen_03_07_24/measurement_Inclusive_part28.root",
#"/vols/cms/jo3717/Noahstest/r3k-bdttools/outputbdt/bdtoutput27_06_24_inclusive_final_focusoninclusive_newnewnewnwen_03_07_24/measurement_Inclusive_part29.root",
#"/vols/cms/jo3717/Noahstest/r3k-bdttools/outputbdt/bdtoutput27_06_24_inclusive_final_focusoninclusive_newnewnewnwen_03_07_24/measurement_Inclusive_part30.root",
#"/vols/cms/jo3717/Noahstest/r3k-bdttools/outputbdt/bdtoutput27_06_24_inclusive_final_focusoninclusive_newnewnewnwen_03_07_24/measurement_Inclusive_part31.root",
#"/vols/cms/jo3717/Noahstest/r3k-bdttools/outputbdt/bdtoutput27_06_24_inclusive_final_focusoninclusive_newnewnewnwen_03_07_24/measurement_Inclusive_part32.root",
#"/vols/cms/jo3717/Noahstest/r3k-bdttools/outputbdt/bdtoutput27_06_24_inclusive_final_focusoninclusive_newnewnewnwen_03_07_24/measurement_Inclusive_part33.root",
#"/vols/cms/jo3717/Noahstest/r3k-bdttools/outputbdt/bdtoutput27_06_24_inclusive_final_focusoninclusive_newnewnewnwen_03_07_24/measurement_Inclusive_part34.root",
#"/vols/cms/jo3717/Noahstest/r3k-bdttools/outputbdt/bdtoutput27_06_24_inclusive_final_focusoninclusive_newnewnewnwen_03_07_24/measurement_Inclusive_part35.root",
#"/vols/cms/jo3717/Noahstest/r3k-bdttools/outputbdt/bdtoutput27_06_24_inclusive_final_focusoninclusive_newnewnewnwen_03_07_24/measurement_Inclusive_part36.root",
#"/vols/cms/jo3717/Noahstest/r3k-bdttools/outputbdt/bdtoutput27_06_24_inclusive_final_focusoninclusive_newnewnewnwen_03_07_24/measurement_Inclusive_part37.root",
#"/vols/cms/jo3717/Noahstest/r3k-bdttools/outputbdt/bdtoutput27_06_24_inclusive_final_focusoninclusive_newnewnewnwen_03_07_24/measurement_Inclusive_part38.root",
#"/vols/cms/jo3717/Noahstest/r3k-bdttools/outputbdt/bdtoutput27_06_24_inclusive_final_focusoninclusive_newnewnewnwen_03_07_24/measurement_Inclusive_part39.root",
#"/vols/cms/jo3717/Noahstest/r3k-bdttools/outputbdt/bdtoutput27_06_24_inclusive_final_focusoninclusive_newnewnewnwen_03_07_24/measurement_Inclusive_part40.root",
#"/vols/cms/jo3717/Noahstest/r3k-bdttools/outputbdt/bdtoutput27_06_24_inclusive_final_focusoninclusive_newnewnewnwen_03_07_24/measurement_Inclusive_part41.root",
#"/vols/cms/jo3717/Noahstest/r3k-bdttools/outputbdt/bdtoutput27_06_24_inclusive_final_focusoninclusive_newnewnewnwen_03_07_24/measurement_Inclusive_part42.root",
#"/vols/cms/jo3717/Noahstest/r3k-bdttools/outputbdt/bdtoutput27_06_24_inclusive_final_focusoninclusive_newnewnewnwen_03_07_24/measurement_Inclusive_part43.root",
#"/vols/cms/jo3717/Noahstest/r3k-bdttools/outputbdt/bdtoutput27_06_24_inclusive_final_focusoninclusive_newnewnewnwen_03_07_24/measurement_Inclusive_part44.root",
#"/vols/cms/jo3717/Noahstest/r3k-bdttools/outputbdt/bdtoutput27_06_24_inclusive_final_focusoninclusive_newnewnewnwen_03_07_24/measurement_Inclusive_part45.root",
#"/vols/cms/jo3717/Noahstest/r3k-bdttools/outputbdt/bdtoutput27_06_24_inclusive_final_focusoninclusive_newnewnewnwen_03_07_24/measurement_Inclusive_part46.root",
#"/vols/cms/jo3717/Noahstest/r3k-bdttools/outputbdt/bdtoutput27_06_24_inclusive_final_focusoninclusive_newnewnewnwen_03_07_24/measurement_Inclusive_part47.root",
#"/vols/cms/jo3717/Noahstest/r3k-bdttools/outputbdt/bdtoutput27_06_24_inclusive_final_focusoninclusive_newnewnewnwen_03_07_24/measurement_Inclusive_part48.root",
#"/vols/cms/jo3717/Noahstest/r3k-bdttools/outputbdt/bdtoutput27_06_24_inclusive_final_focusoninclusive_newnewnewnwen_03_07_24/measurement_Inclusive_part49.root",
#"/vols/cms/jo3717/Noahstest/r3k-bdttools/outputbdt/bdtoutput27_06_24_inclusive_final_focusoninclusive_newnewnewnwen_03_07_24/measurement_Inclusive_part50.root",
#"/vols/cms/jo3717/Noahstest/r3k-bdttools/outputbdt/bdtoutput27_06_24_inclusive_final_focusoninclusive_newnewnewnwen_03_07_24/measurement_Inclusive_part51.root",
#"/vols/cms/jo3717/Noahstest/r3k-bdttools/outputbdt/bdtoutput27_06_24_inclusive_final_focusoninclusive_newnewnewnwen_03_07_24/measurement_Inclusive_part52.root",
#"/vols/cms/jo3717/Noahstest/r3k-bdttools/outputbdt/bdtoutput27_06_24_inclusive_final_focusoninclusive_newnewnewnwen_03_07_24/measurement_Inclusive_part53.root",
#"/vols/cms/jo3717/Noahstest/r3k-bdttools/outputbdt/bdtoutput27_06_24_inclusive_final_focusoninclusive_newnewnewnwen_03_07_24/measurement_Inclusive_part54.root",
#"/vols/cms/jo3717/Noahstest/r3k-bdttools/outputbdt/bdtoutput27_06_24_inclusive_final_focusoninclusive_newnewnewnwen_03_07_24/measurement_Inclusive_part55.root",
#"/vols/cms/jo3717/Noahstest/r3k-bdttools/outputbdt/bdtoutput27_06_24_inclusive_final_focusoninclusive_newnewnewnwen_03_07_24/measurement_Inclusive_part56.root",
#"/vols/cms/jo3717/Noahstest/r3k-bdttools/outputbdt/bdtoutput27_06_24_inclusive_final_focusoninclusive_newnewnewnwen_03_07_24/measurement_Inclusive_part57.root",
#"/vols/cms/jo3717/Noahstest/r3k-bdttools/outputbdt/bdtoutput27_06_24_inclusive_final_focusoninclusive_newnewnewnwen_03_07_24/measurement_Inclusive_part58.root",
#"/vols/cms/jo3717/Noahstest/r3k-bdttools/outputbdt/bdtoutput27_06_24_inclusive_final_focusoninclusive_newnewnewnwen_03_07_24/measurement_Inclusive_part59.root",
#"/vols/cms/jo3717/Noahstest/r3k-bdttools/outputbdt/bdtoutput27_06_24_inclusive_final_focusoninclusive_newnewnewnwen_03_07_24/measurement_Inclusive_part60.root",
#"/vols/cms/jo3717/Noahstest/r3k-bdttools/outputbdt/bdtoutput27_06_24_inclusive_final_focusoninclusive_newnewnewnwen_03_07_24/measurement_Inclusive_part61.root",
#"/vols/cms/jo3717/Noahstest/r3k-bdttools/outputbdt/bdtoutput27_06_24_inclusive_final_focusoninclusive_newnewnewnwen_03_07_24/measurement_Inclusive_part62.root",
#"/vols/cms/jo3717/Noahstest/r3k-bdttools/outputbdt/bdtoutput27_06_24_inclusive_final_focusoninclusive_newnewnewnwen_03_07_24/measurement_Inclusive_part63.root",
#"/vols/cms/jo3717/Noahstest/r3k-bdttools/outputbdt/bdtoutput27_06_24_inclusive_final_focusoninclusive_newnewnewnwen_03_07_24/measurement_Inclusive_part64.root",
#"/vols/cms/jo3717/Noahstest/r3k-bdttools/outputbdt/bdtoutput27_06_24_inclusive_final_focusoninclusive_newnewnewnwen_03_07_24/measurement_Inclusive_part65.root",
#"/vols/cms/jo3717/Noahstest/r3k-bdttools/outputbdt/bdtoutput27_06_24_inclusive_final_focusoninclusive_newnewnewnwen_03_07_24/measurement_Inclusive_part66.root",
#"/vols/cms/jo3717/Noahstest/r3k-bdttools/outputbdt/bdtoutput27_06_24_inclusive_final_focusoninclusive_newnewnewnwen_03_07_24/measurement_Inclusive_part67.root",
]

file_paths = [
"/vols/cms/jo3717/Noahstest/r3k-bdttools/outputbdt/bdtoutput27_06_24_inclusive_final_focusoninclusive_newnewnewnwen_25_07_24_fixedmotherid_jpsi_inclusive_new1/measurement_jpsi_nongenmatch_part1.root",
"/vols/cms/jo3717/Noahstest/r3k-bdttools/outputbdt/bdtoutput27_06_24_inclusive_final_focusoninclusive_newnewnewnwen_25_07_24_fixedmotherid_jpsi_inclusive_new1/measurement_jpsi_nongenmatch_part2.root",
"/vols/cms/jo3717/Noahstest/r3k-bdttools/outputbdt/bdtoutput27_06_24_inclusive_final_focusoninclusive_newnewnewnwen_25_07_24_fixedmotherid_jpsi_inclusive_new1/measurement_jpsi_nongenmatch_part3.root",
"/vols/cms/jo3717/Noahstest/r3k-bdttools/outputbdt/bdtoutput27_06_24_inclusive_final_focusoninclusive_newnewnewnwen_25_07_24_fixedmotherid_jpsi_inclusive_new1/measurement_jpsi_nongenmatch_part4.root",
"/vols/cms/jo3717/Noahstest/r3k-bdttools/outputbdt/bdtoutput27_06_24_inclusive_final_focusoninclusive_newnewnewnwen_25_07_24_fixedmotherid_jpsi_inclusive_new1/measurement_jpsi_nongenmatch_part5.root",
"/vols/cms/jo3717/Noahstest/r3k-bdttools/outputbdt/bdtoutput27_06_24_inclusive_final_focusoninclusive_newnewnewnwen_25_07_24_fixedmotherid_jpsi_inclusive_new1/measurement_jpsi_nongenmatch_part6.root",
"/vols/cms/jo3717/Noahstest/r3k-bdttools/outputbdt/bdtoutput27_06_24_inclusive_final_focusoninclusive_newnewnewnwen_25_07_24_fixedmotherid_jpsi_inclusive_new1/measurement_jpsi_nongenmatch_part7.root",
"/vols/cms/jo3717/Noahstest/r3k-bdttools/outputbdt/bdtoutput27_06_24_inclusive_final_focusoninclusive_newnewnewnwen_25_07_24_fixedmotherid_jpsi_inclusive_new1/measurement_jpsi_nongenmatch_part8.root",
]

def process_files(file_paths_section, section_index):
    tree = ROOT.TChain("mytree")



    for file_path in file_paths_section:
        tree.Add(file_path)
    output_file_name = f"parrallel_full_{section_index}.root"  
    outputfile = ROOT.TFile("outputjpsi/"+output_file_name,"RECREATE")
    outputtree = ROOT.TTree("mytree","mytree")


    idxarray = array('f',[0])
    outputtree.Branch("idx",idxarray,"idx/F")

    bmassarray = array('f',[0])
    outputtree.Branch("Bmass",bmassarray,"Bmass/F")

    Mllarray = array('f',[0])
    outputtree.Branch("Mll",Mllarray,"Mll/F")

    KLmassD0array = array('f',[0])
    outputtree.Branch("KLmassD0",KLmassD0array,"KLmassD0/F")

    BToKEE_cand_idxarray = array('f',[0])
    outputtree.Branch("BToKEE_cand_idx",BToKEE_cand_idxarray,"BToKEE_cand_idx/F")

    ##l1 genpart

    GenPart_L1_idxarray = array('f',[0])
    outputtree.Branch("GenPart_L1_idx",GenPart_L1_idxarray,"GenPart_L1_idx/F")

    GenPart_L1_pdgIdarray = array('f',[0])
    outputtree.Branch("GenPart_L1_pdgId",GenPart_L1_pdgIdarray,"GenPart_L1_pdgId/F")

    GenPart_L1_statusarray = array('f',[0])
    outputtree.Branch("GenPart_L1_status",GenPart_L1_statusarray,"GenPart_L1_status/F")

    GenPart_L1_etaarray = array('f',[0])
    outputtree.Branch("GenPart_L1_eta",GenPart_L1_etaarray,"GenPart_L1_eta/F")

    GenPart_L1_massarray = array('f',[0])
    outputtree.Branch("GenPart_L1_mass",GenPart_L1_massarray,"GenPart_L1_mass/F")

    GenPart_L1_phiarray = array('f',[0])
    outputtree.Branch("GenPart_L1_phi",GenPart_L1_phiarray,"GenPart_L1_phi/F")

    GenPart_L1_ptarray = array('f',[0])
    outputtree.Branch("GenPart_L1_pt",GenPart_L1_ptarray,"GenPart_L1_pt/F")

    GenPart_L1_vxarray = array('f',[0])
    outputtree.Branch("GenPart_L1_vx",GenPart_L1_vxarray,"GenPart_L1_vx/F")

    GenPart_L1_vyarray = array('f',[0])
    outputtree.Branch("GenPart_L1_vy",GenPart_L1_vyarray,"GenPart_L1_vy/F")

    GenPart_L1_vzarray = array('f',[0])
    outputtree.Branch("GenPart_L1_vz",GenPart_L1_vzarray,"GenPart_L1_vz/F")

    ##l2 genpart
    GenPart_L2_idxarray = array('f',[0])
    outputtree.Branch("GenPart_L2_idx",GenPart_L2_idxarray,"GenPart_L2_idx/F")

    GenPart_L2_pdgIdarray = array('f',[0])
    outputtree.Branch("GenPart_L2_pdgId",GenPart_L2_pdgIdarray,"GenPart_L2_pdgId/F")

    GenPart_L2_statusarray = array('f',[0])
    outputtree.Branch("GenPart_L2_status",GenPart_L2_statusarray,"GenPart_L2_status/F")

    GenPart_L2_etaarray = array('f',[0])
    outputtree.Branch("GenPart_L2_eta",GenPart_L2_etaarray,"GenPart_L2_eta/F")

    GenPart_L2_massarray = array('f',[0])
    outputtree.Branch("GenPart_L2_mass",GenPart_L2_massarray,"GenPart_L2_mass/F")

    GenPart_L2_phiarray = array('f',[0])
    outputtree.Branch("GenPart_L2_phi",GenPart_L2_phiarray,"GenPart_L2_phi/F")

    GenPart_L2_ptarray = array('f',[0])
    outputtree.Branch("GenPart_L2_pt",GenPart_L2_ptarray,"GenPart_L2_pt/F")

    GenPart_L2_vxarray = array('f',[0])
    outputtree.Branch("GenPart_L2_vx",GenPart_L2_vxarray,"GenPart_L2_vx/F")

    GenPart_L2_vyarray = array('f',[0])
    outputtree.Branch("GenPart_L2_vy",GenPart_L2_vyarray,"GenPart_L2_vy/F")

    GenPart_L2_vzarray = array('f',[0])
    outputtree.Branch("GenPart_L2_vz",GenPart_L2_vzarray,"GenPart_L2_vz/F")

    ##kaon genpart

    GenPart_K_idxarray = array('f',[0])
    outputtree.Branch("GenPart_K_idx",GenPart_K_idxarray,"GenPart_K_idx/F")

    GenPart_K_pdgIdarray = array('f',[0])
    outputtree.Branch("GenPart_K_pdgId",GenPart_K_pdgIdarray,"GenPart_K_pdgId/F")

    GenPart_K_statusarray = array('f',[0])
    outputtree.Branch("GenPart_K_status",GenPart_K_statusarray,"GenPart_K_status/F")

    GenPart_K_etaarray = array('f',[0])
    outputtree.Branch("GenPart_K_eta",GenPart_K_etaarray,"GenPart_K_eta/F")

    GenPart_K_massarray = array('f',[0])
    outputtree.Branch("GenPart_K_mass",GenPart_K_massarray,"GenPart_K_mass/F")

    GenPart_K_phiarray = array('f',[0])
    outputtree.Branch("GenPart_K_phi",GenPart_K_phiarray,"GenPart_K_phi/F")

    GenPart_K_ptarray = array('f',[0])
    outputtree.Branch("GenPart_K_pt",GenPart_K_ptarray,"GenPart_K_pt/F")

    GenPart_K_vxarray = array('f',[0])
    outputtree.Branch("GenPart_K_vx",GenPart_K_vxarray,"GenPart_K_vx/F")

    GenPart_K_vyarray = array('f',[0])
    outputtree.Branch("GenPart_K_vy",GenPart_K_vyarray,"GenPart_K_vy/F")

    GenPart_K_vzarray = array('f',[0])
    outputtree.Branch("GenPart_K_vz",GenPart_K_vzarray,"GenPart_K_vz/F")



    GenPart_L1_lin_idxvector = vector('float')()
    outputtree.Branch("GenPart_L1_lin_idx", GenPart_L1_lin_idxvector)

    GenPart_L1_lin_pdgIdvector = vector('float')()
    outputtree.Branch("GenPart_L1_lin_pdgId", GenPart_L1_lin_pdgIdvector)

    GenPart_L1_lin_statusvector = vector('float')()
    outputtree.Branch("GenPart_L1_lin_status", GenPart_L1_lin_statusvector)

    GenPart_L2_lin_idxvector = vector('float')()
    outputtree.Branch("GenPart_L2_lin_idx", GenPart_L2_lin_idxvector)

    GenPart_L2_lin_pdgIdvector = vector('float')()
    outputtree.Branch("GenPart_L2_lin_pdgId", GenPart_L2_lin_pdgIdvector)

    GenPart_L2_lin_statusvector = vector('float')()
    outputtree.Branch("GenPart_L2_lin_status", GenPart_L2_lin_statusvector)

    GenPart_K_lin_idxvector = vector('float')()
    outputtree.Branch("GenPart_K_lin_idx", GenPart_K_lin_idxvector)

    GenPart_K_lin_pdgIdvector = vector('float')()
    outputtree.Branch("GenPart_K_lin_pdgId", GenPart_K_lin_pdgIdvector)
    
    GenPart_K_lin_statusvector = vector('float')()
    outputtree.Branch("GenPart_K_lin_status", GenPart_K_lin_statusvector)
    
    bdt_scorearray = array('f',[0])
    outputtree.Branch("bdt_score",bdt_scorearray,"bdt_score/F")

    trigger_ORarray = array('f',[0])
    outputtree.Branch("trigger_OR",trigger_ORarray,"trigger_OR/F")



    print("total entries",tree.GetEntries())
    totalEntries = tree.GetEntries()
    idxvalue=-100
    bcandix=-100    
    objectlist = []
    start = 0
    counter = 0    
    for i in range(totalEntries):
        tree.GetEntry(i+start)
        if (i + 1) % 1000000 == 0:
            print(f"Iteration {counter}")
            counter = counter +1
        idxvalue = tree.idx
        bcandix = tree.BToKEE_cand_idx
        if tree.bdt_score < -4:
            continue
        #print("event i: ",i)
        event_attributes = {
            'idx': tree.idx,
            'Bmass': tree.Bmass,
            'Mll': tree.Mll,
            'KLmassD0': tree.KLmassD0,
            'BToKEE_cand_idx': tree.BToKEE_cand_idx,
            'GenPart_idx': tree.GenPart_idx,
            'GenPart_genPartIdxMother': tree.GenPart_genPartIdxMother,
            'GenPart_pdgId': tree.GenPart_pdgId,
            'GenPart_status': tree.GenPart_status,
            'GenPart_eta': tree.GenPart_eta,
            'GenPart_mass': tree.GenPart_mass,
            'GenPart_phi': tree.GenPart_phi,
            'GenPart_pt': tree.GenPart_pt,
            'GenPart_vx': tree.GenPart_vx,
            'GenPart_vy': tree.GenPart_vy,
            'GenPart_vz': tree.GenPart_vz,
            'Bpt': tree.Bpt,
            'Bcos': tree.Bcos,
            'Bprob': tree.Bprob,
            'BmassErr': tree.BmassErr,
            'Biso': tree.Biso,
            'BsLxy': tree.BsLxy,
            'L1pt': tree.L1pt,
            'L1eta': tree.L1eta,
            'L1phi': tree.L1phi,
            'L1iso': tree.L1iso,
            'L1id': tree.L1id,
            'L2pt': tree.L2pt,
            'L2eta': tree.L2eta,
            'L2phi': tree.L2phi,
            'L2iso': tree.L2iso,
            'L2id': tree.L2id,
            'bdt_score': tree.bdt_score,
            'trigger_OR': tree.trigger_OR,
            'Kpt': tree.Kpt,
            'Keta': tree.Keta,
            'Kphi': tree.Kphi,
        }

        event_obj = Event(**event_attributes)
        objectlist.append(event_obj)
        tree.GetEntry(i+start+1)
        if (tree.idx != idxvalue) or (tree.BToKEE_cand_idx != bcandix):
            #print("event",idxvalue,"bcand",bcandix)
    #        print("len object list", len(objectlist))

            electron1, objectlist = ElectronMatching(objectlist,ele1=True)
            if electron1 == None:
                idxvalue=-100
                bcandix=-100    
                objectlist = []
                continue
            else:
                print("electron1 lineage-----------------------------------")
                ele1lineage = []
                print_mother_lineage(objectlist, electron1,ele1lineage)
                ele1lineage = [x for x in ele1lineage if x.GenPart_pdgId != 22]

                print("Ele1 lin",[x.GenPart_pdgId for x in ele1lineage])
    
    
    
    
    
                electron2, objectlist = ElectronMatching(objectlist,ele2=True)
                print("electron2 lineage-------------------------------------")
                ele2lineage = []
                print_mother_lineage(objectlist, electron2,ele2lineage)
                ele2lineage = [x for x in ele2lineage if x.GenPart_pdgId != 22]

                print("Ele2 lin",[x.GenPart_pdgId for x in ele2lineage])
                
                
                
                if electron2==None or objectlist == None:

                    idxvalue=-100
                    bcandix=-100    
                    objectlist = []  
                    continue
                else:
                    kaon1, objectlist = KaonMatching(objectlist)
                    print("kaon lineage-------------------------------------")
                    kaonlineage = []
                    print_mother_lineage(objectlist, kaon1,kaonlineage)
                    kaonlineage = [x for x in kaonlineage if x.GenPart_pdgId != 22]
                    print("kaon pdgid",kaon1[0].GenPart_pdgId)
                    print("Kaon lin",[x.GenPart_pdgId for x in kaonlineage])

                    print("\n" * 4)  
                    print("-------------------------------------------- break ----------------------------------")
                
                    idxarray[0] = electron1[0].idx
                    bmassarray[0] = electron1[0].Bmass
                    Mllarray[0] = electron1[0].Mll
                    KLmassD0array[0] = electron1[0].KLmassD0
                    BToKEE_cand_idxarray[0] = electron1[0].BToKEE_cand_idx
                    
                    GenPart_L1_idxarray[0] = electron1[0].GenPart_idx
                    GenPart_L1_pdgIdarray[0] = electron1[0].GenPart_pdgId
                    GenPart_L1_statusarray[0] = electron1[0].GenPart_status
                    GenPart_L1_etaarray[0] = electron1[0].GenPart_eta
                    GenPart_L1_massarray[0] = electron1[0].GenPart_mass
                    GenPart_L1_phiarray[0] = electron1[0].GenPart_phi
                    GenPart_L1_ptarray[0] = electron1[0].GenPart_pt
                    GenPart_L1_vxarray[0] = electron1[0].GenPart_vx
                    GenPart_L1_vyarray[0] = electron1[0].GenPart_vy
                    GenPart_L1_vzarray[0] = electron1[0].GenPart_vz
                    
                    GenPart_L2_idxarray[0] = electron2[0].GenPart_idx
                    GenPart_L2_pdgIdarray[0] = electron2[0].GenPart_pdgId
                    GenPart_L2_statusarray[0] = electron2[0].GenPart_status
                    GenPart_L2_etaarray[0] = electron2[0].GenPart_eta
                    GenPart_L2_massarray[0] = electron2[0].GenPart_mass
                    GenPart_L2_phiarray[0] = electron2[0].GenPart_phi
                    GenPart_L2_ptarray[0] = electron2[0].GenPart_pt
                    GenPart_L2_vxarray[0] = electron2[0].GenPart_vx
                    GenPart_L2_vyarray[0] = electron2[0].GenPart_vy
                    GenPart_L2_vzarray[0] = electron2[0].GenPart_vz
                    
                    [GenPart_L1_lin_idxvector.push_back(x.GenPart_idx) for x in ele1lineage]
                    [GenPart_L1_lin_pdgIdvector.push_back(x.GenPart_pdgId) for x in ele1lineage]
                    [GenPart_L1_lin_statusvector.push_back(x.GenPart_status) for x in ele1lineage]
                    [GenPart_L2_lin_idxvector.push_back(x.GenPart_idx) for x in ele2lineage]
                    [GenPart_L2_lin_pdgIdvector.push_back(x.GenPart_pdgId) for x in ele2lineage]
                    [GenPart_L2_lin_statusvector.push_back(x.GenPart_status) for x in ele2lineage]
                    [GenPart_K_lin_idxvector.push_back(x.GenPart_idx) for x in kaonlineage]
                    [GenPart_K_lin_pdgIdvector.push_back(x.GenPart_pdgId) for x in kaonlineage]
                    [GenPart_K_lin_statusvector.push_back(x.GenPart_status) for x in kaonlineage]

                    
                    GenPart_K_idxarray[0] = kaon1[0].GenPart_idx
                    GenPart_K_pdgIdarray[0] = kaon1[0].GenPart_pdgId
                    GenPart_K_statusarray[0] = kaon1[0].GenPart_status
                    GenPart_K_etaarray[0] = kaon1[0].GenPart_eta
                    GenPart_K_massarray[0] = kaon1[0].GenPart_mass
                    GenPart_K_phiarray[0] = kaon1[0].GenPart_phi
                    GenPart_K_ptarray[0] = kaon1[0].GenPart_pt
                    GenPart_K_vxarray[0] = kaon1[0].GenPart_vx
                    GenPart_K_vyarray[0] = kaon1[0].GenPart_vy
                    GenPart_K_vzarray[0] = kaon1[0].GenPart_vz

                    
                    bdt_scorearray[0] = electron1[0].bdt_score
                    trigger_ORarray[0] = electron1[0].trigger_OR
                
                    outputtree.Fill()
                    GenPart_L1_lin_idxvector.clear()
                    GenPart_L1_lin_pdgIdvector.clear()
                    GenPart_L1_lin_statusvector.clear()
                    GenPart_L2_lin_idxvector.clear()
                    GenPart_L2_lin_pdgIdvector.clear()
                    GenPart_L2_lin_statusvector.clear()
                    GenPart_K_lin_idxvector.clear()
                    GenPart_K_lin_pdgIdvector.clear()
                    GenPart_K_lin_statusvector.clear()

                

                idxvalue=-100
                bcandix=-100    
                objectlist = []   
    outputfile.Write()
    outputfile.Close()
        
        

sections = split_file_paths(file_paths, 1)


def main():
    processes = []
    for i, section in enumerate(sections):
        # Create a process for each section
        p = Process(target=process_files, args=(section, i+1))
        processes.append(p)
        p.start()
    for p in processes:
        p.join()

if __name__ == "__main__":
    main()