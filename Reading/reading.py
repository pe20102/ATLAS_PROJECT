#importing all the packages and imports for reading.py
import uproot
import json
import vector
import sys
import numpy as np
import time
import awkward as ak
import os
import redis  # NEW: Added Redis import


#here I am getting the directory of the current script reading.py
current__script = os.path.dirname(os.path.realpath(__file__))

#here I am getting the name of its parent directory where infofile is
directory_after = os.path.dirname(current__script)

#and here the parent directories path is added so infofile can be imported
sys.path.append(directory_after)
import infofile


#original code, not much changed here:
# Include global variables such as lumi, tuple_path, samples, MeV, and GeV
lumi = 10 # fb-1 # data_A,data_B,data_C,data_D

fraction = 1.0 # reduce this is if you want the code to run quicker
                                                                                                                                  
#tuple_path = "Input/4lep/" # local 
tuple_path = "https://atlas-opendata.web.cern.ch/atlas-opendata/samples/2020/4lep/" # web address

#the samples for analysis are defined here:
samples = {

    'data': {
        'list' : ['data_A','data_B','data_C','data_D'],
    },

    r'Background $Z,t\bar{t}$' : { # Z + ttbar
        'list' : ['Zee','Zmumu','ttbar_lep'],
        'color' : "#6b59d3" # purple
    },

    r'Background $ZZ^*$' : { # ZZ
        'list' : ['llll'],
        'color' : "#ff0000" # red
    },

    r'Signal ($m_H$ = 125 GeV)' : { # H -> ZZ -> llll
        'list' : ['ggH125_ZZ4lep','VBFH125_ZZ4lep','WH125_ZZ4lep','ZH125_ZZ4lep'],
        'color' : "#00cdff" # light blue
    },

}

#defining the variables for calculations and analysis
MeV = 0.001
GeV = 1.0

def calc_weight(xsec_weight, events):
    return (
        xsec_weight
        * events.mcWeight
        * events.scaleFactor_PILEUP
        * events.scaleFactor_ELE
        * events.scaleFactor_MUON 
        * events.scaleFactor_LepTRIGGER
    )

def get_xsec_weight(sample):
    info = infofile.infos[sample] # open infofile
    xsec_weight = (lumi*1000*info["xsec"])/(info["sumw"]*info["red_eff"]) #*1000 to go from fb-1 to pb-1
    return xsec_weight # return cross-section weight


def calc_mllll(lep_pt, lep_eta, lep_phi, lep_E):
    # construct awkward 4-vector array
    p4 = vector.zip({"pt": lep_pt, "eta": lep_eta, "phi": lep_phi, "E": lep_E})
    # calculate invariant mass of first 4 leptons
    # [:, i] selects the i-th lepton in each event
    # .M calculates the invariant mass
    return (p4[:, 0] + p4[:, 1] + p4[:, 2] + p4[:, 3]).M * MeV


# cut on lepton charge
# paper: "selecting two pairs of isolated leptons, each of which is comprised of two leptons with the same flavour and opposite charge"
def cut_lep_charge(lep_charge):
    # throw away when sum of lepton charges is not equal to 0
    # first lepton in each event is [:, 0], 2nd lepton is [:, 1] etc
    return lep_charge[:, 0] + lep_charge[:, 1] + lep_charge[:, 2] + lep_charge[:, 3] != 0

# cut on lepton type
# paper: "selecting two pairs of isolated leptons, each of which is comprised of two leptons with the same flavour and opposite charge"
def cut_lep_type(lep_type):
    # for an electron lep_type is 11
    # for a muon lep_type is 13
    # throw away when none of eeee, mumumumu, eemumu
    sum_lep_type = lep_type[:, 0] + lep_type[:, 1] + lep_type[:, 2] + lep_type[:, 3]
    return (sum_lep_type != 44) & (sum_lep_type != 48) & (sum_lep_type != 52)

#for calculating the invariant masses of two pairs of leptons from particle collision data:
def calc_m12_m34(lep_pt, lep_eta, lep_phi, lep_E):
    #the lepton properties are combined into a four-vector for each lepton
    p4 = vector.zip({"pt": lep_pt, "eta": lep_eta, "phi": lep_phi, "E": lep_E})
    #consisting of the transverse momentum (pt), pseudorapidity (eta), azimuthal angle (phi), and energy (E).
    #calculating the invariant mass of the first lepton pair:
    m12 = (p4[:, 0] + p4[:, 1]).M * MeV
    #calculating the invariant mass of the second lepton pair:
    m34 = (p4[:, 2] + p4[:, 3]).M * MeV
    #returning the calculated invariant masses for later plots
    return m12, m34

def read_file(path, sample, worker_beginning, worker_end, worker_id):
    start = time.time() # start the clock
    print("\tProcessing: "+sample) # print which sample is being processed
    data_all = [] # define empty list to hold all data for this sample
    
    # open the tree called mini using a context manager (will automatically close files/resources)
    # The 'mini' tree within the ROOT file is accessed for data analysis
    with uproot.open(path + ":mini") as tree:
         #checking if the sample is simulated (Monte Carlo) data. If so, calculate the cross-section weight
        if 'data' not in sample: xsec_weight = get_xsec_weight(sample) # get cross-section weight

        #here the data is being iterated over in the tree, this loop processes the data in loads for efficiency
        
        for data in tree.iterate(['lep_pt', 'lep_eta', 'lep_phi', 'lep_E', 'lep_charge', 'lep_type', 
                                  'mcWeight', 'scaleFactor_PILEUP', 'scaleFactor_ELE', 'scaleFactor_MUON', 
                                  'scaleFactor_LepTRIGGER'],
                                  library="ak", entry_start=worker_beginning, entry_stop=worker_end):
            # entry_start and entry_stop define the range of data to process, enabling distributed processing

            nIn = len(data) # number of events in this batch

            if 'data' not in sample: # only do this for Monte Carlo simulation files
                # multiply all Monte Carlo weights and scale factors together to give total weight
                data['totalWeight'] = calc_weight(xsec_weight, data)

            # cut on lepton charge using the function cut_lep_charge defined above
            data = data[~cut_lep_charge(data.lep_charge)]

            # cut on lepton type using the function cut_lep_type defined above
            data = data[~cut_lep_type(data.lep_type)]

            # calculation of 4-lepton invariant mass using the function calc_mllll defined above
            data['mllll'] = calc_mllll(data.lep_pt, data.lep_eta, data.lep_phi, data.lep_E)


            #here the invariant masses for two pairs of leptons are calculated
            m12, m34 = calc_m12_m34(data.lep_pt, data.lep_eta, data.lep_phi, data.lep_E)
            #done using the calc_m12_m34 function, which takes lepton properties as inputs
            data['m12'] = m12
            #store the calculated invariant masses (m12 and m34) back into the data array
            data['m34'] = m34

            nOut = len(data) # number of events passing cuts in this batch
            data_all.append(data) # append array from this batch
            elapsed = time.time() - start # time taken to process
            print("\t\t nIn: "+str(nIn)+",\t nOut: \t"+str(nOut)+"\t in "+str(round(elapsed,1))+"s") # events before and after
    
    end_time = time.time() #end the clock
    processing_time = end_time - start  #get the processing time for this worker

    #the processing time is recorded into a JSON file, they're easy to handle and use 
    #the file name includes the worker ID and sample name so the user can easily check specific worker data
    with open(f"/mydir/process_info/new_time_plot_worker{worker_id}_{sample}.json", "w") as f:
        #writing the processing information as a JSON object
        json.dump({
            "worker_id": worker_id,  #identifier for the specific worker
            "sample": sample,  #sample name
            "time": processing_time, #the time taken for the worker to process that load of the sample
            "worker_beginning": worker_beginning, #the starting entry for the working processes
            "worker_end": worker_end #the ending entry for the working processes
        }, f)
    #concatenate all the processed data loads/batches into a single awkward array
    return ak.concatenate(data_all)


if __name__ == "__main__":
    # Connect to Redis
    print("Attempting to connect to Redis...")
    try:
        r = redis.Redis(host='redis', port=6379, decode_responses=True)
        r.ping()  # Test the connection
        print("Successfully connected to Redis")
    except Exception as e:
        print(f"Failed to connect to Redis: {str(e)}")
        sys.exit(1)

    print("Worker started, waiting for tasks...")

    while True:
        try:
            # Try to get work from queue
            work_item_json = r.rpop("work_queue")
            if not work_item_json:
                print("No more work available")
                break

            print(f"Received task: {work_item_json}")
            
            # Process the work item
            work_item = json.loads(work_item_json)
            sample = work_item["sample"]
            worker_beginning = work_item["start"]
            worker_end = work_item["end"]
            worker_id = work_item["worker_id"]

            # Use existing info_library creation for the current sample
            info_library = {sample: "Data/" if 'data' in sample else f"MC/mc_{infofile.infos[sample]['DSID']}." 
                          for sample in [sample]}
            
            capture_file = os.path.join(tuple_path, info_library[sample] + sample + ".4lep.root")

            print(f"Processing {sample} from {worker_beginning} to {worker_end}")
            
            # Process using existing read_file function
            reading_file = read_file(capture_file, sample, worker_beginning, worker_end, worker_id)
            writing_file = f"/mydir/process_info/reading_{sample}-{worker_beginning}-{worker_end}.awkd"
            ak.to_parquet(reading_file, writing_file)
            print(f"Completed task for {sample}")

        except Exception as e:
            print(f"Error processing task: {str(e)}")
            if work_item_json:
                r.lpush("failed_queue", work_item_json)