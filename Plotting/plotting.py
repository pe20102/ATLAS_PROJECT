import numpy as np
import os
import glob
import awkward as ak
import time
import json
import matplotlib.pyplot as plt
from matplotlib.ticker import AutoMinorLocator

#defining the variables from reading.py that are needed in plotting as well:
MeV = 0.001
GeV = 1.0
fraction = 1.0 # reduce this is if you want the code to run quicker
lumi = 10


#defining the samples for analysis and searching:
samples = {

    'data': {
        'list' : ['data_A','data_B','data_C','data_D'],
    },

    r'Background $Z,t\bar{t}$' : { # Z + ttbar
        'list' : ['Zee','Zmumu','ttbar_lep'],
        'color' : "#6b59d3" 
    },

    r'Background $ZZ^*$' : { # ZZ
        'list' : ['llll'],
        'color' : "#ff0000" 
    },

    r'Signal ($m_H$ = 125 GeV)' : { # H -> ZZ -> llll
        'list' : ['ggH125_ZZ4lep','VBFH125_ZZ4lep','WH125_ZZ4lep','ZH125_ZZ4lep'],
        'color' : "#00cdff"
    },

}

#data aggregation function for all the workers batches
#taken from the binded volume they were written in
def data_aggregation(binded_volume="/mydir/process_info/"):
    #defining a list of all the sample names to be processed
    all_samples = ['data_A', 'data_B', 'data_C', 'data_D', 'Zee', 'Zmumu', 'ttbar_lep', 'llll', 'ggH125_ZZ4lep', 'VBFH125_ZZ4lep', 'WH125_ZZ4lep', 'ZH125_ZZ4lep']

    #initialising a dictionary to hold the processed data for each sample
    
    sample_dict = {sample: [] for sample in all_samples}
    #each sample key in the dictionary has an associated list to store the data

    #iterating through files in the specified directory
   
    for id_file in os.listdir(binded_volume):
         #looks for files that are relevant to the analysis since they start with reading_ and are awkwd files
        if id_file.startswith("reading_"):
            #extracting the sample name from the file name
            id_sample = id_file.split("-", 1)[0].replace("reading_", "")
            #if the sample is in the sample dictionary it reads and appends its data
            if id_sample in sample_dict:
                #reading the parquet 
                data_captured = ak.from_parquet(os.path.join(binded_volume, id_file))
                #appending the data
                sample_dict[id_sample].append(data_captured)

    #defining a mapping of categories to their corresponding samples
    category_map = {
        'data': ['data_A', 'data_B', 'data_C', 'data_D'],
        'Background $Z,t\\bar{t}$': ['Zee', 'Zmumu', 'ttbar_lep'],
        'Background $ZZ^*$': ['llll'],
        'Signal ($m_H$ = 125 GeV)': ['ggH125_ZZ4lep', 'VBFH125_ZZ4lep', 'WH125_ZZ4lep', 'ZH125_ZZ4lep']
    }

    #used for categorising the data into different physics backgrounds and signal

    #initialise a dictionary to hold aggregated data for each category
    dictionary_agg = {category: [] for category in category_map}

    #aggregating the data for each category
    for category, samples in category_map.items():
        #concatenating all the data arrays for each sample within a category
        for sample in samples:
            if sample_dict[sample]:
                #appending based on the sample location in the category
                dictionary_agg[category].append(ak.concatenate(sample_dict[sample]))

    #concatenating arrays within each category
    for x in dictionary_agg:
        #if there are multiple data arrays for a category, they are combined into a single array
        dictionary_agg[x] = ak.concatenate(dictionary_agg[x]) if dictionary_agg[x] else []
    #returning the aggregated data dictionary to use for plotting below:
    return dictionary_agg

def plot_processing_time(binded_volume="/mydir/process_info/"):
    # Defining a dictionary to hold the processing time for each worker-sample combination
    processing_durations = {}

    # Using glob to open it iterated through all json files in the specified directory that match the pattern
    for id_file in glob.glob(os.path.join(binded_volume, "new_time_plot_worker*.json")):
        # The files contain processing time data for each worker and sample
        with open(id_file, "r") as file:
            processing_data = json.load(file)  # Load data from the json file
            # Creating a tuple key of worker ID and sample name
            worker_sample_identifier = (processing_data["worker_id"], processing_data["sample"])
            # Store the processing time in the dictionary
            processing_durations[worker_sample_identifier] = processing_data["time"]

    # Create lists of unique workers and samples for plotting
    distinct_workers = sorted(set(worker for worker, sample in processing_durations))
    # For samples as well as workers, same way
    distinct_samples = sorted(set(sample for worker, sample in processing_durations))

    # Data for plotting: a list of processing times for each worker, per sample
    worker_plot_data = {worker: [] for worker in distinct_workers}
    for worker in distinct_workers:
        for sample in distinct_samples:
            # Get the processing time or set it to 0
            processing_time = processing_durations.get((worker, sample), 0)
            # Append the data
            worker_plot_data[worker].append(processing_time)

    # Setting the number of bars based on the samples
    sample_bar_count = len(distinct_samples)

    # Defining the fig size
    plt.figure(figsize=(12, 8))

    # Setting the x indices for the groups
    worker_indices = np.arange(len(distinct_workers))

    # Setting the default width for the bars
    bar_width = 0.8 / sample_bar_count

    # Choosing the colour palette for the bars
    bar_colors = plt.cm.viridis(np.linspace(0, 1, sample_bar_count))
    # Plotting the processing time for each sample as a separate bar in the bar group
    for i, sample in enumerate(distinct_samples):
        # Gathering the processing times for each worker for this sample
        sample_processing_times = [worker_plot_data[worker][i] for worker in distinct_workers]
        # Creating the bars for the sample
        plt.bar(worker_indices + i * bar_width, sample_processing_times, bar_width, label=sample, color=bar_colors[i])

    # Defining the labels and titles
    plt.xlabel('Worker', fontsize=14)
    plt.ylabel('Processing Time (s)', fontsize=14)
    plt.title('Processing Time per Worker per Sample', fontsize=16)
    # Defining the x and y formats as well as the grid and legend
    plt.xticks(worker_indices + bar_width * (sample_bar_count - 1) / 2, distinct_workers, fontsize=12)
    plt.yticks(fontsize=12)
    plt.legend(title="Samples", bbox_to_anchor=(1.05, 1), loc='upper left', fontsize=12)
    plt.grid(True, which='both', linestyle='--', linewidth=0.5)
    # Makes the plot look cleaner
    plt.tight_layout()

    # Saving the plot to the data directory as a file
    plt.savefig(os.path.join(binded_volume, "enhanced_time_plot.png"))
    plt.close()



#original plotting function: (), for the HZZ analysis 
def plot_data(data):
    xmin = 80 * GeV
    xmax = 250 * GeV
    step_size = 5 * GeV

    bin_edges = np.arange(start=xmin, # The interval includes this value
                     stop=xmax+step_size, # The interval doesn't include this value
                     step=step_size ) # Spacing between values
    bin_centres = np.arange(start=xmin+step_size/2, # The interval includes this value
                            stop=xmax+step_size/2, # The interval doesn't include this value
                            step=step_size ) # Spacing between values

    data_x,_ = np.histogram(ak.to_numpy(data['data']['mllll']), 
                            bins=bin_edges ) # histogram the data
    data_x_errors = np.sqrt( data_x ) # statistical error on the data

    signal_x = ak.to_numpy(data[r'Signal ($m_H$ = 125 GeV)']['mllll']) # histogram the signal
    signal_weights = ak.to_numpy(data[r'Signal ($m_H$ = 125 GeV)'].totalWeight) # get the weights of the signal events
    signal_color = samples[r'Signal ($m_H$ = 125 GeV)']['color'] # get the colour for the signal bar

    mc_x = [] # define list to hold the Monte Carlo histogram entries
    mc_weights = [] # define list to hold the Monte Carlo weights
    mc_colors = [] # define list to hold the colors of the Monte Carlo bars
    mc_labels = [] # define list to hold the legend labels of the Monte Carlo bars

    for s in samples: # loop over samples
        if s not in ['data', r'Signal ($m_H$ = 125 GeV)']: # if not data nor signal
            mc_x.append( ak.to_numpy(data[s]['mllll']) ) # append to the list of Monte Carlo histogram entries
            mc_weights.append( ak.to_numpy(data[s].totalWeight) ) # append to the list of Monte Carlo weights
            mc_colors.append( samples[s]['color'] ) # append to the list of Monte Carlo bar colors
            mc_labels.append( s ) # append to the list of Monte Carlo legend labels

    # ***
    # Main plot 
    # ***
    main_axes = plt.gca() # get current axes
    
    # plot the data points
    main_axes.errorbar(x=bin_centres, y=data_x, yerr=data_x_errors,
                       fmt='ko', # 'k' means black and 'o' is for circles 
                       label='Data') 
    
    # plot the Monte Carlo bars
    mc_heights = main_axes.hist(mc_x, bins=bin_edges, 
                                weights=mc_weights, stacked=True, 
                                color=mc_colors, label=mc_labels )
    
    mc_x_tot = mc_heights[0][-1] # stacked background MC y-axis value
    
    # calculate MC statistical uncertainty: sqrt(sum w^2)
    mc_x_err = np.sqrt(np.histogram(np.hstack(mc_x), bins=bin_edges, weights=np.hstack(mc_weights)**2)[0])
    
    # plot the signal bar
    main_axes.hist(signal_x, bins=bin_edges, bottom=mc_x_tot, 
                   weights=signal_weights, color=signal_color,
                   label=r'Signal ($m_H$ = 125 GeV)')
    
    # plot the statistical uncertainty
    main_axes.bar(bin_centres, # x
                  2*mc_x_err, # heights
                  alpha=0.5, # half transparency
                  bottom=mc_x_tot-mc_x_err, color='none', 
                  hatch="////", width=step_size, label='Stat. Unc.' )

    # set the x-limit of the main axes
    main_axes.set_xlim( left=xmin, right=xmax ) 
    
    # separation of x axis minor ticks
    main_axes.xaxis.set_minor_locator( AutoMinorLocator() ) 
    
    # set the axis tick parameters for the main axes
    main_axes.tick_params(which='both', # ticks on both x and y axes
                          direction='in', # Put ticks inside and outside the axes
                          top=True, # draw ticks on the top axis
                          right=True ) # draw ticks on right axis
    
    # x-axis label
    main_axes.set_xlabel(r'4-lepton invariant mass $\mathrm{m_{4l}}$ [GeV]',
                        fontsize=13, x=1, horizontalalignment='right' )
    
    # write y-axis label for main axes
    main_axes.set_ylabel('Events / '+str(step_size)+' GeV',
                         y=1, horizontalalignment='right') 
    
    # set y-axis limits for main axes
    main_axes.set_ylim( bottom=0, top=np.amax(data_x)*1.6 )
    
    # add minor ticks on y-axis for main axes
    main_axes.yaxis.set_minor_locator( AutoMinorLocator() ) 

    # Add text 'ATLAS Open Data' on plot
    plt.text(0.05, # x
             0.93, # y
             'ATLAS Open Data', # text
             transform=main_axes.transAxes, # coordinate system used is that of main_axes
             fontsize=13 ) 
    
    # Add text 'for education' on plot
    plt.text(0.05, # x
             0.88, # y
             'for education', # text
             transform=main_axes.transAxes, # coordinate system used is that of main_axes
             style='italic',
             fontsize=8 ) 
    
    # Add energy and luminosity
    lumi_used = str(lumi*fraction) # luminosity to write on the plot
    plt.text(0.05, # x
             0.82, # y
             '$\sqrt{s}$=13 TeV,$\int$L dt = '+lumi_used+' fb$^{-1}$', # text
             transform=main_axes.transAxes ) # coordinate system used is that of main_axes
    
    # Add a label for the analysis carried out
    plt.text(0.05, # x
             0.76, # y
             r'$H \rightarrow ZZ^* \rightarrow 4\ell$', # text 
             transform=main_axes.transAxes ) # coordinate system used is that of main_axes

    # draw the legend
    main_axes.legend( frameon=False ) # no box around the legend

    plt.savefig("/mydir/process_info/Higgs_Analysis_Plot.png")
    
    return

#m12 against m34 plotting function:
def plot_m12_m34(data):
    #checking if the Higgs signal data is present in the dataset
    if r'Signal ($m_H$ = 125 GeV)' in data:
        #converting the m12 and m34 data to numpy arrays for handling and plotting
        m12 = ak.to_numpy(data[r'Signal ($m_H$ = 125 GeV)']['m12'])
        m34 = ak.to_numpy(data[r'Signal ($m_H$ = 125 GeV)']['m34'])
        #setting up the figure size for the scatter plot
        plt.figure(figsize=(12, 10))
        #creating the scatter plot of m12 versus m34
        #the colour intensity is based on the logarithmic scale of m12 values
        plt.scatter(m12, m34, alpha=0.6, c=np.log(m12 + 1), cmap='viridis')
        #setting the x and y labels
        plt.xlabel(r'$m_{12}$ [GeV]', fontsize=14)
        plt.ylabel(r'$m_{34}$ [GeV]', fontsize=14)
        #setting the title of the plot
        plt.title(r'Scatter plot of $m_{12}$ vs $m_{34}$ for Higgs Signal', fontsize=16)
        #adding a colour bar to indicate the log scale of m12 values
        plt.colorbar(label='Log scale of $m_{12}$')
        #addding grid, minor ticks and applying a tight layout to make the plots look nicer
        plt.grid(True, which='both', linestyle='--', linewidth=0.5)
        plt.minorticks_on()
        plt.tight_layout()
        #saving the plot to a file:
        plt.savefig('/mydir/process_info/M12_M34_Plot.png')
        plt.close()



def plot_m34(data):
     #defining meaningful labels for different data categories
    category_labels = {
        'data': 'Observed Data',
        r'Background $Z,t\bar{t}$': 'Background Z + ttbar',
        r'Background $ZZ^*$': 'Background ZZ*',
        r'Signal ($m_H$ = 125 GeV)': 'Higgs Signal (125 GeV)'
    }
     #colour palette for different categories
    colors = ['blue', 'purple', 'red', 'cyan', 'orange']
    #iterate through each category of data to create individual plots
    for i, category in enumerate(data):
        #checking if data present in the current category
        if len(data[category]) > 0:
            #converting the m34 data for the current category to a numpy array
            m34 = ak.to_numpy(data[category]['m34'])
             #setting up the figure size for the histogram
            plt.figure(figsize=(12, 10))
            #getting the new labels for the plot
            plot_label = category_labels.get(category, category)
            # creating a histogram of m34 values
            plt.hist(m34, bins=50, alpha=0.7, color=colors[i % len(colors)], label=plot_label)
            # setting the x and y labels of the histogram
            plt.xlabel(r'$m_{34}$ [GeV]', fontsize=14)
            plt.ylabel('Events', fontsize=14)
            # setting the title of the histogram including the category label
            plt.title(r'Invariant Mass Distribution of Sub-leading Lepton Pair - ' + plot_label, fontsize=16)
            # adding a legend to the plot
            plt.legend(loc='upper right', fontsize=12)
            # addding grid, minor ticks and applying tight layout to make the plots look nicer
            plt.grid(True, which='both', linestyle='--', linewidth=0.5)
            plt.minorticks_on()
            plt.tight_layout()
            #saving a histogram for the current category, so there will be a file for each category plot
            plt.savefig(f'/mydir/process_info/M34_distribution_{category}.png')
            plt.close()




if __name__ == "__main__":
    dictionary_agg = data_aggregation()
    plot_data(dictionary_agg)
    plot_m12_m34(dictionary_agg)
    plot_m34(dictionary_agg)
    plot_processing_time()