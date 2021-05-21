"""
Script to
1. read all light curves
2. extract their features
3. export the dataset to a csv file
"""

# =====================================================================
# Import
# =====================================================================

# Import internal modules
import os
import time
from threading import Thread, RLock
from typing import List

# Import 3rd party modules
import pandas as pd
import numpy as np

# Import local modules
from process_lightcurve import read_lightcurve

# =====================================================================
# Classes
# =====================================================================

# Instantiate RLock object
writing_csv_lock = RLock()

# Define child class of Thread


class SyncThread(Thread):
    """
    Child class of thread
    to read the light curves in threads
    """

    def __init__(self, lc_list, tce_table):
        """
        Function to create an instance of SyncThread class
        """
        Thread.__init__(self)
        self.lc_list = lc_list
        self.tce_table = tce_table

    def run(self):
        """
        Function to start the thread
        """
        # Track time to run the thread
        start_time = time.time()
        print(f"Starting thread")
        
        # create empty DataFrame
        df_thread = pd.DataFrame(columns=['time', 'flux', 'kepid', 'best_fit_period', 'av_training_set'])

        # for filepath in list of light curves for the thread instance
        for filepath in self.lc_list:
            
            # read light curve file, preprocess and convert into DataFrame
            lc_df = read_lightcurve(filepath, self.tce_table)

            # append DataFrame to `df`DataFrame
            df_thread = df_thread.append(lc_df)

        # compute thread runtime
        end_time = time.time()
        print(end_time - start_time)

        # Export dataframe to csv
        with writing_csv_lock:
            print(f"writing light curves to csv")

            # If file exists, don't repeat writing the header
            file_exists = os.path.isfile(os.path.join(
                "assets", "data", "Kepler", "thread_csv_all.csv"))
            if file_exists:
                df_thread.to_csv(os.path.join(
                    "assets", "data", "Kepler", "thread_csv_all.csv"), header=False, index=False, mode='a')
            else:
                df_thread.to_csv(os.path.join(
                    "assets", "data", "Kepler", "thread_csv_all.csv"), header=True, index=False, mode='a')


# =====================================================================
# Run
# =====================================================================

run_script: bool = True

# Check if file it exists
file_exists = os.path.isfile(os.path.join(
    "assets", "data", "Kepler", "thread_csv_all.csv"))
if file_exists:
    # For safety reasons, ask the user if he's sure to run the script
    delete_or_not: str = input("""
    The csv file already exists.
    This script will replace the current csv file.
    Do you still want to run it? (yes/no): 
    """).lower()
    if delete_or_not in ("yes", "y"):
        os.remove(os.path.join("assets", "data", "Kepler", "thread_csv_all.csv"))
    else:
        run_script = False

if run_script:

    # get kepler tce DataFrame
    csv_path = "/Users/derrickvanfrausum/BeCode_AI/git-repos/astro/core/assets/data/Kepler/q1_q17_dr24_tce_2021.05.17_09.38.15.csv"
    kepler_tce = pd.read_csv(csv_path, comment="#")

    # create empty lists
    file_list = []
    threads = []

    # get all light curve file paths in downloaded_lc folder
    folder_path = os.path.abspath('assets/data/Kepler/downloaded_lc')

    for path, dirs, files in os.walk(folder_path):
        for filename in files:
            filepath = os.path.join(path, filename)
            file_list.append(filepath)

    # slice light curves files list in 5 sets (1 for each thread)
    # splitted_list = np.array_split(file_list, 5)
    splitted_list = np.array_split(file_list[:10], 5)

    # create thread for each set
    for array in splitted_list:
        thread = SyncThread(list(array), kepler_tce)
        
        # Add thread to thread list
        threads.append(thread)
    
    # Launch threads
    for thread in threads:
        thread.start()

    # Wait for all threads to complete
    for t in threads:
        t.join()
    print("Exiting Main Thread")