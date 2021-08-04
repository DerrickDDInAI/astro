"""
Helper functions to preprocess light curves
"""

# =====================================================================
# Import
# =====================================================================

# Import internal modules
import os

# from typing import List

# Import 3rd party modules
import pandas as pd
import numpy as np
import lightkurve as lk

# Import local modules


# =====================================================================
# Functions
# =====================================================================

def download_lightcurve(target_name: str) -> lk.lightcurve.TessLightCurve:
    """
    Function to download the light curve of a target.
    In case the search returns multiple data products:
    it downloads the data with biggest exposure time and most recently observed
    """
    # search for light curves
    search_result = lk.search_lightcurve(target_name)

    try:
        # get max cadence
        exptime_max = np.max(search_result.exptime)

        # get data products with biggest cadence
        exptime_max_result = search_result[search_result.exptime == exptime_max]

        # get index of most recent observation
        most_recent_idx = np.argmax(exptime_max_result.year)

        # get data product
        return exptime_max_result[most_recent_idx].download()
    except:
        return None

# def get_lightcurve_list(target_list: list) -> pd.DataFrame:
#     """
#     Function to download lightcurves from a list of targets
#     """
#     # create empty list
#     lc_list: list = []

#     for target in target_list:

#         # download lightcurve if found
#         lc = download_lightcurve(target)

#         # if lightcurve downloaded
#         if lc:

#             # preprocess lc
#             pipeline_lightcurve

#             # append to list
#             lc_list.append(lc)
    
#     return lc_df

def pipeline_lightcurve(lc):
    """
    function to preprocess 
    * a light curve
    """
    # remove missing values & outliers
    lc = lc.remove_nans().remove_outliers()
    
    # flatten lc
    flat_lc = lc.flatten()

    # compute best fit period
    periodogram = flat_lc.to_periodogram(method="bls", period=np.arange(0.5, 10, 0.001))
    best_fit_period = periodogram.period_at_max_power

    # fold lc according to best fit period
    folded_lc = flat_lc.fold(period=best_fit_period)

    # return a tuple
    return (folded_lc, best_fit_period)


def read_lightcurve(filepath, tce_table) -> pd.DataFrame:
    """
    Function to read light curve stored locally
    and preprocess it.
    """
    # read light curve file
    lc = lk.read(filepath)

    # preprocess light curve
    processed_lc, best_fit_period = pipeline_lightcurve(lc)

    # Convert into DataFrame
    lc_df = processed_lc.to_pandas().reset_index()
    lc_df = lc_df[["time", "flux"]]

    # remove any missing values that would remain (normally removed during preprocessing)
    lc_df.dropna(inplace=True)

    # add target ID to dataframe
    lc_df["kepid"] = processed_lc.meta['TARGETID']

    # add best fit period to dataframe
    lc_df["best_fit_period"] = best_fit_period

    # merge dataframe with tce dataset (mainly to add classification variable)
    lc_df = lc_df.merge(tce_table[["kepid", "av_training_set"]], on="kepid")

    return lc_df