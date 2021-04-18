from math import floor, ceil
from os.path import join

from rss.client import rssFromS3
from pandas import concat, IndexSlice, read_csv, read_json
from itertools import product
import numpy as np




def well_seismic_data():
    """ 
    Returns a dictionary with all the well information
    """
    well_bucket = 's3://sagemaker-gitc2021/poseidon/wells/'
    well_file = 'poseidon_geoml_training_wells.json.gz'
    well_df = read_json(
    path_or_buf=join(well_bucket, well_file),
    compression='gzip')
    well_df.set_index(['well_id', 'twt'], inplace=True)
    
    # This code will provide an easy to access query from the seismic data
    well_names = well_df.index.levels[0].to_list()
    num_wells = len(well_names)
    wells = {}
    il_ranges = {}
    xl_ranges = {}
    for well_name in well_names:
        well = well_df.loc[well_name]
        well_ils_xls = well[['inline', 'xline']]
        il_xl_min = well_ils_xls.min()
        il_xl_max = well_ils_xls.max()

        wells[well_name] = well
        il_ranges[well_name] = floor(il_xl_min[0]), ceil(il_xl_max[0])
        xl_ranges[well_name] = floor(il_xl_min[1]), ceil(il_xl_max[1])
    
    """
    Returns near,mid,far angle stacks and interval velocities 
    """
    seismic_bucket = r's3://sagemaker-gitc2021/poseidon/seismic/rss/'
    
    seismic_files = [
    'psdn11_TbsdmF_Near_Nov_11_32bit',  # near angle stack (AVO)
    'psdn11_TbsdmF_Mid_Nov_11_32bit',  # mid angle stack (AVO)
    'psdn11_TbsdmF_Far_Nov_11_32bit',  # far angle stack (AVO)
    'Final_PSDM_intVel_gridded_D2T_10ms',  # PSDM interval velocities in time
    ]
    
    rss_handles = []
    for seismic_file in seismic_files:
        current_rss = rssFromS3(filename=seismic_bucket + seismic_file, client_kwargs={})
        rss_handles.append(current_rss)
    
    #Integrating seismic data with well data
    print(rss_handles)
    seismic_names = ('near', 'mid', 'far', 'velocity')
    well_seismic = {}
          
    for well in wells.keys():
        il_min, il_max = il_ranges[well]
        xl_min, xl_max = xl_ranges[well]
        
        ils = range(il_min, il_max + 1, 1)  # inline increment is 1
        xls = range(xl_min, xl_max + 1, 1)  # xline increment is 1
        
        ilxl_pairs = list(product(ils, xls))
        
        cube_data = {}
       
        # In rss, for minicube extraction we query trace by trace.
        for rss_handle, seismic_name in zip(rss_handles, seismic_names):
            tmp = []
            
            for trace_ilxl in ilxl_pairs:
                tmp.append(rss_handle.trace(*trace_ilxl)[0])  # rss returns values and a live mask, we don't need the mask now
                
                # Traces aren't in a "cube" shape, they come sequential. So we reshape!
            cube_data[seismic_name] = np.asarray(tmp).reshape(len(ils), len(xls), -1)
            
        well_seismic[well] = cube_data
    
    
if __name__ == '__main__':
    well_seismic_data()