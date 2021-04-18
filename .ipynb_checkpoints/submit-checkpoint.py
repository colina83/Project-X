from pandas import read_json
import numpy as np


well_bucket = 's3://sagemaker-gitc2021/poseidon/wells/'
well_file = 'poseidon_geoml_testing_wells_blank.json.gz'

well_df = read_json(
    path_or_buf=well_bucket + well_file,
    compression='gzip',
)

well_df.set_index(['well_id', 'twt'], inplace=True)

well_df['rhob'] = np.random.uniform(2,3, size = len(well_df))
well_df['p_impedance'] = np.random.uniform(24345,35678, size = len(well_df))
well_df['s_impedance'] = np.random.uniform(14345,23456, size = len(well_df))

my_result = well_df

bucket = 's3://sagemaker-gitc2021/poseidon/wells/submissions/intermediate/'
file_name = 'MyTeam_Intermediate_Results_20210416.json.gz'
# Making sure extension is in the file name.
if not file_name.lower().endswith('.json.gz'):
    file_name += '.json.gz'
my_result.reset_index(inplace=True)
my_result.to_json(
    path_or_buf=bucket + file_name,
    double_precision=4,
    compression='gzip'
)