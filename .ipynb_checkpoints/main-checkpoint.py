from math import floor, ceil
from os.path import join

from rss.client import rssFromS3
from pandas import concat, IndexSlice, read_csv, read_json

well_bucket = 's3://sagemaker-gitc2021/poseidon/wells/'
well_file = 'poseidon_geoml_training_wells.json.gz'

well_df = read_json(
    path_or_buf=join(well_bucket, well_file),
    compression='gzip',
)

well_df.set_index(['well_id', 'twt'], inplace=True)


for well in wells.keys():
    print(f"{well}:\tIL Range: {il_ranges[well]}\t\tXL Range: {xl_ranges[well]}")