import fnmatch
import os
import geopandas as gpd
from rasterstats import point_query
from my_utils import BaseUtils
from tqdm import tqdm

"""
从各个要素的多年平均数据中按采样点提取值，然后组合成一个csv（需要保证具有相同的投影坐标系）


d:/data
----|
    降水.tif
    温度.tif
    湿度.tif
    日照数.tif
    sample_points_pro.shp

"""

base_dir = r'd:/data'

input_dir = base_dir
point_data = gpd.read_file(r'd:/data/sample_points_proj.shp')
output_file = base_dir + '/因子采样数据.csv'


def process(tif_name):
    tif_file = os.path.join(input_dir, tif_name)
    return point_query(point_data['geometry'], tif_file, interpolate='nearest')


files = os.listdir(input_dir)
tif_file_names = fnmatch.filter(files, '*.tif')

columns = []
column_data = []
for tif_name in tqdm(tif_file_names):
    y = process(tif_name)
    columns.append(tif_name.replace('.tif', ''))
    column_data.append(y)

BaseUtils.save2csv_columns(output_file, columns, column_data, drop_row_by_none=True)
