from osgeo import gdal
import numpy as np
from sklearn.metrics import confusion_matrix

'''
计算两组栅格的转移矩阵（混淆矩阵）
'''

# 打开两个栅格数据
ds1 = gdal.Open(r"D:\research\npp研究\基础数据_对齐_贵州\09_clcd土地利用_贵州\CLCD_v01_2000.tif")
ds2 = gdal.Open(r"D:\research\npp研究\基础数据_对齐_贵州\09_clcd土地利用_贵州\CLCD_v01_2020.tif")

band1 = ds1.GetRasterBand(1)
data1 = band1.ReadAsArray().flatten()

band2 = ds2.GetRasterBand(1)
data2 = band2.ReadAsArray().flatten()

# 获取nodata值
nodata_1 = band1.GetNoDataValue()
nodata_2 = band2.GetNoDataValue()

# 将nodata值替换为新的类别值
data1 = np.where(data1 == nodata_1, 255, data1)
data2 = np.where(data2 == nodata_2, 255, data2)

# 计算混淆矩阵
cm = confusion_matrix(data1, data2)

# 显示混淆矩阵
print("Confusion matrix:\n", cm)
