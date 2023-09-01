# coding=utf-8
import fnmatch
import os

import pandas as pd


class FileUtils:
    """
     pat: 过滤关键字
     path: 如果是文件的话则表示文件全路径
     dir: 仅仅表示为路径
    """

    @staticmethod
    def listdir(dir, pat):
        """
        example: FileUtils.listdir(img_dir, '*.tif')

        :param dir:
        :param pat: 过滤关键字
        :return:
        """
        dirs = os.listdir(dir)
        return fnmatch.filter(dirs, pat)

    @staticmethod
    def list_full_dir(dir, pat):
        names = FileUtils.listdir(dir, pat)
        result = []
        for n in names:
            result.append(os.path.join(dir, n))
        return result

    @staticmethod
    def file_rename(file_dir, old_name, new_name):
        os.rename(os.path.join(file_dir, old_name), os.path.join(file_dir, new_name))


class BaseUtils:

    @staticmethod
    def parallel_remove_none(x, y):
        new_x = []
        new_y = []

        for a, b in zip(x, y):
            if a is not None and b is not None:
                new_x.append(a)
                new_y.append(b)
        return new_x, new_y

    @staticmethod
    def save2csv(csv_file, *colum_arr):

        data_map = {}
        columns = []
        for i in range(len(colum_arr)):
            colum_data = colum_arr[i]
            colum_name = 'col_' + str(i)
            columns.append(colum_name)
            data_map[colum_name] = colum_data

        df = pd.DataFrame(data_map, columns=columns)
        df.to_csv(csv_file)

    @staticmethod
    def save2csv_columns(csv_file, column_names, colum_data_arr, drop_row_by_none=False, encoding='GB2312'):

        df = BaseUtils.build_pandas_df(column_names, colum_data_arr)

        if drop_row_by_none:
            df = df.dropna()

        df.to_csv(csv_file, encoding=encoding)

    @staticmethod
    def build_pandas_df(column_names, colum_data_arr):
        if len(column_names) != len(colum_data_arr):
            raise Exception('column name length != column data length!')
        data_map = {}
        for i in range(len(colum_data_arr)):
            colum_data = colum_data_arr[i]
            colum_name = column_names[i]
            data_map[colum_name] = colum_data

        df = pd.DataFrame(data_map, columns=column_names)
        return df


if __name__ == '__main__':

    xlss = FileUtils.listdir(r'D:\project\xlj\hj\hx1996\excel', '*.xls')
    for xls in xlss:
        FileUtils.file_rename(r'D:\project\xlj\hj\hx1996\excel', xls, xls.replace('aster_', 'raster_'))
