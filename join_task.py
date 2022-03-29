from typing import Union
import pandas as pd
import numpy as np

class CSVFile():
    def __init__(self, path_or_file : Union[pd.DataFrame, str]):
        self.path_or_file = path_or_file
        if isinstance(self.path_or_file, str):
            self._is_right_path()
            self.csv_file = pd.read_csv(self.path_or_file)
        elif isinstance(self.path_or_file, pd.DataFrame):
            self.csv_file = self.path_or_file
        else:
            raise TypeError('You can give only string with path or DataFrame as instance attribute ')

    def __str__(self):
        return str(self.csv_file)

    def _is_right_path(self):
        try:
            pd.read_csv(self.path_or_file)
        except FileNotFoundError:
            print('File not found on this path')

def joing_function(output: pd.DataFrame, taken_from_right_file: pd.DataFrame, column_name: str):
    taken_from_right_file = taken_from_right_file[[col for col in taken_from_right_file.columns if col != column_name]]
    output = output.sort_values(by=[column_name])
    cnt = len(output.columns.values)
    for col, data in taken_from_right_file.items():
        output.insert(cnt, col, data)
        cnt += 1
    output_csv = CSVFile(output)
    print(output_csv)

def common_val_search(left_file : pd.DataFrame, right_file : pd.DataFrame, column_name : str):
    common_values = []
    for i, row in left_file.iterrows():
        if row[column_name] in right_file[column_name].unique():
            common_values.append(row[column_name])
    return common_values

def inner_join(left_file : pd.DataFrame, right_file : pd.DataFrame, column_name : str):
    output = pd.DataFrame()
    common_values = common_val_search(left_file, right_file, column_name)
    print(common_values)
    for i, row in left_file.iterrows():
        if row[column_name] in common_values:
            output = output.append(row, ignore_index=True)
    taken_from_right_file = pd.DataFrame()
    for i, row in right_file.iterrows():
        if row[column_name] in common_values:
            taken_from_right_file = taken_from_right_file.append(row, ignore_index=True)
    joing_function(output,taken_from_right_file, column_name)



def insert_row(df, row_number, row_value):
    start_upper = 0
    end_upper = row_number
    start_lower = row_number
    end_lower = df.shape[0]
    upper_half = [*range(start_upper, end_upper, 1)]
    lower_half = [*range(start_lower, end_lower, 1)]
    lower_half = [x.__add__(1) for x in lower_half]
    index_ = upper_half + lower_half
    df.index = index_
    df.loc[row_number] = row_value
    df = df.sort_index()
    return df

def left_or_right_join(left_file : pd.DataFrame, right_file : pd.DataFrame, column_name : str, join_type : str):
    common_values = common_val_search(left_file, right_file, column_name)
    left_file = left_file.sort_values(by=[column_name],ignore_index=True)
    right_file = right_file.sort_values(by=[column_name],ignore_index=True)
    if join_type == 'left'.casefold():
        output = left_file[:]
        opposite_file = right_file[:]
    else:
        output = right_file[:]
        opposite_file = left_file[:]
    taken_from_opposite_file_and_rest = pd.DataFrame()
    not_in_opposite_file_ids = []
    for i, row in output.iterrows():
        if row[column_name] not in common_values:
            not_in_opposite_file_ids.append(i)
    for i, row in opposite_file.iterrows():
        if row[column_name] in common_values:
            taken_from_opposite_file_and_rest = taken_from_opposite_file_and_rest.append(row, ignore_index=True)
    for i in not_in_opposite_file_ids:
            taken_from_opposite_file_and_rest = insert_row(taken_from_opposite_file_and_rest, i, [np.NaN for j in range(len(opposite_file.columns))])
    joing_function(output, taken_from_opposite_file_and_rest, column_name)








def join_method(left_file_path: str, right_file_path: str, column_name: str, join_type: str):
    left_file = CSVFile(left_file_path)
    right_file = CSVFile(right_file_path)
    column_name = column_name.upper()
    [col_name.upper() for col_name in left_file.csv_file.columns]
    [col_name.upper() for col_name in right_file.csv_file.columns]
    if column_name.casefold() in [col_name.casefold() for col_name in
                                  left_file.csv_file.columns] and column_name.casefold() in [col_name.casefold() for
                                                                                             col_name in
                                                                                             right_file.csv_file.columns]:
        if join_type.casefold() == "inner".casefold():
            inner_join(left_file.csv_file, right_file.csv_file, column_name)
        elif join_type.casefold() == "left".casefold() or join_type.casefold() == "right".casefold():
            left_or_right_join(left_file.csv_file, right_file.csv_file, column_name, join_type)
        else:
            raise ValueError('There are 3 types of joining: inner, left, right')
    else:
        raise ValueError('No column with given column_name')


if __name__ == "__main__":
    join_method('D:\\pycharm\\join_task_test\\product.csv', 'D:\\pycharm\\join_task_test\\customer.csv', 'Product_id',
                'right')
