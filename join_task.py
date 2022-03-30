import os
from typing import Union, List, Tuple
import pandas as pd
import numpy as np


class CSVFile:
    """
    A class to represent a csv file.
    ...

    Attributes:
        path_or_file : Union[pd.Dataframe, str]
            Given data to initialization class object.
            User can give path to csv file (as string)
            or directly pd.DataFrame object.

        csv_file : pd.Dataframe
            Dataframe with data from csv file
    
    Methods:
        _is_right_path():
            Checks if file exists in the given path

    """
    def __init__(self, path_or_file: Union[pd.DataFrame, str]):
        """
        Constructs all the necessery attributes for the CSVFile
        object and checks data type of given attributes.
        ...

        Args:
            path_or_file : Union[pd.DataFrame, str]
                Given data to initialization class object.
                User can give path to csv file (as string)
                or directly pd.Dataframe object.

        Raises:
            TypeError
                If given self.path_or_file is other data type
                string or pd.DataFrame
        """
        self.path_or_file = path_or_file
        if isinstance(self.path_or_file, str):
            self._is_right_path()
            self.csv_file = pd.read_csv(self.path_or_file)
        elif isinstance(self.path_or_file, pd.DataFrame):
            self.csv_file = self.path_or_file
        else:
            raise TypeError('You can give only string with path or pd.DataFrame as instance attribute ')

    def __str__(self):
        """
        Defining the way of printing CSVFile object.

        """
        return str(self.csv_file)

    def _is_right_path(self):
        """
        Checks if file exists in the given path

        """
        try:
            pd.read_csv(self.path_or_file)
        except FileNotFoundError:
            print('File not found on this path')


def search_values_in_common(left_file: pd.DataFrame, right_file: pd.DataFrame, column_name: str) -> List:
    """
    Searches values in column with given name, which 
    two dataset have in common.
    ...

    Args:
        left_file : pd.DataFrame
            first dataset

        right_file : pd.DataFrame
            second dataset

        column_name : str
            name of the column, which
            will be used for joining

    Returns:
        common_values : List
            list of the elements in common
    """
    common_values = []
    for i, row in left_file.iterrows():
        if row[column_name] in right_file[column_name].unique():
            common_values.append(row[column_name])
    return common_values


def insert_row_between_others(df: pd.DataFrame, row_number: int, row_value: List) -> pd.DataFrame:
    """
    Inserts new row in index between
    exist rows.
    ...

    Args:
        df : pd.DataFrame
            dataset
        
        row_number : int
            index of new row in dataset
        
        row_value : List
            list of elems, which will be
            in the new row

    Returns:
        df : pd.DataFrame
            dataset with added row
    """
    begin_upper = 0
    end_upper = row_number
    begin_lower = row_number
    end_lower = df.shape[0]

    upper_half = [*range(begin_upper, end_upper, 1)]
    lower_half = [*range(begin_lower, end_lower, 1)]
    lower_half = [x.__add__(1) for x in lower_half]

    idx = upper_half + lower_half
    df.index = idx
    df.loc[row_number] = row_value
    return df.sort_index()


def inner_join(left_file: pd.DataFrame, right_file: pd.DataFrame, column_name: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Function with implementation of join inner.
    ...

    Args:
        left_file : pd.DataFrame
            first dataset

        right_file : pd.DataFrame
            second dataset

        column_name : str
            name of the column, which
            will be used for joining

    Returns:
        proper_file, elems_from_opposite_file : Tuple[pd.DataFrame, pd.DataFrame]
            two dataset prepared from initial dataset for joining
    """
    proper_file = pd.DataFrame()
    common_values = search_values_in_common(left_file, right_file, column_name)

    for i, row in left_file.iterrows():
        if row[column_name] in common_values:
            proper_file = proper_file.append(row, ignore_index=True)

    elems_from_opposite_file = pd.DataFrame()
    for i, row in right_file.iterrows():
        if row[column_name] in common_values:
            elems_from_opposite_file = elems_from_opposite_file.append(row, ignore_index=True)

    return proper_file, elems_from_opposite_file


def left_or_right_join(proper_file: pd.DataFrame, opposite_file: pd.DataFrame, column_name: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Function with implementation of join left 
    and join right.
    ...

    Args:
        proper_file : pd.DataFrame
            first dataset

        opposite_file : pd.DataFrame
            second dataset

        column_name : str
            name of the column, which
            will be used for joining

    Returns:
        proper_file, elems_from_opposite_file : Tuple[pd.DataFrame, pd.DataFrame]
            two dataset prepared from initial dataset for joining
    """
    common_values = search_values_in_common(proper_file, opposite_file, column_name)
    proper_file = proper_file.sort_values(by=[column_name], ignore_index=True)
    opposite_file = opposite_file.sort_values(by=[column_name], ignore_index=True)

    elems_from_opposite_file = pd.DataFrame()
    elems_not_in_opposite_file_ids = []
    for i, row in proper_file.iterrows():
        if row[column_name] not in common_values:
            elems_not_in_opposite_file_ids.append(i)

    for i, row in opposite_file.iterrows():
        if row[column_name] in common_values:
            elems_from_opposite_file = elems_from_opposite_file.append(row, ignore_index=True)

    for i in elems_not_in_opposite_file_ids:
        elems_from_opposite_file = insert_row_between_others(elems_from_opposite_file, i,
                                                             [np.nan for _ in range(len(opposite_file.columns))])

    return proper_file, elems_from_opposite_file


def final_joining(proper_file: pd.DataFrame, elems_from_opposite_file: pd.DataFrame, column_name: str) -> pd.DataFrame:
    """
    Joins previously prepared datasets
    ...

    Args:
        proper_file : pd.DataFrame
            first dataset

        opposite_file : pd.DataFrame
            second dataset

        column_name : str
            name of the column, which
            will be used for joining

    Returns:
        proper_file : pd.DataFrame
            dataset, which is joining result
    """
    elems_from_opposite_file = elems_from_opposite_file[[col for col in elems_from_opposite_file.columns if col != column_name]]
    proper_file = proper_file.sort_values(by=[column_name])
    cnt = len(proper_file.columns.values)

    for col, data in elems_from_opposite_file.items():
        proper_file.insert(cnt, col, data)
        cnt += 1

    return proper_file


def join(left_file_path: str, right_file_path: str, column_name: str, join_type: str):
    """
    Main function that:
    - gets initial values and
      checks their corectness
    - selects appropriate join
      type
    - finishes joining process
    - prints joining result and
      save it to csv file
    ...

    Args:
        left_file_path : str
            path to first csv file

        right_file_path : str
            path to second csv file

        column_name : str
            name of the column, which
            will be used for joining

        join_type : (str)
            one of 3 possible join type: 
            inner, left or right

    Raises:
        ValueError
            if column with given column
            name doesn't exist

        ValueError
            if given join type is other 
            than inner, left or rigth
    """
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
            proper_file, taken_from_opposite_file = inner_join(left_file.csv_file, right_file.csv_file, column_name)
        elif join_type.casefold() == "left".casefold():
            proper_file, taken_from_opposite_file = left_or_right_join(left_file.csv_file, right_file.csv_file,
                                                                       column_name)
        elif join_type.casefold() == "right".casefold():
            proper_file, taken_from_opposite_file = left_or_right_join(right_file.csv_file, left_file.csv_file,
                                                                       column_name)
        else:
            raise ValueError('There are 3 types of joining: inner, left, right')
    else:
        raise ValueError('No column with given column_name')

    output = final_joining(proper_file, taken_from_opposite_file, column_name)
    print(output)
    output.to_csv(os.path.dirname(os.path.abspath(left_file_path)) + '//joining_result.csv', index=False)
