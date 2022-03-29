import pandas as pd

class CSVFile():
    def __init__(self, path_or_file):
        self.path_or_file = path_or_file
        if isinstance(self.path_or_file, str):
            self._is_right_path()
            self.csv_file = pd.read_csv(self.path_or_file)
        elif isinstance(self.path_or_file, pd.DataFrame):
            self.csv_file = self.path_or_file
        else:
            raise TypeError

    def __str__(self):
        return str(self.csv_file)

    def _is_right_path(self):
        try:
            pd.read_csv(self.path_or_file)
        except FileNotFoundError:
            print('File not found on this path')

if __name__ == "__main__":
    cs = CSVFile('D:\\pycharm\\join_task_test\\product.csv')
    cs1 = CSVFile('D:\\pycharm\\join_task_test\\customer.csv')
    print(cs1)




