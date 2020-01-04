# coding: UTF-8

class LogSingleton:
    _instance = None
    log_file_name = None

    def __new__(cls, *args, **keys):
        if cls._instance is None:
            cls._instance = super().__new__(cls)

        return cls._instance

    def set_log_file_name(self, log_file_name):
        self.log_file_name = log_file_name

    def write_log(self, log_str):
        with open(self.log_file_name, 'a', encoding='utf-8') as f:
            f.write(log_str + '\n')

    def write_log_lines(self, log_lines):
        with open(self.log_file_name, 'a', encoding='utf-8') as f:
            f.writelines(log_lines)

    def write_dataframe(self, df):
        df.to_csv(self.log_file_name, sep='\t', mode='a')
