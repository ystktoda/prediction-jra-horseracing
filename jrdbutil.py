# coding: UTF-8
import urllib.request
import os.path
import codecs
import subprocess
import sqlite3
import glob
from enum import Enum
import platform

JRDB_USERID = 'xxxxxxxx'
JRDB_PASSWORD = 'xxxxxxxx'

class FixedByteSize(Enum):
    hjc = [8, 2, 7, 2, 7, 2, 7, 2, 7, 2, 7, 2, 7, 2, 7, 2, 7, 2, 7, 2, 7, 2, 7, 4, 8, 4, 8, 4, 8, 4, 8, 4, 8, 4, 8, 4, 8, 4, 8, 4, 8, 4, 8, 4, 8, 4, 8, 4, 8, 4, 8, 4, 8, 4, 8, 6, 8, 6, 8, 6, 8, 6, 9, 6, 9, 6, 9, 6, 9, 6, 9, 6, 9, 10, 1]
    sed = [8, 2, 8, 8, 36, 4, 1, 1, 1, 2, 2, 2, 3, 1, 1, 50, 2, 8, 2, 1, 4, 3, 12, 12, 6, 2, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 1, 1, 2, 1, 1, 1, 1, 5, 5, 5, 5, 12, 3, 3, 3, 24, 2, 6, 6, 6, 2, 2, 2, 2, 3, 3, 5, 5, 3, 3, 1, 1, 1, 7, 7, 5, 5, 2, 2, 1, 4]
    skb = [8, 2, 8, 8, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 40, 40, 40, 40, 3, 3, 3, 3, 3, 3, 11]
    srb = [8, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 64, 64, 64, 64, 2, 3, 3, 3, 3, 5, 5, 500, 8]
    cza = [5, 1, 8, 12, 30, 6, 1, 4, 8, 4, 40, 8, 3, 12, 12, 3, 3, 3, 12, 12, 3, 3, 20, 20, 8, 29]
    kza = [5, 1, 8, 12, 30, 6, 1, 4, 8, 4, 1, 5, 40, 8, 3, 12, 12, 3, 3, 3, 12, 12, 3, 3, 20, 20, 8, 23]
    kyi = [8, 2, 8, 36, 5, 5, 5, 5, 5, 5, 5, 1, 1, 1, 3, 5, 2, 5, 2, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 5, 5, 5, 1, 1, 4, 3, 2, 1, 2, 2, 1, 12, 3, 1, 12, 4, 16, 16, 16, 16, 16, 8, 8, 8, 8, 8, 1, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 5, 5, 1, 6, 5, 1, 5, 5, 5, 5, 1, 2, 2, 1, 2, 2, 1, 2, 2, 1, 1, 1, 3, 3, 1, 1, 40, 2, 2, 2, 2, 2, 2, 2, 2, 4, 4, 1, 8, 24, 3, 3, 3, 3, 3, 3, 4, 4, 2, 5, 3, 1, 1, 2, 2, 16, 2, 8, 3, 50, 1, 1, 398]
    kka = [8, 2, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 4, 3, 3, 4, 16]
    ukc = [8, 36, 1, 2, 2, 36, 36, 36, 8, 4, 4, 4, 40, 2, 40, 8, 1, 8, 4, 4, 6]
    oz = [8, 2, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5]
    ow = [8, 2, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 3]
    ou = [8, 2, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 8]
    kab = [6, 8, 1, 2, 4, 1, 2, 1, 1, 1, 3, 2, 2, 2, 2, 2, 2, 1, 1, 1, 3, 1, 2, 1, 4, 1, 1, 5, 7]
    bac = [8, 8, 4, 4, 1, 1, 1, 2, 2, 3, 1, 1, 50, 8, 2, 1, 1, 8, 18, 1, 5, 5, 5, 5, 5, 5, 5, 1, 1, 1, 1, 1, 1, 1, 1, 8, 1, 5]
    cyb = [8, 2, 2, 1, 2, 2, 2, 2, 2, 2, 2, 1, 1, 3, 3, 1, 1, 40, 8, 1, 8]
    cha = [8, 2, 2, 8, 1, 2, 1, 2, 1, 1, 3, 3, 3, 3, 3, 3, 3, 1, 1, 2, 2, 7]
    tyb = [8, 2, 5, 5, 5, 5, 5, 5, 5, 1, 1, 1, 5, 12, 3, 1, 2, 1, 6, 6, 4, 3, 3, 1, 1, 1, 29]


class JrdbUtil:
    def setup_basic_auth(self, base_uri, user, password):
        password_mgr = urllib.request.HTTPPasswordMgrWithDefaultRealm()
        password_mgr.add_password(
            realm=None,
            uri=base_uri,
            user=user,
            passwd=password)
        auth_handler = urllib.request.HTTPBasicAuthHandler(password_mgr)
        opener = urllib.request.build_opener(auth_handler)
        urllib.request.install_opener(opener)

    def download_file(self, url):
        filename = os.path.basename(url) or 'index.html'
        print('Downloading ... {0} as {1}'.format(url, filename))
        urllib.request.urlretrieve(url, './lha/' + filename)
        return './lha/' + filename

    def convert_fixed_to_csv(self, file_name, fixed_size):
        input_file = codecs.open(file_name, 'r', 'cp932')
        lines = input_file.readlines() # 1行毎にファイル終端まで全て読む(改行文字も含まれる)
        input_file.close()
        # lines: リスト。要素は1行の文字列データ

        csv_lines = []

        for line in lines:
            csv_line = []
            line_ends = line
            for size in fixed_size:
                c = ''
                for s in line_ends:
                    c += s
                    line_ends = line_ends[1:]
                    if len(c.encode('cp932')) >= size:
                        csv_line.append(c.strip())
                        break
            csv_lines.append(csv_line)

        output_file = codecs.open('csv' + file_name, 'w', 'utf-8')
        for csv in csv_lines:
            output_line = ''
            for s in csv:
                output_line += s + '|'
            output_file.write(output_line[:-1] + '\r\n')
        output_file.close()

    def uncompress_lha(self, file_name, path):
        try:
            subprocess.check_output(['lha',  'xfw=./' + path, file_name])
            return glob.glob('./' + path + '/*')
        except:
            print('Error.')

    def get_split_data_from_fixed(self, file_name, fixed_size):
        input_file = codecs.open(file_name, 'r', 'cp932')
        lines = input_file.readlines() # 1行毎にファイル終端まで全て読む(改行文字も含まれる)
        input_file.close()
        # lines: リスト。要素は1行の文字列データ

        csv_lines = []

        for line in lines:
            csv_line = []
            line_ends = line
            for size in fixed_size:
                c = ''
                for s in line_ends:
                    c += s
                    line_ends = line_ends[1:]
                    if len(c.encode('cp932')) >= size:
                        csv_line.append(c.strip())
                        break
            csv_lines.append(csv_line)

        return csv_lines

    def auth_download_umcompress(self, uri, path):
        self.setup_basic_auth(uri, JRDB_USERID, JRDB_PASSWORD)
        file_name = self.download_file(uri)
        files = self.uncompress_lha(file_name, path)
        return files

class SqliteUtil:
    def __init__(self):
        if os.name == 'nt':
            self.db_directory = 'C:\\tools\\sqlite3\\'
        elif platform.system() == 'Darwin':
            self.db_directory = '/Users/xxxx/db/'
        else:
            self.db_directory = '../db/'
        self.db_name = self.db_directory + 'jrdb.db'

    def insert_split_data(self, table_name, csv_lines):
        conn = sqlite3.connect(self.db_name)
        c = conn.cursor()
        values_str = ''
        for i in range(len(csv_lines[0])):
            values_str += '?,'
        insert_sql = 'insert or replace into ' + table_name + ' values (' + values_str[:-1] + ')'
        c.executemany(insert_sql, csv_lines)
        sql_file = self.db_directory + table_name + '.sql'
        if os.path.exists(sql_file):
            with open(sql_file, 'r') as f:
                for line in f:
                    c.execute(line)
        conn.commit()

if __name__ == "__main__":
    jrdbutil = JrdbUtil()
    sqliteutil = SqliteUtil()
