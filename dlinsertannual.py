# coding: UTF-8
import sys
import os
import datetime
import jrdbutil

JRDB_URL = 'http://www.jrdb.com/member/data/'

def dl_insert(uri, year):
    jrdb_util = jrdbutil.JrdbUtil()
    path = 'jrdbyear' + datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    os.mkdir(path)
    try:
        files = jrdb_util.auth_download_umcompress(uri, path)
        sqlite_util = jrdbutil.SqliteUtil()
        for file in files:
            table_name = os.path.basename(file).split(year[-2:])[0].lower()
            byte_size = jrdbutil.FixedByteSize[table_name].value
            sqlite_util.insert_split_data(table_name, jrdb_util.get_split_data_from_fixed(file, byte_size))
            os.remove(file)
            print(file + ' done.')
    finally:
        os.rmdir(path)

def hjc(year):
    uri = JRDB_URL + 'Hjc/HJC_' + year + '.lzh'
    dl_insert(uri, year)

def sed(year):
    uri = JRDB_URL + 'Sed/SED_' + year + '.lzh'
    dl_insert(uri, year)

def skb(year):
    uri = JRDB_URL + 'Skb/SKB_' + year + '.lzh'
    dl_insert(uri, year)

def srb(year):
    print('sedに同梱')

def cza(year):
    print('最新に全てが含まれている')

def kza(year):
    print('最新に全てが含まれている')

def kyi(year):
    uri = JRDB_URL + 'Kyi/KYI_' + year + '.lzh'
    dl_insert(uri, year)

def kka(year):
    print('年度別には存在しない')

def ukc(year):
    uri = JRDB_URL + 'Ukc/UKC_' + year + '.lzh'
    dl_insert(uri, year)

def oz(year):
    print('年度別には存在しない')

def ow(year):
    print('年度別には存在しない')

def ou(year):
    print('年度別には存在しない')

def kab(year):
    uri = JRDB_URL + 'Kab/KAB_' + year + '.lzh'
    dl_insert(uri, year)

def bac(year):
    uri = JRDB_URL + 'Bac/BAC_' + year + '.lzh'
    dl_insert(uri, year)

def cyb(year):
    uri = JRDB_URL + 'Cyb/CYB_' + year + '.lzh'
    dl_insert(uri, year)

def cha(year):
    print('年度別には存在しない')

def tyb(year):
    uri = JRDB_URL + 'Tyb/TYB_' + year + '.lzh'
    dl_insert(uri, year)

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print('引数の数が不正です。正しくは、第1引数：JRDBデータ種別  第2引数：年度  です。')
        sys.exit()

    data_type = sys.argv[1]
    year = sys.argv[2]

    if data_type == 'hjc':
        hjc(year)
    elif data_type == 'sed':
        sed(year)
    elif data_type == 'skb':
        skb(year)
    elif data_type == 'srb':
        srb(year)
    elif data_type == 'cza':
        cza(year)
    elif data_type == 'kza':
        kza(year)
    elif data_type == 'kyi':
        kyi(year)
    elif data_type == 'kka':
        kka(year)
    elif data_type == 'ukc':
        ukc(year)
    elif data_type == 'oz':
        oz(year)
    elif data_type == 'ow':
        ow(year)
    elif data_type == 'ou':
        ou(year)
    elif data_type == 'kab':
        kab(year)
    elif data_type == 'bac':
        bac(year)
    elif data_type == 'cyb':
        cyb(year)
    elif data_type == 'cha':
        cha(year)
    elif data_type == 'tyb':
        tyb(year)
    else:
        print('JRDBデータ種別が不正です。')

