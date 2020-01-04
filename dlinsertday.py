# coding: UTF-8
import sys
import os
import datetime
import jrdbutil
import urllib

JRDB_URL = 'http://www.jrdb.com/member/data/'
JRDB_TYOKUZEN_URL = 'http://www.jrdb.com/member/'

def dl_insert(uri, yymmdd):
    jrdb_util = jrdbutil.JrdbUtil()
    path = 'jrdb' + datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    os.mkdir(path)
    try:
        files = jrdb_util.auth_download_umcompress(uri, path)
        sqlite_util = jrdbutil.SqliteUtil()
        for file in files:
            table_name = os.path.basename(file).split(yymmdd)[0].lower()
            byte_size = jrdbutil.FixedByteSize[table_name].value
            sqlite_util.insert_split_data(table_name, jrdb_util.get_split_data_from_fixed(file, byte_size))
            os.remove(file)
            print(file + ' done.')
    except urllib.error.HTTPError:
        pass
    finally:
        os.rmdir(path)

def hjc(yymmdd):
    uri = JRDB_URL + 'Hjc/HJC' + yymmdd + '.lzh'
    dl_insert(uri, yymmdd)

def sed(yymmdd):
    uri = JRDB_URL + 'Sed/SED' + yymmdd + '.lzh'
    dl_insert(uri, yymmdd)

def skb(yymmdd):
    uri = JRDB_URL + 'Skb/SKB' + yymmdd + '.lzh'
    dl_insert(uri, yymmdd)

def srb(yymmdd):
    print('sedに同梱')

def cza(yymmdd):
    uri = JRDB_URL + 'Cs/CZA' + yymmdd + '.lzh'
    dl_insert(uri, yymmdd)

def kza(yymmdd):
    uri = JRDB_URL + 'Ks/KZA' + yymmdd + '.lzh'
    dl_insert(uri, yymmdd)

def kyi(yymmdd):
    uri = JRDB_URL + 'Kyi/KYI' + yymmdd + '.lzh'
    dl_insert(uri, yymmdd)

def kka(yymmdd):
    uri = JRDB_URL + 'Kka/KKA' + yymmdd + '.lzh'
    dl_insert(uri, yymmdd)

def ukc(yymmdd):
    uri = JRDB_URL + 'Ukc/UKC' + yymmdd + '.lzh'
    dl_insert(uri, yymmdd)

def oz(yymmdd):
    uri = JRDB_URL + 'Oz/OZ' + yymmdd + '.lzh'
    dl_insert(uri, yymmdd)

def ow(yymmdd):
    uri = JRDB_URL + 'Oz/OW' + yymmdd + '.lzh'
    dl_insert(uri, yymmdd)

def ou(yymmdd):
    uri = JRDB_URL + 'Ou/OU' + yymmdd + '.lzh'
    dl_insert(uri, yymmdd)

def kab(yymmdd):
    uri = JRDB_URL + 'Kab/KAB' + yymmdd + '.lzh'
    dl_insert(uri, yymmdd)

def bac(yymmdd):
    uri = JRDB_URL + 'Bac/BAC' + yymmdd + '.lzh'
    dl_insert(uri, yymmdd)

def cyb(yymmdd):
    uri = JRDB_URL + 'Cyb/CYB' + yymmdd + '.lzh'
    dl_insert(uri, yymmdd)

def cha(yymmdd):
    uri = JRDB_URL + 'Cha/CHA' + yymmdd + '.lzh'
    dl_insert(uri, yymmdd)

def tyb(yymmdd):
    uri = JRDB_URL + 'Tyb/TYB' + yymmdd + '.lzh'
    dl_insert(uri, yymmdd)

def tyokuzen(yymmdd):
    uri = JRDB_TYOKUZEN_URL + '20' + yymmdd + '/tyokuzen/TYB' + yymmdd + '.lzh'
    dl_insert(uri, yymmdd)

if __name__ == "__main__":
    if len(sys.argv) not in [2, 3]:
        print('引数の数が不正です。正しくは、第1引数：JRDBデータ種別  第2引数（任意）：日付（YYMMDD）  です。')
        sys.exit()

    data_type = sys.argv[1]
    yymmdd = ''
    if len(sys.argv) == 3:
        yymmdd = sys.argv[2]
    else:
        yymmdd = datetime.date.today().strftime("%y%m%d")

    if data_type == 'hjc':
        hjc(yymmdd)
    elif data_type == 'sed':
        sed(yymmdd)
    elif data_type == 'skb':
        skb(yymmdd)
    elif data_type == 'srb':
        srb(yymmdd)
    elif data_type == 'cza':
        cza(yymmdd)
    elif data_type == 'kza':
        kza(yymmdd)
    elif data_type == 'kyi':
        kyi(yymmdd)
    elif data_type == 'kka':
        kka(yymmdd)
    elif data_type == 'ukc':
        ukc(yymmdd)
    elif data_type == 'oz':
        oz(yymmdd)
    elif data_type == 'ow':
        ow(yymmdd)
    elif data_type == 'ou':
        ou(yymmdd)
    elif data_type == 'kab':
        kab(yymmdd)
    elif data_type == 'bac':
        bac(yymmdd)
    elif data_type == 'cyb':
        cyb(yymmdd)
    elif data_type == 'cha':
        cha(yymmdd)
    elif data_type == 'tyb':
        tyb(yymmdd)
    elif data_type == 'tyokuzen':
        tyokuzen(yymmdd)
    else:
        print('JRDBデータ種別が不正です。')

