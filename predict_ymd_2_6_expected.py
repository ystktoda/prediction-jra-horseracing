# coding: UTF-8
import train
from sklearn.externals import joblib
import os.path
import requests
import sys
import horsetable as ht
import json
import time
from datetime import datetime

SLACK_URL = 'xxxx'
PREDICT_FILE = '/Users/xxxx/horserace/predict_ymd_2_6_expected.txt'
BET_FILE_DIRECTORY = '/Volumes/[C] Boot Camp/ProgramData/SimplePat2/kaime/'
WIN_BET_MONEY = 10
QUINELLA_BET_MONEY = 5

def post(message):
    values = {
        'text': message,
        'channel': 'horserace',
        'username': 'bot'
    }
    requests.post(SLACK_URL, data=json.dumps(values))

def predict(t_util_2, t_util_6, horse_table_list):
    new_predict_list = []
    old_predict_list = []
    if os.path.exists(PREDICT_FILE):
        with open(PREDICT_FILE, 'r', encoding='utf-8') as f:
            for line in f:
                old_predict_list.append(line.strip())

    horseno_list_by_race = []
    for horse_table in horse_table_list:
        horse_table.predict_2_6_expected(t_util_2.clf, t_util_2.sc, t_util_6.clf, t_util_6.sc)
        win_horseno_list = []
        if horse_table.expected is not None:
            for ex in horse_table.expected:
                win_horseno_list.append(ex[0])

        if len(win_horseno_list) > 1:
            horseno_name = ''
            for horseno in win_horseno_list:
                horseno_name += horseno + ':' + horse_table.get_horse_name(horseno) + ', '
            new_predict_list.append(horse_table.race.program.get_race_info() + ' 流し ' + horseno_name[:-2])
        else:
            new_predict_list.append(horse_table.race.program.get_race_info() + ' 流し推奨馬なし')

        horseno_list_by_race.append(win_horseno_list)

    old_set = set(old_predict_list)
    new_set = set(new_predict_list)
    unmatched_list = list(new_set.difference(old_set))

    message = ''
    for s in unmatched_list:
        message += s + '\n'

    if message != '':
        message = message
        post(message)

    with open(PREDICT_FILE, 'w', encoding='utf-8') as f:
        for text in new_predict_list:
            f.write(text + '\n')

    return horseno_list_by_race


if __name__ == '__main__':
    args = sys.argv

    if len(args) <6:
        print('引数を設定してください。')
        sys.exit()

    print('引数： ' + ', '.join(args[1:]))

    ymd = args[1]
    clf_kind_2 = args[2]
    pkl_file_prefix_2 = args[3]
    clf_kind_6 = args[4]
    pkl_file_prefix_6 = args[5]

    start = time.time()

    t_util_2 = train.TrainUtil(clf_kind_2)
    t_util_6 = train.TrainUtil(clf_kind_6)

    clf_pkl_2 = train.PKL_FILE_DIR + pkl_file_prefix_2 + train.CLF_PKL_FILE_NAME
    sc_pkl_2 = train.PKL_FILE_DIR + pkl_file_prefix_2 + train.SC_PKL_FILE_NAME
    clf_pkl_6 = train.PKL_FILE_DIR + pkl_file_prefix_6 + train.CLF_PKL_FILE_NAME
    sc_pkl_6 = train.PKL_FILE_DIR + pkl_file_prefix_6 + train.SC_PKL_FILE_NAME
    if os.path.isfile(clf_pkl_2) and os.path.isfile(sc_pkl_2) and os.path.isfile(clf_pkl_6) and os.path.isfile(sc_pkl_6):
        t_util_2.clf = joblib.load(clf_pkl_2)
        t_util_2.sc = joblib.load(sc_pkl_2)
        t_util_6.clf = joblib.load(clf_pkl_6)
        t_util_6.sc = joblib.load(sc_pkl_6)
    else:
        print('pklファイルが存在しません。')
        sys.exit()

    ht_util = ht.HorseTableUtil()
    test_race_keys = ht_util.get_race_keys_except_debut_at_ymd(ymd)
    if len(test_race_keys) == 0:
        print('入力日に対象レースはありません。')
        sys.exit()

    horse_table_list = ht_util.get_horse_tables(test_race_keys)
    predict(t_util_2, t_util_6, horse_table_list)

    elapsed_time = time.time() - start
    print('elapsed_time:{0}'.format(elapsed_time) + '[sec]')
    print(datetime.now().strftime("%Y/%m/%d %H:%M:%S"))

