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
PREDICT_FILE = '/Users/xxxx/horserace/predict_ymd_stormy.txt'
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

def predict(t_util_stomy, t_util_expected, horse_table_list):
    new_predict_list = []
    old_predict_list = []
    if os.path.exists(PREDICT_FILE):
        with open(PREDICT_FILE, 'r', encoding='utf-8') as f:
            for line in f:
                old_predict_list.append(line.strip())

    horseno_list_by_race = []
    for horse_table in horse_table_list:
        horse_table.predict_stormy(t_util_stomy.clf, t_util_stomy.sc, t_util_expected.clf, t_util_expected.sc)
        win_horseno_list = []
        if horse_table.expected is not None:
            for ex in horse_table.expected:
                win_horseno_list.append(ex[0])

        if len(win_horseno_list) > 0:
            horseno_name = ''
            for horseno in win_horseno_list:
                horseno_name += horseno + ':' + horse_table.get_horse_name(horseno) + ', '
            new_predict_list.append(horse_table.race.program.get_race_info() + ' 単勝 ' + horseno_name[:-2])
        else:
            new_predict_list.append(horse_table.race.program.get_race_info() + ' 単勝推奨馬なし')

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
    clf_kind_stomy = args[2]
    pkl_file_prefix_stomy = args[3]
    clf_kind_expected = args[4]
    pkl_file_prefix_expected = args[5]

    start = time.time()

    t_util_stomy = train.TrainUtil(clf_kind_stomy)
    t_util_expected = train.TrainUtil(clf_kind_expected)

    clf_pkl_stomy = train.PKL_FILE_DIR + pkl_file_prefix_stomy + train.CLF_PKL_FILE_NAME
    sc_pkl_stomy = train.PKL_FILE_DIR + pkl_file_prefix_stomy + train.SC_PKL_FILE_NAME
    clf_pkl_expected = train.PKL_FILE_DIR + pkl_file_prefix_expected + train.CLF_PKL_FILE_NAME
    sc_pkl_expected = train.PKL_FILE_DIR + pkl_file_prefix_expected + train.SC_PKL_FILE_NAME
    if os.path.isfile(clf_pkl_stomy) and os.path.isfile(sc_pkl_stomy) and os.path.isfile(clf_pkl_expected) and os.path.isfile(sc_pkl_expected):
        t_util_stomy.clf = joblib.load(clf_pkl_stomy)
        t_util_stomy.sc = joblib.load(sc_pkl_stomy)
        t_util_expected.clf = joblib.load(clf_pkl_expected)
        t_util_expected.sc = joblib.load(sc_pkl_expected)
    else:
        print('pklファイルが存在しません。')
        sys.exit()

    ht_util = ht.HorseTableUtil()
    test_race_keys = ht_util.get_race_keys_except_debut_at_ymd(ymd)
    if len(test_race_keys) == 0:
        print('入力日に対象レースはありません。')
        sys.exit()

    horse_table_list = ht_util.get_horse_tables(test_race_keys)
    predict(t_util_stomy, t_util_expected, horse_table_list)

    elapsed_time = time.time() - start
    print('elapsed_time:{0}'.format(elapsed_time) + '[sec]')
    print(datetime.now().strftime("%Y/%m/%d %H:%M:%S"))

