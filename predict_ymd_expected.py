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
PREDICT_FILE_DIRECTORY = '/Users/xxxx/horserace/'
PREDICT_FILE = '_predict_ymd_expected.txt'
BET_FILE_DIRECTORY = '/Volumes/[C] Boot Camp/ProgramData/SimplePat2/kaime/'
WIN_BET_MONEY = 5
QUINELLA_BET_MONEY = 5

def post(message):
    values = {
        'text': message,
        'channel': 'horserace',
        'username': 'bot'
    }
    requests.post(SLACK_URL, data=json.dumps(values))

def predict(t_util_2, horse_table_list):
    new_predict_list = []
    old_predict_list = []
    predict_file_fullpath = PREDICT_FILE_DIRECTORY + t_util_2.clf_kind + PREDICT_FILE
    if os.path.exists(predict_file_fullpath):
        with open(predict_file_fullpath, 'r', encoding='utf-8') as f:
            for line in f:
                old_predict_list.append(line.strip())


    nagashi_horseno_list_by_race = []
    win_horseno_list_by_race = []
    for horse_table in horse_table_list:
        win_expected = horse_table.get_expected(t_util_2.clf, t_util_2.sc)
        nagashi_horseno_list = []
        win_horseno_list = []
        i = 0
        if t_util_2.clf_kind == 'randomforest':
            for ex in win_expected:
                if horse_table.is_over_proba_rf(i, ex):
                    nagashi_horseno_list.append(ex[0])
                    i += 1
                if horse_table.is_over_proba_rf_win(ex):
                    win_horseno_list.append(ex[0])
        else:
            for ex in win_expected:
                if horse_table.is_over_proba_mlp(i, ex):
                    nagashi_horseno_list.append(ex[0])
                    win_horseno_list.append(ex[0])
                    i += 1

        if len(nagashi_horseno_list) > 1:
            horseno_name = ''
            for horseno in nagashi_horseno_list:
                horseno_name += horseno + ':' + horse_table.get_horse_name(horseno) + ', '
            new_predict_list.append(horse_table.race.program.get_race_info() + ' ' + t_util_2.clf_kind + ' 流し ' + horseno_name[:-2])
        else:
            new_predict_list.append(horse_table.race.program.get_race_info() + ' ' + t_util_2.clf_kind + ' 流し推奨馬なし')

        if len(win_horseno_list) > 0:
            horseno_name = ''
            for horseno in win_horseno_list:
                horseno_name += horseno + ':' + horse_table.get_horse_name(horseno) + ', '
            new_predict_list.append(
                horse_table.race.program.get_race_info() + ' ' + t_util_2.clf_kind + ' 単勝 ' + horseno_name[:-2])

        nagashi_horseno_list_by_race.append(nagashi_horseno_list)
        win_horseno_list_by_race.append(win_horseno_list)

    old_set = set(old_predict_list)
    new_set = set(new_predict_list)
    unmatched_list = list(new_set.difference(old_set))

    message = ''
    for s in unmatched_list:
        message += s + '\n'

    if message != '':
        message = message
        post(message)

    with open(predict_file_fullpath, 'w', encoding='utf-8') as f:
        for text in new_predict_list:
            f.write(text + '\n')

    return nagashi_horseno_list_by_race, win_horseno_list_by_race

if __name__ == '__main__':
    args = sys.argv

    if len(args) < 4:
        print('引数を設定してください。')
        sys.exit()

    print('引数： ' + ', '.join(args[1:]))

    ymd = args[1]
    clf_kind_2 = args[2]
    pkl_file_prefix_2 = args[3]

    start = time.time()

    t_util_2 = train.TrainUtil(clf_kind_2)

    clf_pkl_2 = train.PKL_FILE_DIR + pkl_file_prefix_2 + train.CLF_PKL_FILE_NAME
    sc_pkl_2 = train.PKL_FILE_DIR + pkl_file_prefix_2 + train.SC_PKL_FILE_NAME
    if os.path.isfile(clf_pkl_2) and os.path.isfile(sc_pkl_2):
        t_util_2.clf = joblib.load(clf_pkl_2)
        t_util_2.sc = joblib.load(sc_pkl_2)
    else:
        print('pklファイルが存在しません。')
        sys.exit()

    ht_util = ht.HorseTableUtil()
    test_race_keys = ht_util.get_race_keys_except_debut_at_ymd(ymd)
    if len(test_race_keys) == 0:
        print('入力日に対象レースはありません。')
        sys.exit()

    horse_table_list = ht_util.get_horse_tables(test_race_keys)
    predict(t_util_2, horse_table_list)

    elapsed_time = time.time() - start
    print('elapsed_time:{0}'.format(elapsed_time) + '[sec]')
    print(datetime.now().strftime("%Y/%m/%d %H:%M:%S"))

