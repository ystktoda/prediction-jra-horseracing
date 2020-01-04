# coding: UTF-8
import train
from sklearn.externals import joblib
import os.path
import requests
import sys
import horsetable as ht
import json
import time

SLACK_URL = 'xxxx'
PREDICT_FILE = '/Users/xxxx/horserace/predict_ymd_expected.txt'
BET_FILE_DIRECTORY = '/Volumes/C/ProgramData/SimplePat2/kaime/'
WIN_BET_MONEY = 10
QUINELLA_BET_MONEY = 10

def post(message):
    values = {
        'text': message,
        'channel': 'horserace',
        'username': 'bot'
    }
    requests.post(SLACK_URL, data=json.dumps(values))

if __name__ == '__main__':
    args = sys.argv

    if len(args) < 6:
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

    new_predict_list = []
    old_predict_list = []
    if os.path.exists(PREDICT_FILE):
        with open(PREDICT_FILE, 'r', encoding='utf-8') as f:
            for line in f:
                old_predict_list.append(line.strip())

    horse_table_list = ht_util.get_horse_tables(test_race_keys)
    for horse_table in horse_table_list:
        """単勝"""
        win_expected = horse_table.get_expected(t_util_2.clf, t_util_2.sc)
        win_horseno_list = []
        for ex in win_expected:
            if ex[1] >= 0.7 and horse_table.get_win_odds(ex[0]) >= 4:
                win_horseno_list.append(ex[0])
            elif ex[1] >= 0.6 and horse_table.get_win_odds(ex[0]) >= 10:
                win_horseno_list.append(ex[0])

        """馬連"""
        quinella_expected_list = horse_table.get_2_6_expected(t_util_2.clf, t_util_2.sc, t_util_6.clf, t_util_6.sc)
        quinella_horseno_lists = []
        for quinella_expected in quinella_expected_list:
            quinella_horseno_list = []
            for ex in quinella_expected:
                quinella_horseno_list.append(ex[0])
            quinella_horseno_lists.append(quinella_horseno_list)

        bet_file_path = BET_FILE_DIRECTORY + horse_table.race.program.get_bet_file_name()
        if len(win_horseno_list) > 0:
            horseno_name = ''
            for horseno in win_horseno_list:
                horseno_name += horseno + ':' + horse_table.get_horse_name(horseno) + ', '
            new_predict_list.append(horse_table.race.program.get_race_info() + ' ' + horseno_name[:-2])
            if len(quinella_horseno_list) < 4:
                quinella_num = len(quinella_horseno_list)
            else:
                quinella_num = 4
            new_predict_list.append(horse_table.race.program.get_race_info() + ' 馬連流し: ' + quinella_horseno_list[0] + ' - ' + ','.join(quinella_horseno_list[1:quinella_num]))
            if not os.path.isfile(bet_file_path):
                with open(bet_file_path, 'w', encoding='shift_jis') as f:
                    f.write('init')
                time.sleep(15)
                with open(bet_file_path, 'w', encoding='shift_jis') as f:
                    for horseno in win_horseno_list:
                        writestr = ymd + horse_table.race.race_key[:2] + horse_table.race.race_key[-2:] + '01' + horseno + '0000' + '{0:06d}'.format(WIN_BET_MONEY) + '00'
                        f.write(writestr + '\r\n')
                    for i in range(1, quinella_num):
                        if quinella_horseno_list[0] < quinella_horseno_list[i]:
                            horseno = quinella_horseno_list[0] + quinella_horseno_list[i] + '00'
                        else:
                            horseno = quinella_horseno_list[i] + quinella_horseno_list[0] + '00'
                        writestr = ymd + horse_table.race.race_key[:2] + horse_table.race.race_key[-2:] + '04' + horseno + '{0:06d}'.format(QUINELLA_BET_MONEY) + '00'
                        f.write(writestr + '\r\n')
        else:
            new_predict_list.append(horse_table.race.program.get_race_info() + ' 推奨馬なし')

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

    elapsed_time = time.time() - start
    print('elapsed_time:{0}'.format(elapsed_time) + '[sec]')

