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
PREDICT_FILE = '/Users/xxxx/horserace/predict_maiden_ymd.txt'
BET_FILE_DIRECTORY = '/Volumes/[C] Boot Camp/ProgramData/SimplePat2/kaime/'
BET_MONEY = 20

def post(message):
    values = {
        'text': message,
        'channel': 'horserace',
        'username': 'bot'
    }
    requests.post(SLACK_URL, data=json.dumps(values))

if __name__ == '__main__':
    args = sys.argv

    if len(args) < 4:
        print('引数を設定してください。')
        sys.exit()

    print('引数： ' + ', '.join(args[1:]))

    ymd = args[1]
    clf_kind = args[2]
    pkl_file_prefix = args[3]

    start = time.time()

    clf_pkl = train.PKL_FILE_DIR + pkl_file_prefix + train.CLF_PKL_FILE_NAME
    sc_pkl = train.PKL_FILE_DIR + pkl_file_prefix + train.SC_PKL_FILE_NAME
    if os.path.isfile(clf_pkl) and os.path.isfile(sc_pkl):
        clf = joblib.load(clf_pkl)
        sc = joblib.load(sc_pkl)
    else:
        print('pklファイルが存在しません。')
        sys.exit()

    ht_util = ht.HorseTableUtil()
    test_race_keys = ht_util.get_race_keys_debut_at_ymd(ymd)
    if len(test_race_keys) == 0:
        print('入力日に対象レースはありません。')
        sys.exit()

    new_predict_list = []
    old_predict_list = []
    if os.path.exists(PREDICT_FILE):
        with open(PREDICT_FILE, 'r', encoding='utf-8') as f:
            for line in f:
                old_predict_list.append(line.strip())

    horse_table_list = ht_util.get_maiden_horse_tables(test_race_keys)
    for horse_table in horse_table_list:
        top5 = horse_table.get_predict_maiden_top5(clf, sc)
        new_predict_list.append(horse_table.race.program.get_race_info() + ' ' + ', '.join(top5))

        bet_file_path = BET_FILE_DIRECTORY + horse_table.race.program.get_bet_file_name()
        if not os.path.isfile(bet_file_path):
            with open(bet_file_path, 'w', encoding='shift_jis') as f:
                f.write('init')
            time.sleep(15)
            with open(bet_file_path, 'w', encoding='shift_jis') as f:
                if horse_table.get_win_odds(top5[0]) > 50 or horse_table.get_win_odds(top5[0]) < 5:
                    writestr = ymd + horse_table.race.race_key[:2] + horse_table.race.race_key[-2:] + '01' + top5[0] + '0000' + '{0:06d}'.format(BET_MONEY // 10) + '00'
                else:
                    writestr = ymd + horse_table.race.race_key[:2] + horse_table.race.race_key[-2:] + '01' + top5[0] + '0000' + '{0:06d}'.format(BET_MONEY) + '00'
                f.write(writestr + '\r\n')

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

