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
import predict_ymd_expected as p_expected

SLACK_URL = 'xxxx'
BET_FILE_DIRECTORY = '/Volumes/[C] Boot Camp/ProgramData/SimplePat2/kaime/'
WIN_BET_MONEY = 5
RF_QUINELLA_BET_MONEY = 6
MLP_QUINELLA_BET_MONEY = 5
TRIO_BET_MONEY = 2

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
    clf_kind_rf = args[2]
    pkl_file_prefix_rf = args[3]
    clf_kind_mlp = args[4]
    pkl_file_prefix_mlp = args[5]

    start = time.time()

    t_util_rf = train.TrainUtil(clf_kind_rf)
    t_util_mlp = train.TrainUtil(clf_kind_mlp)

    clf_pkl_rf = train.PKL_FILE_DIR + pkl_file_prefix_rf + train.CLF_PKL_FILE_NAME
    sc_pkl_rf = train.PKL_FILE_DIR + pkl_file_prefix_rf + train.SC_PKL_FILE_NAME
    clf_pkl_mlp = train.PKL_FILE_DIR + pkl_file_prefix_mlp + train.CLF_PKL_FILE_NAME
    sc_pkl_mlp = train.PKL_FILE_DIR + pkl_file_prefix_mlp + train.SC_PKL_FILE_NAME
    if os.path.isfile(clf_pkl_rf) and os.path.isfile(sc_pkl_rf) and os.path.isfile(clf_pkl_mlp) and os.path.isfile(sc_pkl_mlp):
        t_util_rf.clf = joblib.load(clf_pkl_rf)
        t_util_rf.sc = joblib.load(sc_pkl_rf)
        t_util_mlp.clf = joblib.load(clf_pkl_mlp)
        t_util_mlp.sc = joblib.load(sc_pkl_mlp)
    else:
        print('pklファイルが存在しません。')
        sys.exit()

    ht_util = ht.HorseTableUtil()
    test_race_keys = ht_util.get_race_keys_except_debut_at_ymd(ymd)
    if len(test_race_keys) == 0:
        print('入力日に対象レースはありません。')
        sys.exit()

    horse_table_list = ht_util.get_horse_tables(test_race_keys)

    rf_nagashi_expected_horseno_list, rf_win_expected_horseno_list = p_expected.predict(t_util_rf, horse_table_list)
    mlp_nagashi_expected_horseno_list, mlp_win_expected_horseno_list = p_expected.predict(t_util_mlp, horse_table_list)

    for horse_table, rf_nagashi_expected_horseno, rf_win_expected_horseno, mlp_win_expected_horseno in zip(
            horse_table_list, rf_nagashi_expected_horseno_list, rf_win_expected_horseno_list,  mlp_win_expected_horseno_list):
        if len(rf_nagashi_expected_horseno) >= 2 or len(rf_win_expected_horseno) >= 1 or len(mlp_win_expected_horseno) >=1:
            bet_file_path = BET_FILE_DIRECTORY + horse_table.race.program.get_bet_file_name()
            if not os.path.isfile(bet_file_path):
                with open(bet_file_path, 'w', encoding='shift_jis') as f:
                    f.write('init')
                time.sleep(15)
                with open(bet_file_path, 'w', encoding='shift_jis') as f:
                    if len(rf_nagashi_expected_horseno) >= 2:
                        for i in range(1, len(rf_nagashi_expected_horseno)):
                            if rf_nagashi_expected_horseno[0] < rf_nagashi_expected_horseno[i]:
                                horseno = rf_nagashi_expected_horseno[0] + rf_nagashi_expected_horseno[i] + '00'
                            else:
                                horseno = rf_nagashi_expected_horseno[i] + rf_nagashi_expected_horseno[0] + '00'
                            writestr = ymd + horse_table.race.race_key[:2] + horse_table.race.race_key[-2:] + '04' + horseno + '{0:06d}'.format(RF_QUINELLA_BET_MONEY) + '00'
                            f.write(writestr + '\r\n')
                    if len(rf_win_expected_horseno) >= 1:
                        for horseno in rf_win_expected_horseno:
                            writestr = ymd + horse_table.race.race_key[:2] + horse_table.race.race_key[-2:] + '01' + horseno + '0000' + '{0:06d}'.format(WIN_BET_MONEY) + '00'
                            f.write(writestr + '\r\n')
                    if len(mlp_win_expected_horseno) >= 1:
                        writestr = ymd + horse_table.race.race_key[:2] + horse_table.race.race_key[-2:] + '01' + mlp_win_expected_horseno[0] + '0000' + '{0:06d}'.format(WIN_BET_MONEY) + '00'
                        f.write(writestr + '\r\n')

    elapsed_time = time.time() - start
    print('elapsed_time:{0}'.format(elapsed_time) + '[sec]')
    print(datetime.now().strftime("%Y/%m/%d %H:%M:%S"))

