# coding: UTF-8
import time

import train
from sklearn.externals import joblib
import os.path
import sys
import pandas as pd
import horsetable as ht
import predict as prdct
from datetime import datetime

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
    test_race_keys = ht_util.get_race_keys_except_debut_after_ymd(ymd)
    if len(test_race_keys) == 0:
        print('入力日に対象レースはありません。')
        sys.exit()

    horse_table_list = t_util_2.get_horse_table_list(test_race_keys)
    predict_util = prdct.PredictUtil()

    df_list = []
    cnt = 0
    for n in range(train.TOP_N):
        df_list.append([])
    for horse_table in horse_table_list:
        horse_table.predict_2_6_expected(t_util_2.clf, t_util_2.sc, t_util_6.clf, t_util_6.sc)
        for n in range(train.TOP_N):
            pr = horse_table.get_predict_return_nagashi(n + 1)
            if pr is not None:
                df_list[n].append(pr)
        cnt += 1

    for n in range(train.TOP_N):
        print(str(n) + ': ' + str(len(df_list[n])) + ' / ' + str(cnt) + ' = ' + str(len(df_list[n]) / cnt))
        if len(df_list[n]) != 0:
            df_all = pd.concat(df_list[n], axis=0)
            predict_util.print_return(df_all, n + 1)

    elapsed_time = time.time() - start
    print('elapsed_time:{0}'.format(elapsed_time) + '[sec]')
    print(datetime.now().strftime("%Y/%m/%d %H:%M:%S"))

