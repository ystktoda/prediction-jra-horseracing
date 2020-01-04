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
    test_race_keys = ht_util.get_race_keys_except_debut_after_ymd(ymd)
    if len(test_race_keys) == 0:
        print('入力日に対象レースはありません。')
        sys.exit()

    horse_table_list = t_util_stomy.get_horse_table_list(test_race_keys)
    predict_util = prdct.PredictUtil()

    df_list = []
    cnt = 0
    for n in range(train.TOP_N):
        df_list.append([])
    for horse_table in horse_table_list:
        horse_table.predict_stormy(t_util_stomy.clf, t_util_stomy.sc, t_util_expected.clf, t_util_expected.sc)
        for n in range(train.TOP_N):
            pr = horse_table.get_predict_return(n + 1)
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

