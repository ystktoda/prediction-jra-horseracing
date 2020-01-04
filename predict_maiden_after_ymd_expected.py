# coding: UTF-8
import time

import train
from sklearn.externals import joblib
import os.path
import sys
import horsetable as ht
from datetime import datetime

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

    t_util = train.TrainUtil(clf_kind)

    clf_pkl = train.PKL_FILE_DIR + pkl_file_prefix + train.CLF_PKL_FILE_NAME
    sc_pkl = train.PKL_FILE_DIR + pkl_file_prefix + train.SC_PKL_FILE_NAME
    if os.path.isfile(clf_pkl) and os.path.isfile(sc_pkl):
        t_util.clf = joblib.load(clf_pkl)
        t_util.sc = joblib.load(sc_pkl)
    else:
        print('pklファイルが存在しません。')
        sys.exit()

    ht_util = ht.HorseTableUtil()
    test_race_keys = ht_util.get_race_keys_debut_after_ymd(ymd)
    if len(test_race_keys) == 0:
        print('入力日に対象レースはありません。')
        sys.exit()

    t_util.print_expected_return_maiden(test_race_keys)

    elapsed_time = time.time() - start
    print('elapsed_time:{0}'.format(elapsed_time) + '[sec]')
    print(datetime.now().strftime("%Y/%m/%d %H:%M:%S"))
