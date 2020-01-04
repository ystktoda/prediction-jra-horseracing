# coding: UTF-8
import time

import train
from sklearn.externals import joblib
import os.path
import sys

if __name__ == '__main__':
    args = sys.argv

    if len(args) < 3:
        print('引数を設定してください。')
        sys.exit()

    argstr = ''
    for num in range(1, len(args)):
        argstr += args[num] + ', '
    print('引数： ' + argstr[:-2])

    if len(args) == 3:
        clf_kind = args[1]
        race_cnt = int(args[2])
        payout = 0
    else:
        clf_kind = args[1]
        race_cnt = int(args[2])
        payout = args[3]

    file_name_prefix = clf_kind + '_over' + str(payout) + '_' + str(race_cnt) + '_order6'

    start = time.time()

    t_util = train.TrainUtil(clf_kind, race_cnt)

    train_race_keys, test_race_keys = t_util.get_race_keys_quinella_payout(payout)

    clf_pkl = train.PKL_FILE_DIR + file_name_prefix + train.CLF_PKL_FILE_NAME
    sc_pkl = train.PKL_FILE_DIR + file_name_prefix + train.SC_PKL_FILE_NAME
    if os.path.isfile(clf_pkl):
        t_util.clf = joblib.load(clf_pkl)
        t_util.get_x_y_within_order6(train_race_keys)
    else:
        t_util.train_grid_within_order6(train_race_keys, clf_pkl, sc_pkl)

    t_util.print_best_estimator(test_race_keys)
    t_util.predict(test_race_keys)
    t_util.print_return(test_race_keys)

    elapsed_time = time.time() - start
    print('elapsed_time:{0}'.format(elapsed_time) + '[sec]')
