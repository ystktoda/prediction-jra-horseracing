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

    print('引数： ' + ', '.join(args[1:]))

    order_limit = None
    if len(args) == 3:
        clf_kind = args[1]
        race_cnt = int(args[2])
        payout = 0
    elif len(args) == 4:
        clf_kind = args[1]
        race_cnt = int(args[2])
        payout = args[3]
    else:
        clf_kind = args[1]
        race_cnt = int(args[2])
        payout = args[3]
        order_limit = int(args[4])

    if order_limit is None:
        prefix_last = ''
    else:
        prefix_last = '_order' + str(order_limit)
    file_name_prefix = clf_kind + '_over' + str(payout) + '_' + str(race_cnt) + prefix_last

    start = time.time()

    t_util = train.TrainUtil(clf_kind, race_cnt)

    train_race_keys, test_race_keys = t_util.get_race_keys_quinella_payout(payout)

    clf_pkl = train.PKL_FILE_DIR + file_name_prefix + train.CLF_PKL_FILE_NAME
    sc_pkl = train.PKL_FILE_DIR + file_name_prefix + train.SC_PKL_FILE_NAME
    if os.path.isfile(clf_pkl):
        t_util.clf = joblib.load(clf_pkl)
        t_util.get_x_y(train_race_keys, order_limit)
    else:
        t_util.train(train_race_keys, clf_pkl, sc_pkl, order_limit)

    t_util.predict(test_race_keys)
    t_util.print_return(test_race_keys)

    elapsed_time = time.time() - start
    print('elapsed_time:{0}'.format(elapsed_time) + '[sec]')
