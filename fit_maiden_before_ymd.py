# coding: UTF-8
import time

import train
import horsetable as ht
import sys
import datetime

if __name__ == '__main__':
    args = sys.argv

    if len(args) < 3:
        print('引数を設定してください。')
        sys.exit()

    print('引数： ' + ', '.join(args[1:]))

    payout = 0
    ymd = datetime.datetime.now().strftime('%Y%m%d')
    order_limit = None
    if len(args) == 3:
        clf_kind = args[1]
        race_cnt = int(args[2])
    elif len(args) == 4:
        clf_kind = args[1]
        race_cnt = int(args[2])
        payout = args[3]
    elif len(args) == 5:
        clf_kind = args[1]
        race_cnt = int(args[2])
        payout = args[3]
        ymd = args[4]
    else:
        clf_kind = args[1]
        race_cnt = int(args[2])
        payout = args[3]
        ymd = args[4]
        order_limit = int(args[5])

    if order_limit is None:
        prefix_last = ''
    else:
        prefix_last = '_order' + str(order_limit)
    file_name_prefix = 'maiden_' + clf_kind + '_over' + str(payout) + '_' + str(race_cnt) + '_before' + ymd + prefix_last

    start = time.time()

    t_util = train.TrainUtil(clf_kind, race_cnt)
    ht_util = ht.HorseTableUtil()

    train_race_keys = ht_util.get_race_keys_debut_quinella(payout, ymd, race_cnt)

    clf_pkl = train.PKL_FILE_DIR + file_name_prefix + train.CLF_PKL_FILE_NAME
    sc_pkl = train.PKL_FILE_DIR + file_name_prefix + train.SC_PKL_FILE_NAME
    t_util.train_maiden(train_race_keys, clf_pkl, sc_pkl, order_limit)

    elapsed_time = time.time() - start
    print('elapsed_time:{0}'.format(elapsed_time) + '[sec]')
