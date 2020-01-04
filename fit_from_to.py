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
    to_ymd = datetime.datetime.now().strftime('%Y%m%d')
    order_limit = None
    if len(args) == 3:
        from_ymd = args[1]
        clf_kind = args[2]
    elif len(args) == 4:
        from_ymd = args[1]
        clf_kind = args[2]
        payout = args[3]
    elif len(args) == 5:
        from_ymd = args[1]
        clf_kind = args[2]
        payout = args[3]
        to_ymd = args[4]
    else:
        from_ymd = args[1]
        clf_kind = args[2]
        payout = args[3]
        to_ymd = args[4]
        order_limit = int(args[5])

    if order_limit is None:
        prefix_last = ''
    else:
        prefix_last = '_order' + str(order_limit)
    file_name_prefix = clf_kind + '_over' + str(payout) + '_from' + from_ymd + '_to' + to_ymd + prefix_last

    start = time.time()

    t_util = train.TrainUtil(clf_kind)
    ht_util = ht.HorseTableUtil()

    train_race_keys = ht_util.get_race_keys_except_debut_quinella_from_to(payout, from_ymd, to_ymd)

    clf_pkl = train.PKL_FILE_DIR + file_name_prefix + train.CLF_PKL_FILE_NAME
    sc_pkl = train.PKL_FILE_DIR + file_name_prefix + train.SC_PKL_FILE_NAME
    t_util.train(train_race_keys, clf_pkl, sc_pkl, order_limit)

    elapsed_time = time.time() - start
    print('elapsed_time:{0}'.format(elapsed_time) + '[sec]')
    print(datetime.datetime.now().strftime("%Y/%m/%d %H:%M:%S"))
