# coding: UTF-8
import sys
import horsetable as ht
from joblib import Parallel, delayed
import time
import datetime

if __name__ == '__main__':
    args = sys.argv

    if len(args) < 2:
        print('引数を設定してください。')
        sys.exit()

    print('引数： ' + ', '.join(args[1:]))

    from_ymd = args[1]
    to_ymd = datetime.datetime.now().strftime('%Y%m%d')
    if len(args) > 2:
        to_ymd = args[2]

    start = time.time()

    ht_util = ht.HorseTableUtil()
    race_keys = ht_util.get_race_keys_debut_quinella_from_to(0, from_ymd, to_ymd)
    if len(race_keys) == 0:
        print('入力期間に対象レースはありません。')
        sys.exit()

    Parallel(n_jobs=-1)([delayed(ht_util.dump_maiden_horse_table)(race_key) for race_key in race_keys])

    elapsed_time = time.time() - start
    print('elapsed_time:{0}'.format(elapsed_time) + '[sec]')
    print(datetime.datetime.now().strftime("%Y/%m/%d %H:%M:%S"))

