# coding: UTF-8
import pandas as pd
import horsetable as ht
import time
import datetime
import sys

class PredictUtil(object):

    def __init__(self):
        args = sys.argv
        self.argstr = ''
        for num in range(1, len(args)):
            self.argstr += args[num] + '_'
        self.yyyymmddhhmmss = datetime.datetime.now().strftime('%Y%m%d-%H%M%S')

    def print_return(self, df_all, n):
        df_result_list = []

        win_list = []
        win_list.append(df_all['win_hit'].mean())
        win_list.append(df_all['win_ret'].mean())
        win_list.append(df_all['win_ret'].std(ddof=False))
        df_result_list.append(pd.DataFrame([win_list], index=['win'], columns=['hit', 'ret', 'ret_std']))

        place_list = []
        place_list.append(df_all['place_hit'].mean())
        place_list.append(df_all['place_ret'].mean())
        place_list.append(df_all['place_ret'].std(ddof=False))
        df_result_list.append(pd.DataFrame([place_list], index=['place'], columns=['hit', 'ret', 'ret_std']))

        if n > 1:
            quinella_place_list = []
            quinella_place_list.append(df_all['quinella_place_hit'].mean())
            quinella_place_list.append(df_all['quinella_place_ret'].mean())
            quinella_place_list.append(df_all['quinella_place_ret'].std(ddof=False))
            df_result_list.append(pd.DataFrame([quinella_place_list], index=['quinella_place'], columns=['hit', 'ret', 'ret_std']))

            quinella_list = []
            quinella_list.append(df_all['quinella_hit'].mean())
            quinella_list.append(df_all['quinella_ret'].mean())
            quinella_list.append(df_all['quinella_ret'].std(ddof=False))
            df_result_list.append(pd.DataFrame([quinella_list], index=['quinella'], columns=['hit', 'ret', 'ret_std']))

            exacta_list = []
            exacta_list.append(df_all['exacta_hit'].mean())
            exacta_list.append(df_all['exacta_ret'].mean())
            exacta_list.append(df_all['exacta_ret'].std(ddof=False))
            df_result_list.append(pd.DataFrame([exacta_list], index=['exacta'], columns=['hit', 'ret', 'ret_std']))

            if n > 2:
                if 'trio_hit' in df_all.columns:
                    trio_list = []
                    trio_list.append(df_all['trio_hit'].mean())
                    trio_list.append(df_all['trio_ret'].mean())
                    trio_list.append(df_all['trio_ret'].std(ddof=False))
                    df_result_list.append(pd.DataFrame([trio_list], index=['trio'], columns=['hit', 'ret', 'ret_std']))

                if 'trifecta_hit' in df_all.columns:
                    trifecta_list = []
                    trifecta_list.append(df_all['trifecta_hit'].mean())
                    trifecta_list.append(df_all['trifecta_ret'].mean())
                    trifecta_list.append(df_all['trifecta_ret'].std(ddof=False))
                    df_result_list.append(pd.DataFrame([trifecta_list], index=['trifecta'], columns=['hit', 'ret', 'ret_std']))

#         self.log.write_log('---- Top-' + str(n))
#         self.log.write_dataframe(pd.concat(df_result_list, axis=0))
        with open('./predict/' + self.argstr[:-1] + '_' + self.yyyymmddhhmmss + '.log', 'a', encoding='utf-8') as f:
            f.write('---- Top-' + str(n) + '\n')
            f.write('{0}\n'.format(pd.concat(df_result_list, axis=0)))

    def print_baseline_return(self, race_keys, n):
        df_list = []
        for race_key in race_keys:
            result = ht_util.get_result(race_key)
            df_list.append(result.get_baseline_return(n))

        df_all = pd.concat(df_list, axis=0)

        self.print_return(df_all, n)


if __name__ == "__main__":
    start = time.time()
    p_util = PredictUtil()
    ht_util = ht.HorseTableUtil()
    race_keys = ht_util.get_race_keys(1000)
    for i in range(1, 4):
        p_util.print_baseline_return(race_keys, i)
    elapsed_time = time.time() - start
    print('elapsed_time:{0}'.format(elapsed_time) + '[sec]')
