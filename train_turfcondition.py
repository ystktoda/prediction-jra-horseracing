# coding: UTF-8
import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, classification_report
from sklearn.model_selection import train_test_split, GridSearchCV
import horsetable as ht
import time
# import matplotlib.pyplot as plt
import predict as prdct
from sklearn.externals import joblib
import os.path
import sys
import pickle
import logutil

PKL_FILE_NAME = 'clf.pkl.cmp'
RACE_CNT = 5000

class TrainUtil(object):

    def __init__(self, distance=None):
        self.clf = RandomForestClassifier(n_estimators=100, max_leaf_nodes=50)
        self.log = logutil.LogSingleton()
        self.distance = distance
        self.ht_util = ht.HorseTableUtil()

    def get_race_keys_goodturf(self):
        race_keys = self.ht_util.get_race_keys_except_debut_goodturf(RACE_CNT)
        train_race_keys, test_race_keys = [], []
        for i, race_key in enumerate(race_keys):
            if i < RACE_CNT * 0.2:
                test_race_keys.append(race_key)
            else:
                train_race_keys.append(race_key)

        return (train_race_keys, test_race_keys)

    def get_race_keys_softturf(self):
        cnt = RACE_CNT
        race_keys = self.ht_util.get_race_keys_except_debut_softturf(cnt)
        train_race_keys, test_race_keys = [], []
        for i, race_key in enumerate(race_keys):
            if i < cnt * 0.2:
                test_race_keys.append(race_key)
            else:
                train_race_keys.append(race_key)

        return (train_race_keys, test_race_keys)

    def get_horse_table_list(self, race_keys):
        horse_table_list = self.ht_util.load_horse_tables(race_keys, self.distance)
        if horse_table_list is None:
            horse_table_list = self.ht_util.dump_horse_tables(race_keys, self.distance)
        return horse_table_list

    def dump_xy(self, race_keys, x, y):
        if self.distance is None:
            x_file_name = race_keys[0] + '-' + race_keys[-1] + '_x.pickle'
            y_file_name = race_keys[0] + '-' + race_keys[-1] + '_y.pickle'
        else:
            x_file_name = self.distance + '_' + race_keys[0] + '-' + race_keys[-1] + '_x.pickle'
            y_file_name = self.distance + '_' + race_keys[0] + '-' + race_keys[-1] + '_y.pickle'

        with open(x_file_name, mode='wb') as fx:
            pickle.dump(x, fx)
        with open(y_file_name, mode='wb') as fy:
            pickle.dump(y, fy)

    def load_xy(self, race_keys):
        if self.distance is None:
            x_file_name = race_keys[0] + '-' + race_keys[-1] + '_x.pickle'
            y_file_name = race_keys[0] + '-' + race_keys[-1] + '_y.pickle'
        else:
            x_file_name = self.distance + '_' + race_keys[0] + '-' + race_keys[-1] + '_x.pickle'
            y_file_name = self.distance + '_' + race_keys[0] + '-' + race_keys[-1] + '_y.pickle'
        if os.path.isfile(x_file_name) and os.path.isfile(y_file_name):
            with open(x_file_name, mode='rb') as fx:
                x = pickle.load(fx)
            with open(y_file_name, mode='rb') as fy:
                y = pickle.load(fy)
            return x, y
        else:
            return None, None

    def get_x_y(self, race_keys):
        x, y = self.load_xy(race_keys)
        if x is None or y is None:
            xlist = []
            ylist = []
            for i, race_key in enumerate(race_keys):
                horse_table = self.ht_util.get_horse_table(race_key)
                xlist.append(horse_table.get_x())
                ylist.extend(horse_table.get_y())
                if (i + 1) % 200 == 0:
                    self.log.write_log('count: ' + str(i + 1))

            self.x_columns = pd.concat(xlist, axis=0).columns

            x = pd.concat(xlist, axis=0).values
            y = np.array(ylist)
            self.dump_xy(race_keys, x, y)

        self.log.write_log('get_x_y finished.')
        return x, y

    def split_data(self, race_keys):
        x, y = self.get_x_y(race_keys)
        x_train, x_test, y_train, y_test = train_test_split(x, y)
        return (x_train, x_test, y_train, y_test)

    def train_test(self, x_train, x_test, y_train, y_test, pkl):
        self.clf.fit(x_train, y_train)
        joblib.dump(self.clf, pkl, compress=True)

        predict = self.clf.predict(x_test)
        self.test(predict, x_test, y_test)

    def train(self, race_keys, pkl):
        x_train, y_train = self.get_x_y(race_keys)
        self.clf.fit(x_train, y_train)
        joblib.dump(self.clf, pkl, compress=True)

    def test(self, predict, x_test, y_test):
        ac_score = accuracy_score(y_test, predict)
        cl_report = classification_report(y_test, predict)
        self.log.write_log('正解率= ' + str(ac_score))
        self.log.write_log('レポート=')
        self.log.write_log(cl_report)
        importances = self.clf.feature_importances_
        self.log.write_log("{0:<25}".format("特徴量") + "重要度")
        for (name, importance) in zip(self.x_columns, importances):
            self.log.write_log("{0:<25}".format(name) + str(importance))

#         plt.barh(range(len(importances)), importances, tick_label=self.x_columns, align='center')
#         plt.show()

    def predict(self, test_race_keys):
        xlist = []
        ylist = []

        horse_table_list = self.get_horse_table_list(test_race_keys)
        for horse_table in horse_table_list:
            xlist.append(horse_table.get_x())
            ylist.extend(horse_table.get_y())

        self.x_columns = pd.concat(xlist, axis=0).columns

        x_test = pd.concat(xlist, axis=0).values
        y_test = np.array(ylist)

        predict = self.clf.predict(x_test)
        self.test(predict, x_test, y_test)

    def print_return(self, test_race_keys):
        horse_table_list = self.get_horse_table_list(test_race_keys)
        predict_util = prdct.PredictUtil()

        df_list = []
        for n in range(4):
            df_list.append([])
        for horse_table in horse_table_list:
            horse_table.predict(self.clf)
            horse_table.print_expected()
            for n in range(4):
                df_list[n].append(horse_table.get_predict_return(n + 1))

        for n in range(4):
            df_all = pd.concat(df_list[n], axis=0)
            predict_util.print_return(df_all, n + 1)

    def print_oishisa_return(self, test_race_keys):
        horse_table_list = self.get_horse_table_list(test_race_keys)
        predict_util = prdct.PredictUtil()
        for n in range(1, 4):
            df_list = []
            for horse_table in horse_table_list:
                horse_table.predict_oishisa(self.clf)
                df_list.append(horse_table.get_oishisa_return(n))

            df_all = pd.concat(df_list, axis=0)
            predict_util.print_return(df_all, n)

    def print_oishisa_place_return(self, test_race_keys):
        horse_table_list = self.get_horse_table_list(test_race_keys)
        predict_util = prdct.PredictUtil()

        for n in range(1, 4):
            df_list = []
            for horse_table in horse_table_list:
                horse_table.predict_oishisa_place(self.clf)
                df_list.append(horse_table.get_oishisa_place_return(n))

            df_all = pd.concat(df_list, axis=0)
            predict_util.print_return(df_all, n)

    def train_grid(self, race_keys, pkl):
        x_train, y_train= self.get_x_y(race_keys)
        parameters = {'n_estimators': [5, 10, 20, 30, 50, 100, 150],
                      'max_features': ['auto', 'sqrt', 'log2', None],
                      'max_leaf_nodes': [2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22, 24, 26, 28, 30, 32, 34, 36, 38, 40, 42, 44, 46, 48, 50]}
        self.clf = GridSearchCV(RandomForestClassifier(), parameters)
        self.clf.fit(x_train, y_train)
        joblib.dump(self.clf, pkl, compress=True)

    def print_best_estimator(self, race_keys):
        self.log.write_log('ベストパラメタを表示')
        self.log.write_log(str(self.clf.best_estimator_))
        self.log.write_log('トレーニングデータでCVした時の平均スコア')
        for params, mean_score, all_scores in self.clf.grid_scores_:
            self.log.write_log('{:.3f} (+/- {:.3f}) for {}'.format(mean_score, all_scores.std() / 2, params))

        x_test, y_test = self.get_x_y(race_keys)
        predict = self.clf.predict(x_test)
        ac_score = accuracy_score(y_test, predict)
        cl_report = classification_report(y_test, predict)
        self.log.write_log('正解率= ' + str(ac_score))
        self.log.write_log('レポート=')
        self.log.write_log(cl_report)

    def print_oishisa_place(self, test_race_keys):
        for race_key in test_race_keys:
            horse_table = self.ht_util.get_horse_table(race_key)
            horse_table.predict_oishisa_place(self.clf)
            horse_table.print_oishisa_place()

    def print_predict_winodds_over_10(self, test_race_keys):
        for race_key in test_race_keys:
            horse_table = self.ht_util.get_horse_table(race_key)
            horse_table.predict(self.clf)
            horse_table.print_predict_winodds_over_10()

if __name__ == '__main__':
    args = sys.argv

    if len(args) < 2:
        print('引数を設定してください。')
        sys.exit()
    elif len(args) == 2:
        print('引数： ' + args[1])
        case = args[1]
        turf_condition = None
        log_file_name = case + '_None_' + str(RACE_CNT) + '.log'
        log = logutil.LogSingleton()
        log.set_log_file_name(log_file_name)
        log.write_log('引数： ' + args[1])
    else:
        argstr = ''
        for num in range(1, len(args)):
            argstr += args[num] + ', '
        print('引数： ' + argstr[:-2])
        case = args[1]
        turf_condition = args[2]
        log_file_name = case + '_' + turf_condition + '_' + str(RACE_CNT) + '.log'
        log = logutil.LogSingleton()
        log.set_log_file_name(log_file_name)
        log.write_log('引数： ' + argstr[:-2])

    start = time.time()

    t_util = TrainUtil()

    if turf_condition == 'good':
        train_race_keys, test_race_keys = t_util.get_race_keys_goodturf()
    else:
        train_race_keys, test_race_keys = t_util.get_race_keys_softturf()

    pkl = str(RACE_CNT) + '_' + PKL_FILE_NAME if turf_condition is None else turf_condition + '_' + str(RACE_CNT) + '_' + PKL_FILE_NAME
    if os.path.isfile(pkl):
        t_util.clf = joblib.load(pkl)

    else:
        if case == 'grid':
            t_util.train_grid(train_race_keys, pkl)
        else:
            t_util.train(train_race_keys, pkl)

    if case == 'return':
        t_util.print_return(test_race_keys)
    elif case == 'oishisa':
        t_util.print_oishisa_return(test_race_keys)
    elif case == 'oishisa_place':
        t_util.print_oishisa_place_return(test_race_keys)
    elif case == 'grid':
        t_util.print_best_estimator(test_race_keys)
    else:
        t_util.predict(test_race_keys)

    elapsed_time = time.time() - start
    print('elapsed_time:{0}'.format(elapsed_time) + '[sec]')
    log.write_log('elapsed_time:{0}'.format(elapsed_time) + '[sec]')
