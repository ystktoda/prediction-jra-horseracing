# coding: UTF-8
import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.neural_network import MLPClassifier
from sklearn.metrics import accuracy_score, classification_report
from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.preprocessing import StandardScaler
from joblib import Parallel, delayed
import horsetable as ht
import time
# import matplotlib.pyplot as plt
import predict as prdct
from sklearn.externals import joblib
import os.path
import sys
import pickle
import datetime

CLF_PKL_FILE_NAME = '_clf.pkl.cmp'
SC_PKL_FILE_NAME = '_sc.pkl.cmp'
PKL_FILE_DIR = './clfpkl/'
TOP_N = 5

class TrainUtil(object):

    def __init__(self, clf_kind, race_cnt=None, distance=None):
        self.race_cnt = race_cnt
        if clf_kind == 'randomforest':
            self.clf = RandomForestClassifier(n_estimators=30, max_leaf_nodes=48, max_features=None, n_jobs=-1)
        elif clf_kind == 'mlp':
            self.clf = MLPClassifier(hidden_layer_sizes=(100, 90, 80, 70, 60, 50, 40, 30, 20, 10), solver='sgd', max_iter=5000, batch_size='auto')
        elif clf_kind == 'mlp50':
            self.clf = MLPClassifier(hidden_layer_sizes=(50), solver='sgd', max_iter=5000, batch_size='auto')
        elif clf_kind == 'mlp70':
            self.clf = MLPClassifier(hidden_layer_sizes=(70), solver='sgd', max_iter=5000, batch_size='auto')
        elif clf_kind == 'mlp80':
            self.clf = MLPClassifier(hidden_layer_sizes=(80), solver='sgd', max_iter=5000, batch_size='auto')
        elif clf_kind == 'mlp90':
            self.clf = MLPClassifier(hidden_layer_sizes=(90), solver='sgd', max_iter=5000, batch_size='auto')
        elif clf_kind == 'mlp100':
            self.clf = MLPClassifier(hidden_layer_sizes=(100), solver='sgd', max_iter=5000, batch_size='auto')
        elif clf_kind == 'mlp100-50':
            self.clf = MLPClassifier(hidden_layer_sizes=(100, 50), solver='sgd', max_iter=5000, batch_size='auto')
        elif clf_kind == 'mlpgrid':
            self.clf = GridSearchCV(MLPClassifier())
        self.clf_kind = clf_kind
        self.sc = StandardScaler()
        self.distance = distance
        self.ht_util = ht.HorseTableUtil()
        args = sys.argv
        self.argstr = ''
        for num in range(1, len(args)):
            self.argstr += args[num] + '_'

    def get_race_keys(self):
        race_keys = self.ht_util.get_race_keys_except_debut_by_distance(self.race_cnt)
        train_race_keys, test_race_keys = [], []
        for i, race_key in enumerate(race_keys):
            if i < self.race_cnt // 5:
                test_race_keys.append(race_key)
            else:
                train_race_keys.append(race_key)

        return (train_race_keys, test_race_keys)

    def get_race_keys_by_distance(self):
        cnt = self.race_cnt
        race_keys = self.ht_util.get_race_keys_except_debut_by_distance(cnt, self.distance)
        train_race_keys, test_race_keys = [], []
        for i, race_key in enumerate(race_keys):
            if i < cnt // 5:
                test_race_keys.append(race_key)
            else:
                train_race_keys.append(race_key)

        return (train_race_keys, test_race_keys)

    def get_race_keys_quinella_payout(self, quinella_payout):
        test_cnt = self.race_cnt // 5
        train_cnt = self.race_cnt - test_cnt
        test_race_keys = self.ht_util.get_race_keys_except_debut_by_distance(test_cnt)
        ymd = self.ht_util.get_race_ymd(test_race_keys[-1])
        train_race_keys = self.ht_util.get_race_keys_except_debut_quinella(quinella_payout, ymd, train_cnt)

        return (train_race_keys, test_race_keys)

    def get_race_keys_quinella_payout_race_course(self, quinella_payout, race_course):
        test_cnt = self.race_cnt // 5
        train_cnt = self.race_cnt - test_cnt
        test_race_keys = self.ht_util.get_race_keys_except_debut_by_race_course(test_cnt, race_course)
        ymd = self.ht_util.get_race_ymd(test_race_keys[-1])
        train_race_keys = self.ht_util.get_race_keys_except_debut_quinella_race_course(quinella_payout, ymd, race_course, train_cnt)

        return (train_race_keys, test_race_keys)

    def get_maiden_race_keys_quinella_payout(self, quinella_payout):
        test_cnt = self.race_cnt // 5
        train_cnt = self.race_cnt - test_cnt
        test_race_keys = self.ht_util.get_race_keys_debut_by_distance(test_cnt)
        ymd = self.ht_util.get_race_ymd(test_race_keys[-1])
        train_race_keys = self.ht_util.get_race_keys_debut_quinella(quinella_payout, ymd, train_cnt)

        return (train_race_keys, test_race_keys)

    def get_race_keys_quinella_payout_distance(self, quinella_payout, distance):
        test_cnt = self.race_cnt // 5
        train_cnt = self.race_cnt - test_cnt
        test_race_keys = self.ht_util.get_race_keys_except_debut_by_distance(test_cnt, distance)
        ymd = self.ht_util.get_race_ymd(test_race_keys[-1])
        train_race_keys = self.ht_util.get_race_keys_except_debut_quinella_distance(quinella_payout, ymd, distance, train_cnt)

        return (train_race_keys, test_race_keys)

    def get_race_keys_quinella_payout_racecond(self, quinella_payout, race_condition):
        test_cnt = self.race_cnt // 5
        train_cnt = self.race_cnt - test_cnt
        test_race_keys = self.ht_util.get_race_keys_except_debut_by_race_condition(test_cnt, race_condition)
        ymd = self.ht_util.get_race_ymd(test_race_keys[-1])
        train_race_keys = self.ht_util.get_race_keys_except_debut_quinella_racecond(quinella_payout, ymd, race_condition, train_cnt)

        return (train_race_keys, test_race_keys)

    def get_horse_table_list(self, race_keys):
        horse_table_list = self.ht_util.load_horse_tables(race_keys, self.distance)
        if horse_table_list is None:
            horse_table_list = self.ht_util.dump_horse_tables(race_keys, self.distance)
        return horse_table_list

    def get_maiden_horse_table_list(self, race_keys):
        horse_table_list = self.ht_util.load_horse_tables(race_keys, self.distance)
        if horse_table_list is None:
            horse_table_list = self.ht_util.dump_maiden_horse_tables(race_keys, self.distance)
        return horse_table_list

    def dump_xy(self, race_keys, xs, ys, order=None):
        prefix = './xy/' + str(order) + '_' if order is not None else './xy/'

        for race_key, x, y in zip(race_keys, xs, ys):
            if self.distance is None:
                x_file_name = prefix + race_key + '_x.pickle'
                y_file_name = prefix + race_key + '_y.pickle'
            else:
                x_file_name = prefix + self.distance + '_' + race_key + '_x.pickle'
                y_file_name = prefix + self.distance + '_' + race_key + '_y.pickle'

            with open(x_file_name, mode='wb') as fx:
                pickle.dump(x, fx)
            with open(y_file_name, mode='wb') as fy:
                pickle.dump(y, fy)

    def load_xy(self, race_keys, order=None):
        xlist = []
        ylist = []
        prefix = './xy/' + str(order) + '_' if order is not None else './xy/'
        for race_key in race_keys:
            if self.distance is None:
                x_file_name = prefix + race_key + '_x.pickle'
                y_file_name = prefix + race_key + '_y.pickle'
            else:
                x_file_name = prefix + self.distance + '_' + race_key + '_x.pickle'
                y_file_name = prefix + self.distance + '_' + race_key + '_y.pickle'

            if os.path.isfile(x_file_name) and os.path.isfile(y_file_name):
                with open(x_file_name, mode='rb') as fx:
                    xlist.append(pickle.load(fx))
                with open(y_file_name, mode='rb') as fy:
                    ylist.append(pickle.load(fy))
            else:
                return None, None
        return np.array(xlist), np.array(ylist)

    def append_xlist_ylist(self, race_key, i, order_limit=None):
        horse_table = self.ht_util.get_horse_table(race_key)
        if (i + 1) % 200 == 0:
            print('count: ' + str(i + 1))
        return (horse_table.get_x(order_limit), horse_table.get_y(order_limit))

    def append_xlist_ylist_maiden(self, race_key, i, order_limit=None):
        horse_table = self.ht_util.get_maiden_horse_table(race_key)
        if (i + 1) % 200 == 0:
            print('count: ' + str(i + 1))
        return (horse_table.get_x_maiden(order_limit), horse_table.get_y(order_limit))

    def get_x_y(self, race_keys, order_limit=None):
        x_org, y = self.load_xy(race_keys, order_limit)
        if x_org is None or y is None:
            xlist = []
            ylist = []
            xys = Parallel(n_jobs=-1)([delayed(self.append_xlist_ylist)(race_key, i, order_limit) for i, race_key in enumerate(race_keys)])
            for xy in xys:
                xlist.append(xy[0])
                ylist.extend(xy[1])

            self.x_columns = pd.concat(xlist, axis=0).columns

            x_org = pd.concat(xlist, axis=0).values
            y = np.array(ylist)
            self.dump_xy(race_keys, x_org, y, order_limit)

        self.sc.fit(x_org)
        x = self.sc.transform(x_org)

        print('get_x_y finished.')
        return x, y

    def get_x_y_maiden(self, race_keys, order_limit=None):
        x_org, y = self.load_xy(race_keys, order_limit)
        if x_org is None or y is None:
            xlist = []
            ylist = []
            xys = Parallel(n_jobs=-1)([delayed(self.append_xlist_ylist_maiden)(race_key, i, order_limit) for i, race_key in enumerate(race_keys)])
            for xy in xys:
                xlist.append(xy[0])
                ylist.extend(xy[1])

            self.x_columns = pd.concat(xlist, axis=0).columns

            x_org = pd.concat(xlist, axis=0).values
            y = np.array(ylist)
            self.dump_xy(race_keys, x_org, y, order_limit)

        self.sc.fit(x_org)
        x = self.sc.transform(x_org)

        print('get_x_y_maiden finished.')
        return x, y

    def split_data(self, race_keys):
        x, y = self.get_x_y(race_keys)
        x_train, x_test, y_train, y_test = train_test_split(x, y)
        return (x_train, x_test, y_train, y_test)

    def train_test(self, x_train, x_test, y_train, y_test, clf_pkl):
        self.clf.fit(x_train, y_train)
        joblib.dump(self.clf, clf_pkl, compress=True)

        predict = self.clf.predict(x_test)
        self.test(predict, x_test, y_test)

    def train(self, race_keys, clf_pkl, sc_pkl, order_limit=None):
        x_train, y_train = self.get_x_y(race_keys, order_limit)
        self.clf.fit(x_train, y_train)
        joblib.dump(self.clf, clf_pkl, compress=True)
        joblib.dump(self.sc, sc_pkl, compress=True)

    def train_maiden(self, race_keys, clf_pkl, sc_pkl, order_limit=None):
        x_train, y_train = self.get_x_y_maiden(race_keys, order_limit)
        self.clf.fit(x_train, y_train)
        joblib.dump(self.clf, clf_pkl, compress=True)
        joblib.dump(self.sc, sc_pkl, compress=True)

    def test(self, predict, x_test, y_test):
        ac_score = accuracy_score(y_test, predict)
        cl_report = classification_report(y_test, predict)
        yyyymmddhhmmss = datetime.datetime.now().strftime('%Y%m%d-%H%M%S')
        with open('./report/' + self.argstr[:-1] + '_' + yyyymmddhhmmss + '.log', 'a', encoding='utf-8') as f:
            f.write('正解率= ' + str(ac_score) + '\n')
            f.write('レポート=\n')
            f.write(cl_report + '\n')
            if hasattr(self.clf, 'feature_importances_'):
                f.write('{0:<25}'.format('特徴量') + '重要度\n')
                importances = self.clf.feature_importances_
                for (name, importance) in zip(self.x_columns, importances):
                    f.write("{0:<25}".format(name) + str(importance) + '\n')
            if hasattr(self.clf, 'best_estimator_'):
                f.write('ベストパラメタを表示\n')
                f.write(str(self.clf.best_estimator_) + '\n')
                f.write('トレーニングデータでCVした時の平均スコア\n')
                for params, mean_score, all_scores in self.clf.grid_scores_:
                    f.write('{:.3f} (+/- {:.3f}) for {}'.format(mean_score, all_scores.std() / 2, params) + '\n')

    def predict(self, test_race_keys, order_limit=None):
        xlist = []
        ylist = []

        horse_table_list = self.get_horse_table_list(test_race_keys)
        for horse_table in horse_table_list:
            xlist.append(horse_table.get_x_all())
            ylist.extend(horse_table.get_y_all(order_limit))

        self.x_columns = pd.concat(xlist, axis=0).columns

        x_org = pd.concat(xlist, axis=0).values
        x_test = self.sc.transform(x_org)
        y_test = np.array(ylist)

        predict = self.clf.predict(x_test)
        self.test(predict, x_test, y_test)

    def predict_maiden(self, test_race_keys, order_limit=None):
        xlist = []
        ylist = []

        horse_table_list = self.get_maiden_horse_table_list(test_race_keys)
        for horse_table in horse_table_list:
            xlist.append(horse_table.get_x_all_maiden())
            ylist.extend(horse_table.get_y_all(order_limit))

        self.x_columns = pd.concat(xlist, axis=0).columns

        x_org = pd.concat(xlist, axis=0).values
        x_test = self.sc.transform(x_org)
        y_test = np.array(ylist)

        predict = self.clf.predict(x_test)
        self.test(predict, x_test, y_test)

    def print_return(self, test_race_keys):
        horse_table_list = self.get_horse_table_list(test_race_keys)

        df_list = []
        for n in range(TOP_N):
            df_list.append([])
        for horse_table in horse_table_list:
            horse_table.predict(self.clf, self.sc)
            horse_table.print_expected()
            for n in range(TOP_N):
                pr = horse_table.get_predict_return(n + 1)
                if pr is not None:
                    df_list[n].append(pr)

        self.print_return_by_top_n(df_list)

    def print_expected_return(self, test_race_keys):
        horse_table_list = self.get_horse_table_list(test_race_keys)

        df_list = []
        cnt = 0
        for n in range(TOP_N):
            df_list.append([])
        for horse_table in horse_table_list:
            horse_table.predict(self.clf, self.sc)
            for n in range(TOP_N):
                pr = horse_table.get_predict_expected_return(n + 1, self.clf_kind)
                if pr is not None:
                    df_list[n].append(pr)
            cnt += 1

        self.print_return_by_top_n(df_list, cnt)

    def print_expected_return_by_race_course(self, test_race_keys):
        horse_table_list = self.get_horse_table_list(test_race_keys)

        for rc_name in ht.RACE_COURSE:
            print(rc_name)
            df_list = []
            cnt = 0
            for n in range(TOP_N):
                df_list.append([])
            for horse_table in horse_table_list:
                if ht.RACE_COURSE[int(horse_table.race.race_key[:2]) - 1] != rc_name:
                    continue
                horse_table.predict(self.clf, self.sc)
                for n in range(TOP_N):
                    pr = horse_table.get_predict_expected_return(n + 1, self.clf_kind)
                    if pr is not None:
                        df_list[n].append(pr)
                cnt += 1

            self.print_return_by_top_n(df_list, cnt)

    def print_expected_fav1_return(self, test_race_keys):
        horse_table_list = self.get_horse_table_list(test_race_keys)

        df_list = []
        cnt = 0
        for n in range(TOP_N):
            df_list.append([])
        for horse_table in horse_table_list:
            horse_table.predict(self.clf, self.sc)
            for n in range(TOP_N):
                pr = horse_table.get_predict_expected_return_fav1(n + 1, self.clf_kind)
                if pr is not None:
                    df_list[n].append(pr)
            cnt += 1

        self.print_return_by_top_n(df_list, cnt)

    def print_expected_nagashi_return(self, test_race_keys):
        horse_table_list = self.get_horse_table_list(test_race_keys)

        df_list_by_individually = []
        df_list_by_seq = []
        cnt = 0
        for n in range(TOP_N):
            df_list_by_individually.append([])
            df_list_by_seq.append([])
        for horse_table in horse_table_list:
            horse_table.predict(self.clf, self.sc)
            for n in range(TOP_N):
                pr_by_individually = horse_table.get_predict_expected_return_nagashi_by_individually(n + 1, self.clf_kind)
                if pr_by_individually is not None:
                    df_list_by_individually[n].append(pr_by_individually)
                pr_by_seq = horse_table.get_predict_expected_return_nagashi_by_seq(n + 1, self.clf_kind)
                if pr_by_seq is not None:
                    df_list_by_seq[n].append(pr_by_seq)
            cnt += 1

        self.print_return_by_top_n(df_list_by_individually, cnt)
        self.print_return_by_top_n(df_list_by_seq, cnt)

    def print_return_maiden(self, test_race_keys):
        horse_table_list = self.get_maiden_horse_table_list(test_race_keys)

        df_list = []
        for n in range(TOP_N):
            df_list.append([])
        for horse_table in horse_table_list:
            horse_table.predict_maiden(self.clf, self.sc)
            horse_table.print_expected()
            for n in range(TOP_N):
                pr = horse_table.get_predict_return(n + 1)
                if pr is not None:
                    df_list[n].append(pr)

        self.print_return_by_top_n(df_list)

    def print_expected_return_maiden(self, test_race_keys):
        horse_table_list = self.get_maiden_horse_table_list(test_race_keys)

        df_list = []
        cnt = 0
        for n in range(TOP_N):
            df_list.append([])
        for horse_table in horse_table_list:
            horse_table.predict_maiden(self.clf, self.sc)
            for n in range(TOP_N):
                pr = horse_table.get_predict_expected_return_maiden(n + 1)
                if pr is not None:
                    df_list[n].append(pr)
            cnt += 1

        self.print_return_by_top_n(df_list, cnt)

    def train_grid(self, race_keys, clf_pkl, sc_pkl, order_limit=None):
        x_train, y_train= self.get_x_y(race_keys, order_limit)
        self.grid_main(clf_pkl, sc_pkl, x_train, y_train)

    def grid_main(self, clf_pkl, sc_pkl, x_train, y_train):
        if self.clf_kind == 'randomforest':
            parameters = {'n_estimators': [5, 10, 20, 30, 50, 100, 150],
                          'max_features': ['auto', 'sqrt', 'log2', None],
                          'max_leaf_nodes': [2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22, 24, 26, 28, 30, 32, 34, 36, 38, 40, 42, 44, 46, 48, 50]}
            self.clf = GridSearchCV(RandomForestClassifier(), parameters)
        elif self.clf_kind == 'mlp':
            parameters = {'hidden_layer_sizes': [(50), (70), (80), (90), (100)],
                          'solver': ['sgd'],
                          'max_iter': [5000],
                          'batch_size': ['auto'],
                          'alpha': [0.001, 0.0001, 0.00001, 0.000001],
                          'learning_rate_init': [0.01, 0.001, 0.0001, 0.00001, 0.000001]}
            self.clf = GridSearchCV(MLPClassifier(), parameters)
        self.clf.fit(x_train, y_train)
        joblib.dump(self.clf, clf_pkl, compress=True)
        joblib.dump(self.sc, sc_pkl, compress=True)

    def print_best_estimator(self, race_keys):
        x_test, y_test = self.get_x_y(race_keys)
        predict = self.clf.predict(x_test)
        ac_score = accuracy_score(y_test, predict)
        cl_report = classification_report(y_test, predict)

        yyyymmddhhmmss = datetime.datetime.now().strftime('%Y%m%d-%H%M%S')
        with open('./report/' + self.argstr[:-1] + '_' + yyyymmddhhmmss + '.log', 'a', encoding='utf-8') as f:
            f.write('ベストパラメタを表示\n')
            f.write(str(self.clf.best_estimator_) + '\n')
            f.write('トレーニングデータでCVした時の平均スコア\n')
            for params, mean_score, all_scores in self.clf.grid_scores_:
                f.write('{:.3f} (+/- {:.3f}) for {}'.format(mean_score, all_scores.std() / 2, params) + '\n')
            f.write('正解率= ' + str(ac_score) + '\n')
            f.write('レポート=\n')
            f.write(cl_report + '\n')

    def print_return_by_top_n(self, df_list, cnt=None):
        predict_util = prdct.PredictUtil()
        for n in range(TOP_N):
            if cnt is not None:
                print(str(n) + ': ' + str(len(df_list[n])) + ' / ' + str(cnt) + ' = ' + str(len(df_list[n]) / cnt))
            if len(df_list[n]) != 0:
                df_all = pd.concat(df_list[n], axis=0)
                predict_util.print_return(df_all, n + 1)

if __name__ == '__main__':
    args = sys.argv

    if len(args) < 3:
        print('引数を設定してください。')
        sys.exit()

    print('引数： ' + ', '.join(args[1:]))

    if len(args) == 3:
        clf_kind = args[1]
        race_cnt = int(args[2])
        case = None
        distance = None
        file_name_prefix = clf_kind + '_None_None_' + str(race_cnt)
    elif len(args) == 4:
        clf_kind = args[1]
        race_cnt = int(args[2])
        case = args[3]
        distance = None
        file_name_prefix = clf_kind + '_' + case + '_None' + '_' + str(race_cnt)
    else:
        clf_kind = args[1]
        race_cnt = int(args[2])
        case = args[3]
        distance = args[4]
        file_name_prefix = clf_kind + '_' + case + '_' + distance + '_' + str(race_cnt)

    start = time.time()

    t_util = TrainUtil(clf_kind, race_cnt, distance)

    if distance is None:
        train_race_keys, test_race_keys = t_util.get_race_keys()
    else:
        train_race_keys, test_race_keys = t_util.get_race_keys_by_distance()

    clf_pkl = PKL_FILE_DIR + file_name_prefix + CLF_PKL_FILE_NAME
    sc_pkl = PKL_FILE_DIR + file_name_prefix + SC_PKL_FILE_NAME
    if os.path.isfile(clf_pkl):
        t_util.clf = joblib.load(clf_pkl)
        t_util.get_x_y(train_race_keys)
    else:
        if case == 'grid':
            t_util.train_grid(train_race_keys, clf_pkl, sc_pkl)
        else:
            t_util.train(train_race_keys, clf_pkl, sc_pkl)

    if case == 'return':
        t_util.print_return(test_race_keys)
    elif case == 'oishisa':
        t_util.print_oishisa_return(test_race_keys)
    elif case == 'oishisa_place':
        t_util.print_oishisa_place_return(test_race_keys)
    elif case == 'grid':
        t_util.print_best_estimator(test_race_keys)

    t_util.predict(test_race_keys)

    elapsed_time = time.time() - start
    print('elapsed_time:{0}'.format(elapsed_time) + '[sec]')
