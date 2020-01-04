# coding: UTF-8
from sqlalchemy import Column, Integer, String
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.sqlite import REAL
from joblib import Parallel, delayed
import pandas as pd
import itertools
import datetime
import os
from _operator import concat
import platform
import pickle
import sys

Base = declarative_base()

RACE_COURSE = ['札幌', '函館', '福島', '新潟', '東京', '中山', '中京', '京都', '阪神', '小倉']

class SessionFactory(object):

    def __init__(self, sql_connection, echo=False):
        self.engine = create_engine(sql_connection, echo=echo)
        Base.metadata.create_all(self.engine)

    def create(self):
        Session = sessionmaker(bind=self.engine)
        return Session()


class SessionContext(object):

    def __init__(self, session):
        self.session = session

    def __enter__(self):
        return self.session

    def __exit__(self, exc_type, exc_value, traceback):
        self.session.close()


class SessionContextFactory(object):

    def __init__(self, sql_connection, echo=False):
        self.session_factory = SessionFactory(sql_connection, echo)

    def create(self):
        return SessionContext(self.session_factory.create())

class HorseTable(object):
    """
    馬柱クラス
    """

    def __init__(self, race, horses):
        """
        :param race: Raceクラス
        :type race: Race
        :param horses: Horseクラスのリスト. 馬番順に格納
        :type horses: list of Horse
        """
        self.race = race
        self.horses = horses
        if race.program.raceconditioncd in ('A1', 'A2'):
            self.set_average_maiden()
        else:
            self.set_average()
        args = sys.argv
        self.argstr = ''
        for num in range(1, len(args)):
            self.argstr += args[num] + '_'

    def set_average(self):
        time_diff_list = []
        age_list = []
        place_ratio_list = []
        race_style_list = []
        prev_first3f_list = []
        prev_last3f_list  = []
        burden_list = []
        earnings_list = []
        idm_list = []
        jockey_index_list = []
        info_index_list = []
        odds_index_list = []
        paddock_index_list = []
        composite_index_list = []

        for horse in self.horses:
            time_diff_list.append(horse.pre_race_results[0].timediff)
            age_list.append((datetime.datetime.strptime(self.race.program.ymd, '%Y%m%d')
                             - datetime.datetime.strptime(horse.horse_profile.birthday, '%Y%m%d')).days)
            place_ratio_list.append(horse.place_ratio)
            prev_first3f_list.append(horse.pre_race_results[0].first3ftime)
            prev_last3f_list.append(horse.pre_race_results[0].last3ftime)
            race_style_list.append(int(horse.horse_race.racestylecd))
            burden = horse.horse_race.burdenweight
            if horse.horse_profile.sexcd == '2':
                burden += 20
            burden_list.append(burden)
            earnings_list.append(horse.horse_race.earnings)
            idm_list.append(horse.last_minute.idm)
            jockey_index_list.append(horse.last_minute.jockeyindex)
            info_index_list.append(horse.last_minute.infoindex)
            odds_index_list.append(horse.last_minute.oddsindex)
            paddock_index_list.append(horse.last_minute.paddockindex)
            composite_index_list.append(horse.last_minute.compositeindex)

        self.ave_timediff = sum(time_diff_list) / len(time_diff_list)
        self.ave_age = sum(age_list) / len(age_list)
        self.ave_place_ratio = sum(place_ratio_list) / len(place_ratio_list)
        self.ave_prev_first3f = sum(prev_first3f_list) / len(prev_first3f_list)
        self.ave_prev_last3f = sum(prev_last3f_list) / len(prev_last3f_list)
        self.ave_race_style = sum(race_style_list) / len(race_style_list)
        self.ave_burden = sum(burden_list) / len(burden_list)
        self.ave_earnings = sum(earnings_list) / len(earnings_list)
        self.ave_idm = sum(idm_list) / len(idm_list)
        self.ave_jockey_index = sum(jockey_index_list) / len(jockey_index_list)
        self.ave_info_index = sum(info_index_list) / len(info_index_list)
        self.ave_odds_index = sum(odds_index_list) / len(odds_index_list)
        self.ave_paddock_index = sum(paddock_index_list) / len(paddock_index_list)
        self.ave_composite_index = sum(composite_index_list) / len(composite_index_list)

    def set_average_maiden(self):
        idm_list = []
        jockey_index_list = []
        info_index_list = []
        odds_index_list = []
        paddock_index_list = []
        composite_index_list = []

        for horse in self.horses:
            idm_list.append(horse.last_minute.idm)
            jockey_index_list.append(horse.last_minute.jockeyindex)
            info_index_list.append(horse.last_minute.infoindex)
            odds_index_list.append(horse.last_minute.oddsindex)
            paddock_index_list.append(horse.last_minute.paddockindex)
            composite_index_list.append(horse.last_minute.compositeindex)

        self.ave_idm = sum(idm_list) / len(idm_list)
        self.ave_jockey_index = sum(jockey_index_list) / len(jockey_index_list)
        self.ave_info_index = sum(info_index_list) / len(info_index_list)
        self.ave_odds_index = sum(odds_index_list) / len(odds_index_list)
        self.ave_paddock_index = sum(paddock_index_list) / len(paddock_index_list)
        self.ave_composite_index = sum(composite_index_list) / len(composite_index_list)

    def print_table(self):
        """
        簡易馬柱を表示する
        """
        # レース名の表示
        print(self.race.get_race_title())

        # 馬番+馬名+前レース名+前着順の表示
        for i, horse in enumerate(self.horses):
            horse_no = i + 1
            print("{}. {} prev:{} order:{}".format(horse_no, horse.get_name(),
                                                   horse.pre_race_results[0].racename, horse.pre_race_results[0].order))

    def get_horse_by_horseno(self, horseno):
        for horse in self.horses:
            if horse.horse_race.horseno == horseno:
                return horse

        return None

    def set_x_df(self, horse):
        x = []
        """枠順"""
        x.append(horse.horse_race.horseno)
        """距離"""
        x.append(self.race.program.distance)
        """芝ダ障害コード"""
        x.append(self.race.program.tracktypecd)
        """右左"""
        x.append(self.race.program.trackrotationcd)
        """内外"""
        x.append(self.race.program.trackinoutcd)
        """頭数"""
        x.append(self.race.program.startersno)
        """前走距離との差"""
        x.append(self.race.program.distance - horse.pre_race_results[0].distance)
        """馬体重"""
        x.append(horse.last_minute.horseweight)
        """馬体重増減"""
        x.append(horse.last_minute.horseweight - horse.pre_race_results[0].horseweight)
        """馬出遅率"""
        x.append(horse.horse_race.latestartavg)
        """騎手期待3着内率"""
        x.append(horse.horse_race.jockeyexpectedplaceavg)
        """入厩何走目"""
        x.append(horse.horse_race.nyukyuraces)
        """ローテーション"""
        x.append(horse.horse_race.rotation)
        """上昇度"""
        x.append(horse.horse_race.elevationcd)
        """調教指数"""
        x.append(horse.horse_race.trainingindex)
        """厩舎指数"""
        x.append(horse.horse_race.trainerindex)
        """激走指数"""
        x.append(horse.horse_race.gekisouindex)
        """重適正コード"""
        x.append(horse.horse_race.softfitcd)
        """見習区分"""
        x.append(horse.horse_race.apprenticekbn)
        """芝適正コード"""
        x.append(horse.horse_race.turfaptitudecd)
        """ダ適正コード"""
        x.append(horse.horse_race.dirtaptitudecd)
        """輸送区分"""
        x.append(horse.horse_race.carriagekbn)
        """馬スタート指数"""
        x.append(horse.horse_race.startindex)
        """距離適性1"""
        x.append(horse.horse_race.distanceaptitudecd1)
#             """人気指数"""
#             x.append(horse.horse_race.favindex)
        """調教矢印コード"""
        x.append(horse.horse_race.trainingcd)
        """厩舎評価コード"""
        x.append(horse.horse_race.trainerappraisalcd)
        """ブリンカー"""
        x.append(horse.horse_race.blinkercd)
        """性別"""
        x.append(horse.horse_race.sexcd)
        """激走順位"""
        x.append(horse.horse_race.gekisouranking)
        """LS指数順位"""
        x.append(horse.horse_race.lsindexranking)
        """テン指数順位"""
        x.append(horse.horse_race.first3findexranking)
        """ペース指数順位"""
        x.append(horse.horse_race.paceindexranking)
        """上がり指数順位"""
        x.append(horse.horse_race.last3findexranking)
        """位置指数順位"""
        x.append(horse.horse_race.positionindexranking)
        """降級フラグ"""
        x.append(horse.horse_race.downgradeflg)
        """入厩何日前"""
        x.append(horse.horse_race.nukyudays)
        """放牧先ランク"""
        x.append(horse.horse_race.grazingarearank)
        """厩舎ランク"""
        x.append(horse.horse_race.stablerank)
        """前走レースペース"""
        x.append(horse.pre_race_results[0].racepace)
        """前走距離"""
        x.append(horse.pre_race_results[0].distance)
        """前走単勝人気"""
        x.append(horse.pre_race_results[0].winfav)
        """馬具変更情報"""
        x.append(horse.last_minute.harnesschange)
        """脚元情報"""
        x.append(horse.last_minute.leginfo)
        """馬場状態コード"""
        x.append(horse.last_minute.turfconditioncd)
#         """オッズ印"""
#         x.append(horse.last_minute.oddsmark)
        """パドック印"""
        x.append(horse.last_minute.paddockmark)
        """直前総合印"""
        x.append(horse.last_minute.compositemark)
        """天候コード"""
        x.append(horse.last_minute.weathercd)
        """芝ダ別複勝率"""
        x.append(horse.cond_place_ratio)
        """芝ダ別父馬連対率"""
        if self.race.program.tracktypecd == '1':
            x.append(horse.horse_ex.sireturfquinellaratio)
        elif self.race.program.tracktypecd == '2':
            x.append(horse.horse_ex.siredirtquinellaratio)
        else:
            x.append(0)
        """父馬産駒連対平均距離"""
        x.append(horse.horse_ex.sirequinelladistance)
        """母父馬産駒連対平均距離"""
        x.append(horse.horse_ex.bsirequinelladistance)
        """前走タイム差"""
        x.append(horse.pre_race_results[0].timediff)
        """馬齢"""
        x.append((datetime.datetime.strptime(self.race.program.ymd, '%Y%m%d')
                  - datetime.datetime.strptime(horse.horse_profile.birthday, '%Y%m%d')).days)
        """複勝率"""
        x.append(horse.place_ratio)
        """前走前3Fタイム"""
        x.append(horse.pre_race_results[0].first3ftime)
        """前走後3Fタイム"""
        x.append(horse.pre_race_results[0].last3ftime)
        """脚質"""
        x.append(int(horse.horse_race.racestylecd))
        """負担重量"""
        burden = horse.horse_race.burdenweight
        if horse.horse_profile.sexcd == '2':
            burden += 20
        x.append(burden)
        """賞金総額"""
        x.append(horse.horse_race.earnings)
        """IDM"""
        x.append(horse.last_minute.idm)
#         """騎手指数"""
#         x.append(horse.last_minute.jockeyindex)
        """情報指数"""
        x.append(horse.last_minute.infoindex)
#         """オッズ指数"""
#         x.append(horse.last_minute.oddsindex)
        """パドック指数"""
        x.append(horse.last_minute.paddockindex)
        """総合指数"""
        x.append(horse.last_minute.compositeindex)

        """ave_前走タイム差"""
        x.append(horse.pre_race_results[0].timediff - self.ave_timediff)
        """ave_馬齢"""
        x.append((datetime.datetime.strptime(self.race.program.ymd, '%Y%m%d')
                  - datetime.datetime.strptime(horse.horse_profile.birthday, '%Y%m%d')).days - self.ave_age)
        """ave_複勝率"""
        x.append(horse.place_ratio  - self.ave_place_ratio)
        """ave_前走前3Fタイム"""
        x.append(horse.pre_race_results[0].first3ftime - self.ave_prev_first3f)
        """ave_前走後3Fタイム"""
        x.append(horse.pre_race_results[0].last3ftime - self.ave_prev_last3f)
        """ave_脚質"""
        x.append(int(horse.horse_race.racestylecd) - self.ave_race_style)
        """ave_負担重量"""
        burden = horse.horse_race.burdenweight
        if horse.horse_profile.sexcd == '2':
            burden += 20
        x.append(burden - self.ave_burden)
        """ave_賞金総額"""
        x.append(horse.horse_race.earnings - self.ave_earnings)
        """ave_IDM"""
        x.append(horse.last_minute.idm - self.ave_idm)
#         """ave_騎手指数"""
#         x.append(horse.last_minute.jockeyindex - self.ave_jockey_index)
        """ave_情報指数"""
        x.append(horse.last_minute.infoindex - self.ave_info_index)
#         """ave_オッズ指数"""
#         x.append(horse.last_minute.oddsindex - self.ave_odds_index)
        """ave_パドック指数"""
        x.append(horse.last_minute.paddockindex - self.ave_paddock_index)
        """ave_総合指数"""
        x.append(horse.last_minute.compositeindex - self.ave_composite_index)
        """競馬場周回長さ"""
        x.append(self.race.round_length)
        """競馬場直線長さ"""
        x.append(self.race.straight_length)
        """競馬場高低差"""
        x.append(self.race.height_diff)
        """条件グループコード"""
        x.append(horse.horse_race.raceconditiongroupcd)
        """開催何日目"""
        x.append(self.race.holding.daysinarow)
        """前走から芝ダート変更有無"""
        if horse.pre_race_results[0].tracktypecd == self.race.program.tracktypecd:
            x.append(0)
        else:
            x.append(1)
        """芝種類"""
        if self.race.program.tracktypecd == '1':
            x.append(self.race.holding.turfkind)
        else:
            x.append('0')

        df = pd.DataFrame([x])
        df.columns = ['horseno', 'distance', 'track_type_cd', 'track_rotation_cd', 'track_inout_cd', 'starters_no',
                      'prev_distance_diff', 'horse_weight', 'delta_weight', 'late_start_avg',
                      'jockey_place_ratio', 'nyukyu_races', 'rotation', 'elevation_cd', 'training_index',
                      'trainer_index', 'gekisou_index', 'soft_fit_cd', 'apprentice_kbn', 'turf_aptitude_cd',
                      'dirt_aptitude_cd', 'carriage_kbn', 'start_index', 'distance_aptitude_cd',
#                           'fav_index',
                      'training_cd', 'trainer_appraisal_cd', 'blinker_cd', 'sex_cd', 'gekisou_ranking', 'ls_index_ranking',
                      'first3f_index_ranking', 'pace_index_ranking', 'last3f_index_ranking', 'position_index_ranking',
                      'down_grade_flg', 'nyukyu_days', 'grazing_area_rank', 'stable_rank', 'prev_race_pace', 'prev_distance',
                      'prev_winfav', 'harnes_change', 'leg_info',
                      'turf_condition_cd',
#                       'odds_mark',
                      'paddock_mark', 'composite_mark', 'weather_cd', 'cond_place_ratio',
                      'sire_track_quinella_ratio', 'sire_quinella_distance', 'bsire_quinella_distance', 'prev_time_diff',
                      'age', 'place_ratio', 'prev_first3f_time', 'prev_last3f_time', 'run_style', 'burden',
                      'earnings', 'idm',
#                       'jockey_index',
                      'info_index',
#                       'odds_index',
                      'paddock_index', 'composite_index',
                      'ave_prev_time_diff', 'ave_age', 'ave_place_ratio', 'ave_prev_first3f_time', 'ave_prev_last3f_time',
                      'ave_run_style', 'ave_burden', 'ave_earnings', 'ave_idm',
#                       'ave_jockey_index',
                      'ave_info_index',
#                       'ave_odds_index',
                      'ave_paddock_index', 'ave_composite_index'
                    ,'round_length', 'straight_length', 'height_diff', 'race_condition_group_cd', 'days_in_a_row'
                    ,'track_type_diff', 'turf_kind'
                      ]
        return df

    def get_x(self, order_limit=None):
        if len(self.horses) == 0:
            print(self.race.race_key + ' horses = 0')
        xpd = []
        for horse in self.horses:
            if horse.horse_race is None or horse.last_minute is None or horse.pre_race_results is None or horse.horse_ex is None:
                print(self.race.race_key + ' 属性なし')
                return
            if order_limit is None:
                if horse.race_result.order <= 6 or horse.race_result.winfav == 1:
                    xpd.append(self.set_x_df(horse))
            else:
                if horse.race_result.order <= order_limit or horse.race_result.winfav == 1:
                    xpd.append(self.set_x_df(horse))
        return pd.concat(xpd, axis=0)

    def get_x_all(self):
        if len(self.horses) == 0:
            print(self.race.race_key + ' horses = 0')
        xpd = []
        for horse in self.horses:
            if horse.horse_race is None or horse.last_minute is None or horse.pre_race_results is None or horse.horse_ex is None:
                print(self.race.race_key + ' 属性なし')
                return
            xpd.append(self.set_x_df(horse))
        return pd.concat(xpd, axis=0)

    def set_x_maiden_df(self, horse):
        x = []
        """枠順"""
        x.append(horse.horse_race.horseno)
        """距離"""
        x.append(self.race.program.distance)
        """芝ダ障害コード"""
        x.append(self.race.program.tracktypecd)
        """右左"""
        x.append(self.race.program.trackrotationcd)
        """内外"""
        x.append(self.race.program.trackinoutcd)
        """頭数"""
        x.append(self.race.program.startersno)
        """馬体重"""
        x.append(horse.last_minute.horseweight)
        """騎手期待3着内率"""
        x.append(horse.horse_race.jockeyexpectedplaceavg)
        """上昇度"""
        x.append(horse.horse_race.elevationcd)
        """調教指数"""
        x.append(horse.horse_race.trainingindex)
        """厩舎指数"""
        x.append(horse.horse_race.trainerindex)
        """激走指数"""
        x.append(horse.horse_race.gekisouindex)
        """重適正コード"""
        x.append(horse.horse_race.softfitcd)
        """見習区分"""
        x.append(horse.horse_race.apprenticekbn)
        """芝適正コード"""
        x.append(horse.horse_race.turfaptitudecd)
        """ダ適正コード"""
        x.append(horse.horse_race.dirtaptitudecd)
        """輸送区分"""
        x.append(horse.horse_race.carriagekbn)
        """馬スタート指数"""
        x.append(horse.horse_race.startindex)
        """距離適性1"""
        x.append(horse.horse_race.distanceaptitudecd1)
        """調教矢印コード"""
        x.append(horse.horse_race.trainingcd)
        """厩舎評価コード"""
        x.append(horse.horse_race.trainerappraisalcd)
        """ブリンカー"""
        x.append(horse.horse_race.blinkercd)
        """性別"""
        x.append(horse.horse_race.sexcd)
        """激走順位"""
        x.append(horse.horse_race.gekisouranking)
        """LS指数順位"""
        x.append(horse.horse_race.lsindexranking)
        """テン指数順位"""
        x.append(horse.horse_race.first3findexranking)
        """ペース指数順位"""
        x.append(horse.horse_race.paceindexranking)
        """上がり指数順位"""
        x.append(horse.horse_race.last3findexranking)
        """位置指数順位"""
        x.append(horse.horse_race.positionindexranking)
        """入厩何日前"""
        x.append(horse.horse_race.nukyudays)
        """放牧先ランク"""
        x.append(horse.horse_race.grazingarearank)
        """厩舎ランク"""
        x.append(horse.horse_race.stablerank)
        """馬具変更情報"""
        x.append(horse.last_minute.harnesschange)
        """脚元情報"""
        x.append(horse.last_minute.leginfo)
        """馬場状態コード"""
        x.append(horse.last_minute.turfconditioncd)
#         """オッズ印"""
#         x.append(horse.last_minute.oddsmark)
        """パドック印"""
        x.append(horse.last_minute.paddockmark)
        """直前総合印"""
        x.append(horse.last_minute.compositemark)
        """天候コード"""
        x.append(horse.last_minute.weathercd)
        """芝ダ別父馬連対率"""
        if self.race.program.tracktypecd == '1':
            x.append(horse.horse_ex.sireturfquinellaratio)
        elif self.race.program.tracktypecd == '2':
            x.append(horse.horse_ex.siredirtquinellaratio)
        else:
            x.append(0)
        """父馬産駒連対平均距離"""
        x.append(horse.horse_ex.sirequinelladistance)
        """母父馬産駒連対平均距離"""
        x.append(horse.horse_ex.bsirequinelladistance)
        """馬齢"""
        x.append((datetime.datetime.strptime(self.race.program.ymd, '%Y%m%d')
                  - datetime.datetime.strptime(horse.horse_profile.birthday, '%Y%m%d')).days)
        """脚質"""
        x.append(int(horse.horse_race.racestylecd))
        """負担重量"""
        burden = horse.horse_race.burdenweight
        if horse.horse_profile.sexcd == '2':
            burden += 20
        x.append(burden)
        """IDM"""
        x.append(horse.last_minute.idm)
        """情報指数"""
        x.append(horse.last_minute.infoindex)
#         """オッズ指数"""
#         x.append(horse.last_minute.oddsindex)
        """パドック指数"""
        x.append(horse.last_minute.paddockindex)
        """総合指数"""
        x.append(horse.last_minute.compositeindex)
        """ave_IDM"""
        x.append(horse.last_minute.idm - self.ave_idm)
        """ave_情報指数"""
        x.append(horse.last_minute.infoindex - self.ave_info_index)
#         """ave_オッズ指数"""
#         x.append(horse.last_minute.oddsindex - self.ave_odds_index)
        """ave_パドック指数"""
        x.append(horse.last_minute.paddockindex - self.ave_paddock_index)
        """ave_総合指数"""
        x.append(horse.last_minute.compositeindex - self.ave_composite_index)
#         """競馬場周回長さ"""
#         x.append(self.race.round_length)
#         """競馬場直線長さ"""
#         x.append(self.race.straight_length)
#         """競馬場高低差"""
#         x.append(self.race.height_diff)
#         """条件グループコード"""
#         x.append(horse.horse_race.raceconditiongroupcd)
#         """開催何日目"""
#         x.append(self.race.holding.daysinarow)
        df = pd.DataFrame([x])
        df.columns = ['horseno', 'distance', 'track_type_cd', 'track_rotation_cd', 'track_inout_cd', 'starters_no',
                      'horse_weight',
                      'jockey_place_ratio', 'elevation_cd', 'training_index',
                      'trainer_index', 'gekisou_index', 'soft_fit_cd', 'apprentice_kbn', 'turf_aptitude_cd',
                      'dirt_aptitude_cd', 'carriage_kbn', 'start_index', 'distance_aptitude_cd',
                      'training_cd', 'trainer_appraisal_cd', 'blinker_cd', 'sex_cd', 'gekisou_ranking', 'ls_index_ranking',
                      'first3f_index_ranking', 'pace_index_ranking', 'last3f_index_ranking', 'position_index_ranking',
                      'nyukyu_days', 'grazing_area_rank', 'stable_rank',
                      'harnes_change', 'leg_info',
                      'turf_condition_cd',
#                       'odds_mark',
                      'paddock_mark', 'composite_mark', 'weather_cd',
                      'sire_track_quinella_ratio', 'sire_quinella_distance', 'bsire_quinella_distance',
                      'age', 'run_style', 'burden',
                      'idm',
                      'info_index',
#                       'odds_index',
                      'paddock_index', 'composite_index',
                      'ave_idm',
                      'ave_info_index',
#                       'ave_odds_index',
                      'ave_paddock_index', 'ave_composite_index'
#                       ,'round_length', 'straight_length', 'height_diff', 'race_condition_group_cd', 'days_in_a_row'
                      ]
        return df

    def get_x_maiden(self, order_limit=None):
        if len(self.horses) == 0:
            print(self.race.race_key + ' horses = 0')
        xpd = []
        for horse in self.horses:
            if horse.horse_race is None or horse.last_minute is None or horse.horse_ex is None:
                print(self.race.race_key + ' 属性なし')
                return
            if order_limit is None:
                if horse.race_result.order <= 6 or horse.race_result.winfav == 1:
                    xpd.append(self.set_x_maiden_df(horse))
            else:
                if horse.race_result.order <= order_limit or horse.race_result.winfav == 1:
                    xpd.append(self.set_x_maiden_df(horse))
        return pd.concat(xpd, axis=0)

    def get_x_all_maiden(self):
        if len(self.horses) == 0:
            print(self.race.race_key + ' horses = 0')
        xpd = []
        for horse in self.horses:
            if horse.horse_race is None or horse.last_minute is None or horse.horse_ex is None:
                print(self.race.race_key + ' 属性なし')
                return
            xpd.append(self.set_x_maiden_df(horse))
        return pd.concat(xpd, axis=0)


    def get_y(self, order_limit=None):
        ylist = []
        for horse in self.horses:
            if order_limit is None:
                if horse.race_result.order <= 2:
                    ylist.append(1)
                elif horse.race_result.order <= 6 or horse.race_result.winfav == 1:
                    ylist.append(2)
            else:
                if horse.race_result.order <= order_limit / 2:
                    ylist.append(1)
                elif horse.race_result.order <= order_limit or horse.race_result.winfav == 1:
                    ylist.append(2)
        return ylist

    def get_y_all(self, order_limit=None):
        ylist = []
        for horse in self.horses:
            if order_limit is None:
                if horse.race_result.order <= 2:
                    ylist.append(1)
                else:
                    ylist.append(2)
            else:
                if horse.race_result.order <= order_limit / 2:
                    ylist.append(1)
                else:
                    ylist.append(2)
        return ylist

    def predict(self, clf, sc):
        x = self.get_x_all()
        x_transform = sc.transform(x)
        predict_proba = clf.predict_proba(x_transform)
        d = {}
        for horseno, p in zip(x['horseno'], predict_proba):
            d[horseno] = p[0]
        self.expected = sorted(d.items(), key=lambda x:x[1], reverse=True)

        total = 0.0
        for ex in self.expected:
            total += ex[1]

        self.expected_winodds08 = []
        self.expected_winodds = []
        for ex in self.expected:
            self.expected_winodds08.append(total / ex[1] * 0.8)
            self.expected_winodds.append(total / ex[1])

    def predict_maiden(self, clf, sc):
        x = self.get_x_all_maiden()
        x_transform = sc.transform(x)
        predict_proba = clf.predict_proba(x_transform)
        d = {}
        for horseno, p in zip(x['horseno'], predict_proba):
            d[horseno] = p[0]
        self.expected = sorted(d.items(), key=lambda x:x[1], reverse=True)

        total = 0.0
        for ex in self.expected:
            total += ex[1]

        self.expected_winodds08 = []
        self.expected_winodds = []
        for ex in self.expected:
            self.expected_winodds08.append(total / ex[1] * 0.8)
            self.expected_winodds.append(total / ex[1])

    def get_expected(self, clf, sc):
        self.predict(clf, sc)
        return self.expected

    def get_maiden_expected(self, clf, sc):
        self.predict_maiden(clf, sc)
        return self.expected

    def get_predict_top5(self, clf, sc):
        self.predict(clf, sc)
        horseno_list = []
        for ex in self.expected:
            horseno_list.append(ex[0])
        return horseno_list[:5]

    def get_predict_maiden_top5(self, clf, sc):
        self.predict_maiden(clf, sc)
        horseno_list = []
        for ex in self.expected:
            horseno_list.append(ex[0])
        return horseno_list[:5]

    def predict_2_6(self, clf_2, sc_2, clf_6, sc_6):
        x = self.get_x_all()
        x_transform_2 = sc_2.transform(x)
        predict_proba_2 = clf_2.predict_proba(x_transform_2)
        d = {}
        for horseno, p in zip(x['horseno'], predict_proba_2):
            d[horseno] = p[0]
        expected_2 = sorted(d.items(), key=lambda x:x[1], reverse=True)

        x_transform_6 = sc_6.transform(x)
        predict_proba_6 = clf_6.predict_proba(x_transform_6)
        d = {}
        for horseno, p in zip(x['horseno'], predict_proba_6):
            d[horseno] = p[0]
        expected_6 = sorted(d.items(), key=lambda x:x[1], reverse=True)

        self.expected = expected_2[0:1]
        for ex6 in expected_6:
            for ex in self.expected:
                has_horseno = False
                if ex6[0] == ex[0]:
                    has_horseno = True
                    break
            if not has_horseno:
                self.expected.append(ex6)

    def predict_2_6_expected(self, clf_2, sc_2, clf_6, sc_6):
        x = self.get_x_all()
        x_transform_2 = sc_2.transform(x)
        predict_proba_2 = clf_2.predict_proba(x_transform_2)
        d = {}
        for horseno, p in zip(x['horseno'], predict_proba_2):
            d[horseno] = p[0]
        expected_2 = sorted(d.items(), key=lambda x:x[1], reverse=True)

        x_transform_6 = sc_6.transform(x)
        predict_proba_6 = clf_6.predict_proba(x_transform_6)
        d = {}
        for horseno, p in zip(x['horseno'], predict_proba_6):
            d[horseno] = p[0]
        expected_6 = sorted(d.items(), key=lambda x:x[1], reverse=True)

        ex_1st = None
        for i, ex2 in enumerate(expected_2):
            if self.is_over_proba_50(i, ex2):
                ex_1st = ex2
                break
        if ex_1st is None:
            self.expected = None
            return

        self.expected = [ex_1st]

        for ex6 in expected_6:
            for ex in self.expected:
                has_horseno = False
                if ex6[0] == ex[0]:
                    has_horseno = True
                    break
            if not has_horseno and self.is_over_proba_100_10(i, ex6):
                self.expected.append(ex6)

    def predict_stormy(self, clf_stormy, sc_stormy, clf_expected, sc_expected):
        x = self.get_x_all()
        x_transform_stomy = sc_stormy.transform(x)
        predict_proba_stomy = clf_stormy.predict_proba(x_transform_stomy)
        d = {}
        for horseno, p in zip(x['horseno'], predict_proba_stomy):
            d[horseno] = p[0]
        expected_stomy = sorted(d.items(), key=lambda x:x[1], reverse=True)

        x_transform_expected = sc_expected.transform(x)
        predict_proba_expected = clf_expected.predict_proba(x_transform_expected)
        d = {}
        for horseno, p in zip(x['horseno'], predict_proba_expected):
            d[horseno] = p[0]
        expected_e = sorted(d.items(), key=lambda x:x[1], reverse=True)

        if self.is_under_proba_50(expected_stomy[0]):
            self.expected = expected_e
            if len(self.expected) == 0:
                self.expected = None
        else:
            self.expected = None

    def get_2_6_expected(self, clf_2, sc_2, clf_6, sc_6):
        self.predict_2_6_expected(clf_2, sc_2, clf_6, sc_6)
        return self.expected

    def predict_4_6(self, clf_4, sc_4, clf_6, sc_6):
        x = self.get_x_all()
        x_transform_4 = sc_4.transform(x)
        predict_proba_4 = clf_4.predict_proba(x_transform_4)
        d = {}
        for horseno, p in zip(x['horseno'], predict_proba_4):
            d[horseno] = p[0]
        expected_4 = sorted(d.items(), key=lambda x:x[1], reverse=True)

        x_transform_6 = sc_6.transform(x)
        predict_proba_6 = clf_6.predict_proba(x_transform_6)
        d = {}
        for horseno, p in zip(x['horseno'], predict_proba_6):
            d[horseno] = p[0]
        expected_6 = sorted(d.items(), key=lambda x:x[1], reverse=True)

        self.expected = expected_4[0:2]
        for ex6 in expected_6:
            for ex in self.expected:
                has_horseno = False
                if ex6[0] == ex[0]:
                    has_horseno = True
                    break
            if not has_horseno:
                self.expected.append(ex6)

    def predict_2clf(self, clf1, sc1, clf2, sc2):
        x = self.get_x_all()
        x_transform1 = sc1.transform(x)
        predict_proba1 = clf1.predict_proba(x_transform1)
        d1 = {}
        for horseno, p in zip(x['horseno'], predict_proba1):
            d1[horseno] = p[0]
        self.expected1 = sorted(d1.items(), key=lambda x:x[1], reverse=True)

        x_transform2 = sc2.transform(x)
        predict_proba2 = clf2.predict_proba(x_transform2)
        d2 = {}
        for horseno, p in zip(x['horseno'], predict_proba2):
            d2[horseno] = p[0]
        self.expected2 = sorted(d2.items(), key=lambda x:x[1], reverse=True)

    def get_predict_return(self, n):
        horseno_list = []
        num = 0
        i = 0
        if self.expected is None:
            return None

        while num < n:
            if num >= len(self.expected) or i >= len(self.expected):
                return None
            else:
                horseno_list.append(self.expected[i][0])
                i += 1
                num += 1
        return self.race.payback.get_hit_ret(horseno_list)

    def get_predict_return_nagashi(self, n):
        horseno_list = []
#         num = 0
        if self.expected is None:
            return None
#         while num < n:
#             if num >= len(self.expected):
#                 return None
#             else:
#                 horseno_list.append(self.expected[num][0])
#                 num += 1
#
#         return self.race.payback.get_hit_ret_nagashi(horseno_list)

        for ex in self.expected:
            horseno_list.append(ex[0])

        if len(horseno_list) == n:
            return self.race.payback.get_hit_ret_nagashi(horseno_list)
        else:
            return None

    def get_predict_expected_return(self, n, clf_kind):
        horseno_list = []
#         num = 0
#         i = 0
#         while num < n:
#             if num >= len(self.expected) or i >= len(self.expected):
#                 return None
#             else:
#                 if self.is_over_proba(i, self.expected[i]):
#                     horseno_list.append(self.expected[i][0])
#                     num += 1
#                 i += 1
#
#         return self.race.payback.get_hit_ret(horseno_list)

        i = 0
        if clf_kind == 'randomforest':
            for ex in self.expected:
                if self.is_over_proba_rf(i, ex):
                    horseno_list.append(ex[0])
                    i += 1
        else:
            for ex in self.expected:
                if self.is_over_proba_mlp(i, ex):
                    horseno_list.append(ex[0])
                    i += 1

        if len(horseno_list) == n:
            return self.race.payback.get_hit_ret(horseno_list)
        else:
            return None

    def get_predict_expected_return_maiden(self, n):
        horseno_list = []

        i = 0
        for ex in self.expected:
            if self.is_over_proba_maiden(i, ex):
                horseno_list.append(ex[0])
                i += 1

#         if len(horseno_list) >= n:
#             return self.race.payback.get_hit_ret(horseno_list[:n])
        if len(horseno_list) == n:
            return self.race.payback.get_hit_ret(horseno_list)
        else:
            return None

    def get_predict_stormy_return(self, n):
        horseno_list = []

        for i, ex in enumerate(self.expected):
            if self.is_over_proba_100_10(i, ex):
                horseno_list.append(ex[0])

        if len(horseno_list) == n:
            return self.race.payback.get_hit_ret(horseno_list)
        else:
            return None

    def get_predict_expected_return_fav1(self, n, clf_kind):
        horseno_list = []

        i = 0
        if clf_kind == 'randomforest':
            for ex in self.expected:
                if self.is_over_proba_rf(i, ex):
                    horseno_list.append(ex[0])
                    i += 1
        else:
            for ex in self.expected:
                if self.is_over_proba_high_winodds_mlp(ex):
                    horseno_list.append(ex[0])
                    i += 1

        fav1horseno = self.get_expected_fav1horseno()

        if fav1horseno is not None and len(horseno_list) > 0 and fav1horseno not in horseno_list:
            horseno_list.insert(0, fav1horseno)
        if len(horseno_list) == n:
            return self.race.payback.get_hit_ret_nagashi(horseno_list)
        else:
            return None

    def get_predict_expected_return_nagashi_by_individually(self, n, clf_kind):
        horseno_list = []
        i = 0
        if clf_kind == 'randomforest':
            for ex in self.expected:
                if self.is_over_proba_rf(i, ex):
                    horseno_list.append(ex[0])
                    i += 1
        else:
            for ex in self.expected:
                if self.is_over_proba_mlp(i, ex):
                    horseno_list.append(ex[0])
                    i += 1

        if len(horseno_list) == n:
            return self.race.payback.get_hit_ret_nagashi(horseno_list)
        else:
            return None

    def get_predict_expected_return_nagashi_by_seq(self, n, clf_kind):
        horseno_list = []
        i = 0
        if clf_kind == 'randomforest':
            for ex in self.expected:
                if self.is_over_proba_rf(i, ex):
                    horseno_list.append(ex[0])
                    i += 1
        else:
            for ex in self.expected:
                if self.is_over_proba_mlp(i, ex):
                    horseno_list.append(ex[0])
                    i += 1
        if len(horseno_list) < n:
            return None

        # return self.race.payback.get_hit_ret_nagashi(horseno_list[:n])
        if n == 1:
            return self.race.payback.get_hit_ret_nagashi([horseno_list[0]])
        else:
            return self.race.payback.get_hit_ret_nagashi([horseno_list[0], horseno_list[n-1]])

    def get_predict_return_2clf(self, n):
        horseno_list1 = []
        horseno_list2 = []
        i = 0
        while i < n:
            if i >= len(self.expected1) or i >= len(self.expected2):
                return None
            else:
                horseno_list1.append(self.expected1[i][0])
                horseno_list2.append(self.expected2[i][0])
                i += 1

        if len(set(horseno_list1).difference(set(horseno_list2))) != 0:
            return None

        return self.race.payback.get_hit_ret(horseno_list1)

    def print_expected(self):
        yyyymmdd = datetime.datetime.now().strftime('%Y%m%d')
        with open('./expected/' + self.argstr[:-1] + '_' + yyyymmdd + '.log', 'a', encoding='utf-8') as f:
            for p, winodds08, winodds in zip(self.expected, self.expected_winodds08, self.expected_winodds):
                f.write('{0}\t{1}\t{2}\t{3}\t{4}\t{5}\t{6}\t{7}\t{8}\t{9}\t{10}\t{11}\t{12}\t{13}\n'
                        .format(RACE_COURSE[int(self.race.race_key[:2]) - 1],
                                self.race.program.ymd,
                                self.race.program.classificationcd,
                                self.race.program.raceconditioncd,
                                self.race.program.tracktypecd,
                                self.race.program.distance,
                                p[1],
                                self.get_horse_by_horseno(p[0]).last_minute.winodds,
                                self.get_horse_by_horseno(p[0]).last_minute.placeodds,
                                self.race.payback.get_win_ret([p[0]]),
                                self.race.payback.get_place_ret([p[0]]),
                                self.get_horse_by_horseno(p[0]).race_result.order,
                                winodds08,
                                winodds))

    def print_predict_winodds_over_10(self):
        yyyymmdd = datetime.datetime.now().strftime('%Y%m%d')
        with open('./expected/' + self.argstr[:-1] + '_' + yyyymmdd + '.log', 'a', encoding='utf-8') as f:
            for ex in self.expected:
                if ex[1] >= 0.3 and self.get_horse_by_horseno(ex[0]).last_minute.winodds >= 10:
                    f.write('{0},{1},{2}\n'.format(ex[1], self.race.payback.get_win_ret([ex[0]]), self.race.payback.get_place_ret([ex[0]])))

    def get_win_odds(self, horseno):
        return self.get_horse_by_horseno(horseno).last_minute.winodds

    def get_horse_name(self, horseno):
        return self.get_horse_by_horseno(horseno).get_name()

    def get_fav1horseno(self):
        min_winodds = self.horses[0].last_minute.winodds
        ret = self.horses[0].horse_race.horseno
        for i in range(1, len(self.horses)):
            if self.horses[i].last_minute.winodds < min_winodds:
                min_winodds = self.horses[i].last_minute.winodds
                ret = self.horses[i].horse_race.horseno
        return ret

    def get_expected_fav1horseno(self):
        for ex in self.expected:
            if self.is_over_average_proba_mlp(ex):
                return ex[0]
        return None

#     def is_over_proba(self, i, ex):
#         if ((ex[1] >= 0.9 and self.get_win_odds(ex[0]) >= 7 and self.get_win_odds(ex[0]) <= 15)
#             or (i > 1 and ex[1] >= 0.8 and ex[1] < 0.9 and self.get_win_odds(ex[0]) >= 15 and self.get_win_odds(ex[0]) <= 30)):
#             return True
#         else:
#             return False
#     def is_over_proba(self, i, ex):
#         if ex[1] >= 0.6:
#             return True
#         else:
#             return False

    def is_over_proba_mlp(self, i, ex):
        if ((ex[1] >= 0.6 and self.get_win_odds(ex[0]) > 3 and self.get_win_odds(ex[0]) <= 5)
            or (ex[1] >= 0.56 and self.get_win_odds(ex[0]) > 5 and self.get_win_odds(ex[0]) <= 6)
            or (ex[1] >= 0.55 and self.get_win_odds(ex[0]) > 6 and self.get_win_odds(ex[0]) <= 8)
            or (ex[1] >= 0.52 and self.get_win_odds(ex[0]) > 8 and self.get_win_odds(ex[0]) <= 9)
            or (ex[1] >= 0.49 and self.get_win_odds(ex[0]) > 9 and self.get_win_odds(ex[0]) <= 10)
            or (ex[1] >= 0.45 and self.get_win_odds(ex[0]) > 10 and self.get_win_odds(ex[0]) <= 11)
            or (ex[1] >= 0.44 and self.get_win_odds(ex[0]) > 11 and self.get_win_odds(ex[0]) <= 12)
            or (ex[1] >= 0.42 and self.get_win_odds(ex[0]) > 12 and self.get_win_odds(ex[0]) <= 14)
            or (ex[1] >= 0.41 and self.get_win_odds(ex[0]) > 14 and self.get_win_odds(ex[0]) <= 15)
            or (ex[1] >= 0.35 and self.get_win_odds(ex[0]) > 15 and self.get_win_odds(ex[0]) <= 17)
            or (ex[1] >= 0.34 and self.get_win_odds(ex[0]) > 17 and self.get_win_odds(ex[0]) <= 18)):

#         if ((ex[1] >= 0.65 and self.get_win_odds(ex[0]) > 1.5 and self.get_win_odds(ex[0]) <= 2)
#             or (ex[1] >= 0.6 and self.get_win_odds(ex[0]) > 2 and self.get_win_odds(ex[0]) <= 3)
#             or (ex[1] >= 0.53 and self.get_win_odds(ex[0]) > 3 and self.get_win_odds(ex[0]) <= 7)
#             or (ex[1] >= 0.5 and self.get_win_odds(ex[0]) > 7 and self.get_win_odds(ex[0]) <= 10)
#             or (ex[1] >= 0.47 and self.get_win_odds(ex[0]) > 10 and self.get_win_odds(ex[0]) <= 12)
#             or (ex[1] >= 0.46 and self.get_win_odds(ex[0]) > 12 and self.get_win_odds(ex[0]) <= 30)
#             or (ex[1] >= 0.4 and self.get_win_odds(ex[0]) > 30 and self.get_win_odds(ex[0]) <= 50)):

            return True
        else:
            return False

    def is_over_proba_high_winodds_mlp(self, ex):
        if ((ex[1] >= 0.4 and self.get_win_odds(ex[0]) > 6 and self.get_win_odds(ex[0]) <= 9)
            or (ex[1] >= 0.35 and self.get_win_odds(ex[0]) > 9 and self.get_win_odds(ex[0]) <= 18)
            or (ex[1] >= 0.3 and self.get_win_odds(ex[0]) > 18)):
            return True
        else:
            return False

    def is_over_average_proba_mlp(self, ex):
        if ((ex[1] >= 0.6 and self.get_win_odds(ex[0]) <= 1.5)
            or (ex[1] >= 0.55 and self.get_win_odds(ex[0]) > 1.5 and self.get_win_odds(ex[0]) <= 2)
            or (ex[1] >= 0.5 and self.get_win_odds(ex[0]) > 2 and self.get_win_odds(ex[0]) <= 3)
            or (ex[1] >= 0.45 and self.get_win_odds(ex[0]) > 3 and self.get_win_odds(ex[0]) <= 4)
            or (ex[1] >= 0.4 and self.get_win_odds(ex[0]) > 4 and self.get_win_odds(ex[0]) <= 5)
            or (ex[1] >= 0.35 and self.get_win_odds(ex[0]) > 5 and self.get_win_odds(ex[0]) <= 7)
            or (ex[1] >= 0.25 and self.get_win_odds(ex[0]) > 7 and self.get_win_odds(ex[0]) <= 14)
            or (ex[1] >= 0.2 and self.get_win_odds(ex[0]) > 14)):
            return True
        else:
            return False

    def is_over_proba_rf(self, i, ex):
        if ((ex[1] >= 0.65 and self.get_win_odds(ex[0]) <= 1.5)
            or (ex[1] >= 0.62 and self.get_win_odds(ex[0]) > 1.5 and self.get_win_odds(ex[0]) <= 2)
            or (ex[1] >= 0.57 and self.get_win_odds(ex[0]) > 2 and self.get_win_odds(ex[0]) <= 3)
            or (ex[1] >= 0.53 and self.get_win_odds(ex[0]) > 3 and self.get_win_odds(ex[0]) <= 4)
            or (ex[1] >= 0.49 and self.get_win_odds(ex[0]) > 4 and self.get_win_odds(ex[0]) <= 5)
            or (ex[1] >= 0.47 and self.get_win_odds(ex[0]) > 5 and self.get_win_odds(ex[0]) <= 6)
            or (ex[1] >= 0.44 and self.get_win_odds(ex[0]) > 6 and self.get_win_odds(ex[0]) <= 7)
            or (ex[1] >= 0.42 and self.get_win_odds(ex[0]) > 7 and self.get_win_odds(ex[0]) <= 8)
            or (ex[1] >= 0.39 and self.get_win_odds(ex[0]) > 8 and self.get_win_odds(ex[0]) <= 9)
            or (ex[1] >= 0.38 and self.get_win_odds(ex[0]) > 9 and self.get_win_odds(ex[0]) <= 11)
            or (i > 0 and ex[1] >= 0.37 and self.get_win_odds(ex[0]) > 11 and self.get_win_odds(ex[0]) <= 12)
            or (i > 0 and ex[1] >= 0.35 and self.get_win_odds(ex[0]) > 12 and self.get_win_odds(ex[0]) <= 13)
            or (i > 0 and ex[1] >= 0.34 and self.get_win_odds(ex[0]) > 13 and self.get_win_odds(ex[0]) <= 14)
            or (i > 0 and ex[1] >= 0.33 and self.get_win_odds(ex[0]) > 14 and self.get_win_odds(ex[0]) <= 15)
            or (i > 0 and ex[1] >= 0.32 and self.get_win_odds(ex[0]) > 15 and self.get_win_odds(ex[0]) <= 16)
            or (i > 0 and ex[1] >= 0.31 and self.get_win_odds(ex[0]) > 16 and self.get_win_odds(ex[0]) <= 18)
            or (i > 0 and ex[1] >= 0.29 and self.get_win_odds(ex[0]) > 18 and self.get_win_odds(ex[0]) <= 20)
            or (i > 0 and ex[1] >= 0.27 and self.get_win_odds(ex[0]) > 20 and self.get_win_odds(ex[0]) <= 30)
            or (i > 0 and ex[1] >= 0.24 and self.get_win_odds(ex[0]) > 30 and self.get_win_odds(ex[0]) <= 40)
            or (i > 0 and ex[1] >= 0.22 and self.get_win_odds(ex[0]) > 40 and self.get_win_odds(ex[0]) <= 50)):
            return True
        else:
            return False

    def is_over_proba_rf_win(self, ex):
        if ((ex[1] >= 0.47 and self.get_win_odds(ex[0]) > 5 and self.get_win_odds(ex[0]) <= 6)
            or (ex[1] >= 0.44 and self.get_win_odds(ex[0]) > 6 and self.get_win_odds(ex[0]) <= 7)
            or (ex[1] >= 0.42 and self.get_win_odds(ex[0]) > 7 and self.get_win_odds(ex[0]) <= 8)
            or (ex[1] >= 0.39 and self.get_win_odds(ex[0]) > 8 and self.get_win_odds(ex[0]) <= 9)
            or (ex[1] >= 0.38 and self.get_win_odds(ex[0]) > 9 and self.get_win_odds(ex[0]) <= 11)):
            return True
        else:
            return False

    def is_over_proba_maiden(self, i, ex):
        if (
            (ex[1] >= 0.6 and self.get_win_odds(ex[0]) <= 2)
            or (ex[1] >= 0.54 and self.get_win_odds(ex[0]) > 2 and self.get_win_odds(ex[0]) <= 3)
            or (ex[1] >= 0.5 and self.get_win_odds(ex[0]) > 3 and self.get_win_odds(ex[0]) <= 4)
            or (ex[1] >= 0.48 and self.get_win_odds(ex[0]) > 4 and self.get_win_odds(ex[0]) <= 5)
            or (ex[1] >= 0.45 and self.get_win_odds(ex[0]) > 5 and self.get_win_odds(ex[0]) <= 6)
            or (ex[1] >= 0.42 and self.get_win_odds(ex[0]) > 6 and self.get_win_odds(ex[0]) <= 7)
            or (ex[1] >= 0.38 and self.get_win_odds(ex[0]) > 7 and self.get_win_odds(ex[0]) <= 8)
            or (ex[1] >= 0.36 and self.get_win_odds(ex[0]) > 8 and self.get_win_odds(ex[0]) <= 9)
            or (ex[1] >= 0.31 and self.get_win_odds(ex[0]) > 9 and self.get_win_odds(ex[0]) <= 10)
            or (ex[1] >= 0.29 and self.get_win_odds(ex[0]) > 10 and self.get_win_odds(ex[0]) <= 11)
            or (ex[1] >= 0.28 and self.get_win_odds(ex[0]) > 11 and self.get_win_odds(ex[0]) <= 12)
            or (ex[1] >= 0.27 and self.get_win_odds(ex[0]) > 12 and self.get_win_odds(ex[0]) <= 13)
            or (ex[1] >= 0.25 and self.get_win_odds(ex[0]) > 13 and self.get_win_odds(ex[0]) <= 14)
            or (ex[1] >= 0.22 and self.get_win_odds(ex[0]) > 14 and self.get_win_odds(ex[0]) <= 15)
            or (ex[1] >= 0.21 and self.get_win_odds(ex[0]) > 15 and self.get_win_odds(ex[0]) <= 16)
            or (ex[1] >= 0.2 and self.get_win_odds(ex[0]) > 16)):
#         if ((ex[1] >= 0.53 and self.get_win_odds(ex[0]) > 3 and self.get_win_odds(ex[0]) <= 7)
#             or (ex[1] >= 0.5 and self.get_win_odds(ex[0]) > 7 and self.get_win_odds(ex[0]) <= 10)
#             or (ex[1] >= 0.47 and self.get_win_odds(ex[0]) > 10 and self.get_win_odds(ex[0]) <= 12)):
#         if ((ex[1] >= 0.65 and self.get_win_odds(ex[0]) <= 3)
#             or (ex[1] >= 0.6 and self.get_win_odds(ex[0]) > 3 and self.get_win_odds(ex[0]) <= 7)
#             or (ex[1] >= 0.5 and self.get_win_odds(ex[0]) > 7)):
            return True
        else:
            return False

    def is_over_proba_50(self, i, ex):
        if ((ex[1] >= 0.55 and self.get_win_odds(ex[0]) > 3 and self.get_win_odds(ex[0]) <= 7)
            or (ex[1] >= 0.5 and self.get_win_odds(ex[0]) > 7)):
            return True
        else:
            return False

    def is_over_proba_100_10(self, i, ex):
        if ((ex[1] >= 0.9 and self.get_win_odds(ex[0]) >= 7 and self.get_win_odds(ex[0]) <= 15)
            or (i > 1 and ex[1] >= 0.8 and ex[1] < 0.9 and self.get_win_odds(ex[0]) >= 15 and self.get_win_odds(ex[0]) <= 30)):
            return True
        else:
            return False

    def is_under_proba_50(self, ex):
        if ex[1] < 0.6:
            return True
        else:
            return False


class Race(object):
    """
    レースクラス
    """

    def __init__(self, race_key, program, holding, payback=None):
        """
        :param race_key: レースキー
        :param program: 番組情報
        :param holding: 開催情報
        :param payback: 払戻情報
        """
        self.race_key = race_key
        self.program = program
        self.holding = holding
        self.payback = payback
        self.set_race_info()

    def set_race_info(self):
        if self.race_key[:2] == '01':
            if self.program.tracktypecd == '1':
                self.round_length = 1660
                self.straight_length = 269
                self.height_diff = 0.6
            elif self.program.tracktypecd == '2':
                self.round_length = 1487
                self.straight_length = 264
                self.height_diff = 0.0
        elif self.race_key[:2] == '02':
            if self.program.tracktypecd == '1':
                self.round_length = 1677
                self.straight_length = 300
                self.height_diff = 3.4
            elif self.program.tracktypecd == '2':
                self.round_length = 1476
                self.straight_length = 260
                self.height_diff = 3.4
        elif self.race_key[:2] == '03':
            if self.program.tracktypecd == '1':
                self.round_length = 1628
                self.straight_length = 300
                self.height_diff = 1.8
            elif self.program.tracktypecd == '2':
                self.round_length = 1445
                self.straight_length = 296
                self.height_diff = 2.1
        elif self.race_key[:2] == '04':
            if self.program.tracktypecd == '1':
                if self.program.trackinoutcd == '1':
                    self.round_length = 1648
                    self.straight_length = 359
                    self.height_diff = 0.7
                elif self.program.trackinoutcd == '2':
                    self.round_length = 2248
                    self.straight_length = 659
                    self.height_diff = 2.2
                else:
                    self.round_length = 1000
                    self.straight_length = 1000
                    self.height_diff = 2.2
            elif self.program.tracktypecd == '2':
                self.round_length = 1472
                self.straight_length = 354
                self.height_diff = 0.5
        elif self.race_key[:2] == '05':
            if self.program.tracktypecd == '1':
                self.round_length = 2140
                self.straight_length = 526
                self.height_diff = 2.7
            elif self.program.tracktypecd == '2':
                self.round_length = 1899
                self.straight_length = 502
                self.height_diff = 2.5
        elif self.race_key[:2] == '06':
            if self.program.tracktypecd == '1':
                if self.program.trackinoutcd == '1':
                    self.round_length = 1705
                elif self.program.trackinoutcd == '2':
                    self.round_length = 1877
                self.straight_length = 310
                self.height_diff = 5.3
            elif self.program.tracktypecd == '2':
                self.round_length = 1493
                self.straight_length = 308
                self.height_diff = 4.4
        elif self.race_key[:2] == '07':
            if self.program.tracktypecd == '1':
                self.round_length = 1725
                self.straight_length = 413
                self.height_diff = 3.5
            elif self.program.tracktypecd == '2':
                self.round_length = 1530
                self.straight_length = 312
                self.height_diff = 3.4
        elif self.race_key[:2] == '08':
            if self.program.tracktypecd == '1':
                if self.program.trackinoutcd == '1':
                    self.round_length = 1840
                    self.straight_length = 323
                    self.height_diff = 3.1
                elif self.program.trackinoutcd == '2':
                    self.round_length = 1951
                    self.straight_length = 399
                    self.height_diff = 4.3
            elif self.program.tracktypecd == '2':
                self.round_length = 1608
                self.straight_length = 329
                self.height_diff = 3.0
        elif self.race_key[:2] == '09':
            if self.program.tracktypecd == '1':
                if self.program.trackinoutcd == '1':
                    self.round_length = 1713
                    self.straight_length = 359
                    self.height_diff = 1.8
                elif self.program.trackinoutcd == '2':
                    self.round_length = 2113
                    self.straight_length = 476
                    self.height_diff = 2.3
            elif self.program.tracktypecd == '2':
                self.round_length = 1518
                self.straight_length = 353
                self.height_diff = 1.5
        elif self.race_key[:2] == '10':
            if self.program.tracktypecd == '1':
                self.round_length = 1652
                self.straight_length = 293
                self.height_diff = 3.0
            elif self.program.tracktypecd == '2':
                self.round_length = 1445
                self.straight_length = 291
                self.height_diff = 2.9

    def get_race_title(self):
        """
        レース名を取得する
        """
        return self.program.racename

class Program(Base):
    """
    番組クラス
    """
    __tablename__ = 'bac'
    racekey = Column(String, primary_key=True)
    ymd = Column(String)
    hhmm = Column(String)
    distance = Column(Integer)
    tracktypecd = Column(String)
    trackrotationcd = Column(String)
    trackinoutcd = Column(String)
    classificationcd = Column(String)
    raceconditioncd = Column(String)
    symbolcd = Column(String)
    weightcd = Column(String)
    gradecd = Column(String)
    racename = Column(String)
    times = Column(String)
    startersno = Column(Integer)
    coursecd = Column(String)
    stagekbn = Column(String)
    raceshortname = Column(String)
    racename9 = Column(String)
    datakbn = Column(String)
    prizemoney1st = Column(Integer)
    prizemoney2nd = Column(Integer)
    prizemoney3rd = Column(Integer)
    prizemoney4th = Column(Integer)
    prizemoney5th = Column(Integer)
    prizemoneyin1st = Column(Integer)
    prizemoneyin2nd = Column(Integer)
    winflg = Column(String)
    placeflg = Column(String)
    bracketquinellaflg = Column(String)
    quinellaflg = Column(String)
    exactaflg = Column(String)
    quinellaplaceflg = Column(String)
    trioflg = Column(String)
    trifectaflg = Column(String)
    reserveflg = Column(String)
    win5flg = Column(String)

    def get_race_title(self):
        """
        レース名を取得する
        """
        return self.racename

    def get_race_info(self):
        return self.ymd + ' ' + self.hhmm[:2] + ':' + self.hhmm[2:] + ' ' + RACE_COURSE[int(self.racekey[:2]) - 1] + self.racekey[-2:] + 'R ' + self.racename

    def get_bet_file_name(self):
        return self.ymd[2:] + RACE_COURSE[int(self.racekey[:2]) - 1] + self.racekey[-2:] + 'R.SBT'


class Holding(Base):
    """
    開催クラス
    """
    __tablename__ = 'kab'
    stagekey = Column(String, primary_key=True)
    ymd = Column(String)
    stagekbn = Column(String)
    dayofweek = Column(String)
    racecoursename = Column(String)
    weathercd = Column(String)
    turfconditoncd = Column(String)
    turfconditoninside = Column(String)
    turfconditonmiddle = Column(String)
    turfconditonoutside = Column(String)
    turfconditondiff = Column(String)
    straightconditoninnermost = Column(String)
    straightconditoninside = Column(String)
    straightconditonmiddle = Column(String)
    straightconditonoutside = Column(String)
    straightconditonoutermost = Column(String)
    dirtconditoncd = Column(String)
    dirtconditoninside = Column(String)
    dirtconditonmiddle = Column(String)
    dirtconditonoutside = Column(String)
    dirtconditondiff = Column(String)
    datakbn = Column(String)
    daysinarow = Column(String)
    turfkind = Column(String)
    plantlength = Column(REAL)
    compaction = Column(String)
    antifreezing = Column(String)
    middleprecipitation = Column(REAL)

class Payback(Base):
    """
    払戻クラス
    """
    __tablename__ = 'hjc'
    racekey = Column(String, primary_key=True)
    winno1 = Column(String)
    winpayout1 = Column(Integer)
    winno2 = Column(String)
    winpayout2 = Column(Integer)
    winno3 = Column(String)
    winpayout3 = Column(Integer)
    placeno1 = Column(String)
    placepayout1 = Column(Integer)
    placeno2 = Column(String)
    placepayout2 = Column(Integer)
    placeno3 = Column(String)
    placepayout3 = Column(Integer)
    placeno4 = Column(String)
    placepayout4 = Column(Integer)
    placeno5 = Column(String)
    placepayout5 = Column(Integer)
    bracketquinellano1 = Column(String)
    bracketquinellapayout1 = Column(Integer)
    bracketquinellano2 = Column(String)
    bracketquinellapayout2 = Column(Integer)
    bracketquinellano3 = Column(String)
    bracketquinellapayout3 = Column(Integer)
    quinellano1 = Column(String)
    quinellapayout1 = Column(Integer)
    quinellano2 = Column(String)
    quinellapayout2 = Column(Integer)
    quinellano3 = Column(String)
    quinellapayout3 = Column(Integer)
    quinellaplaceno1 = Column(String)
    quinellaplacepayout1 = Column(Integer)
    quinellaplaceno2 = Column(String)
    quinellaplacepayout2 = Column(Integer)
    quinellaplaceno3 = Column(String)
    quinellaplacepayout3 = Column(Integer)
    quinellaplaceno4 = Column(String)
    quinellaplacepayout4 = Column(Integer)
    quinellaplaceno5 = Column(String)
    quinellaplacepayout5 = Column(Integer)
    quinellaplaceno6 = Column(String)
    quinellaplacepayout6 = Column(Integer)
    quinellaplaceno7 = Column(String)
    quinellaplacepayout7 = Column(Integer)
    exactano1 = Column(String)
    exactapayout1 = Column(Integer)
    exactano2 = Column(String)
    exactapayout2 = Column(Integer)
    exactano3 = Column(String)
    exactapayout3 = Column(Integer)
    exactano4 = Column(String)
    exactapayout4 = Column(Integer)
    exactano5 = Column(String)
    exactapayout5 = Column(Integer)
    exactano6 = Column(String)
    exactapayout6 = Column(Integer)
    triono1 = Column(String)
    triopayout1 = Column(Integer)
    triono2 = Column(String)
    triopayout2 = Column(Integer)
    triono3 = Column(String)
    triopayout3 = Column(Integer)
    trifectano1 = Column(String)
    trifectapayout1 = Column(Integer)
    trifectano2 = Column(String)
    trifectapayout2 = Column(Integer)
    trifectano3 = Column(String)
    trifectapayout3 = Column(Integer)
    trifectano4 = Column(String)
    trifectapayout4 = Column(Integer)
    trifectano5 = Column(String)
    trifectapayout5 = Column(Integer)
    trifectano6 = Column(String)
    trifectapayout6 = Column(Integer)

    def get_win_hit(self, horseno_list):
        for horseno in horseno_list:
            if horseno in (self.winno1, self.winno2, self.winno3):
                return 1.0
        return 0.0

    def get_win_ret(self, horseno_list):
        payout = 0.0
        for horseno in horseno_list:
            if horseno == self.winno1:
                payout += self.winpayout1
            elif horseno == self.winno2:
                payout += self.winpayout2
            elif horseno == self.winno3:
                payout += self.winpayout3
        return payout / len(horseno_list)  / 100

    def get_place_hit(self, horseno_list):
        for horseno in horseno_list:
            if horseno in (self.placeno1, self.placeno2, self.placeno3, self.placeno4, self.placeno5):
                return 1.0
        return 0.0

    def get_place_ret(self, horseno_list):
        payout = 0.0
        for horseno in horseno_list:
            if horseno == self.placeno1:
                payout += self.placepayout1
            elif horseno == self.placeno2:
                payout += self.placepayout2
            elif horseno == self.placeno3:
                payout += self.placepayout3
            elif horseno == self.placeno4:
                payout += self.placepayout4
            elif horseno == self.placeno5:
                payout += self.placepayout5
        return payout / len(horseno_list)  / 100

    def get_quinella_hit(self, horseno_list):
        clist = list(itertools.combinations(horseno_list, 2));
        for c in clist:
            if c[0] + c[1] in (self.quinellano1, self.quinellano2, self.quinellano3):
                return 1.0
            elif c[1] + c[0] in (self.quinellano1, self.quinellano2, self.quinellano3):
                return 1.0
        return 0.0

    def get_quinella_ret(self, horseno_list):
        payout = 0.0
        clist = list(itertools.combinations(horseno_list, 2));
        for c in clist:
            if c[0] + c[1] == self.quinellano1 or c[1] + c[0] == self.quinellano1:
                payout += self.quinellapayout1
            elif c[0] + c[1] == self.quinellano2 or c[1] + c[0] == self.quinellano2:
                payout += self.quinellapayout2
            elif c[0] + c[1] == self.quinellano3 or c[1] + c[0] == self.quinellano3:
                payout += self.quinellapayout3
        return payout / len(clist)  / 100

    def get_quinella_hit_nagashi(self, horseno_list):
        for horseno in horseno_list:
            if horseno_list[0] + horseno in (self.quinellano1, self.quinellano2, self.quinellano3):
                return 1.0
            elif horseno + horseno_list[0] in (self.quinellano1, self.quinellano2, self.quinellano3):
                return 1.0
        return 0.0

    def get_quinella_ret_nagashi(self, horseno_list):
        payout = 0.0
        for horseno in horseno_list:
            if horseno_list[0] + horseno == self.quinellano1 or horseno + horseno_list[0] == self.quinellano1:
                payout += self.quinellapayout1
            elif horseno_list[0] + horseno == self.quinellano2 or horseno + horseno_list[0] == self.quinellano2:
                payout += self.quinellapayout2
            elif horseno_list[0] + horseno == self.quinellano3 or horseno + horseno_list[0] == self.quinellano3:
                payout += self.quinellapayout3
        return payout / (len(horseno_list) - 1)  / 100

    def get_quinella_place_hit(self, horseno_list):
        clist = list(itertools.combinations(horseno_list, 2));
        for c in clist:
            if c[0] + c[1] in (self.quinellaplaceno1, self.quinellaplaceno2, self.quinellaplaceno3, self.quinellaplaceno4, self.quinellaplaceno5, self.quinellaplaceno6, self.quinellaplaceno7):
                return 1.0
            elif c[1] + c[0] in (self.quinellaplaceno1, self.quinellaplaceno2, self.quinellaplaceno3, self.quinellaplaceno4, self.quinellaplaceno5, self.quinellaplaceno6, self.quinellaplaceno7):
                return 1.0
        return 0.0

    def get_quinella_place_ret(self, horseno_list):
        payout = 0.0
        clist = list(itertools.combinations(horseno_list, 2));
        for c in clist:
            if c[0] + c[1] == self.quinellaplaceno1 or c[1] + c[0] == self.quinellaplaceno1:
                payout += self.quinellaplacepayout1
            elif c[0] + c[1] == self.quinellaplaceno2 or c[1] + c[0] == self.quinellaplaceno2:
                payout += self.quinellaplacepayout2
            elif c[0] + c[1] == self.quinellaplaceno3 or c[1] + c[0] == self.quinellaplaceno3:
                payout += self.quinellaplacepayout3
            elif c[0] + c[1] == self.quinellaplaceno4 or c[1] + c[0] == self.quinellaplaceno4:
                payout += self.quinellaplacepayout4
            elif c[0] + c[1] == self.quinellaplaceno5 or c[1] + c[0] == self.quinellaplaceno5:
                payout += self.quinellaplacepayout5
            elif c[0] + c[1] == self.quinellaplaceno6 or c[1] + c[0] == self.quinellaplaceno6:
                payout += self.quinellaplacepayout6
            elif c[0] + c[1] == self.quinellaplaceno7 or c[1] + c[0] == self.quinellaplaceno7:
                payout += self.quinellaplacepayout7
        return payout / len(clist)  / 100

    def get_quinella_place_hit_nagashi(self, horseno_list):
        for horseno in horseno_list:
            if horseno_list[0] + horseno in (self.quinellaplaceno1, self.quinellaplaceno2, self.quinellaplaceno3, self.quinellaplaceno4, self.quinellaplaceno5, self.quinellaplaceno6, self.quinellaplaceno7):
                return 1.0
            elif horseno + horseno_list[0] in (self.quinellaplaceno1, self.quinellaplaceno2, self.quinellaplaceno3, self.quinellaplaceno4, self.quinellaplaceno5, self.quinellaplaceno6, self.quinellaplaceno7):
                return 1.0
        return 0.0

    def get_quinella_place_ret_nagashi(self, horseno_list):
        payout = 0.0
        for horseno in horseno_list:
            if horseno_list[0] + horseno == self.quinellaplaceno1 or horseno + horseno_list[0] == self.quinellaplaceno1:
                payout += self.quinellaplacepayout1
            elif horseno_list[0] + horseno == self.quinellaplaceno2 or horseno + horseno_list[0] == self.quinellaplaceno2:
                payout += self.quinellaplacepayout2
            elif horseno_list[0] + horseno == self.quinellaplaceno3 or horseno + horseno_list[0] == self.quinellaplaceno3:
                payout += self.quinellaplacepayout3
            elif horseno_list[0] + horseno == self.quinellaplaceno4 or horseno + horseno_list[0] == self.quinellaplaceno4:
                payout += self.quinellaplacepayout4
            elif horseno_list[0] + horseno == self.quinellaplaceno5 or horseno + horseno_list[0] == self.quinellaplaceno5:
                payout += self.quinellaplacepayout5
            elif horseno_list[0] + horseno == self.quinellaplaceno6 or horseno + horseno_list[0] == self.quinellaplaceno6:
                payout += self.quinellaplacepayout6
            elif horseno_list[0] + horseno == self.quinellaplaceno7 or horseno + horseno_list[0] == self.quinellaplaceno7:
                payout += self.quinellaplacepayout7
        return payout / (len(horseno_list) - 1)  / 100

    def get_exacta_hit(self, horseno_list):
        plist = list(itertools.permutations(horseno_list, 2))
        for p in plist:
            if p[0] + p[1] in (self.exactano1, self.exactano2, self.exactano3, self.exactano4, self.exactano5, self.exactano6):
                return 1.0
        return 0.0

    def get_exacta_ret(self, horseno_list):
        payout = 0.0
        plist = list(itertools.permutations(horseno_list, 2))
        for p in plist:
            if p[0] + p[1] == self.exactano1:
                payout += self.exactapayout1
            elif p[0] + p[1] == self.exactano2:
                payout += self.exactapayout2
            elif p[0] + p[1] == self.exactano3:
                payout += self.exactapayout3
            elif p[0] + p[1] == self.exactano4:
                payout += self.exactapayout4
            elif p[0] + p[1] == self.exactano5:
                payout += self.exactapayout5
            elif p[0] + p[1] == self.exactano6:
                payout += self.exactapayout6
        return payout / len(plist)  / 100

    def get_exacta_hit_nagashi(self, horseno_list):
        for horseno in horseno_list:
            if horseno_list[0] + horseno in (self.exactano1, self.exactano2, self.exactano3, self.exactano4, self.exactano5, self.exactano6):
                return 1.0
            elif horseno + horseno_list[0] in (self.exactano1, self.exactano2, self.exactano3, self.exactano4, self.exactano5, self.exactano6):
                return 1.0
        return 0.0

    def get_exacta_ret_nagashi(self, horseno_list):
        payout = 0.0
        for horseno in horseno_list:
            if horseno_list[0] + horseno == self.exactano1 or horseno + horseno_list[0] == self.exactano1:
                payout += self.exactapayout1
            elif horseno_list[0] + horseno == self.exactano2 or horseno + horseno_list[0] == self.exactano2:
                payout += self.exactapayout2
            elif horseno_list[0] + horseno == self.exactano3 or horseno + horseno_list[0] == self.exactano3:
                payout += self.exactapayout3
            elif horseno_list[0] + horseno == self.exactano4 or horseno + horseno_list[0] == self.exactano4:
                payout += self.exactapayout4
            elif horseno_list[0] + horseno == self.exactano5 or horseno + horseno_list[0] == self.exactano5:
                payout += self.exactapayout5
            elif horseno_list[0] + horseno == self.exactano6 or horseno + horseno_list[0] == self.exactano6:
                payout += self.exactapayout6
        return payout / ((len(horseno_list) - 1) * 2) / 100

    def get_trio_hit(self, horseno_list):
        clist = list(itertools.combinations(sorted(horseno_list), 3));
        for c in clist:
            if c[0] + c[1] + c[2] in (self.triono1, self.triono2, self.triono3):
                return 1.0
        return 0.0

    def get_trio_ret(self, horseno_list):
        payout = 0.0
        clist = list(itertools.combinations(sorted(horseno_list), 3))
        for c in clist:
            if c[0] + c[1] + c[2] == self.triono1:
                payout += self.triopayout1
            elif c[0] + c[1] + c[2] == self.triono2:
                payout += self.triopayout2
            elif c[0] + c[1] + c[2] == self.triono3:
                payout += self.triopayout3
        return payout / len(clist)  / 100

    def get_trio_hit_nagashi(self, horseno_list):
        clist = list(itertools.combinations(sorted(horseno_list[1:]), 2))
        for c in clist:
            if c[0] + c[1] + horseno_list[0] in (self.triono1, self.triono2, self.triono3):
                return 1.0
            elif c[0] + horseno_list[0] + c[1] in (self.triono1, self.triono2, self.triono3):
                return 1.0
            elif horseno_list[0] + c[0] + c[1] in (self.triono1, self.triono2, self.triono3):
                return 1.0
        return 0.0

    def get_trio_ret_nagashi(self, horseno_list):
        payout = 0.0
        clist = list(itertools.combinations(sorted(horseno_list[1:]), 2))
        for c in clist:
            if c[0] + c[1] + horseno_list[0] == self.triono1 or c[0] + horseno_list[0] + c[1] == self.triono1 or horseno_list[0] + c[0] + c[1] == self.triono1:
                payout += self.triopayout1
            elif c[0] + c[1] + horseno_list[0] == self.triono2 or c[0] + horseno_list[0] + c[1] == self.triono2 or horseno_list[0] + c[0] + c[1] == self.triono2:
                payout += self.triopayout2
            elif c[0] + c[1] + horseno_list[0] == self.triono3 or c[0] + horseno_list[0] + c[1] == self.triono3 or horseno_list[0] + c[0] + c[1] == self.triono3:
                payout += self.triopayout3
        return payout / len(clist)  / 100

    def get_trifecta_hit(self, horseno_list):
        plist = list(itertools.permutations(horseno_list, 3))
        for p in plist:
            if p[0] + p[1] + p[2] in (self.trifectano1, self.trifectano2, self.trifectano3, self.trifectano4, self.trifectano5, self.trifectano6):
                return 1.0
        return 0.0

    def get_trifecta_ret(self, horseno_list):
        payout = 0.0
        plist = list(itertools.permutations(horseno_list, 3))
        for p in plist:
            if p[0] + p[1] + p[2] == self.trifectano1:
                payout += self.trifectapayout1
            elif p[0] + p[1] + p[2] == self.trifectano2:
                payout += self.trifectapayout2
            elif p[0] + p[1] + p[2] == self.trifectano3:
                payout += self.trifectapayout3
            elif p[0] + p[1] + p[2] == self.trifectano4:
                payout += self.trifectapayout4
            elif p[0] + p[1] + p[2] == self.trifectano5:
                payout += self.trifectapayout5
            elif p[0] + p[1] + p[2] == self.trifectano6:
                payout += self.trifectapayout6
        return payout / len(plist)  / 100

    def get_trifecta_hit_nagashi(self, horseno_list):
        plist = list(itertools.permutations(horseno_list[1:], 2))
        for p in plist:
            if p[0] + p[1] + horseno_list[0] in (self.trifectano1, self.trifectano2, self.trifectano3, self.trifectano4, self.trifectano5, self.trifectano6):
                return 1.0
            elif p[0] + horseno_list[0] + p[1] in (self.trifectano1, self.trifectano2, self.trifectano3, self.trifectano4, self.trifectano5, self.trifectano6):
                return 1.0
            elif horseno_list[0] + p[0] + p[1] in (self.trifectano1, self.trifectano2, self.trifectano3, self.trifectano4, self.trifectano5, self.trifectano6):
                return 1.0
        return 0.0

    def get_trifecta_ret_nagashi(self, horseno_list):
        payout = 0.0
        plist = list(itertools.permutations(horseno_list[1:], 2))
        for p in plist:
            if p[0] + p[1] + horseno_list[0] == self.trifectano1 or p[0] + horseno_list[0] + p[1] == self.trifectano1 or horseno_list[0] + p[0] + p[1] == self.trifectano1:
                payout += self.trifectapayout1
            elif p[0] + p[1] + horseno_list[0] == self.trifectano2 or p[0] + horseno_list[0] + p[1] == self.trifectano2 or horseno_list[0] + p[0] + p[1] == self.trifectano2:
                payout += self.trifectapayout2
            elif p[0] + p[1] + horseno_list[0] == self.trifectano3 or p[0] + horseno_list[0] + p[1] == self.trifectano3 or horseno_list[0] + p[0] + p[1] == self.trifectano3:
                payout += self.trifectapayout3
            elif p[0] + p[1] + horseno_list[0] == self.trifectano4 or p[0] + horseno_list[0] + p[1] == self.trifectano4 or horseno_list[0] + p[0] + p[1] == self.trifectano4:
                payout += self.trifectapayout4
            elif p[0] + p[1] + horseno_list[0] == self.trifectano5 or p[0] + horseno_list[0] + p[1] == self.trifectano5 or horseno_list[0] + p[0] + p[1] == self.trifectano5:
                payout += self.trifectapayout5
            elif p[0] + p[1] + horseno_list[0] == self.trifectano6 or p[0] + horseno_list[0] + p[1] == self.trifectano6 or horseno_list[0] + p[0] + p[1] == self.trifectano6:
                payout += self.trifectapayout6
        return payout / (len(plist) * 3)  / 100

    def get_hit_ret(self, horseno_list):
        df = pd.DataFrame()

        if len(horseno_list) > 0:
            df['win_hit'] = [self.get_win_hit(horseno_list)]
            df['win_ret'] = [self.get_win_ret(horseno_list)]
            df['place_hit'] = [self.get_place_hit(horseno_list)]
            df['place_ret'] = [self.get_place_ret(horseno_list)]
            if len(horseno_list) > 1:
                df['quinella_place_hit'] = [self.get_quinella_place_hit(horseno_list)]
                df['quinella_place_ret'] = [self.get_quinella_place_ret(horseno_list)]
                df['quinella_hit'] = [self.get_quinella_hit(horseno_list)]
                df['quinella_ret'] = [self.get_quinella_ret(horseno_list)]
                df['exacta_hit'] = [self.get_exacta_hit(horseno_list)]
                df['exacta_ret'] = [self.get_exacta_ret(horseno_list)]
                if len(horseno_list) > 2:
                    df['trio_hit'] = [self.get_trio_hit(horseno_list)]
                    df['trio_ret'] = [self.get_trio_ret(horseno_list)]
                    df['trifecta_hit'] = [self.get_trifecta_hit(horseno_list)]
                    df['trifecta_ret'] = [self.get_trifecta_ret(horseno_list)]

        else:
            df['win_hit'] = [0.0]
            df['win_ret'] = [0.0]
            df['place_hit'] = [0.0]
            df['place_ret'] = [0.0]
            if len(horseno_list) > 1:
                df['quinella_place_hit'] = [0.0]
                df['quinella_place_ret'] = [0.0]
                df['quinella_hit'] = [0.0]
                df['quinella_ret'] = [0.0]
                df['exacta_hit'] = [0.0]
                df['exacta_ret'] = [0.0]
                if len(horseno_list) > 2:
                    df['trio_hit'] = [0.0]
                    df['trio_ret'] = [0.0]
                    df['trifecta_hit'] = [0.0]
                    df['trifecta_ret'] = [0.0]
        return df

    def get_hit_ret_nagashi(self, horseno_list):
        df = pd.DataFrame()

        if len(horseno_list) > 0:
            df['win_hit'] = [self.get_win_hit(horseno_list)]
            df['win_ret'] = [self.get_win_ret(horseno_list)]
            df['place_hit'] = [self.get_place_hit(horseno_list)]
            df['place_ret'] = [self.get_place_ret(horseno_list)]
            if len(horseno_list) > 1:
                df['quinella_place_hit'] = [self.get_quinella_place_hit_nagashi(horseno_list)]
                df['quinella_place_ret'] = [self.get_quinella_place_ret_nagashi(horseno_list)]
                df['quinella_hit'] = [self.get_quinella_hit_nagashi(horseno_list)]
                df['quinella_ret'] = [self.get_quinella_ret_nagashi(horseno_list)]
                df['exacta_hit'] = [self.get_exacta_hit_nagashi(horseno_list)]
                df['exacta_ret'] = [self.get_exacta_ret_nagashi(horseno_list)]
                if len(horseno_list) > 2:
                    df['trio_hit'] = [self.get_trio_hit_nagashi(horseno_list)]
                    df['trio_ret'] = [self.get_trio_ret_nagashi(horseno_list)]
                    df['trifecta_hit'] = [self.get_trifecta_hit_nagashi(horseno_list)]
                    df['trifecta_ret'] = [self.get_trifecta_ret_nagashi(horseno_list)]

        else:
            df['win_hit'] = [0.0]
            df['win_ret'] = [0.0]
            df['place_hit'] = [0.0]
            df['place_ret'] = [0.0]
            if len(horseno_list) > 1:
                df['quinella_place_hit'] = [0.0]
                df['quinella_place_ret'] = [0.0]
                df['quinella_hit'] = [0.0]
                df['quinella_ret'] = [0.0]
                df['exacta_hit'] = [0.0]
                df['exacta_ret'] = [0.0]
                if len(horseno_list) > 2:
                    df['trio_hit'] = [0.0]
                    df['trio_ret'] = [0.0]
                    df['trifecta_hit'] = [0.0]
                    df['trifecta_ret'] = [0.0]
        return df

class Horse(object):
    """
    馬クラス
    """

    def __init__(self, horse_id, horse_profile, horse_race, horse_ex, last_minute, pre_race_results, race_result=None):
        """
        :param horse_id: 血統登録番号
        :param horse_profile: 馬属性情報
        :param horse_race: 馬毎レース情報
        :param horse_ex: 馬毎レース拡張情報
        :param last_minute: 直前情報
        :param pre_race_results: 過去戦績のリスト
        :param race_result: 当レースの結果
        """
        self.horse_id = horse_id
        self.horse_profile = horse_profile
        self.horse_race = horse_race
        self.horse_ex = horse_ex
        self.last_minute = last_minute
        self.pre_race_results = pre_race_results
        self.race_result = race_result
        self.create_horse_summary()

    def get_name(self):
        """
        馬名を取得する
        """
        return self.horse_profile.horsename

    def create_horse_summary(self):
        """
        馬の概要を作成する
        """

        if self.pre_race_results is None:
            return

        """複勝率"""
        order_list = []
        place_cnt = 0
        for result in self.pre_race_results:
            order_list.append(result.order)
            if result.order <= 3:
                place_cnt += 1
        if len(order_list) == 0:
            self.place_ratio = 0
        else:
            self.place_ratio = place_cnt / len(order_list)

        """芝ダ別複勝率"""
        condcnt = self.horse_ex.cond1stcnt + self.horse_ex.cond2ndcnt + self.horse_ex.cond3rdcnt + self.horse_ex.condothercnt
        if condcnt == 0:
            self.cond_place_ratio = 0.0
        else:
            self.cond_place_ratio = (self.horse_ex.cond1stcnt + self.horse_ex.cond2ndcnt + self.horse_ex.cond3rdcnt) / condcnt



class HorseProfile(Base):
    """
    馬属性クラス
    """
    __tablename__ = 'ukc'
    horseid = Column(String, primary_key=True)
    horsename = Column(String)
    sexcd = Column(String)
    colorcd = Column(String)
    horsesymblecd = Column(String)
    sirename = Column(String)
    damname = Column(String)
    broodmaresirename = Column(String)
    birthday = Column(String)
    sirebirthyear = Column(String)
    dambirthyear = Column(String)
    broodmaresirebirthyear = Column(String)
    ownername = Column(String)
    ownerpartycd = Column(String)
    breedername = Column(String)
    productionarea = Column(String)
    deregistrationflg = Column(String)
    dataymd = Column(String)
    sirebloodcd = Column(String)
    broodmaresirebloodcd = Column(String)

class HorseRace(Base):
    """
    馬毎レースクラス
    """
    __tablename__ = 'kyi'
    racekey = Column(String, primary_key=True)
    horseno = Column(String, primary_key=True)
    horseid = Column(String)
    horsename = Column(String)
    idm = Column(REAL)
    jockeyindex = Column(REAL)
    infoindex = Column(REAL)
    compositeindex = Column(REAL)
    racestylecd = Column(String)
    distanceaptitudecd1 = Column(String)
    elevationcd = Column(String)
    rotation = Column(Integer)
    basewinodds = Column(REAL)
    basewinfav = Column(Integer)
    baseplaceodds = Column(REAL)
    baseplacefav = Column(Integer)
    specificmarknum1 = Column(Integer)
    specificmarknum2 = Column(Integer)
    specificmarknum3 = Column(Integer)
    specificmarknum4 = Column(Integer)
    specificmarknum5 = Column(Integer)
    totalmarknum1 = Column(Integer)
    totalmarknum2 = Column(Integer)
    totalmarknum3 = Column(Integer)
    totalmarknum4 = Column(Integer)
    totalmarknum5 = Column(Integer)
    favindex = Column(Integer)
    trainingindex = Column(REAL)
    trainerindex = Column(REAL)
    trainingcd = Column(String)
    trainerappraisalcd = Column(String)
    jockeyextop2ratio = Column(REAL)
    gekisouindex = Column(Integer)
    hoofcd = Column(String)
    softfitcd = Column(String)
    classcd = Column(String)
    blinkercd = Column(String)
    jockeyname = Column(String)
    burdenweight = Column(Integer)
    apprenticekbn = Column(String)
    trainername = Column(String)
    deptarea = Column(String)
    horseresultkeyprev1 = Column(String)
    horseresultkeyprev2 = Column(String)
    horseresultkeyprev3 = Column(String)
    horseresultkeyprev4 = Column(String)
    horseresultkeyprev5 = Column(String)
    rasekeyprev1 = Column(String)
    rasekeyprev2 = Column(String)
    rasekeyprev3 = Column(String)
    rasekeyprev4 = Column(String)
    rasekeyprev5 = Column(String)
    bracketno = Column(String)
    totalmark = Column(String)
    idmmark = Column(String)
    infomark = Column(String)
    jockeymark = Column(String)
    trainermark = Column(String)
    trainingmark = Column(String)
    gekisoumark = Column(String)
    turfaptitudecd = Column(String)
    dirtaptitudecd = Column(String)
    jockeycd = Column(String)
    trainercd = Column(String)
    earnings = Column(Integer)
    prizemoney = Column(Integer)
    raceconditiongroupcd = Column(String)
    first3findex = Column(REAL)
    paceindex = Column(REAL)
    last3findex = Column(REAL)
    positionindex = Column(REAL)
    paceexpectation = Column(String)
    middleposition = Column(Integer)
    middlelead = Column(Integer)
    middleinoutside = Column(String)
    last3fposition = Column(Integer)
    last3flead = Column(Integer)
    last3finoutside = Column(String)
    goalposition = Column(Integer)
    goallead = Column(Integer)
    goalinoutside = Column(String)
    racedevelopmentcd = Column(String)
    distanceaptitudecd2 = Column(String)
    horseweightduringweek = Column(Integer)
    horseweightincdecduringweek = Column(Integer)
    cancelflg = Column(String)
    sexcd = Column(String)
    ownername = Column(String)
    ownerpartycd = Column(String)
    horsesymbolcd = Column(String)
    gekisouranking = Column(Integer)
    lsindexranking = Column(Integer)
    first3findexranking = Column(Integer)
    paceindexranking = Column(Integer)
    last3findexranking = Column(Integer)
    positionindexranking = Column(Integer)
    jockeyexpectedwinavg = Column(REAL)
    jockeyexpectedplaceavg = Column(REAL)
    carriagekbn = Column(String)
    runningstyle = Column(String)
    bodytype = Column(String)
    bodytype1 = Column(String)
    bodytype2 = Column(String)
    bodytype3 = Column(String)
    specialnote1 = Column(String)
    specialnote2 = Column(String)
    specialnote3 = Column(String)
    startindex = Column(REAL)
    latestartavg = Column(REAL)
    prerace = Column(String)
    prejockycd = Column(String)
    luckyindex = Column(Integer)
    luckymark = Column(String)
    downgradeflg = Column(String)
    gekisoutype = Column(String)
    restreasoncd = Column(String)
    flag = Column(String)
    nyukyuraces = Column(Integer)
    nyukyuymd = Column(String)
    nukyudays = Column(Integer)
    grazingarea = Column(String)
    grazingarearank = Column(String)
    stablerank = Column(String)

class HorseEx(Base):
    """
    馬毎レース拡張クラス
    """
    __tablename__ = 'kka'
    racekey = Column(String, primary_key=True)
    horseno = Column(String, primary_key=True)
    jra1stcnt = Column(Integer)
    jra2ndcnt = Column(Integer)
    jra3rdcnt = Column(Integer)
    jraothercnt = Column(Integer)
    contact1stcnt = Column(Integer)
    contact2ndcnt = Column(Integer)
    contact3rdcnt = Column(Integer)
    contactothercnt = Column(Integer)
    otherrace1stcnt = Column(Integer)
    otherrace2ndcnt = Column(Integer)
    otherrace3rdcnt = Column(Integer)
    otherraceothercnt = Column(Integer)
    cond1stcnt = Column(Integer)
    cond2ndcnt = Column(Integer)
    cond3rdcnt = Column(Integer)
    condothercnt = Column(Integer)
    conddistance1stcnt = Column(Integer)
    conddistance2ndcnt = Column(Integer)
    conddistance3rdcnt = Column(Integer)
    conddistanceothercnt = Column(Integer)
    trackdistance1stcnt = Column(Integer)
    trackdistance2ndcnt = Column(Integer)
    trackdistance3rdcnt = Column(Integer)
    trackdistanceothercnt = Column(Integer)
    rotation1stcnt = Column(Integer)
    rotation2ndcnt = Column(Integer)
    rotation3rdcnt = Column(Integer)
    rotationothercnt = Column(Integer)
    round1stcnt = Column(Integer)
    round2ndcnt = Column(Integer)
    round3rdcnt = Column(Integer)
    roundothercnt = Column(Integer)
    jockey1stcnt = Column(Integer)
    jockey2ndcnt = Column(Integer)
    jockey3rdcnt = Column(Integer)
    jockeyothercnt = Column(Integer)
    good1stcnt = Column(Integer)
    good2ndcnt = Column(Integer)
    good3rdcnt = Column(Integer)
    goodothercnt = Column(Integer)
    soft1stcnt = Column(Integer)
    soft2ndcnt = Column(Integer)
    soft3rdcnt = Column(Integer)
    softothercnt = Column(Integer)
    heavy1stcnt = Column(Integer)
    heavy2ndcnt = Column(Integer)
    heavy3rdcnt = Column(Integer)
    heavyothercnt = Column(Integer)
    s1stcnt = Column(Integer)
    s2ndcnt = Column(Integer)
    s3rdcnt = Column(Integer)
    sothercnt = Column(Integer)
    m1stcnt = Column(Integer)
    m2ndcnt = Column(Integer)
    m3rdcnt = Column(Integer)
    mothercnt = Column(Integer)
    h1stcnt = Column(Integer)
    h2ndcnt = Column(Integer)
    h3rdcnt = Column(Integer)
    hothercnt = Column(Integer)
    season1stcnt = Column(Integer)
    season2ndcnt = Column(Integer)
    season3rdcnt = Column(Integer)
    seasonothercnt = Column(Integer)
    bracket1stcnt = Column(Integer)
    bracket2ndcnt = Column(Integer)
    bracket3rdcnt = Column(Integer)
    bracketothercnt = Column(Integer)
    jockeydistance1stcnt = Column(Integer)
    jockeydistance2ndcnt = Column(Integer)
    jockeydistance3rdcnt = Column(Integer)
    jockeydistanceothercnt = Column(Integer)
    jockeytrackdistance1stcnt = Column(Integer)
    jockeytrackdistance2ndcnt = Column(Integer)
    jockeytrackdistance3rdcnt = Column(Integer)
    jockeytrackdistanceothercnt = Column(Integer)
    jockeytrainer1stcnt = Column(Integer)
    jockeytrainer2ndcnt = Column(Integer)
    jockeytrainer3rdcnt = Column(Integer)
    jockeytrainerothercnt = Column(Integer)
    jockeyowner1stcnt = Column(Integer)
    jockeyowner2ndcnt = Column(Integer)
    jockeyowner3rdcnt = Column(Integer)
    jockeyownerothercnt = Column(Integer)
    jockeyblinker1stcnt = Column(Integer)
    jockeyblinker2ndcnt = Column(Integer)
    jockeyblinker3rdcnt = Column(Integer)
    jockeyblinkerothercnt = Column(Integer)
    trainerowner1stcnt = Column(Integer)
    trainerowner2ndcnt = Column(Integer)
    trainerowner3rdcnt = Column(Integer)
    trainerownerothercnt = Column(Integer)
    sireturfquinellaratio = Column(Integer)
    siredirtquinellaratio = Column(Integer)
    sirequinelladistance = Column(Integer)
    bsireturfquinellaratio = Column(Integer)
    bsiredirtquinellaratio = Column(Integer)
    bsirequinelladistance = Column(Integer)

class LastMinute(Base):
    """
    直前情報クラス
    """
    __tablename__ = 'tyb'
    racekey = Column(String, primary_key=True)
    horseno = Column(String, primary_key=True)
    idm = Column(REAL)
    jockeyindex = Column(REAL)
    infoindex = Column(REAL)
    oddsindex = Column(REAL)
    paddockindex = Column(REAL)
    compositeindex = Column(REAL)
    harnesschange = Column(String)
    leginfo = Column(String)
    cancelflag = Column(String)
    jockeycd = Column(String)
    jockeyname = Column(String)
    burdenweight = Column(Integer)
    apprenticekbn = Column(String)
    turfconditioncd = Column(String)
    weathercd = Column(String)
    winodds = Column(REAL)
    placeodds = Column(REAL)
    oddstime = Column(String)
    horseweight = Column(Integer)
    horseweightincdec = Column(Integer)
    oddsmark = Column(String)
    paddockmark = Column(String)
    compositemark = Column(String)



class RaceResult(Base):
    """
    過去戦績クラス
    """
    __tablename__ = 'sed'
    racekey = Column(String, primary_key=True)
    horseno = Column(String, primary_key=True)
    horseid = Column(String)
    ymd = Column(String)
    horsename = Column(String)
    distance = Column(Integer)
    tracktypecd = Column(String)
    trackrotationcd = Column(String)
    trackinoutcd = Column(String)
    turfconditioncd = Column(String)
    classificationcd = Column(String)
    raceconditioncd = Column(String)
    symbolcd = Column(String)
    weightcd = Column(String)
    gradecd = Column(String)
    racename = Column(String)
    startersno = Column(Integer)
    raceshortname = Column(String)
    order = Column(Integer)
    abnormalitykbn = Column(String)
    finishtime = Column(Integer)
    burdenweight = Column(Integer)
    jockeyname = Column(String)
    trainername = Column(String)
    winodds = Column(REAL)
    winfav = Column(Integer)
    idm = Column(Integer)
    idmrawscore = Column(Integer)
    idmturfdiff = Column(Integer)
    idmpace = Column(Integer)
    idmdwelt = Column(Integer)
    idmposition = Column(Integer)
    idmadverse = Column(Integer)
    idmfirstadverse = Column(Integer)
    idmmiddleadverse = Column(Integer)
    idmlastadverse = Column(Integer)
    idmrace = Column(Integer)
    takecoursecd = Column(String)
    elevationcd = Column(String)
    abilitycd = Column(String)
    horsebodycd = Column(String)
    signcd = Column(String)
    racepace = Column(String)
    horsepace = Column(String)
    first3findex = Column(REAL)
    last3findex = Column(REAL)
    horsepaceindex = Column(REAL)
    racepaceindex = Column(REAL)
    winhorsename = Column(String)
    timediff = Column(Integer)
    first3ftime = Column(Integer)
    last3ftime = Column(Integer)
    remark = Column(String)
    placeodds = Column(REAL)
    winodds10 = Column(REAL)
    placeodds10 = Column(REAL)
    cornerposition1 = Column(Integer)
    cornerposition2 = Column(Integer)
    cornerposition3 = Column(Integer)
    cornerposition4 = Column(Integer)
    first3fdiff = Column(Integer)
    last3fdiff = Column(Integer)
    jockeycd = Column(String)
    trainercd = Column(String)
    horseweight = Column(Integer)
    horseweightincdec = Column(Integer)
    weathercd = Column(String)
    coursecd = Column(String)
    racestylecd = Column(String)
    winpayout = Column(Integer)
    placepayout = Column(Integer)
    addedmoney = Column(Integer)
    prizemoney = Column(Integer)
    racepaceflow = Column(String)
    horsepaceflow = Column(String)
    takecourse4cd = Column(String)

class Result(object):
    """
    レース結果クラス
    """

    def __init__(self, payback, race_results):
        """
        :param payback: Paybackクラス
        :type payback: Payback
        :param pre_race_results: RaceResultクラスのリスト. 馬番順に格納
        :type pre_race_results: list of RaceResult
        """
        self.payback = payback
        self.race_results = race_results

    def get_baseline_return(self, n):
        top_n_horseno = []

        for race_result in self.race_results:
            if race_result.winfav <= n:
                top_n_horseno.append(race_result.horseno)

        return self.payback.get_hit_ret(top_n_horseno)

    def get_y(self):
        ylist = []
        for race_result in self.race_results:
            if race_result.order < 4:
                ylist.append(1)
            elif race_result.order < 8:
                ylist.append(2)
            else:
                ylist.append(3)
        return ylist


class HorseTableUtil(object):

    def __init__(self):
        if os.name == 'nt':
            self.db_name = 'sqlite:///C:\\tools\\sqlite3\\jrdb.db'
        elif platform.system() == 'Darwin':
            self.db_name = 'sqlite:////Users/xxxx/db/jrdb.db'
        else:
            self.db_name = 'sqlite:///../db/jrdb.db'

    def get_program(self, race_key):
        sc_factory = SessionContextFactory(self.db_name)
        with sc_factory.create() as session:
            result = session.query(Program).filter(Program.racekey == race_key).first()

        return result

    def get_holding(self, race_key):
        sc_factory = SessionContextFactory(self.db_name)
        with sc_factory.create() as session:
            result = session.query(Holding).filter(Holding.stagekey == race_key[:6]).first()

        return result

    def get_payback(self, race_key):
        sc_factory = SessionContextFactory(self.db_name)
        with sc_factory.create() as session:
            result = session.query(Payback).filter(Payback.racekey == race_key).first()

        return result

    def get_horse_profile(self, horse_id):
        sc_factory = SessionContextFactory(self.db_name)
        with sc_factory.create() as session:
            horse_profile = session.query(HorseProfile).filter(HorseProfile.horseid == horse_id).first()

        return horse_profile

    def get_horse_ex(self, race_key, horseno):
        sc_factory = SessionContextFactory(self.db_name)
        with sc_factory.create() as session:
            horse_ex = session.query(HorseEx).filter(HorseEx.racekey == race_key, HorseEx.horseno == horseno).first()

        return horse_ex

    def get_last_minute(self, race_key, horseno):
        sc_factory = SessionContextFactory(self.db_name)
        with sc_factory.create() as session:
            last_minute = session.query(LastMinute).filter(LastMinute.racekey == race_key, LastMinute.horseno == horseno).first()

        return last_minute

    def get_race_results_by_horse_id(self, horse_id, ymd):
        sc_factory = SessionContextFactory(self.db_name)
        with sc_factory.create() as session:
            race_results= session.query(RaceResult).filter(RaceResult.horseid == horse_id, RaceResult.ymd < ymd).all()
        return sorted(race_results, key=lambda race_result: race_result.ymd, reverse=True)

    def get_race_results_by_race_key(self, race_key):
        sc_factory = SessionContextFactory(self.db_name)
        with sc_factory.create() as session:
            race_results= session.query(RaceResult).filter(RaceResult.racekey == race_key).all()
        return sorted(race_results, key=lambda race_result: race_result.horseno)

    def get_race_result(self, race_key, horseno):
        sc_factory = SessionContextFactory(self.db_name)
        with sc_factory.create() as session:
            race_result= session.query(RaceResult).filter(RaceResult.racekey == race_key, RaceResult.horseno == horseno).first()
        return race_result

    def get_horses(self, race_key, ymd):
        sc_factory = SessionContextFactory(self.db_name)
        with sc_factory.create() as session:
            horse_races = session.query(HorseRace).filter(HorseRace.racekey == race_key).all()

        horses = []
        for horse_race in sorted(horse_races, key=lambda horse_race: horse_race.horseno):
            horse_profile = self.get_horse_profile(horse_race.horseid)
            horse_ex = self.get_horse_ex(race_key, horse_race.horseno)
            last_minute = self.get_last_minute(race_key, horse_race.horseno)
            race_results = self.get_race_results_by_horse_id(horse_race.horseid, ymd)
            race_result = self.get_race_result(race_key, horse_race.horseno)
            if race_result is not None:
                if len(race_results) == 0 or race_result.abnormalitykbn != '0':
                    continue
            else:
                if len(race_results) == 0:
                    continue
            horse = Horse(horse_race.horseid, horse_profile, horse_race, horse_ex, last_minute, race_results, race_result)
            horses.append(horse)

        return horses

    def get_maiden_horses(self, race_key, ymd):
        sc_factory = SessionContextFactory(self.db_name)
        with sc_factory.create() as session:
            horse_races = session.query(HorseRace).filter(HorseRace.racekey == race_key).all()

        horses = []
        for horse_race in sorted(horse_races, key=lambda horse_race: horse_race.horseno):
            horse_profile = self.get_horse_profile(horse_race.horseid)
            horse_ex = self.get_horse_ex(race_key, horse_race.horseno)
            last_minute = self.get_last_minute(race_key, horse_race.horseno)
            race_result = self.get_race_result(race_key, horse_race.horseno)
            if race_result is not None:
                if race_result.abnormalitykbn != '0':
                    continue
            horse = Horse(horse_race.horseid, horse_profile, horse_race, horse_ex, last_minute, None, race_result)
            horses.append(horse)

        return horses

    def get_horse_table(self, race_key):
        horse_table = self.load_horse_table(race_key)
        if horse_table is not None:
            return horse_table
        program = self.get_program(race_key)
        holding = self.get_holding(race_key)
        payback = self.get_payback(race_key)
        race = Race(race_key, program, holding, payback)
        horses = self.get_horses(race_key, program.ymd)
        return HorseTable(race, horses)

    def get_maiden_horse_table(self, race_key):
        horse_table = self.load_horse_table(race_key)
        if horse_table is not None:
            return horse_table
        program = self.get_program(race_key)
        holding = self.get_holding(race_key)
        payback = self.get_payback(race_key)
        race = Race(race_key, program, holding, payback)
        horses = self.get_maiden_horses(race_key, program.ymd)
        return HorseTable(race, horses)


    def get_race_keys(self, count):
        race_keys = []
        sc_factory = SessionContextFactory(self.db_name)
        with sc_factory.create() as session:
            for race_key in session.query(Program.racekey).order_by(Program.ymd.desc()).limit(count):
                race_keys.append(race_key.racekey)
        return race_keys

    def get_race_keys_except_debut_by_race_condition(self, count, race_condition=None):
        race_keys = []
        sc_factory = SessionContextFactory(self.db_name)
        with sc_factory.create() as session:
            if race_condition is None:
                results = session.query(Program.racekey).order_by(Program.ymd.desc()).filter(Program.classificationcd != '20', Program.raceconditioncd.notin_(['A1', 'A2'])).limit(count)
            else:
                results = session.query(Program.racekey).order_by(Program.ymd.desc()).filter(Program.classificationcd != '20', Program.raceconditioncd == race_condition).limit(count)
            for race_key in results:
                race_keys.append(race_key.racekey)
        return race_keys

    def get_race_keys_except_debut_by_distance(self, count, distance=None):
        race_keys = []
        sc_factory = SessionContextFactory(self.db_name)
        with sc_factory.create() as session:
            if distance is None:
                results = session.query(Program.racekey).order_by(Program.ymd.desc()).filter(Program.classificationcd != '20', Program.raceconditioncd.notin_(['A1', 'A2'])).limit(count)
            elif distance == 's':
                results = session.query(Program.racekey).order_by(Program.ymd.desc()).filter(Program.classificationcd != '20', Program.raceconditioncd.notin_(['A1', 'A2']), Program.distance <= 1300).limit(count)
            elif distance == 'm':
                results = session.query(Program.racekey).order_by(Program.ymd.desc()).filter(Program.classificationcd != '20', Program.raceconditioncd.notin_(['A1', 'A2']), Program.distance > 1300, Program.distance <= 1600).limit(count)
            elif distance == 'i':
                results = session.query(Program.racekey).order_by(Program.ymd.desc()).filter(Program.classificationcd != '20', Program.raceconditioncd.notin_(['A1', 'A2']), Program.distance > 1600, Program.distance < 2000).limit(count)
            elif distance == 'l':
                results = session.query(Program.racekey).order_by(Program.ymd.desc()).filter(Program.classificationcd != '20', Program.raceconditioncd.notin_(['A1', 'A2']), Program.distance >= 2000).limit(count)
            else:
                print('距離区分不正')
                return None
            for race_key in results:
                race_keys.append(race_key.racekey)
        return race_keys

    def get_race_keys_except_debut_by_race_course(self, count, race_course):
        race_keys = []
        sc_factory = SessionContextFactory(self.db_name)
        with sc_factory.create() as session:
            results = session.query(Program.racekey).order_by(Program.ymd.desc()).filter(Program.classificationcd != '20', Program.raceconditioncd.notin_(['A1', 'A2']), Program.racekey.startswith(race_course)).limit(count)
            for race_key in results:
                race_keys.append(race_key.racekey)
        return race_keys

    def get_race_keys_debut_by_distance(self, count, distance=None):
        race_keys = []
        sc_factory = SessionContextFactory(self.db_name)
        with sc_factory.create() as session:
            if distance is None:
                results = session.query(Program.racekey).order_by(Program.ymd.desc()).filter(Program.classificationcd != '20', Program.raceconditioncd.in_(['A1', 'A2'])).limit(count)
            elif distance == 's':
                results = session.query(Program.racekey).order_by(Program.ymd.desc()).filter(Program.classificationcd != '20', Program.raceconditioncd.in_(['A1', 'A2']), Program.distance <= 1300).limit(count)
            elif distance == 'm':
                results = session.query(Program.racekey).order_by(Program.ymd.desc()).filter(Program.classificationcd != '20', Program.raceconditioncd.in_(['A1', 'A2']), Program.distance > 1300, Program.distance <= 1600).limit(count)
            elif distance == 'i':
                results = session.query(Program.racekey).order_by(Program.ymd.desc()).filter(Program.classificationcd != '20', Program.raceconditioncd.in_(['A1', 'A2']), Program.distance > 1600, Program.distance < 2000).limit(count)
            elif distance == 'l':
                results = session.query(Program.racekey).order_by(Program.ymd.desc()).filter(Program.classificationcd != '20', Program.raceconditioncd.in_(['A1', 'A2']), Program.distance >= 2000).limit(count)
            else:
                print('距離区分不正')
                return None
            for race_key in results:
                race_keys.append(race_key.racekey)
        return race_keys

    def get_race_ymd(self, race_key):
        sc_factory = SessionContextFactory(self.db_name)
        with sc_factory.create() as session:
            result = session.query(Program.ymd).filter(Program.racekey == race_key).first()
        return result.ymd

    def get_race_keys_except_debut_at_ymd(self, ymd):
        race_keys = []
        sc_factory = SessionContextFactory(self.db_name)
        with sc_factory.create() as session:
            results = session.query(Program.racekey).distinct().join(LastMinute, Program.racekey == LastMinute.racekey).order_by(Program.racekey).filter(Program.classificationcd != '20', Program.raceconditioncd.notin_(['A1', 'A2']), Program.ymd == ymd)
            for race_key in results:
                race_keys.append(race_key.racekey)
        return race_keys

    def get_race_keys_debut_at_ymd(self, ymd):
        race_keys = []
        sc_factory = SessionContextFactory(self.db_name)
        with sc_factory.create() as session:
            results = session.query(Program.racekey).distinct().join(LastMinute, Program.racekey == LastMinute.racekey).order_by(Program.racekey).filter(Program.classificationcd != '20', Program.raceconditioncd.in_(['A1', 'A2']), Program.ymd == ymd)
            for race_key in results:
                race_keys.append(race_key.racekey)
        return race_keys

    def get_race_keys_except_debut_after_ymd(self, ymd):
        race_keys = []
        sc_factory = SessionContextFactory(self.db_name)
        with sc_factory.create() as session:
            results = session.query(Program.racekey).order_by(Program.ymd.desc()).filter(Program.classificationcd != '20', Program.raceconditioncd.notin_(['A1', 'A2']), Program.ymd >= ymd)
            for race_key in results:
                race_keys.append(race_key.racekey)
        return race_keys

    def get_race_keys_debut_after_ymd(self, ymd):
        race_keys = []
        sc_factory = SessionContextFactory(self.db_name)
        with sc_factory.create() as session:
            results = session.query(Program.racekey).order_by(Program.ymd.desc()).filter(Program.classificationcd != '20', Program.raceconditioncd.in_(['A1', 'A2']), Program.ymd >= ymd)
            for race_key in results:
                race_keys.append(race_key.racekey)
        return race_keys

    def get_race_keys_except_debut_after_year(self, year, count=None):
        race_keys = []
        sc_factory = SessionContextFactory(self.db_name)
        with sc_factory.create() as session:
            if count is None:
                results = session.query(Program.racekey).order_by(Program.ymd.desc()).filter(Program.classificationcd != '20', Program.raceconditioncd.notin_(['A1', 'A2']), Program.ymd >= concat(year, '0101'))
            else:
                results = session.query(Program.racekey).order_by(Program.ymd.desc()).filter(Program.classificationcd != '20', Program.raceconditioncd.notin_(['A1', 'A2']), Program.ymd >= concat(year, '0101')).limit(count)
            for race_key in results:
                race_keys.append(race_key.racekey)
        return race_keys

    def get_race_keys_except_debut_goodturf(self, count=None):
        race_keys = []
        sc_factory = SessionContextFactory(self.db_name)
        with sc_factory.create() as session:
            if count is None:
                results = session.query(Program.racekey).distinct().join(LastMinute, Program.racekey == LastMinute.racekey).order_by(Program.ymd.desc()).filter(Program.classificationcd != '20', Program.raceconditioncd.notin_(['A1', 'A2']), LastMinute.turfconditioncd == '10')
            else:
                results = session.query(Program.racekey).distinct().join(LastMinute, Program.racekey == LastMinute.racekey).order_by(Program.ymd.desc()).filter(Program.classificationcd != '20', Program.raceconditioncd.notin_(['A1', 'A2']), LastMinute.turfconditioncd == '10').limit(count)
            for race_key in results:
                race_keys.append(race_key.racekey)
        return race_keys

    def get_race_keys_except_debut_softturf(self, count=None):
        race_keys = []
        sc_factory = SessionContextFactory(self.db_name)
        with sc_factory.create() as session:
            if count is None:
                results = session.query(Program.racekey).distinct().join(LastMinute, Program.racekey == LastMinute.racekey).order_by(Program.ymd.desc()).filter(Program.classificationcd != '20', Program.raceconditioncd.notin_(['A1', 'A2']), LastMinute.turfconditioncd > '10')
            else:
                results = session.query(Program.racekey).distinct().join(LastMinute, Program.racekey == LastMinute.racekey).order_by(Program.ymd.desc()).filter(Program.classificationcd != '20', Program.raceconditioncd.notin_(['A1', 'A2']), LastMinute.turfconditioncd > '10').limit(count)
            for race_key in results:
                race_keys.append(race_key.racekey)
        return race_keys

    def get_race_keys_except_debut_quinella(self, quinella_payout, ymd, count=None):
        race_keys = []
        sc_factory = SessionContextFactory(self.db_name)
        with sc_factory.create() as session:
            if count is None:
                results = session.query(Program.racekey).distinct().join(Payback, Program.racekey == Payback.racekey).order_by(Program.ymd.desc()).filter(Program.classificationcd != '20', Program.raceconditioncd.notin_(['A1', 'A2']), Program.ymd < ymd, Payback.quinellapayout1 >= quinella_payout)
            else:
                results = session.query(Program.racekey).distinct().join(Payback, Program.racekey == Payback.racekey).order_by(Program.ymd.desc()).filter(Program.classificationcd != '20', Program.raceconditioncd.notin_(['A1', 'A2']), Program.ymd < ymd, Payback.quinellapayout1 >= quinella_payout).limit(count)
            for race_key in results:
                race_keys.append(race_key.racekey)
        return race_keys

    def get_race_keys_except_debut_quinella_from_to(self, quinella_payout, from_ymd, to_ymd):
        race_keys = []
        sc_factory = SessionContextFactory(self.db_name)
        with sc_factory.create() as session:
            results = session.query(Program.racekey).distinct().join(Payback, Program.racekey == Payback.racekey).order_by(Program.ymd.desc()).filter(Program.classificationcd != '20', Program.raceconditioncd.notin_(['A1', 'A2']), Program.ymd > from_ymd, Program.ymd < to_ymd, Payback.quinellapayout1 >= quinella_payout)
            for race_key in results:
                race_keys.append(race_key.racekey)
        return race_keys

    def get_race_keys_debut_quinella_from_to(self, quinella_payout, from_ymd, to_ymd):
        race_keys = []
        sc_factory = SessionContextFactory(self.db_name)
        with sc_factory.create() as session:
            results = session.query(Program.racekey).distinct().join(Payback, Program.racekey == Payback.racekey).order_by(Program.ymd.desc()).filter(Program.classificationcd != '20', Program.raceconditioncd.in_(['A1', 'A2']), Program.ymd > from_ymd, Program.ymd < to_ymd, Payback.quinellapayout1 >= quinella_payout)
            for race_key in results:
                race_keys.append(race_key.racekey)
        return race_keys

    def get_race_keys_except_debut_quinella_race_course(self, quinella_payout, ymd, race_course, count=None):
        race_keys = []
        sc_factory = SessionContextFactory(self.db_name)
        with sc_factory.create() as session:
            if count is None:
                results = session.query(Program.racekey).distinct().join(Payback, Program.racekey == Payback.racekey).order_by(Program.ymd.desc()).filter(Program.classificationcd != '20', Program.raceconditioncd.notin_(['A1', 'A2']), Program.ymd < ymd, Payback.quinellapayout1 >= quinella_payout, Program.racekey.startswith(race_course))
            else:
                results = session.query(Program.racekey).distinct().join(Payback, Program.racekey == Payback.racekey).order_by(Program.ymd.desc()).filter(Program.classificationcd != '20', Program.raceconditioncd.notin_(['A1', 'A2']), Program.ymd < ymd, Payback.quinellapayout1 >= quinella_payout, Program.racekey.startswith(race_course)).limit(count)
            for race_key in results:
                race_keys.append(race_key.racekey)
        return race_keys

    def get_race_keys_debut_quinella(self, quinella_payout, ymd, count=None):
        race_keys = []
        sc_factory = SessionContextFactory(self.db_name)
        with sc_factory.create() as session:
            if count is None:
                results = session.query(Program.racekey).distinct().join(Payback, Program.racekey == Payback.racekey).order_by(Program.ymd.desc()).filter(Program.classificationcd != '20', Program.raceconditioncd.in_(['A1', 'A2']), Program.ymd < ymd, Payback.quinellapayout1 >= quinella_payout)
            else:
                results = session.query(Program.racekey).distinct().join(Payback, Program.racekey == Payback.racekey).order_by(Program.ymd.desc()).filter(Program.classificationcd != '20', Program.raceconditioncd.in_(['A1', 'A2']), Program.ymd < ymd, Payback.quinellapayout1 >= quinella_payout).limit(count)
            for race_key in results:
                race_keys.append(race_key.racekey)
        return race_keys

    def get_race_keys_except_debut_quinella_distance(self, quinella_payout, ymd, distance, count):
        race_keys = []
        sc_factory = SessionContextFactory(self.db_name)
        with sc_factory.create() as session:
            if distance == 's':
                results = session.query(Program.racekey).distinct().join(Payback, Program.racekey == Payback.racekey).order_by(Program.ymd.desc()).filter(Program.classificationcd != '20', Program.raceconditioncd.notin_(['A1', 'A2']), Program.ymd < ymd, Payback.quinellapayout1 >= quinella_payout, Program.distance <= 1300).limit(count)
            elif distance == 'm':
                results = session.query(Program.racekey).distinct().join(Payback, Program.racekey == Payback.racekey).order_by(Program.ymd.desc()).filter(Program.classificationcd != '20', Program.raceconditioncd.notin_(['A1', 'A2']), Program.ymd < ymd, Payback.quinellapayout1 >= quinella_payout, Program.distance > 1300, Program.distance <= 1600).limit(count)
            elif distance == 'i':
                results = session.query(Program.racekey).distinct().join(Payback, Program.racekey == Payback.racekey).order_by(Program.ymd.desc()).filter(Program.classificationcd != '20', Program.raceconditioncd.notin_(['A1', 'A2']), Program.ymd < ymd, Payback.quinellapayout1 >= quinella_payout, Program.distance > 1600, Program.distance < 2000).limit(count)
            elif distance == 'l':
                results = session.query(Program.racekey).distinct().join(Payback, Program.racekey == Payback.racekey).order_by(Program.ymd.desc()).filter(Program.classificationcd != '20', Program.raceconditioncd.notin_(['A1', 'A2']), Program.ymd < ymd, Payback.quinellapayout1 >= quinella_payout, Program.distance >= 2000).limit(count)
            else:
                print('距離区分不正')
                return None
            for race_key in results:
                race_keys.append(race_key.racekey)
        return race_keys

    def get_race_keys_except_debut_quinella_racecond(self, quinella_payout, ymd, race_condition, count=None):
        race_keys = []
        sc_factory = SessionContextFactory(self.db_name)
        with sc_factory.create() as session:
            if count is None:
                results = session.query(Program.racekey).distinct().join(Payback, Program.racekey == Payback.racekey).order_by(Program.ymd.desc()).filter(Program.classificationcd != '20', Program.raceconditioncd == race_condition, Program.ymd < ymd, Payback.quinellapayout1 >= quinella_payout)
            else:
                results = session.query(Program.racekey).distinct().join(Payback, Program.racekey == Payback.racekey).order_by(Program.ymd.desc()).filter(Program.classificationcd != '20', Program.raceconditioncd == race_condition, Program.ymd < ymd, Payback.quinellapayout1 >= quinella_payout).limit(count)
            for race_key in results:
                race_keys.append(race_key.racekey)
        return race_keys

    def get_horse_order_level(self, race_key):
        level = []
        sc_factory = SessionContextFactory(self.db_name)
        with sc_factory.create() as session:
            for result in session.query(RaceResult.order).order_by(RaceResult.horseno).filter(RaceResult.racekey == race_key).all():
                if result.order <= 3:
                    level.append('1')
                elif result.order <= 7:
                    level.append('2')
                else:
                    level.append('3')
        return level

    def get_result(self, race_key):
        return Result(self.get_payback(race_key), self.get_race_results_by_race_key(race_key))

    def get_horse_tables(self, race_keys):
        horse_table_list = Parallel(n_jobs=-1)([delayed(self.get_horse_table)(race_key) for race_key in race_keys])
        return horse_table_list

    def get_maiden_horse_tables(self, race_keys):
        horse_table_list = Parallel(n_jobs=-1)([delayed(self.get_maiden_horse_table)(race_key) for race_key in race_keys])
        return horse_table_list

    def dump_horse_table(self, race_key):
        file_name = './horsetable/' + race_key + '.pickle'
        if os.path.isfile(file_name):
            return
        horse_table = self.get_horse_table(race_key)

        with open(file_name, mode='wb') as f:
            pickle.dump(horse_table, f)

    def dump_maiden_horse_table(self, race_key):
        file_name = './horsetable/' + race_key + '.pickle'
        if os.path.isfile(file_name):
            return
        horse_table = self.get_maiden_horse_table(race_key)

        with open(file_name, mode='wb') as f:
            pickle.dump(horse_table, f)

    def dump_horse_tables(self, race_keys, distance=None):
        if distance is None:
            file_name = race_keys[0] + '-' + race_keys[-1] + '_horse_tables.pickle'
        else:
            file_name = distance + '_' + race_keys[0] + '-' + race_keys[-1] + '_horse_tables.pickle'
        horse_table_list = self.get_horse_tables(race_keys)

        with open(file_name, mode='wb') as f:
            pickle.dump(horse_table_list, f)

        return horse_table_list

    def dump_maiden_horse_tables(self, race_keys, distance=None):
        if distance is None:
            file_name = race_keys[0] + '-' + race_keys[-1] + '_horse_tables.pickle'
        else:
            file_name = distance + '_' + race_keys[0] + '-' + race_keys[-1] + '_horse_tables.pickle'
        horse_table_list = self.get_maiden_horse_tables(race_keys)

        with open(file_name, mode='wb') as f:
            pickle.dump(horse_table_list, f)

        return horse_table_list

    def load_horse_table(self, race_key):
        file_name = './horsetable/' + race_key + '.pickle'
        if os.path.isfile(file_name):
            with open(file_name, mode='rb') as f:
                return pickle.load(f)
        else:
            return None

    def load_horse_tables(self, race_keys, distance=None):
        if distance is None:
            file_name = race_keys[0] + '-' + race_keys[-1] + '_horse_tables.pickle'
        else:
            file_name = distance + '_' + race_keys[0] + '-' + race_keys[-1] + '_horse_tables.pickle'
        if os.path.isfile(file_name):
            with open(file_name, mode='rb') as f:
                return pickle.load(f)
        else:
            return None

if __name__ == "__main__":
    ht_util = HorseTableUtil()
    print(ht_util.get_race_keys_except_debut_softturf(10000))
