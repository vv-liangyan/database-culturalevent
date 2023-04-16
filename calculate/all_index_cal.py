import pandas as pd
import datetime
import re

'''
columns: id_event,start_date_final,end_date_final,time,type,tag,type_place,nm_place,address_ori,volume_event
豆瓣同一id_event会对应不同的nm_event,同一个nm_event也会对应不同的id_event(活动时间不同)
大麦同一id_event会对应不同的nm_event
id_event在excel打开后会变，直接从数据库读
'''


# year_define = 2023
# month_define = 2


# pd.set_option('display.max_columns', None)
class Calculator:
    def __init__(self, year_define, month_define):
        self.year_define = year_define
        self.month_define = month_define

    def time_span(self, year_define, month_define):
        year_define = year_define
        if month_define in [1, 3, 5, 7, 8, 10, 12]:
            day = '31'
        elif month_define in [4, 6, 9, 11]:
            day = '30'
        else:
            day = '28'
        start_season = str(year_define) + '-' + str(month_define) + '-' + '01'
        end_season = str(year_define) + '-' + str(month_define) + '-' + str(day)
        start_season = datetime.datetime.strptime(start_season, '%Y-%m-%d')
        end_season = datetime.datetime.strptime(end_season, '%Y-%m-%d')
        return start_season, end_season

    # # %% 在线online判定
    # df['online'] = 0
    # df.loc[df['nm_event'].str.contains(r'线上|E享|腾讯会议|直播间|直播'), 'online'] = '1'

    def group_func(self, data):
        '''
        如果连续两条数据时间相隔一天，则视为连续举办的活动，赋同一个new_group ID；
        并将该组最小日期作为new_group的开始日期，最大日期作为new_group的结束日期
        如果连续两条数据时间相隔大于一天，则视为非连续活动，重新赋new_group ID
        '''

        dt = data['start_date_final']
        day = pd.Timedelta('1d')
        breaks = dt.diff() != day
        groups = breaks.cumsum()
        data['new_group'] = groups
        data['new_group'] = data['new_group'].map(str)
        data['new_group'] = data['id_event'] + '_' + data['new_group']
        data['start_date_cal'] = data.groupby('new_group')['start_date_final'].transform('min')
        data['end_date_cal'] = data.groupby('new_group')['end_date_final'].transform('max')
        data['n_event'] = data.groupby('new_group')['n_event'].transform('sum')
        return data

    def delay_mark(self, data, start_season, end_season):
        '''
        延期活动标为 1
        超出时间区间举办活动标为 2
        将起/止日期改为时间区间限制日期
        '''

        # data.insert(0, 'delay', 0)
        # data['delay'] = data['delay'].apply(pd.to_numeric, errors='coerce')  # object转数字
        data.loc[
            data['nm_event'].str.contains(
                r'临时闭|暂停营业|取消|延期|停播|停演|暂停|停展|延迟|闭馆|闭展'), 'delay'] = '1'
        for i in range(len(data)):
            if (data['end_date_cal'][i] < start_season) or (data['start_date_cal'][i] > end_season):
                data['delay'][i] = '2'
            data['delay'].astype(str)
            data.loc[data['delay'].str.contains('1|2'), 'duration_final'] = 0
        return data

    def x_duration_cal(self, data):
        '''
        :param data:
        :return:
        '''

        list_twoday = r'每周六日|周六日及节假日|周六/日|每周六，周日|每周六、周日|周六日|每周六周日|每周六、周日|周六/周日|周六、日|2日|两日'
        list_oneday = r'每周六|每周日|一日|1日|周日|周六|周一|周二|周三|周四|周五|3.5H|2H|3H|周末单天|本周六|解压周末|天天发团|每天发团|周一到周日|天天发'
        list_vacation = r'中秋|五一|端午|清明|元旦'
        list_other = r'马术|京郊旅行团招募|私家团|付费培训|剧本杀|相亲|沙龙'

        for i in range(len(data)):
            if ('展览' in str(data['type'][i])) is False and data['duration_final'][i] > 3:
                if re.findall(list_twoday, data['nm_event'][i]):  # 两日活动
                    data['duration_final'][i] = data['duration_final'][i] / 3.5
                if re.findall(list_oneday, data['nm_event'][i]):
                    data['duration_final'][i] = data['duration_final'][i] / 7  # 一日活动
                if re.findall(list_vacation, data['nm_event'][i]):  # 假期活动
                    data['duration_final'][i] = 3
                if re.findall(list_other, data['nm_event'][i]):  # 其他情况判断
                    data['duration_final'][i] = data['duration_final'][i] / 7
        return data

    def x_event(self, data):
        '''
        根据活动类型和人数阶梯，计算人数系数 x_event
        :param data:
        :return:
        '''

        for i in range(len(data)):
            try:
                if data['event_source'][i] == 'hdx':
                    if any(type in data['type'][i] for type in "展览") and any(
                            type not in data['type'][i] for type in "演出"):
                        if data['volume_event'][i] >= 500:
                            data['x_event'][i] = 0.5
                    if any(type in data['type'][i] for type in "社交|知识") and any(
                            type not in data['type'][i] for type in "演出|展览"):
                        if data['volume_event'][i] >= 1000:
                            data['x_event'][i] = 0.02
                        if 1000 > data['volume_event'][i] >= 500:
                            data['x_event'][i] = 0.1
                        if 500 > data['volume_event'][i] >= 100:
                            data['x_event'][i] = 0.2
                if any(tag_child in data['tag'][i] for tag_child in "亲子"):
                    if data['volume_event'][i] >= 1000:
                        data['x_event'][i] = 0.1

            except:
                continue
        return data

        # def x_corona(data):
        '''
        根据活动类型计算疫情系数 x_corona
        :param data:
        :return:
        '''
        # data["x_corona"] = 1.00

        # for i in range(len(data)):
        #     try:
        #         if any(j in data['type'][i] for j in r'演出|展览'):
        #             data['x_corona'][i] = 0.5
        #         else:
        #             data['x_corona'][i] = 1.0
        #     except:
        #         continue
        # return data

    def x_duration(self, data):
        '''
        根据活动类型计算时间系数 x_duration
        :param data:
        :return:
        '''
        data["x_duration"] = 1.00

        type_Alist = '展览集会'
        type_Blist = '演出放映'

        for i in range(len(data)):
            try:
                if re.findall(type_Alist, data['type'][i]):
                    data['x_duration'][i] = 0.6
                if re.findall(type_Blist, data['type'][i]):
                    data['x_duration'][i] = 0.8
                else:
                    data['x_duration'][i] = 0.6
            except:
                continue
        return data

    def x_count(self, data):
        data['x_count'] = 1
        for i in range(len(data)):
            try:
                if data['count_final'] > 60:
                    data['x_count'] = 0.4
                else:
                    data['x_count'] = 1
            except:
                continue
        return data

    def normalize_column(self, df, column_name):
        """对DataFrame中的某一列进行归一化处理"""
        # 求出该列的最小值和最大值
        min_val = df[column_name].min()
        max_val = df[column_name].max()
        # 对该列的每个元素进行归一化处理
        df[column_name + '_normalized'] = (df[column_name] - min_val) / (max_val - min_val)
        # 返回处理后的DataFrame
        return df

    def calculate(self, df):
        """
        热度指数赋系数计算
        :param df:
        :return:
        """

        # df['id_event'].astype(str)
        df['type'] = df['type'].replace('展览集会', '展览集市')
        # seasons = self.time_span(self.year_define, self.month_define)
        # start_season = seasons[0]  # 单月开始日期
        # end_season = seasons[1]  # 单月结束日期
        start_season = '2022-12-01'
        end_season = '2023-03-31'
        start_season = datetime.datetime.strptime(start_season, '%Y-%m-%d')
        end_season = datetime.datetime.strptime(end_season, '%Y-%m-%d')

        df = df[df['start_date_final'].notnull()]
        df = df[df['end_date_final'].notnull()]
        # 起止日期转datetime
        df['start_date_final'] = df['start_date_final'].astype(str)
        df['end_date_final'] = df['end_date_final'].astype(str)
        df['start_date_final'] = pd.to_datetime(df['start_date_final'].str.replace('^-', 'invalid', regex=True),
                                                errors='coerce')
        df['end_date_final'] = pd.to_datetime(df['end_date_final'].str.replace('^-', 'invalid', regex=True),
                                              errors='coerce')
        # 大麦
        df_2 = df.loc[df['event_source'] == 'dmw'].drop_duplicates(
            ['id_event', 'start_date_final', 'end_date_final', 'performance_stime_final', 'nm_place'],
            ignore_index=True)  # 根据时间地点名称去重
        df_2['start_date_event'] = df_2.groupby('id_event')['start_date_final'].transform('min')  # 整组活动不考虑时间截面，原始的起止日期
        df_2['end_date_event'] = df_2.groupby('id_event')['end_date_final'].transform('max')

        # 判断不在本月截面内的数据in_time,有部分在本月内为0，全部不在本月内为1
        df_2['out_time'] = 0
        df_2.loc[df_2['start_date_final'] >= end_season, 'out_time'] = 1
        df_2.loc[df_2['end_date_final'] <= start_season, 'out_time'] = 1
        df_2.drop(df_2[(df_2['out_time'] == 1)].index, inplace=True)  # 删去非本月的活动

        df_2.loc[df_2['start_date_final'] == df_2['end_date_final'], 'date_judge'] = 1
        df_2_1 = df_2[df_2['date_judge'] == 1]  # 同一个nm_event,相同起止日期
        df_2.loc[df_2['start_date_final'] != df_2['end_date_final'], 'date_judge'] = 0
        df_2_0 = df_2[df_2['date_judge'] == 0]  # 同一个nm_event,不同起止日期

        # 计算一天多场的场次数（同一个nm_event和相同起止日期下统计时间次数）
        df_2_1['time_cal'] = df_2_1['performance_stime_final']
        df_2_1['n_event'] = df_2_1.groupby(['start_date_final', 'id_event'])['time_cal'].transform('nunique')
        df_2_1 = df_2_1.drop_duplicates(['id_event', 'start_date_final'], ignore_index=True)  # 一天一条
        # test=df_2_1[df_2_1['id_event']==670503000000]
        # print(test)

        df_2_1['id_event'] = df_2_1['id_event'].map(str)
        df_2_1 = df_2_1.sort_values('start_date_final', ascending=True)
        group_2 = df_2_1.groupby('id_event', as_index=False)
        df_2_1 = group_2.apply(self.group_func).drop_duplicates(['new_group'], ignore_index=True)

        # 计算每一条的天数 duration_cal
        df_2_1['duration_cal'] = df_2_1['end_date_cal'] - df_2_1['start_date_cal']
        df_2_1['duration_cal'] = df_2_1['duration_cal'].astype('timedelta64[D]').astype(float)  # 将字段格式转为float计算
        df_2_1['duration_cal'] = df_2_1['duration_cal'] + 1

        df_2_1['duration_final'] = df_2_1.groupby(['id_event'])['n_event'].transform('sum')  # 合计场次数duration_final
        df_2_1['duration_cal'] = df_2_1.groupby(['id_event'])['duration_cal'].transform('sum')  # 合计天数duration_cal

        df_2_1['start_date_cal'] = df_2_1.groupby('id_event')['start_date_cal'].transform('min')  # 整组活动考虑时间截面的起止日期
        df_2_1['end_date_cal'] = df_2_1.groupby('id_event')['end_date_cal'].transform('max')

        df_2_1 = df_2_1.drop_duplicates(['id_event'], ignore_index=True)

        # 起止日期不同的
        df_2_0['start_date_cal'] = df_2_0['start_date_final']
        df_2_0['end_date_cal'] = df_2_0['end_date_final']
        df_2_0.loc[df_2_0['start_date_cal'] <= start_season, 'start_date_cal'] = start_season
        df_2_0.loc[df_2_0['end_date_cal'] >= end_season, 'end_date_cal'] = end_season

        df_2_0['time_cal'] = df_2_0['performance_stime_final']
        df_2_0['n_event'] = df_2_0.groupby(['start_date_final', 'id_event'])['time_cal'].transform('nunique')  # 计算场次数

        # 计算每一条的天数 duration_cal
        df_2_0['duration_cal'] = df_2_0['end_date_cal'] - df_2_0['start_date_cal']
        df_2_0['duration_cal'] = df_2_0['duration_cal'].astype('timedelta64[D]').astype(float)  # 将字段格式转为float计算
        df_2_0['duration_cal'] = df_2_0['duration_cal'] + 1
        df_2_0 = df_2_0.sort_values('duration_cal', ascending=False).drop_duplicates(['id_event'], keep='first',
                                                                                     ignore_index=True)  # 根据nm_event去重，取累计时长较大的

        df_2_0['duration_final'] = df_2_0['duration_cal']

        df_2 = pd.concat([df_2_0, df_2_1])
        df_2['count_final'] = df_2_0['duration_final']

        # %% 豆瓣活动行
        df_1 = df.loc[df['event_source'] != 'dmw'].drop_duplicates(
            ['nm_event', 'start_date_final', 'end_date_final', 'performance_stime_final', 'nm_place'],
            ignore_index=True)  # 去重
        df_1['start_date_event'] = df_1.groupby('id_event')['start_date_final'].transform('min')  # 整组活动不考虑时间截面，原始的起止日期
        df_1['end_date_event'] = df_1.groupby('id_event')['end_date_final'].transform('max')

        # 判断不在本月截面内的数据in_time,有部分在本月内为0，全部不在本月内为1
        df_1['out_time'] = 0
        df_1.loc[df_1['start_date_final'] >= end_season, 'out_time'] = 1
        df_1.loc[df_1['end_date_final'] <= start_season, 'out_time'] = 1
        df_1.drop(df_1[(df_1['out_time'] == 1)].index, inplace=True)  # 删去非本月的活动

        df_1['n_event'] = df_1.groupby(['start_date_final', 'nm_event'])['performance_stime_final'].transform('nunique')
        df_1['id_event'] = df_1['id_event'].map(str)
        df_1 = df_1.sort_values('start_date_final', ascending=True)
        Group = df_1.groupby(['nm_event', 'id_event'], as_index=False)
        df_1 = Group.apply(self.group_func)

        df_1.loc[df_1['start_date_final'] <= start_season, 'start_date_cal'] = start_season
        df_1.loc[df_1['end_date_final'] >= end_season, 'end_date_cal'] = end_season

        df_1 = df_1.groupby('nm_event', as_index=False).apply(
            lambda x: x.sort_values('start_date_cal', ascending=True).drop_duplicates(['end_date_cal'], keep='first',
                                                                                      ignore_index=True))  # 如果结束时间一样，取开始时间较早的一项

        df_1['duration_cal'] = df_1['end_date_cal'] - df_1['start_date_cal']
        df_1['duration_cal'] = df_1['duration_cal'].astype('timedelta64[D]').astype(float)  # 将字段格式转为float计算
        df_1['duration_cal'] = df_1['duration_cal'] + 1

        df_1 = df_1.sort_values('duration_cal', ascending=False).drop_duplicates(['nm_event'], keep='first',
                                                                                 ignore_index=True)  # 根据nm_event去重，取累计时长较大的

        df_1['duration_final'] = df_1.groupby(['nm_event'])['duration_cal'].transform('sum')  # 累计活动场次（豆瓣活动行）

        # %% 合并
        data_output = pd.concat([df_2, df_1])
        # 不同数据源,根据nm_event去重，取累计时长较长的一项
        data_output = data_output.sort_values('duration_final', ascending=False).drop_duplicates(['nm_event'],
                                                                                                 keep='first',
                                                                                                 ignore_index=True)

        data_output['count_final'] = data_output['duration_final']
        # %% delay打标
        data_output['delay'] = '0'
        self.delay_mark(data_output, start_season, end_season)  # 'delay' 活动时长归零

        # %% 赋系数
        data_output['nm_event'].fillna('0', inplace=True)
        data_output.pipe(self.x_duration_cal)
        # data_output=pd.DataFrame(data_output)

        data_output["x_event"] = 1.00
        data_output["x_duration"] = 1.00

        data_output_index = data_output.pipe(self.x_event)

        data_output_index = data_output_index.pipe(self.x_duration)

        data_output_index = data_output_index.pipe(self.x_count)

        # %% 算热度
        data_output_index = data_output_index.drop_duplicates('new_group', ignore_index=True)  # 同一场次去重
        # data_output_index['volume_event'].fillna('0', inplace=True)
        data_output_index['volume_event'] = data_output_index['volume_event'].replace('None', 0)
        data_output_index['volume_event'] = data_output_index['volume_event'].astype(float)  # 统一计算因子字段类型为float
        data_output_index['x_event'] = data_output_index['x_event'].astype(float)
        data_output_index['volume_final'] = data_output_index['volume_event'] * data_output_index['x_event']
        data_output_index['event_index'] = data_output_index['volume_event'] * data_output_index['x_event'] * \
                                           data_output_index[
                                               'duration_final'] * data_output_index['x_duration'] * data_output_index[
                                               'x_count']

        # 输出
        data_output_index = data_output_index[data_output_index['delay'] == '0']
        data_output_index.rename(columns={'duration_cal': 'duration_days'}, inplace=True)   # 重命名字段
        data_output_index['year_define'] = int(self.year_define)
        data_output_index['month_define'] = int(self.month_define)
        # data_output_index = self.normalize_column(data_output_index, 'event_index') #归一暂时不做
        data_output_index.drop(columns=['new_group', 'out_time', 'date_judge', 'delay'], inplace=True)

        return data_output_index

# %% 落点挂街道
# data = spatialjoin_points_within_polygons(data_output_index, csv_polygon_path)
# print(data.head(10))
# data.to_csv(r'D:\00-业务项目\G-郎园\文化数据\2023-02.csv', index=False)

# %% 限定输出的字段和字段类型
# data_output_index['duration_day'] = data_output_index['duration_cal']  # 【有效活动时间】作为【持续时间】输出
# data_output_index['year_define'] = int(year_define)
# data_output_index['month_define'] = int(month_define)
# data_output_index['duration_day'] = data_output_index['duration_day'].astype(int)
# # data_output_index['nm_county'] = data_output_index['nm_county'].astype(str)
# # data_output_index['nm_town'] = data_output_index['nm_town'].astype(str)
# # data_output_index['nm_village'] = data_output_index['nm_village'].astype(str)
# data_output_index['type'] = data_output_index['type'].astype(str)
# data_output_index['tag'] = data_output_index['tag'].astype(str)
# data_output_index['type_place'] = data_output_index['type_place'].astype(str)
# data_output_index['nm_place'] = data_output_index['nm_place'].astype(str)
# data_output_index['start_date_event'] = data_output_index['start_date_event'].astype(str)
# data_output_index['end_date_event'] = data_output_index['end_date_event'].astype(str)
#
# data_output_index['event_index'] = data_output_index['event_index'].astype(float)
# data_output_index['lon_earth'] = data_output_index['lon_earth'].astype(float)
# data_output_index['lat_earth'] = data_output_index['lat_earth'].astype(float)

# print(data.dtypes)

# cols = ['year_define', 'month_define', 'nm_event', 'event_source', 'nm_county', 'nm_town', 'nm_village', 'type',
#         'tag', 'type_place', 'nm_place', 'event_index', 'start_date_event', 'end_date_event', 'duration_day',
#         'lon_earth', 'lat_earth']
# Output = data_output_index[cols]
# Output.to_csv(r'D:\04-数据产品\D-榜单\榜单计算\！！重新合并！！\跨年\所有的合并\test.csv', index=False)
# print(Output.head(10))
