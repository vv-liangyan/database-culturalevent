import csv
import pandas as pd

# Load input file
# read_path = r'D:\04-数据产品\D-榜单\榜单计算\！！重新合并！！\跨年\0213之后的\0213场所识别.csv'
# mapping_file = r'D:\03-常用代码\榜单\index_cal_sj\data\place_volume.csv'


class Volume_predict:
    def __init__(self):
        self.mapping_file_path = dict(pd.read_csv(r'D:\03-常用代码\榜单\index_cal_sj\data\place_volume.csv').values)

    # def _build_mapping_dict(self):
    #     # 创建映射字典
    #     mapping_dict = {}
    #     # 打开CSV文件并读取内容
    #     with open(self.mapping_file_path, 'r', newline='', encoding='utf-8') as csvfile:
    #         reader = csv.DictReader(csvfile)
    #         for row in reader:
    #             key_value = row[self.key_column_name]
    #             mapping_dict[key_value] = row[self.value_column_name]
    #     self.mapping_dict = mapping_dict

    # def _build_dataframe(self):
    #     self._build_mapping_dict()
    #     # 打开CSV文件并读取内容
    #     with open(self.input_df, 'r', newline='', encoding='utf-8') as csvfile:
    #         reader = csv.DictReader(csvfile)
    #         # 创建新的DataFrame
    #         data = []
    #         for row in reader:
    #             key_values = row[self.key_column_name].split(',')  # 按照指定分隔符切割多个键值
    #             mapped_values_list = []  # 用于存储所有映射结果的列表
    #             for key_value in key_values:
    #                 mapped_value = self.mapping_dict.get(key_value.strip(), '')  # 获取当前键值对应的映射结果
    #                 mapped_values_list.append(mapped_value)  # 将映射结果添加到列表中
    #
    #             # 将多个映射结果拼接成一个字符串，并将其保存到指定列
    #             if row['event_source'] == 'hdx':
    #                 row['volume_event'] = row['volume']
    #             else:
    #                 row['volume_event'] = ','.join(filter(None, mapped_values_list))
    #             data.append(row)
    #
    #         # 将DataFrame赋值给实例变量
    #         self.df = pd.DataFrame(data)

    def predict_volume(self, data):
        data["nm_place"].fillna('nan', inplace=True)
        data['volume_event'] = None  # 添加一个新的列 'volume_event'，并初始化为空值
        data['volume_event'] = data['volume_event'].astype(str)
        for i in range(len(data)):
            try:
                if data['nm_place'][i]:
                    place_list = data['nm_place'][i].split(',')
                    volume_list = []
                    for place in place_list:
                        volume = self.mapping_file_path.get(place)
                        if volume:
                            volume_list.append(volume)
                    if len(volume_list) > 0:
                        data['volume_event'][i] = float(min(volume_list))  # 多个场所取较小值
                        # data['volume_event'][i] = ','.join(set(volume_list))  # 赋值场所volume
                else:
                    continue
            except:
                continue
        return data

    # def _map_to_dataframe(self):
    #     self._build_mapping_dict()
    #     self._build_dataframe()
    #     return self.df

    def volume_rule(self, df):
        df = self.predict_volume(df)
        df['tag'].fillna('nan', inplace=True)
        df['tag'].astype(str)
        df['type'].fillna('nan', inplace=True)
        df['type'].astype(str)
        # 根据活动属性
        df.loc[df['nm_event'].str.contains(
            '保龄球|精讲|私家团|讲解|研学|研讨会|狼人杀|桌游|剧本|DIY|手工|瑜伽|桌游|沙龙|彩绘|读|推理|体验课|花道|油画|彩铅|素描|配饰|穿搭|画画|手绘|读书会|摄影|徒步|相亲|脱单|CP|交友|茶|旅拍|拍照|约拍|行摄|游园|飞盘|VR体验'), 'volume_event'] = 10
        df.loc[df['nm_event'].str.contains('羽毛球'), 'volume_event'] = 12
        df.loc[df['nm_event'].str.contains('约拍|写真|心理|亲密关系|情感'), 'volume_event'] = 3
        df.loc[df['nm_event'].str.contains('滑雪|私家团|讲解|精讲'), 'volume_event'] = 20
        df.loc[df['nm_place'].str.contains('滑冰|冰乐园', na=False), 'volume_event'] = 500

        df_null = df[df['volume_event'].isnull()]
        df_notnull = df[~df['volume_event'].isnull()]
        df_null.loc[(df_null['tag'].str.contains('亲子')), 'volume_event'] = 10
        df_null.loc[
            (df_null['type'].str.contains('运动出游')), 'volume_event'] = 50
        df_null.loc[
            (df_null['type'].str.contains('社交')), 'volume_event'] = 20
        df_null.loc[
            (df_null['type'].str.contains('展览')), 'volume_event'] = 200
        df = pd.concat([df_null, df_notnull])

        for i in range(len(df)):
            try:
                if ("脱口秀" in df['tag'][i]) and ("笑果" not in df['nm_event'][i]) and (df['volume_event'][i] >= 150):
                    df['volume_event'][i] = 150
                if "社交聚会" in df['type'][i] and df['volume_event'][i] >= 100:
                    df['volume_event'][i] = 20

            except:
                continue

        df.loc[df['nm_event'].str.contains('滑雪|滑冰|冰乐园', na=False), 'type'] = '运动出游'
        df.loc[df['nm_event'].str.contains('剧场', na=False), 'type'] = '演出放映'
        df.loc[df['nm_event'].str.contains('徒步|户外', na=False), 'type'] = '运动出游'
        # df.loc[df['type_place'].str.contains('户外', na=False), 'type'] = '运动出游'
        df.loc[df['tag'].str.contains('脱口秀', na=False), 'type'] = '演出放映'

        return df

# def volume_rules(df):
#     # self._build_mapping_dict()
#     # self._build_dataframe()
#     # 根据活动属性
#     df_null = df[df['volume_event'].isnull()]
#     df_notnull = df[~df['volume_event'].isnull()]
#
#     df_null.loc[df_null['nm_event'].str.contains(
#         '保龄球|精讲|私家团|讲解|研学|研讨会|狼人杀|轰趴|推理|桌游|剧本|侦探|DIY|手工|瑜伽|桌游|沙龙|彩绘|读|推理|体验课|花道|油画|彩铅|素描|配饰|穿搭|画画|手绘|沉浸|读书会|摄影|徒步|相亲|脱单|CP|交友|茶|旅拍|摄影|拍照|约拍|行摄|游园|飞盘'), 'volume_event'] = 10
#     df_null.loc[df_null['nm_event'].str.contains('羽毛球'), 'volume_event'] = 12
#     df_null.loc[df_null['nm_event'].str.contains('约拍|写真|心理|亲密关系|情感'), 'volume_event'] = 3
#     df_null.loc[df_null['nm_event'].str.contains('滑雪'), 'volume_event'] = 20
#
#     df_null.loc[df_null['tag'].str.contains('亲子'), 'volume_event'] = 10
#
#     df_null.loc[(df_null['type'].str.contains('运动出游')), 'volume_event'] = 50
#     df_null.loc[df_null['type'].str.contains('社交聚会'), 'volume_event'] = 20
#     df_null.loc[df_null['type'].str.contains('展览集会'), 'volume_event'] = 200
#     df = pd.concat([df_null, df_notnull])
#
#     df.loc[df['nm_place'].str.contains('滑冰|冰乐园'), 'volume_event'] = 500
#
#     # 再判断
#     for i in range(len(df)):
#         try:
#             if any(tag in df['tag'][i] for tag in "脱口秀") and any(
#                     tag not in df['nm_event'][i] for tag in "笑果"):
#                 if df['volume_event'][i] > 150:
#                     df['volume_event'][i] = 150
#             if any(type in df['type'][i] for type in "社交聚会") and df['volume_event'][i] >= 100:
#                 df['volume_event'][i] = 20
#         except:
#             continue
#
#         df.loc[df['nm_event'].str.contains('VR体验'), 'volume_event'] = 10
#         # 部分tag直接赋值
#         df.loc[df['nm_event'].str.contains('私家团|讲解|精讲'), 'volume_event'] = 20
#         return df





# if __name__ == '__main__':
    # mapper = Volume_predict(read_path)
    # df = mapper.volume_rule()
# print(df)
# df.to_csv(r'D:\04-数据产品\D-榜单\榜单计算\！！重新合并！！\跨年\0213之前的_volumetest.csv')

# df_test=volume_rule(df)
