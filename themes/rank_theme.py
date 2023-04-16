import pandas as pd


# df = pd.read_csv(r'D:\04-数据产品\D-榜单\榜单计算\！！重新合并！！\跨年\所有的合并\1-2月\test_Jan&Feb.csv', encoding='utf-8')
# df = df[df['type_event'] != 'Void']
# df['tag'].fillna('nan')

class Rank_theme:
    def __init__(self, column1, column2):
        self.column1 = column1
        self.column2 = column2

    # 活动类型、关键词为主键累计热度
    def split(self, data, column):
        df_1 = data[column].str.split(',', expand=True).stack()
        df_1 = df_1.reset_index(level=1, drop=True)
        df_1.name = column
        df_new = data.drop([column], axis=1).join(df_1).reset_index().drop(columns='index')
        df_new = df_new[~df_new[column].isnull()]
        return df_new

    def cal_tag_place_index(self, df):
        """
        :param column1: 'tag'
        :param column2: 希望统计tag聚合下的哪些维度: 'type_place' / 'nm_place'  / 'type_event'
        :return:
        """
        df_column1 = self.split(df, self.column1)  # 算关键词热度
        df_column1 = self.split(df_column1, self.column2)  # 多个场所需要拆开将热度累计
        # df_column2=self.split(df_column1, column2)
        #         tag_list=['
        # 展览集会：艺术、文物、绘画、影像、VR、插画、科技、展会、论坛、市集
        # 演出放映：喜剧、脱口秀、话剧、音乐会、亲子、相声、戏剧、多媒体、舞台剧、音乐剧、杂技、京剧、折子戏、戏曲
        # 社交聚会：剧本杀、沙龙、饭局、交友、体验、DIY、读书会、相亲、桌游、英语、心理咨询
        # 知识教育：亲子、零基础、体验、科学、教育、研学、沙龙、心灵、演讲、创业、投资
        # 运动出游：徒步、飞盘、亲子、登山、滑雪、滑冰、摄影、骑行、攀岩、环湖、露营']

        df_column1['agg_index'] = df_column1.groupby(by=[self.column1, self.column2])['event_index'].transform('sum')
        df_column1['n_event'] = df_column1.groupby(by=[self.column1, self.column2])['nm_event'].transform('count')  # 活动个数
        df_column1_output = df_column1.drop_duplicates([self.column1, self.column2], ignore_index=True)
        return df_column1_output

    def cal_tag_index(self, df):
        """
        打断tag，聚合计算tag的累计热度值
        :param column1: 'tag'
        :return:
        """
        df_column1 = self.split(df, self.column1)  # 算关键词热度
        df_column1['agg_index'] = df_column1.groupby(by=[self.column1])['event_index'].transform('sum')
        df_column1['n_event'] = df_column1.groupby(by=[self.column1])['nm_event'].transform('count')  # 活动个数
        df_column1_output = df_column1.drop_duplicates([self.column1], ignore_index=True)
        return df_column1_output


    # # 关键词下的场所热度
    # df_tag=split(df, 'tag')
    # df_tag_nmplace_index = cal_tag_place(df_tag, 'tag', 'nm_place')
    # print(df_tag_nmplace_index)
