import pandas as pd

pd.set_option('display.max_columns', None)
#  TODO: 以type_event 分组, 多个分类问题？
df = pd.read_csv(r'D:\04-数据产品\D-榜单\榜单计算\！！重新合并！！\跨年\所有的合并\1-2月\test_Jan&Feb.csv', encoding='utf-8')
df = df[df['type_event'] != 'Void']
df['tag'].fillna('nan')
#df = df[df['delay'] == 0]


# 活动类型、关键词为主键累计热度
def split(data, column):
    df_1 = data[column].str.split(',', expand=True).stack()
    df_1 = df_1.reset_index(level=1, drop=True)
    df_1.name = column
    df_new = data.drop([column], axis=1).join(df_1).reset_index().drop(columns='index')
    df_new = df_new[~df_new[column].isnull()]
    return df_new

# 看板2：活动类型分月数据
df_type = split(df, 'type_event')
df_type['type_index'] = df_type.groupby(by=['type_event'])['event_index'].transform('sum')  # 活动类型热度
df_type['n_event'] = df_type.groupby(by=['type_event'])['nm_event'].transform('count')  # 活动数量
print(df_type)

#df_type.to_csv(r'D:\04-数据产品\D-榜单\榜单计算\！！重新合并！！\跨年\所有的合并\1-2月\type_index.csv')


cols=['year_define','month_define', 'type_event', 'type_index', 'n_event']
df_type=df_type[cols].drop_duplicates('type_event')


# 看板2：关键词热度数据
df_tag = split(df, 'tag')
df_tag['tag_index'] = df_tag.groupby(by=['tag'])['event_index'].transform('sum')  # 关键词热度
#print(df_tag)
#df_tag.to_csv(r'D:\04-数据产品\D-榜单\榜单计算\！！重新合并！！\跨年\所有的合并\1-2月\tag_index.csv')
df_tag_type=df_tag[['type_event', 'tag', 'event_index']].drop_duplicates(['type_event', 'tag'], ignore_index=True)
df_tag_type=split(df_tag_type,'type_event')
df_tag_type['tag_type_index'] = df_tag_type.groupby(by=['type_event','tag'])['event_index'].transform('sum')  # 活动类型热度
#print(df_tag_type)
#df_tag_type.to_csv(r'D:\04-数据产品\D-榜单\榜单计算\！！重新合并！！\跨年\所有的合并\1-2月\tag_type.csv')

df_tag['count_tag'] = df_tag.groupby(by=['tag', 'type_event'])['type_event'].transform('count')  # 关键词分活动类型词频
df_tag_index = df_tag[['year_define','month_define', 'tag', 'type_event', 'tag_index', 'count_tag']].drop_duplicates(
    ['tag', 'type_event'], ignore_index=True)
df_tag_index = df_tag_index.sort_values('tag_index', ascending=False)  # 根据热度降序排列
#print(df_tag_index)
#df_tag_index.to_csv(r'D:\04-数据产品\D-榜单\榜单计算\！！重新合并！！\跨年\所有的合并\2月\tag_index.csv')

# 看板2：关键词举办设施（设施类型+设施名称）
def df_tag_place(column1,column2):
    """
    :param column1: 'tag' / 'type_event'
    :param column2: 'type_place' / 'nm_place'
    :return:
    """
    df_tag_place = df_tag[~df_tag[column1].isnull()]
    df_tag_place['tag_index'] = df_tag_place.groupby(by=[column1, column2])['event_index'].transform('sum')  # 关键词分场所统计热度
    df_tag_place['n_event'] = df_tag_place.groupby(by=[column1, column2])['nm_event'].transform('count')  # 关键词分月统计活动个数
    df_tag_place_index=df_tag_place[['year_define','month_define', column1, column2, 'tag_index','n_event']].drop_duplicates(['tag', column2], ignore_index=True)
    df_tag_place_index = df_tag_place_index.sort_values('tag_index', ascending=False)  # 根据热度降序排列
    return df_tag_place_index

df_tag_nmplace_index = df_tag_place('tag','nm_place')   ####这里是看板1中的表3-1

#df_tag_index.to_csv(r'D:\04-数据产品\D-榜单\榜单计算\！！重新合并！！\跨年\所有的合并\2月\tag_nmplace_index.csv')
test_1 = df_tag_nmplace_index[df_tag_nmplace_index['tag'] == '亲子']
#print(test_1)

#test_1.to_csv(r'D:\04-数据产品\D-榜单\榜单计算\！！重新合并！！\跨年\所有的合并\2月\tag_qinzi.csv')






df_tag_typeplace_index = df_tag_place('tag','type_place')   ####这里是看板2中的表3-2
test1 = df_tag_typeplace_index[df_tag_typeplace_index['tag'] == '亲子']
#df_tag_typeplace_index.to_csv(r'D:\04-数据产品\D-榜单\榜单计算\！！重新合并！！\跨年\所有的合并\2月\tag_typeplace_index.csv')
#print(test1)




df_placetype_event = split(df, 'type_place')
df_placetype_event['placetype_index_event'] = df_placetype_event.groupby(['type_event', 'type_place'])[
    'event_index'].transform('sum')

df_eventtype = split(df, 'type_event')

# 分活动类型聚合
#df_eventtype.to_csv(r'D:\04-数据产品\D-榜单\榜单计算\主题计算\df_eventtype.csv')
# df_eventtype['eventtype_index'] = df_placetype.groupby(['type_event'])['event_index'].transform('sum')

# print(df_tag)
# print(df_place)
# print(df_placetype)
# print(df_placetype_event)
# print(df_eventtype)
# df_tag.to_csv(r'D:\04-数据产品\D-榜单\榜单计算\2022Q4\重新合并！！\跨年\result\tag_index_202301.csv')
# df_place.to_csv(r'D:\04-数据产品\D-榜单\榜单计算\主题计算\place_index.csv')
# df_placetype.to_csv(r'D:\04-数据产品\D-榜单\榜单计算\主题计算\placetype_index.csv')

