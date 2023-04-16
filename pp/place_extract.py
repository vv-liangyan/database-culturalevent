from dict_match import dict_match
import numpy as np
from LAC import LAC
import flask
import pandas as pd


class Placeprocess:
    '''
    唯一ID为 'index'
    '''
    def __init__(self):
        self.lac = LAC(mode='lac')
        self.ori2new_typedict = dict(pd.read_csv(r'D:\03-常用代码\榜单\index_cal_sj\data\place_type.csv').values)
        self.DictSlot = dict_match.DictSlot(r'D:\03-常用代码\榜单\index_cal_sj\data\whhd_place.txt')
        self.app = flask.Flask(__name__)
        self.app.config["DEBUG"] = True
        self.app.config['JSON_AS_ASCII'] = False

    def get_LOC(self,d):
        return [k for k, v in d.items() if v == 'LOC']

    def get_PER(self,d):
        return [k for k, v in d.items() if v == 'PER']

    def get_ORG(self,d):
        return [k for k, v in d.items() if v == 'ORG']

    def lac_loc(self, text):
        text_seg = list(self.lac.run(text))
        add_dict = dict(zip(text_seg[0], text_seg[1]))
        loc = self.get_LOC(add_dict)
        return loc

    def lac_org(self, text):
        text_seg = list(self.lac.run(text))
        add_dict = dict(zip(text_seg[0], text_seg[1]))
        org = self.get_ORG(add_dict)
        return org

    def place_to_type(self, data):
        data["type_place"] = 'nan'
        for i in range(len(data["index"])):
            try:
                if data['nm_place'][i]:
                    place_list = data['nm_place'][i].split(',')
                    type_list = []
                    for place in place_list:
                        type_list.append(self.ori2new_typedict[place])
                        if len(set(type_list)) != 0:
                            data['type_place'][i] = ','.join(set(type_list))
                else:
                    # 映射表中未出现的场所，根据关键词得场所类别
                    for i in range(len(data["index"])):
                        if not data['type_place'][i]:
                            if '艺术中心|艺术园' in data["nm_place"][i]:
                                data['type_place'][i] = '艺术中心'
                            elif '小区' in data["nm_place"][i]:
                                data['type_place'][i] = '住宅楼'
                            elif any(place in data["nm_place"][i] for place in
                                     "产业|科技园|孵化|文创园|文化园|创意园"):
                                data['type_place'][i] = '产业园区'
                            elif any(place in data["nm_place"][i] for place in "酒店|宾馆"):
                                data['type_place'][i] = '酒店'
                            elif any(place in data["nm_place"][i] for place in "影城|电影院|电影城|影楼"):
                                data['type_place'][i] = '电影院'
                            elif any(place in data["nm_place"][i] for place in "酒吧|bar|BAR|茶馆"):
                                data['type_place'][i] = '酒吧茶馆'
                            elif '咖啡|coffee|Coffee|Cafe|cafe|COFFEE' in data["nm_place"][i]:
                                data['type_place'][i] = '咖啡厅'
                            elif '购物中心|商场' in data["nm_place"][i]:
                                data['type_place'][i] = '综合商场'
                            elif '大学|附中|小学|中学' in data["nm_place"][i]:
                                data['type_place'][i] = '学校'
                            elif '图书馆' in data["nm_place"][i]:
                                data['type_place'][i] = '图书馆'
                            elif '美术馆' in data["nm_place"][i]:
                                data['type_place'][i] = '美术馆'
                            elif '滑雪场|羽毛球|篮球|足球|' in data["nm_place"][i]:
                                data['type_place'][i] = '其他体育场馆'
                            elif '剧场' in data["nm_place"][i]:
                                data['type_place'][i] = '剧场'
                            else:
                                continue
            except:
                continue
        return data

    def unify_string(self, s):
        '''
        # 统一字符顺序
        :return:
        '''
        lst = s.split(',')
        lst_sorted = sorted(lst)
        return ','.join(lst_sorted)

    def process_all(self, df):
        # load input
        # df = pd.read_csv(r'D:\00-业务项目\H-浙文投\文化\2022_文化复苏.csv', encoding='utf-8-sig')

        # # load 场所词库: whhd_place
        # DictSlot = dict_match.DictSlot(r'D:\03-常用代码\places_new\whhd_place.txt')
        # # load 场所类别映射表: place_type
        # type_reference = pd.read_csv(r'D:\03-常用代码\places_new\place_type.csv')

        df['nm_event'] = df['nm_event'].fillna('na')
        df['address_ori'] = df['address_ori'].fillna('na')

        # 将所有英文字符统一为大写
        df['nm_event'] = df['nm_event'].str.upper()
        df['nm_place_ori'] = df['nm_place_ori'].str.upper()
        df['address_ori'] = df['address_ori'].str.upper()

        # 地址不是地铁站,先用addr后用nm
        df_1 = df[
            ~df['address_ori'].str.contains(
                '地铁|火车站|和平西桥|北京传媒大学|北京语言大学|惠新西街|出口|东大桥站|广渠门|青年路|北海公园(北门)|NAGA上院|丽晶酒店|A口|B口|C口|D口|E口|F口')]
        df_1 = df_1[df_1['address_ori'].notnull()]
        df_1["new_address_ori"] = df_1["address_ori"].apply(self.DictSlot.predict)
        df_1["new_nm_event"] = df_1["nm_event"].apply(self.DictSlot.predict)
        df_1["resu"] = df_1["new_address_ori"]
        df_1 = df_1.reset_index()
        for i in range(len(df_1["address_ori"])):
            if df_1["new_address_ori"][i] == []:
                df_1["resu"][i] = df_1["new_nm_event"][i]

        # 地址是地铁站,用nm
        df_2 = df[
            df['address_ori'].str.contains(
                '地铁|火车站|和平西桥|北京传媒大学|北京语言大学|惠新西街|出口|东大桥站|广渠门|青年路|北海公园(北门)|NAGA上院|丽晶酒店|A口|B口|C口|D口|E口|F口')]
        df_2["resu"] = df_2["nm_event"].apply(self.DictSlot.predict)

        # 合并结果
        df_3 = pd.concat([df_1, df_2])  # 所有活动

        # 是大麦网\公众号的,用nm_place_ori结果更新
        df_dmw = df_3[df_3['event_source'].str.contains('dmw') == True]
        df_dbhdx = df_3[df_3['event_source'].str.contains('dmw') == False]
        df_dmw['nm_place_ori'] = df_dmw['nm_place_ori'].fillna('na')
        df_dmw['resu'] = df_dmw['nm_place_ori'].apply(self.DictSlot.predict)
        df = pd.concat([df_dbhdx, df_dmw])
        df_e = df.explode('resu')

        # 生成逆向提槽结果
        df_e = pd.concat([df_e, df_e["resu"].apply(pd.Series)], axis=1)
        df_slot_value_merge = df_e.groupby('index').apply(
            lambda x: x if x['slot_value'] is np.nan else ','.join(set(map(str, x['slot_value']))))
        df_slot_value_merge = pd.DataFrame(df_slot_value_merge, columns=['slot_value'])
        df_4 = pd.merge(df, df_slot_value_merge, on=['index'])
        df_4.loc[df_4['slot_value'] == 'nan', 'slot_value'] = ''  # 逆向提槽无结果的，slot_value改为空
        df = df_4

        # LAC提slot_value为空的场所名称
        # lac = LAC()
        # 需要删掉的行政区划&错误字符list
        list_govern = ['海淀', '朝阳', '丰台', '门头沟', '石景山', '房山', '通州', '顺义', '昌平', '大兴', '怀柔', '平谷',
                       '延庆', '密云', '东城', '西城', '海淀', '北京东城', '北京海淀', '北京朝阳', '北京丰台', '北京门头沟',
                       '北京石景山', '北京房山', '北京通州', '北京顺义', '北京昌平', '北京大兴', '北京怀柔', '北京平谷',
                       '北京延庆', '北京密云', '北京东城', '北京海淀', '北京朝阳', '北京丰台', '北京门头沟', '北京石景山',
                       '北京房山', '北京通州', '北京顺义', '北京昌平', '北京大兴', '北京怀柔', '北京平谷', '北京延庆',
                       '北京密云', '中国', '北京', '北京市', '东城区', '北京东城区', '北京市东城区', '西城区',
                       '北京市西城区',
                       '海淀区', '朝阳区', '丰台区', '门头沟区', '石景山区', '房山区', '通州区', '顺义区', '昌平区',
                       '大兴区',
                       '怀柔区', '平谷区', '延庆县', '密云区', '密云县', '北京市海淀区', '北京市朝阳区', '北京市丰台区',
                       '北京市门头沟区', '北京市石景山区', '北京市房山区', '北京市通州区', '北京市顺义区', '北京市昌平区',
                       '北京市大兴区', '北京市怀柔区', '北京市平谷区', '北京市延庆县', '北京市密云区', '东直门', '亚洲',
                       '非洲',
                       '华北', '日本', '瑞士', '京', '京城', '漫山', '北京地区', '河北', '北京站', '北宋', '佛罗伦萨',
                       '欧洲',
                       '泰国',
                       '贝加尔湖畔', '波兰', '察哈尔', '威尼斯', '中日', '美国', '西班牙', '广渠门', '华为', '网易', '江南',
                       '京津冀', '敦煌',
                       '北京胡同', '零基础组乐队', '玛雅', '贵州']

        df['resu_lac'] = df['slot_value']
        df['nm_place'] = df['slot_value']

        for i in range(len(df["index"])):
            string = df['nm_event'][i]
            lac_after = self.lac_org(string)
            if lac_after == []:
                lac_after = self.lac_loc(string)  # org返不回来的用loc
            lac_after = list(set(lac_after) - set(list_govern))  # 去除lac中包含行政区划的字符
            df['resu_lac'][i] = ','.join(lac_after)
            if not df['slot_value'][i]:
                df['nm_place'][i] = df['resu_lac'][i]  # 逆向提槽为空的用lac结果补上

        df["nm_place"] = df["nm_place"].fillna("0")

        df["nm_place"] = df["nm_place"].apply(self.unify_string)  # 统一字符顺序
        data_result = self.place_to_type(df)  # 映射场所类型
        # data_result=data_result.drop(['new_nm_event','new_address_ori'])
        return data_result



if __name__ == '__main__':
    # df = pd.read_csv(r'D:\04-数据产品\D-榜单\榜单计算\！！重新合并！！\跨年\所有的合并\all_concat.csv',encoding='utf-8-sig')
    pp = Placeprocess()
