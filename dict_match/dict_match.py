#!/usr/bin/env python3
# -*- encoding: utf-8 -*-
'''
@File    :   dict_match.py
@Time    :   2022/05/12 16:41:34
@Author  :   zsong 
@Version :   1.0
@Contact :   szd@urbanxyz.com
@License :   (C)Copyright 2021-2022
@Desc    :   最大逆向匹配进行提槽
'''

class DictSlot:
    def __init__(self, dict_path):
        slot_dict = {}
        with open (dict_path, encoding='utf-8') as f:
            for line in f:
                ll = line.strip().split('\t')
                if len(ll) != 2:
                    continue
                slot_info = ll[1].split('+++')
                if len(slot_info) != 2:
                    continue
                if ll[0] in slot_dict:
                    slot_dict[ll[0]].append({'slot_name': slot_info[0], 'slot_value': slot_info[1]})
                else:
                    slot_dict[ll[0]] = [{'slot_name': slot_info[0], 'slot_value': slot_info[1]}]
        self.slot_dict = slot_dict
    

    def predict(self, query):
        query_len = len(query)
        idx = 0
        idy = query_len
        slot_get = []
        tmp_slot_get_len = 0
        while idy > 0:
            while idx < idy:
                if query[idx:idy] in self.slot_dict:
                    for item in self.slot_dict[query[idx:idy]]:
                        slot_get.append({'slot_word': query[idx:idy],
                        'slot_name': item['slot_name'],
                        'slot_value': item['slot_value'],
                        'slot_start': idx,
                        'slot_end': idy
                        })
                    break
                idx = idx + 1
            if len(slot_get) != tmp_slot_get_len:
                idy = idx
                idx = 0
                tmp_slot_get_len = len(slot_get)
            else:
                idx = 0
                idy = idy - 1
        return slot_get


if __name__ == '__main__':
    dict_sloter = DictSlot(r'D:\03-常用代码\places_new\whhd_place.txt')
    print(dict_sloter.predict("我想去北京的故宫"))
    #print(dict_sloter.slot_dict)

