import psycopg2
from sqlalchemy import create_engine
import pandas as pd
from pp import place_extract
from volume_predict import Volume_predict
from all_index_cal import Calculator
from themes import rank_theme

pd.set_option('display.max_columns', None)
# 建立数据库连接
conn = psycopg2.connect(database="urbanxyz-data-datawarehouse"
                        , user="postgres"
                        , password="sde"
                        , host="192.168.255.220"
                        , port=5432)

sampletime_start = '2022-01-01'  # 活动抓取时间截面 - 从当前月回溯3个月
sampletime_end = '2023-04-12'
year_define = 2023  # 活动实际发生月
month_define = 3

temp_table_1 = 'dwa_p_beijing_culturalevent_2022q42023q1'  # 场所识别后入库表名
temp_table_2 = 'dwa_p_beijing_culturalevent_2022q42023q1_expand'  # 场所按条展开后入库表名
table_ranklist_all = 'dwa_p_beijing_culturalevent_2022q42023q1_ranklist'  # 月度活动热度计算 入库表
table_ranklist_tag = 'dwa_culturalevent_ranklist_tag_2023q1'  # 关键词热度主题表入库表
table_ranklist_tag_place = 'dwa_culturalevent_ranklist_tag_place_202303'  # 关键词下的场所热度主题表入库表
column_sel = 'nm_place'  # 希望以tag聚合的字段


def pp(sampletime_start, sampletime_end):
    """
    场所识别，场所分类
    :return: urbanxyz_data_test.data_culturalevent_2023test_pp
    """
    # 读DWM表进行场所识别,结果传入test库
    # query = "SELECT * FROM urbanxyz_data_dwm.dwm_d_beijing_culturalevent_s_202212 union distinct SELECT * FROM urbanxyz_data_dwm.dwm_d_beijing_culturalevent_d_2023"
    query = f"SELECT * FROM urbanxyz_data_dwm.dwm_d_beijing_culturalevent_s_202212 union distinct SELECT * FROM urbanxyz_data_dwm.dwm_d_beijing_culturalevent_d_2023 WHERE sampletime >= '{sampletime_start}' and sampletime <= '{sampletime_end}'"

    df = pd.read_sql(query, conn)
    pp = place_extract.Placeprocess()
    df_pp = pp.process_all(df).drop(
        columns=['level_0', 'resu', 'slot_value', 'resu_lac', 'new_address_ori', 'new_nm_event'], inplace=False)
    engine = create_engine('postgresql://postgres:sde@192.168.255.220:5432/urbanxyz-data-datawarehouse')
    df_pp.to_sql(temp_table_1, con=engine, schema='urbanxyz_data_test', if_exists='replace', index=False)
    conn.commit()


def time_clean_expand(temp_table_2, temp_table_1):
    """
    将大麦网的场次展开
    :param sampletime_start:需要的采集开始日期
    :param sampletime_end:需要的采集结束日期
    :return:入库
    """
    # 打开本地 psql 文件，读取其中的语句,设置采集开始和结束日期
    with open(r"D:\03-常用代码\榜单\index_cal_sj\time_clean.sql", "r", encoding='utf-8') as f:
        query = f.read()
        query = query.replace('temp_table_2', temp_table_2).replace('temp_table_1', temp_table_1)

    # 执行 psql 语句
    cur = conn.cursor()
    cur.execute(query)
    conn.commit()


def calculate_predict(year_define, month_define,temp_table_2):
    '''
    all_index_cal(volume_predict）
    :return:urbanxyz_data_test.table_output
    '''
    query = f"SELECT * FROM urbanxyz_data_test.{temp_table_2}"

    df = pd.read_sql(query, conn)
    v_instance = Volume_predict()
    df_volume = v_instance.predict_volume(df)
    df_volume['nm_place'].unique()

    df_volume = v_instance.volume_rule(df_volume)  # 赋volume后的结果

    df_calculator = Calculator(year_define, month_define)
    df_index = df_calculator.calculate(df_volume)  # 计算index后的结果
    df_index.to_excel(r'D:\00-业务项目\突如其来的\文化复苏\脱口秀\dwa_p_beijing_culturalevent_2022q42023q1_index.xlsx')

    # 输出榜单表
    engine = create_engine('postgresql://postgres:sde@192.168.255.220:5432/urbanxyz-data-datawarehouse')
    df_index.to_sql(table_ranklist_all, con=engine, schema='urbanxyz_data_test', if_exists='replace', index=False)
    conn.commit()


def calculate_theme(column_sel, table_ranklist_all, table_ranklist_tag, table_ranklist_tag_place):
    query = f"SELECT * FROM urbanxyz_data_test.{table_ranklist_all}"
    # query = "SELECT * FROM urbanxyz_data_test.dwa_culturalevent_ranklist_all_202303"
    # formatted_query = query.format(table_ranklist_all)
    df = pd.read_sql(query, conn)
    t_instance = rank_theme.Rank_theme('tag', column_sel)
    df_tag = t_instance.cal_tag_index(df)
    df_tag_place = t_instance.cal_tag_place_index(df)
    # 输出主题表
    engine = create_engine('postgresql://postgres:sde@192.168.255.220:5432/urbanxyz-data-datawarehouse')
    df_tag.to_sql(table_ranklist_tag, con=engine, schema='urbanxyz_data_test', if_exists='replace', index=False)
    df_tag_place.to_sql(table_ranklist_tag_place, con=engine, schema='urbanxyz_data_test', if_exists='replace', index=False)
    conn.commit()


if __name__ == "__main__":
    # pp(sampletime_start, sampletime_end)
    # time_clean_expand(temp_table_2, temp_table_1)
    calculate_predict(year_define, month_define, temp_table_2)
    # calculate_theme(column_sel, table_ranklist_all, table_ranklist_tag, table_ranklist_tag_place)
