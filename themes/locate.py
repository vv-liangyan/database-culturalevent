import geopandas as gpd
from shapely.geometry import Point, Polygon
from shapely import wkb
import pandas as pd
from shapely.geometry import box
from shapely import wkt
import fiona
import os


class geo:
    def __init__(self):
        pass
        # self.file_path = file_path
        # self.file_name = table_name
        # self.gdf = gpd.read_file(os.path.join(file_path, table_name))

    # 提取面内的点
    def spatialjoin_points_within_polygons(self, points_dataframe, polygons_dataframe, wkt_column_polygon):
        """
        进行空间连接（spatial join），并将面数据的特定列添加到点数据表中。

        参数：
        - points_file: 点数据文件名，可以是包含点数据的 shapefile、GeoJSON 文件或 CSV 文件。根据'lon_earth''lat_earth'生成几何图形。
        - polygons_file: 面数据文件名，可以是包含面数据的 shapefile、GeoJSON 文件或 CSV 文件。根据 'geom'(wkb/wkt)生成几何图形。

        返回值：
        - 一个包含点数据和相应面数据列的 geopandas 数据框。
        """
        # 读取面数据
        polygons_dataframe['geometry'] = polygons_dataframe[wkt_column_polygon].apply(wkt.loads)  # WKT
        # polygons_dataframe['geometry'] = polygons_dataframe[geom_column_polygon].apply(lambda x: wkb.loads(bytes.fromhex(x)))  #WKB

        # 创建点数据的几何列
        points_dataframe['lon_earth'] = points_dataframe['lon_earth'].fillna(0)
        points_dataframe['lat_earth'] = points_dataframe['lat_earth'].fillna(0)
        points_dataframe['geometry'] = points_dataframe.apply(
            lambda row: Point(float(row['lon_earth']), float(row['lat_earth'])), axis=1)
        points = gpd.GeoDataFrame(points_dataframe, geometry='geometry')
        polygons = gpd.GeoDataFrame(polygons_dataframe, geometry='geometry')

        # 进行空间连接
        joined_data = gpd.sjoin(points, polygons, how='left', op='within').drop(columns=['geometry', 'index_right'])

        return joined_data

    # 四至转图形
    def bounds_to_polygon(self, bounds):
        # 假设bounds格式为 'xmin,ymin,xmax,ymax'
        coords = bounds.split(',')
        # 转换为float类型
        coords = [float(coord) for coord in coords]
        # 创建Polygon对象
        polygon = Polygon(
            [(coords[0], coords[1]), (coords[2], coords[1]), (coords[2], coords[3]), (coords[0], coords[3])])
        return polygon

    # 其他文件格式转geodataframe
    def file_trans_geodf(self, filepath):
        """
        读取Shapefile (.shp)\GeoPackage (.gpkg)\MapInfo (.tab)\Comma Separated Value (.csv)\Microsoft Excel (.xlsx)\SQLite (.sqlite)件并转换为GeoDataFrame
        :param shp_file: shp文件路径
        :return: GeoDataFrame
        """
        # 读取shp文件
        with fiona.open(filepath, 'r') as src:
            # 从Fiona格式转换为GeoDataFrame
            gdf = gpd.GeoDataFrame.from_features(src, crs=src.crs)
        return gdf

    # dissolve
    def dissolve_geos(self, input, dissolve_column):
        # 读取输入文件
        gdf = gpd.read_file(input)
        # 读取面数据
        input['geometry'] = input[dissolve_column].apply(wkt.loads)  # WKT

        # 执行 dissolve 操作
        dissolved_gdf = gdf.dissolve(by=dissolve_column)

        # # 如果指定了输出文件名，则保存为文件
        # if output_file:
        #     dissolved_gdf.to_file(output_file, driver='GeoJSON')

        return dissolved_gdf

    # merge合并
    def merge_geos(self, gdf1, gdf2, by_col, driver):
        """

        :param gdf1:
        :param gdf2:
        :param by_col: str, The name of the common attribute column to merge on.
        :param out_file: str, file_save path
        :param driver: file_format:'ESRI Shapefile'/'GeoJSON'/'GPKG'
        :return:
        """
        merged = gpd.overlay(gdf1, gdf2, how='union')
        merged_gdf = merged.dissolve(by=by_col)
        # merged.to_file(out_file, driver=driver)
        return merged_gdf



# points = pd.read_csv(r'D:\00-业务项目\H-浙文投\POI\DZDP.csv', encoding='utf-8-sig')
# polygon = pd.read_csv(r'D:\00-业务项目\H-浙文投\边界\隆福寺边界.csv', encoding='utf-8-sig')
#
# output = spatialjoin_points_within_polygons(points, polygon, 'wkt')
# output = output[output['wkt_geom'].notnull()]
# print(output)
# output.to_csv(r'D:\00-业务项目\H-浙文投\POI\隆福\dzdp_lfs.csv')
