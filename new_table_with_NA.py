from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col

spark_session = SparkSession.builder.appName('SparkLaba').master("local[*]").getOrCreate()
csv = spark_session.read.options(delimiter=',').csv('file:////Users/ADMIN/Desktop/SparkLaba/brooklyn_sales_map.csv')

count = csv.count()

# ------------------------------------
# Выбираем 10 столбцов из исходной таблицы и собираем DataFrame
column_1 = csv.select('_c8')
column_2 = csv.select('_c12')
column_3 = csv.select('_c15')
column_4 = csv.select('_c22')
column_5 = csv.select('_c20')
column_6 = csv.select('_c17')
column_7 = csv.select('_c16')
column_8 = csv.select('_c22')
column_9 = csv.select('_c19')
column_10 = csv.select('_c13')

column_1_list = list(column_1.take(count))
column_2_list = list(column_2.take(count))
column_3_list = list(column_3.take(count))
column_4_list = list(column_4.take(count))
column_5_list = list(column_5.take(count))
column_6_list = list(column_6.take(count))
column_7_list = list(column_7.take(count))
column_8_list = list(column_8.take(count))
column_9_list = list(column_9.take(count))
column_10_list = list(column_10.take(count))

zipped_tuple = tuple(
    zip(column_1_list, column_2_list, column_3_list, column_4_list, column_5_list, column_6_list, column_7_list,
        column_8_list, column_9_list, column_10_list))

columns = ["building_class", "residential_units", "land_sqft", "year_of_sale", "sale_price", "year_built",
           "gross_sqft", "year_of_sale", "building_class_at_sale", "commercial_units"]

new_data_frame = spark_session.createDataFrame(data=zipped_tuple, schema=columns)
# ------------------------------------
for i in new_data_frame.take(10):
    print(i)
# ------------------------------------
# Количество строк до удаления
print(new_data_frame.count())
# ------------------------------------

new_data_frame = new_data_frame.withColumn("zero_count", lit(0))

# ------------------------------------
# Удаление строк с преобладанием 0 и NA
for row in new_data_frame.take(new_data_frame.count()):
    i = 0
    for value in row:
        for _ in value:
            try:
                if int(_) == 0 or str(_) == 'NA':
                    i += 1
            except TypeError:
                pass
            except ValueError:
                pass
    new_data_frame = new_data_frame.withColumn("zero_count", col("zero_count") + i)

new_data_frame = new_data_frame.filter(new_data_frame["zero_count"] != 10)

# ------------------------------------
# Количество строк после удаления
print(new_data_frame.count())
# ------------------------------------
