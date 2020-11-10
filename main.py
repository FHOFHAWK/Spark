from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

list_of_zip_code_and_tax_class_dicts = []
tax_classes_list = []
zip_codes_list = []
average_years_list = []
total_sale_price_list = []

spark = SparkSession.builder.appName('SparkLaba').master("local[*]").getOrCreate()
# csv - DataFrame
csv = spark.read.options(delimiter=',').csv('file:////Users/ADMIN/Desktop/SparkLaba/brooklyn_sales_map.csv')

# Добавление
csv_data_frame = csv.withColumn("age_of_house", lit('None'))
count = csv_data_frame.count()

# Значения:
# line[4] - tax_class
# line[11] - zip_code
# line[20] - sale_price
# line[22] - year_of_sale

# Заполнение list_of_zip_code_and_tax_class_dicts уникальными значениям zip_code + tax_class
for line in csv_data_frame.take(count):
    try:
        new_zip_code_and_tax_class_dict = {line[11]: line[4]}
        if new_zip_code_and_tax_class_dict not in list_of_zip_code_and_tax_class_dicts:
            list_of_zip_code_and_tax_class_dicts.append(new_zip_code_and_tax_class_dict)
    except TypeError:
        pass
    except ValueError:
        pass

# Удаление header`а таблицы и разбор листа словарей на отдельные листы
list_of_zip_code_and_tax_class_dicts.remove({'zip_code': 'tax_class'})
for item in list_of_zip_code_and_tax_class_dicts:
    for key in item:
        zip_codes_list.append(key)
        tax_classes_list.append(item[key])

# Формирование листа средних лет продажи по уникальным zip_code + tax_class
for i in range(len(zip_codes_list)):
    after_filter = csv_data_frame.filter(
        (csv_data_frame['_c11'] == zip_codes_list[i]) & (csv_data_frame['_c4'] == tax_classes_list[i]))

    total_sale_price = 0
    filter_count = after_filter.count()
    year = 0
    for row in after_filter.take(filter_count):
        try:
            year += int(row['_c22'])
            total_sale_price += int(row['_c20'])
        except TypeError:
            pass
        except ValueError:
            pass
    try:
        average_years_list.append(str(int(year / filter_count)))
        total_sale_price_list.append(total_sale_price)
    except ZeroDivisionError:
        average_years_list.append('0')

zipped_lists = zip(tax_classes_list, zip_codes_list, total_sale_price_list, average_years_list)
tupled_lists = tuple(zipped_lists)

columns = ["tax_class", "zip_code", "sale_price", "year_of_sale"]
data_frame = spark.createDataFrame(data=tupled_lists, schema=columns)

data_frame.write.format('csv').option('header', True).mode('overwrite').option('sep', '|').save(
    'file:////Users/ADMIN/Desktop/SparkLaba/result.csv')
