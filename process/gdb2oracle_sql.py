from osgeo import ogr
from tqdm import tqdm
from xpinyin import Pinyin
import oracledb
import datetime

# GDB路径
gdb_path = r"C:\Users\wrr\Documents\ArcGIS\花溪_研究区_住宿业_餐饮业.gdb"

# GDB中待导入的表
table_names = ['花溪_餐饮业', '花溪_服务业']

# 设置Oracle数据库连接参数
oracle_host = 'localhost'
oracle_port = 1521
oracle_sid = 'ORCL'
oracle_username = 'root'
oracle_password = 'Sa123456'

"""

！！！！需要安装GDAL库！！！！

Oracle用户的权限问题，以下是一些相关解决措施

1. 创建用户的时候提示用户名无效，这是因为现版本oracle用户名有C##开头的限制，去除c##限制
alter session set "_ORACLE_SCRIPT"=true;

2. 创建用户（当默认用户运行程序失败时，则需要使用sysdba账号，也就是安装oracle后的默认帐号，去创建一个新用户）
create user xx identified by Sa123456 

3. 赋予用户权限，xx是用户名，可自行替换
grant connect,resource to xx;

4. 赋予表空间权限
grant unlimited tablespace to xx;

5. 提交修改
commit;


以上是通过sqlplus（win+r，然后输入sqlplus启动）登录oracle初始账号执行
然后输入用户名称
再输入密码，如密码Sa123456，那么将需要输入为 Sa123456 as sysdba
登录进去后再根据以上创建用户以及赋予权限，然后使用新用户登录即可（初始用户是没法去创建表插入记录的）

注意：还需要安装gdal依赖

"""


def gdb_type2oracle_type(type_num):
    map = {
        ogr.OFTInteger: 'int',
        ogr.OFTInteger64: 'int',
        ogr.OFTReal: 'float',
        ogr.OFTString: 'nvarchar2(50)',

        ogr.OFTDate: 'date',
        ogr.OFTTime: 'date',
        ogr.OFTDateTime: 'date'
    }
    return map[type_num]


p = Pinyin()
driver = ogr.GetDriverByName("OpenFileGDB")
gdb_dataset = driver.Open(gdb_path, 0)
layer_count = gdb_dataset.GetLayerCount()


def get_pinyin_lower(content):
    c = p.get_pinyin(content, '_')
    return c.lower()


name_index = {}
index_name = {}
name_list = []
for i in range(layer_count):
    layer = gdb_dataset.GetLayer(i)
    name = layer.GetName()
    name_index[name] = i
    index_name[i] = name
    name_list.append(name)

print(f'GDB layers have: {name_list}')

for tn in table_names:
    if tn not in name_list:
        raise Exception(f'表 {tn} 不在GDB数据库中，请检查！')

dsn = oracledb.makedsn(oracle_host, oracle_port, oracle_sid)
oracle_conn = oracledb.connect(user=oracle_username, password=oracle_password, dsn=dsn)
oracle_cursor = oracle_conn.cursor()

for tn in table_names:
    layer = gdb_dataset.GetLayer(name_index[tn])
    layer_defn = layer.GetLayerDefn()
    field_count = layer_defn.GetFieldCount()

    field_definitions = []
    fields_str = ''
    # 遍历字段定义
    for i in range(field_count):
        field_defn = layer_defn.GetFieldDefn(i)
        field_name = field_defn.GetName()
        field_type = gdb_type2oracle_type(field_defn.GetType())
        field_width = field_defn.GetWidth()

        field_definitions.append((field_name, field_type))
        fields_str += f"\"{get_pinyin_lower(field_name)}\","

    fields_str = fields_str[0:-1]

    # 创建Oracle表
    table_name = get_pinyin_lower(tn)
    create_table_query = f"CREATE TABLE \"{table_name}\" ("
    for field_def in field_definitions:
        field_name, field_type = field_def
        create_table_query += f"\"{get_pinyin_lower(field_name)}\" {field_type}, "
    create_table_query = create_table_query.rstrip(', ')
    create_table_query += ")"

    print(f'{tn}: {create_table_query}')

    oracle_cursor.execute(create_table_query)

    # 开启事物
    oracle_conn.begin()

    try:
        # 插入数据
        insert_query_prefix = f"INSERT INTO \"{table_name}\" ({fields_str}) VALUES ("

        for feature in tqdm(layer):
            insert_query = insert_query_prefix
            for field_def in field_definitions:
                field_name, _ = field_def
                # 这里如果是时间字段可能会有问题，因为时间字段读取出来的好像是字符串
                field_value = feature.GetField(field_name)
                if isinstance(field_value, str):
                    field_value = field_value.replace("'", "''")

                    if field_value == '':
                        insert_query += "NULL, "
                    else:
                        insert_query += f"'{field_value}', "

                elif field_value is None:
                    insert_query += "NULL, "
                elif isinstance(field_value, datetime.date) or isinstance(field_value, datetime.datetime):
                    date_str = field_value.strftime('%Y-%m-%d %H:%M:%S')
                    insert_query += f"to_date(\'{date_str}\', \'YYYY-MM-DD HH24:MI:SS\'), "
                else:
                    insert_query += f"{field_value}, "

            insert_query = insert_query.rstrip(', ')
            insert_query += ")"
            # print(insert_query)
            oracle_cursor.execute(insert_query)

        # 提交更改并关闭连接
        oracle_conn.commit()
        print('-----------------\n')

    except Exception as e:
        print(f"An error occurred: {str(e)}")
        # 回滚事务
        oracle_conn.rollback()

oracle_cursor.close()
oracle_conn.close()
