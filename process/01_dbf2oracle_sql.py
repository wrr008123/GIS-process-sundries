import datetime

import dbfread
import oracledb
from tqdm import tqdm
from xpinyin import Pinyin


"""

代码验证过没有问题，如果出现异常，大概率就是Oracle用户的权限有问题，以下是一些相关解决措施

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

注意：还需要通过cmd，输入pip install xpinyin安装依赖

"""

p = Pinyin()

# 设置Oracle数据库连接参数
oracle_host = 'localhost'
oracle_port = 1521
oracle_sid = 'ORCL'
oracle_username = 'root'
oracle_password = 'Sa123456'

# DBF文件路径
dbf_file_path = r'D:\project\wrr\贵阳市.dbf'

# 打开DBF文件
dbf_table = dbfread.DBF(dbf_file_path, encoding='utf-8')

# 提取表名
table_name = dbf_table.name
table_name = p.get_pinyin(table_name, '_')


def dbf_type2oracle_type(type_char):
    map = {
        'N': 'int',
        'F': 'float',
        'D': 'date',
        'C': 'nvarchar2(50)'
    }
    return map[type_char]


# 提取字段名和类型
field_definitions = []
fields_str = ''
for field in dbf_table.fields:
    field_name = field.name
    field_type = dbf_type2oracle_type(field.type)
    field_definitions.append((field_name, field_type))
    fields_str += f"\"{p.get_pinyin(field_name, '_')}\","
fields_str = fields_str[0:-1]

print(f'fields: {fields_str}')

# 创建Oracle表
create_table_query = f"CREATE TABLE \"{table_name}\" ("
for field_def in field_definitions:
    field_name, field_type = field_def
    create_table_query += f"\"{p.get_pinyin(field_name, '_')}\" {field_type}, "
create_table_query = create_table_query.rstrip(', ')
create_table_query += ")"

print(create_table_query)

# 连接Oracle数据库

# dsn = f'{oracle_username}/{oracle_password}@{oracle_host}:{oracle_port}/orclpdb'
# oracle_conn = connection = oracledb.connect(dsn)

dsn = oracledb.makedsn(oracle_host, oracle_port, oracle_sid)
oracle_conn = oracledb.connect(user=oracle_username, password=oracle_password, dsn=dsn)
oracle_cursor = oracle_conn.cursor()

oracle_cursor.execute(create_table_query)
 

# 开启事物
oracle_conn.begin()

try:
    # 插入数据
    insert_query_prefix = f"INSERT INTO \"{table_name}\" ({fields_str}) VALUES ("
    for record in tqdm(dbf_table):
        insert_query = insert_query_prefix
        for field_def in field_definitions:
            field_name, _ = field_def
            field_value = record[field_name]
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
        oracle_cursor.execute(insert_query)

    # 提交更改并关闭连接
    oracle_conn.commit()

except Exception as e:
    print(f"An error occurred: {str(e)}")
    # 回滚事务
    oracle_conn.rollback()

oracle_cursor.close()
oracle_conn.close()
