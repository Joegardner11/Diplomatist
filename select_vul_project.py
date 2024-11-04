import pymysql
db = pymysql.connect(host='localhost',
                     port=3306,
                     user='root',
                     password='admin',
                     db='analytic_zip')

#
# def select_vul_name():
#     cur = db.cursor()
#     cur.execute("SELECT cpe FROM c_vulnerability")  # SELECT DISTINCT dependency_name FROM jar_dependencies
#     cpe_records = cur.fetchall()
#     for record in cpe_records:
#         cpe_name = str(record[0]).replace('["', '').replace('"]', '').split(':')
#         if len(cpe_name) > 4:
#             name = cpe_name[4]
#             cur.execute("UPDATE c_vulnerability SET name = %s WHERE cpe = %s", (name, record[0]))
#             db.commit()  # Commit the changes to the database



def select_vul_name():   #将c的漏洞库名字进行截取并存在name下
    cur = db.cursor()
    cur.execute("SELECT cpe FROM c_vulnerability")
    cpe_records = cur.fetchall()
    for record in cpe_records:
        cpe_value = record[0]
        if cpe_value:  # Check if cpe is not empty or None
            cpe_values = cpe_value.strip('[]').split('","')  # Split multiple CPEs
            names_set = set()
            for cpe_value in cpe_values:
                cpe_name = cpe_value.split(':')
                if len(cpe_name) > 4:
                    name = cpe_name[4]
                    names_set.add(name)
            if names_set:
                name_str = ','.join(names_set)  # Join names with ',' if more than one
                # print(name_str)
                cur.execute("UPDATE c_vulnerability SET name = %s WHERE cpe = %s", (name_str, record[0]))
                db.commit()


def fetch_and_compare_dependencies():
    cur = db.cursor()
    # 查询c_rust_dependencies表中的so_component数据
    cur.execute("SELECT so_component FROM c_rust_dependencies where so_component is not NULL")
    component_data = cur.fetchall()
    # 对so_component进行预处理，先以逗号分割，再取每个分割后的字符串的@@前的部分
    component_set = set()
    for row in component_data:
        components = row[0].split(',')
        for component in components:
            component_set.add(component)   # component.split('@@')[0]
    component_set_length = len(component_set)
    print(f"Length of component_set: {component_set_length}")
    # 查询c_vulnerability表中的name数据
    cur.execute("SELECT name FROM c_vulnerability where name is not NULL")
    name_data = cur.fetchall()
    # 假设name字段同样包含逗号分隔的多值，进行类似处理
    name_set = set()
    for row in name_data:
        names = row[0].split(',')
        for name in names:
            name_set.add(name)
    # 比对和处理数据
    for component in component_set:
        compare=component.split('@@')[0]
        if compare in name_set:
            # 如果找到匹配，将数据插入c_depen_vul_name表
            cur.execute("INSERT IGNORE INTO c_depen_vul_name (vul_name) VALUES (%s)", (component,))
        # 提交事务并关闭连接
        db.commit()



if __name__ == "__main__":
    # select_vul_name()

    fetch_and_compare_dependencies()