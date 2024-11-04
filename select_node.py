import pymysql
from threading import Lock
lock = Lock()
db = pymysql.connect(host='localhost',
                     port=3306,
                     user='root',
                     password='admin',
                     db='dependencies_npm')


def insert_Nodes(Item_infor):  # 向数据库里存储节点信息   node_id,node_version,node_dependency  ////改成了一次性存取多项数据的形式
    cur = db.cursor()
    with lock:
        try:
            sql_check = "select * from export"
            cur.execute(sql_check)
            Item_infor = [row[0] for row in cur]
            print(f"{len(Item_infor)} records")

            # # 创建新表depen_npm_infor
            # sql_create_table = "CREATE TABLE IF NOT EXISTS depen_npm_infor (goal_node VARCHAR(255), dependency VARCHAR(255))"
            # cur.execute(sql_create_table)

            for infor in Item_infor:
                sql_select = f"SELECT ini_node FROM nodes_dependencies WHERE goal_node LIKE '{infor}%%'"
                cur.execute(sql_select)

                # 将查询结果插入新表depen_npm_infor
                sql_insert = "INSERT IGNORE INTO  depen_npm_infor (name) VALUES (%s)"
                results = cur.fetchall()
                cur.executemany(sql_insert, results)
                db.commit()

            print(f"Inserted {len(Item_infor)} new records.")

        except Exception as e:
            print(f"Error: {str(e)}")
            db.rollback()

        finally:
            cur.close()


# def insert_Nodes(Item_infor):    #向数据库里存储节点信息   node_id,node_version,node_dependency  ////改成了一次性存取多项数据的形式
#     cur = db.cursor()
#     with lock:
#         try:
#             sql_check="select * from export"
#             cur.execute(sql_check)
#             Item_infor=[row[0] for row in cur]
#             print(f"{len(Item_infor)} records")
#             for infor in Item_infor:
#                 sql_select = f"select * from nodes_dependencies where goal_node like '{infor}%%'"
#                 cur.execute(sql_select)   #一次存入一个项目及其所有的依赖项
#                 db.commit()
#                 print(f"Inserted {len(Item_infor)} new records.")
#         else:
#             print("NO new records to insert.")
#         existing_items.clear()
#         cur.close()   #修改这里的提交方式，频繁的写入数据库，改为阶段性 或者读取文件进行存取
#         #db.close()
#     except Exception as e:
#         print(e)
#         db.rollback()



if __name__ == '__main__':
    Item_infor = []
    insert_Nodes(Item_infor)