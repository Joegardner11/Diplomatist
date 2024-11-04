import pymysql
db = pymysql.connect(host='localhost',
                     port=3306,
                     user='root',
                     password='admin',
                     db='dependencies_npm')


def fetch_project_names():
    try:
        with db.cursor() as cursor:
            query = """
            SELECT DISTINCT project_name
            FROM jar_dependencies
            WHERE project_name IN (
                SELECT project_name
                FROM jar_dependencies
                GROUP BY project_name
                HAVING COUNT(DISTINCT category) = 1
            )
            """
            cursor.execute(query)
            project_names = [item[0] for item in cursor.fetchall()]  # Collecting project names
            print(project_names)
        return project_names
    except Exception as e:
        print("Error while fetching edges:", e)
        db.rollback()  # 错误时回滚

def fetch_dependencies(project_names):
    results = []
    if not project_names:
        print("No project names provided.")
        return []  # Return an empty list if no project names
    try:
        with db.cursor() as cursor:
            # 遍历每个项目名并执行查询
            for project_name in project_names:
                print("project_name===",project_name)
                cursor.execute("""select * from test_nodes_c where node_ID = %s""", (project_name,))
                fetched_results = cursor.fetchall()  # 先获取查询结果并保存
                print("查询结果为", fetched_results)  # 打印查询结果
                results.extend(fetched_results)  # 将获取的结果添加到结果列表中
        print("results的结果为",results)
        return results
    except pymysql.Error as e:
        print("Error while fetching dependencies:", e)
        # return results  # Return collected results even if some queries fail
    finally:
        if db.open:
            db.close()  # Ensure the connection is closed properly

def save_to_file(data, filename="cross_two.txt"):
    with open(filename, "w") as file:
        for row in data:
            file.write('\t'.join(str(field) for field in row) + '\n')


def main():
    try:
        project_names = fetch_project_names()
        # print(project_names)
        if project_names:
            dependencies = fetch_dependencies(project_names)
            if dependencies:
                save_to_file(dependencies)
            else:
                print("No dependencies data to save.")
        else:
            print("No project names found.")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()




