import pymysql
db = pymysql.connect(host='localhost',
                     port=3306,
                     user='root',
                     password='admin',
                     db='dependencies_npm')

def migrate_C_data():
    try:
        with db.cursor() as cursor:
            query = """
               SELECT node_ID, platform, latest_release_published_at
               FROM test_nodes_c
               WHERE platform = 'Maven'
               """
            cursor.execute(query)
            records = cursor.fetchall()
            insert_query = """
                INSERT INTO cross_language_infor (project_name, goal_platform, latest_release_published_at)
                VALUES (%s, 'C/C++', %s)
                """
            for record in records:
                node_id, platform, latest_release_published_at = record
                cursor.execute(insert_query, (node_id, latest_release_published_at))
                # 提交更改并关闭连接
                db.commit()
    except Exception as e:
        print(f"An error occurred: {e}")


def migrate_other_data():
    try:
        with db.cursor() as cursor:
            query = """
               SELECT DISTINCT j.project_name, j.category
                FROM jar_dependencies j
                WHERE j.project_name IN (
                    SELECT project_name
                    FROM jar_dependencies
                    GROUP BY project_name
                    HAVING COUNT(DISTINCT category) = 2     #  2, 3 ,4
                )
               """
            cursor.execute(query)
            records = cursor.fetchall()
            insert_query = """
                INSERT INTO cross_language_infor (project_name, goal_platform, latest_release_published_at)
                VALUES (%s, %s, NULL)
                """
            for record in records:
                project_name, category = record
                cursor.execute(insert_query, (project_name, category))
                # 提交更改并关闭连接
                db.commit()
    except Exception as e:
        print(f"An error occurred: {e}")


def fetch_valid_node_ids():
    try:
        with db.cursor() as cursor:
            query = """   
            SELECT DISTINCT t.node_ID, j.category
            FROM test_nodes_c t
            JOIN jar_dependencies j ON t.node_ID = j.project_name
            WHERE t.platform = 'Maven' AND j.category != 'r'
            GROUP BY t.node_ID, j.category
            HAVING COUNT(DISTINCT j.category) = 1
            """
            cursor.execute(query)
            valid_node_ids = cursor.fetchall()  # Collecting valid node IDs and their categories
            print(valid_node_ids)
        return valid_node_ids
    except Exception as e:
        print("Error while fetching valid node IDs:", e)
        db.rollback()  # 错误时回滚
        return []


def migrate_data():
    valid_node_ids = fetch_valid_node_ids()
    if not valid_node_ids:
        print("No valid node IDs to migrate.")
        return

    try:
        with db.cursor() as cursor:
            insert_query = """
                INSERT INTO cross_language_infor (project_name, goal_platform, latest_release_published_at)
                VALUES (%s, %s, NULL)
                ON DUPLICATE KEY UPDATE
                latest_release_published_at = VALUES(latest_release_published_at)
            """
            for node_id, category in valid_node_ids:
                cursor.execute(insert_query, (node_id, category))

            # 提交更改
            db.commit()
    except Exception as e:
        print(f"An error occurred during migration: {e}")
        db.rollback()  # Rollback in case of error
    finally:
        if db.open:
            db.close()  # Ensure the connection is closed properly


def fetch_null_latest_release_project_names():
    try:
        with db.cursor() as cursor:
            query = """
            SELECT DISTINCT project_name
            FROM cross_language_infor
            WHERE latest_release_published_at IS NULL
            """
            cursor.execute(query)
            project_names = [item[0] for item in cursor.fetchall()]  # Collecting project names
            print(project_names)
        return project_names
    except Exception as e:
        print("Error while fetching project names with null latest_release_published_at:", e)
        db.rollback()  # 错误时回滚
        return []


def fetch_latest_release_for_projects(project_names):
    results = []
    if not project_names:
        print("No project names provided.")
        return []  # Return an empty list if no project names
    try:
        with db.cursor() as cursor:
            # 遍历每个项目名并执行查询
            for project_name in project_names:
                print("Fetching latest_release_published_at for project_name:", project_name)
                cursor.execute("""
                    SELECT latest_release_published_at
                    FROM nodes_maven
                    WHERE node_ID = %s
                """, (project_name,))
                fetched_result = cursor.fetchone()  # 获取查询结果
                if fetched_result:
                    results.append((fetched_result[0], project_name))  # 记录 latest_release_published_at 和 project_name
                else:
                    print(f"No latest_release_published_at found for project_name: {project_name}")
        print("Fetched results:", results)
        return results
    except pymysql.Error as e:
        print("Error while fetching latest_release_published_at:", e)
        return []



def update_cross_language_infor(latest_releases):
    if not latest_releases:
        print("No latest releases to update.")
        return

    try:
        db.ping(reconnect=True)  # 确保连接仍然有效
        with db.cursor() as cursor:
            update_query = """
                UPDATE cross_language_infor
                SET latest_release_published_at = %s
                WHERE project_name = %s
            """
            for latest_release_published_at, project_name in latest_releases:
                cursor.execute(update_query, (latest_release_published_at, project_name))

            # 提交更改
            db.commit()
    except Exception as e:
        print(f"An error occurred during update: {e}")
        db.rollback()  # Rollback in case of error



if __name__ == "__main__":
    # migrate_C_data()
    # migrate_other_data()
    # migrate_data()

    project_names = fetch_null_latest_release_project_names()
    latest_releases = fetch_latest_release_for_projects(project_names)
    update_cross_language_infor(latest_releases)
