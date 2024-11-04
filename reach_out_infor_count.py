import pymysql
db = pymysql.connect(host='localhost',
                     port=3306,
                     user='root',
                     password='admin',
                     db='dependencies_npm')


def get_reachable_nodes_by_platform(node_id, cursor, visited=None, reachable_nodes_by_platform=None):
    if visited is None:
        visited = set()
    if reachable_nodes_by_platform is None:
        reachable_nodes_by_platform = {}
    visited.add(node_id)
    cursor.execute("""
        SELECT DISTINCT goal_node
        FROM nodes_dependencies_all
        WHERE ini_node = %s
    """, (node_id,))

    for result in cursor.fetchall():
        goal_node = result[0]
        if goal_node not in visited:
            cursor.execute("""
                SELECT platform
                FROM nodes_all
                WHERE node_ID = %s
            """, (goal_node,))
            platform_result = cursor.fetchone()
            if platform_result:
                platform = platform_result[0]
                if platform not in reachable_nodes_by_platform:
                    reachable_nodes_by_platform[platform] = set()
                reachable_nodes_by_platform[platform].add(goal_node)
                get_reachable_nodes_by_platform(goal_node, cursor, visited, reachable_nodes_by_platform)
    return reachable_nodes_by_platform


def update_reach_out_infor():
    try:
        with db.cursor() as cursor:
            # 查询 project_name
            cursor.execute("SELECT project_name FROM reach_out_infor")
            project_names = [item[0] for item in cursor.fetchall()]

            # 初始化字典
            platform_columns = {
                'Rubygems': 'reach_out_ruby',
                'NPM': 'reach_out_js',
                'Pypi': 'reach_out_python',
                'Packagist': 'reach_out_php',
                'Cargo': 'reach_out_other',
                'C/C++': 'reach_out_other'
            }

            for project_name in project_names:
                reachable_nodes_by_platform = get_reachable_nodes_by_platform(project_name, cursor)
                update_query = """
                    UPDATE reach_out_infor
                    SET reach_out_ruby = %s,
                        reach_out_js = %s,
                        reach_out_python = %s,
                        reach_out_php = %s,
                        reach_out_other = %s
                    WHERE project_name = %s
                """
                # 获取各个平台的可达节点数量
                reach_out_ruby = len(reachable_nodes_by_platform.get('Rubygems', set()))
                reach_out_js = len(reachable_nodes_by_platform.get('NPM', set()))
                reach_out_python = len(reachable_nodes_by_platform.get('Pypi', set()))
                reach_out_php = len(reachable_nodes_by_platform.get('Packagist', set()))

                # 合并 Cargo 和 C/C++
                reach_out_other = len(reachable_nodes_by_platform.get('Cargo', set())) + len(
                    reachable_nodes_by_platform.get('C/C++', set()))

                cursor.execute(update_query, (
                reach_out_ruby, reach_out_js, reach_out_python, reach_out_php, reach_out_other, project_name))

                # 提交更改
                db.commit()
    except Exception as e:
        print(f"An error occurred during update: {e}")
        db.rollback()  # Rollback in case of error



def fetch_and_insert_reach_out_info():
    try:
        with db.cursor() as cursor:
            # 查询数据
            query = """
            SELECT DISTINCT n.node_ID, n.reach_out
            FROM jar_dependencies j
            JOIN nodes_all n ON j.project_name = n.node_ID
            ORDER BY CAST(n.reach_out AS UNSIGNED) DESC;
            """
            cursor.execute(query)
            results = cursor.fetchall()  # Collecting the results

            # 插入数据
            insert_query = """
                INSERT INTO reach_out_infor (project_name, reach_out, reach_out_js, reach_out_python, reach_out_ruby,reach_out_php,reach_out_other)
                VALUES (%s, %s, NULL, NULL, NULL, NULL, NULL)
            """
            for result in results:
                node_id, reach_out = result
                cursor.execute(insert_query, (node_id, reach_out))
                # 提交更改
                db.commit()
    except Exception as e:
        print(f"An error occurred during fetch and insert: {e}")
        db.rollback()  # Rollback in case of error


if __name__ == "__main__":
    # fetch_and_insert_reach_out_info()
    update_reach_out_infor()