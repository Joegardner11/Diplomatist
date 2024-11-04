import networkx as nx
import pymysql

db = pymysql.connect(host='localhost',
                     port=3306,
                     user='root',
                     password='admin',
                     db='dependencies_npm')


def select_nodes():
    nodes_dict = {}
    with db.cursor() as cur:
        sql_select = "SELECT node_ID FROM nodes_all"
        try:
            cur.execute(sql_select)
            nodes = cur.fetchall()
            db.commit()
            for node in nodes:
                node_ID = node[0]
                nodes_dict[node_ID] = {}  # 只需要节点ID信息，所以暂时为空字典
            cur.close()
        except Exception as e:
            print("Error while fetching nodes:", e)
            db.rollback()
    return nodes_dict


def select_edges():
    edges_list = []
    with db.cursor() as cur:
        sql_select = "SELECT ini_node, goal_node FROM nodes_dependencies_all"
        try:
            cur.execute(sql_select)
            edges = cur.fetchall()
            db.commit()
            for edge in edges:
                ini_node, goal_node = edge
                edges_list.append((ini_node, goal_node))
            cur.close()
        except Exception as e:
            print("Error while fetching edges:", e)
            db.rollback()
    return edges_list


def hits_algorithm(graph):
    return nx.hits(graph)


def update_hubs_authorities_in_db(hubs, authorities):
    with db.cursor() as cur:
        for node_ID, hub_value in hubs.items():
            authority_value = authorities[node_ID]
            sql_update = f"UPDATE nodes_all SET hubs = {hub_value}, authorities = {authority_value} WHERE node_ID = '{node_ID}'"
            try:
                cur.execute(sql_update)
                db.commit()
            except Exception as e:
                print("Error updating hubs and authorities:", e)
                db.rollback()


if __name__ == '__main__':
    # 从数据库中获取节点和边信息
    nodes_data = select_nodes()
    edges_data = select_edges()

    # 创建有向图对象
    G = nx.DiGraph()

    # 添加节点和边到图中
    for node_ID in nodes_data.keys():
        G.add_node(node_ID)
    G.add_edges_from(edges_data)

    # 使用HITS算法计算节点的Hubs和Authorities值
    hubs, authorities = hits_algorithm(G)

    # 将计算得到的Hubs和Authorities值存入数据库表中对应节点属性
    update_hubs_authorities_in_db(hubs, authorities)
