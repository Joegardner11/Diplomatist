import itertools
from pygraph.classes.digraph import digraph
import pymysql

# 数据库连接参数
db = pymysql.connect(host='localhost',
                     port=3306,
                     user='root',
                     password='admin',
                     db='dependencies_npm')

def select_nodes():
    nodes_dict = {}  # 用于保存节点数据
    with db.cursor() as cur:
        sql_select = "SELECT node_ID, version, language, platform, repository_url, normalized_licenses, latest_release_published_at, repository_license, project_rank, project_stars FROM nodes_all_changed"
        try:
            cur.execute(sql_select)
            nodes = cur.fetchall()  # 获取所有节点
            db.commit()  # 提交查询结果
            # 将节点信息保存到字典中
            for node in nodes:
                node_ID, version, language, platform, repository_url, normalized_licenses, latest_release_published_at, repository_license, project_rank, project_stars = node
                nodes_dict[node_ID] = {
                    "version": version,
                    "language": language,
                    "platform": platform,
                    "repository_url": repository_url,
                    "normalized_licenses": normalized_licenses,
                    "latest_release_published_at": latest_release_published_at,
                    "repository_license": repository_license,
                    "project_rank": project_rank,
                    "project_stars": project_stars,
                }
        except Exception as e:
            print("Error while fetching nodes:", e)
            db.rollback()  # 回滚在错误时
    return nodes_dict  # 返回包含节点信息的字典

def select_edges():
    edges_list = []  # 用于保存边数据
    with db.cursor() as cur:
        sql_select = "SELECT ini_node, goal_node FROM nodes_dependencies_all"
        try:
            cur.execute(sql_select)
            edges = cur.fetchall()  # 获取所有边
            db.commit()  # 提交查询结果
            # 将边信息保存到列表中
            for edge in edges:
                ini_node, goal_node = edge
                edges_list.append((ini_node, goal_node))  # 保存边
        except Exception as e:
            print("Error while fetching edges:", e)
            db.rollback()  # 错误时回滚
    return edges_list  # 返回包含边数据的列表

# 检查node_ID是否存在于数据库
def check_node_exists(node_ID):
    sql_check = f"SELECT COUNT(*) FROM nodes_all_changed WHERE node_ID = '{node_ID}'"
    with db.cursor() as cur:
        cur.execute(sql_check)
        result = cur.fetchone()
        return result[0] > 0  # 如果结果大于0，说明节点存在

# 更新PageRank结果到数据库
def update_page_ranks_in_db(page_ranks):
    with db.cursor() as cur:
        for key, value in page_ranks.items():
            # 检查node_ID是否存在
            if check_node_exists(key):
                # 如果存在，更新对应节点的PR值
                sql_update = f"UPDATE nodes_all_changed SET PR = {value[0]} WHERE node_ID = '{key}'"
                try:
                    cur.execute(sql_update)  # 执行更新
                    db.commit()  # 确保所有更改生效
                except Exception as e:
                    print("Error updating PageRank:", e)  # 处理错误
                    db.rollback()  # 如果出错则回滚
            else:
                print(f"node_ID {key} does not exist.")  # 节点不存在时的警告

class MapReduce:
    __doc__ = '''提供map_reduce功能'''

    @staticmethod
    def map_reduce(i, mapper, reducer):
        intermediate = []  # 存放所有的(intermediate_key, intermediate_value)
        for (key, value) in i.items():
            intermediate.extend(mapper(key, value))

        groups = {}
        for key, group in itertools.groupby(sorted(intermediate, key=lambda im: im[0]), key=lambda x: x[0]):
            groups[key] = [y for x, y in group]

        return [reducer(intermediate_key, groups[intermediate_key]) for intermediate_key in groups]

class PRMapReduce:
    __doc__ = '''计算PR值'''

    def __init__(self, dg):
        self.damping_factor = 0.85  # 阻尼系数,即α
        self.max_iterations = 10000  # 最大迭代次数
        self.min_delta = 1e-8  # 确定迭代是否结束的参数,即ϵ
        self.num_of_pages = len(dg.nodes())  # 总网页数

        self.graph = {}
        for node in dg.nodes():
            self.graph[node] = [1.0, len(dg.neighbors(node)), dg.neighbors(node)]

    def ip_mapper(self, input_key, input_value):
        if input_value[1] == 0:
            return [(1, input_value[0])]
        else:
            return []

    def ip_reducer(self, input_key, input_value_list):
        return sum(input_value_list)

    def pr_mapper(self, input_key, input_value):
        return [(input_key, 0.0)] + [(out_link, input_value[0] / input_value[1]) for out_link in input_value[2]]

    def pr_reducer_inter(self, intermediate_key, intermediate_value_list, dp):
        return (intermediate_key,
                self.damping_factor * sum(intermediate_value_list) +
                self.damping_factor * dp / self.num_of_pages +
                (1.0 - self.damping_factor) / self.num_of_pages)

    def page_rank(self):
        iteration = 1
        change = 1
        while change > self.min_delta and iteration <= self.max_iterations:
            print("Iteration: " + str(iteration))

            dangling_list = MapReduce.map_reduce(self.graph, self.ip_mapper, self.ip_reducer)
            if dangling_list:
                dp = dangling_list[0]
            else:
                dp = 0

            new_pr = MapReduce.map_reduce(self.graph, self.pr_mapper, lambda x, y: self.pr_reducer_inter(x, y, dp))

            change = sum([abs(new_pr[i][1] - self.graph[new_pr[i][0]][0]) for i in range(self.num_of_pages)])
            print("Change: " + str(change))

            for i in range(self.num_of_pages):
                self.graph[new_pr[i][0]][0] = new_pr[i][1]
            iteration += 1

        return self.graph

def normalized_page_ranks(page_ranks):
    pr_values = [value[0] for value in page_ranks.values()]
    pr_min = min(pr_values)
    pr_max = max(pr_values)
    normalized_page_ranks = {}
    for key, value in page_ranks.items():
        pr = value[0]
        normalized_pr = (pr - pr_min) / (pr_max - pr_min)
        normalized_page_ranks[key] = normalized_pr
    return normalized_page_ranks

if __name__ == '__main__':
    dg = digraph()

    nodes_data = select_nodes()
    edges_data = select_edges()

    for node_ID, node_attr in nodes_data.items():
        dg.add_node(node_ID, node_attr)

    for edge in edges_data:
        ini_node, goal_node = edge
        if not dg.has_node(ini_node):
            dg.add_node(ini_node)
        if not dg.has_node(goal_node):
            dg.add_node(goal_node)
        dg.add_edge((ini_node, goal_node))

    pr = PRMapReduce(dg)
    page_ranks = pr.page_rank()

    # normalized_pr = normalized_page_ranks(page_ranks)
    update_page_ranks_in_db(page_ranks)

    # print("The final normalized page rank is")
    # for key, value in page_ranks.items():
    #     print(key + " : ", value)
