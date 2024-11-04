# -*- codeing = utf-8 -*-
# @Time : 2024/4/22 0022 16:04
# @Author : 张娅西
# @File : PR_MapReduce.py
# @Software : PyCharm
import itertools

from pygraph.classes.digraph import digraph
import pymysql
db = pymysql.connect(host='localhost',
                     port=3306,
                     user='root',
                     password='admin',
                     db='dependencies_npm')





def select_nodes():
    nodes_dict = {}  # 用于保存节点数据
    with db.cursor() as cur:
        sql_select = "SELECT node_ID, version, language, platform, repository_url, normalized_licenses,latest_release_published_at, repository_license, project_rank,project_stars FROM nodes_all"
        print(sql_select)
        try:
            cur.execute(sql_select)
            nodes = cur.fetchall()  # 获取所有节点
            db.commit()  # 提交查询结果
            # 将节点信息保存到字典中
            for node in nodes:
                node_ID, version,language, platform, repository_url,normalized_licenses,latest_release_published_at, repository_license,project_rank,project_stars = node
                nodes_dict[node_ID] = {
                    "version": version,
                    "language":language,
                    "platform": platform,
                    "repository_url":repository_url,
                    "normalized_licenses":normalized_licenses,
                    "latest_release_published_at":latest_release_published_at,
                    "repository_license":repository_license,
                    "project_rank":project_rank,
                    "project_stars":project_stars,
                }
            cur.close()
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
            cur.close()  # 关闭游标
        except Exception as e:
            print("Error while fetching edges:", e)
            db.rollback()  # 错误时回滚
    return edges_list  # 返回包含边数据的列表


# 检查node_ID是否存在于数据库
def check_node_exists(node_ID):
    sql_check = f"SELECT COUNT(*) FROM nodes_all WHERE node_ID = '{node_ID}'"
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
                sql_update = f"UPDATE nodes_all SET PR = {value[0]} WHERE node_ID = '{key}'"
                try:
                    cur.execute(sql_update)  # 执行更新
                    # 提交所有更改
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
        """
        map_reduce方法
        :param i: 需要MapReduce的集合
        :param mapper: 自定义mapper方法
        :param reducer: 自定义reducer方法
        :return: 以自定义reducer方法的返回值为元素的一个列表
        """
        intermediate = []  # 存放所有的(intermediate_key, intermediate_value)
        for (key, value) in i.items():
            intermediate.extend(mapper(key, value))

        # sorted返回一个排序好的list，因为list中的元素是一个个的tuple，key设定按照tuple中第几个元素排序
        # groupby把迭代器中相邻的重复元素挑出来放在一起,key设定按照tuple中第几个元素为关键字来挑选重复元素
        # 下面的循环中groupby返回的key是intermediate_key，而group是个list，是1个或多个
        # 有着相同intermediate_key的(intermediate_key, intermediate_value)
        groups = {}
        for key, group in itertools.groupby(sorted(intermediate, key=lambda im: im[0]), key=lambda x: x[0]):
            groups[key] = [y for x, y in group]
        # groups是一个字典，其key为上面说到的intermediate_key，value为所有对应intermediate_key的intermediate_value
        # 组成的一个列表
        return [reducer(intermediate_key, groups[intermediate_key]) for intermediate_key in groups]


class PRMapReduce:
    __doc__ = '''计算PR值'''

    def __init__(self, dg):
        self.damping_factor = 0.85  # 阻尼系数,即α
        self.max_iterations = 10000  # 最大迭代次数
        self.min_delta = 1e-20  # 0.00001  # 确定迭代是否结束的参数,即ϵ
        self.num_of_pages = len(dg.nodes())  # 总网页数

        # graph表示整个网络图。是字典类型。
        # graph[i][0] 存放第i网页的PR值
        # graph[i][1] 存放第i网页的出链数量
        # graph[i][2] 存放第i网页的出链网页，是一个列表
        self.graph = {}
        for node in dg.nodes():
            self.graph[node] = [1.0 / self.num_of_pages, len(dg.neighbors(node)), dg.neighbors(node)]

    def ip_mapper(self, input_key, input_value):
        """
        看一个网页是否有出链，返回值中的 1 没有什么物理含义，只是为了在
        map_reduce中的groups字典的key只有1，对应的value为所有的悬挂网页
        的PR值
        :param input_key: 网页名，如 A
        :param input_value: self.graph[input_key]
        :return: 如果没有出链，即悬挂网页，那么就返回[(1,这个网页的PR值)]；否则就返回[]
        """
        if input_value[1] == 0:
            return [(1, input_value[0])]
        else:
            return []

    def ip_reducer(self, input_key, input_value_list):
        """
        计算所有悬挂网页的PR值之和
        :param input_key: 根据ip_mapper的返回值来看，这个input_key就是:1
        :param input_value_list: 所有悬挂网页的PR值
        :return: 所有悬挂网页的PR值之和
        """
        return sum(input_value_list)

    def pr_mapper(self, input_key, input_value):
        """
        mapper方法
        :param input_key: 网页名，如 A
        :param input_value: self.graph[input_key]，即这个网页的相关信息
        :return: [(网页名, 0.0), (出链网页1, 出链网页1分得的PR值), (出链网页2, 出链网页2分得的PR值)...]
        """
        return [(input_key, 0.0)] + [(out_link, input_value[0] / input_value[1]) for out_link in input_value[2]]

    def pr_reducer_inter(self, intermediate_key, intermediate_value_list, dp):
        """
        reducer方法
        :param intermediate_key: 网页名，如 A
        :param intermediate_value_list: A所有分得的PR值的列表:[0.0,分得的PR值,分得的PR值...]
        :param dp: 所有悬挂网页的PR值之和
        :return: (网页名，计算所得的PR值)
        """
        return (intermediate_key,
                self.damping_factor * sum(intermediate_value_list) +
                self.damping_factor * dp / self.num_of_pages +
                (1.0 - self.damping_factor) / self.num_of_pages)

    def page_rank(self):
        """
        计算PR值，每次迭代都需要两次调用MapReduce。一次是计算悬挂网页PR值之和，一次
        是计算所有网页的PR值
        :return: self.graph，其中的PR值已经计算好
        """
        iteration = 1  # 迭代次数
        change = 1  # 记录每轮迭代后的PR值变化情况，初始值为1保证至少有一次迭代
        while change > self.min_delta:
            print("Iteration: " + str(iteration))

            # 因为可能存在悬挂网页，所以才有下面这个dangling_list
            # dangling_list存放的是[所有悬挂网页的PR值之和]
            # dp表示所有悬挂网页的PR值之和
            dangling_list = MapReduce.map_reduce(self.graph, self.ip_mapper, self.ip_reducer)
            if dangling_list:
                dp = dangling_list[0]
            else:
                dp = 0

            # 因为MapReduce.map_reduce中要求的reducer只能有两个参数，而我们
            # 需要传3个参数（多了一个所有悬挂网页的PR值之和,即dp），所以采用
            # 下面的lambda表达式来达到目的
            # new_pr为一个列表，元素为:(网页名，计算所得的PR值)
            new_pr = MapReduce.map_reduce(self.graph, self.pr_mapper, lambda x, y: self.pr_reducer_inter(x, y, dp))

            # 计算此轮PR值的变化情况
            change = sum([abs(new_pr[i][1] - self.graph[new_pr[i][0]][0]) for i in range(self.num_of_pages)])
            print("Change: " + str(change))

            # 更新PR值
            for i in range(self.num_of_pages):
                self.graph[new_pr[i][0]][0] = new_pr[i][1]
            iteration += 1
        return self.graph


def normalized_page_ranks(page_ranks):
    # 找到最大和最小 PageRank 值
    pr_values = [value[0] for value in page_ranks.values()]
    pr_min = min(pr_values)
    pr_max = max(pr_values)
    normalized_page_ranks = {}
    for key, value in page_ranks.items():
        pr = value[0]
        # 将 PageRank 归一化到 [0, 1] 范围
        normalized_pr = (pr - pr_min) / (pr_max - pr_min)
        normalized_page_ranks[key] = normalized_pr
    return normalized_page_ranks


if __name__ == '__main__':
    dg = digraph()

    # dg.add_nodes(["A", "B", "C", "D", "E"])
    #
    # dg.add_edge(("A", "B"))
    # dg.add_edge(("A", "C"))
    # dg.add_edge(("A", "D"))
    # dg.add_edge(("B", "D"))
    # dg.add_edge(("C", "E"))
    # dg.add_edge(("D", "E"))
    # dg.add_edge(("B", "E"))
    # dg.add_edge(("E", "A"))

    nodes_data = select_nodes()

    edges_data = select_edges()

    # 使用保存的节点数据来添加节点
    for node_ID, node_attr in nodes_data.items():
        dg.add_node(node_ID, node_attr)

    for edge in edges_data:
        ini_node, goal_node = edge
        # 如果节点不存在于有向图中，确保添加它们
        if not dg.has_node(ini_node):
            dg.add_node(ini_node)  # 确保节点存在
        if not dg.has_node(goal_node):
            dg.add_node(goal_node)  # 确保节点存在
        dg.add_edge((ini_node, goal_node))  # 添加边

    pr = PRMapReduce(dg)
    page_ranks = pr.page_rank()
    # 将计算后的PageRank结果存入数据库
    update_page_ranks_in_db(page_ranks)


    #进行归一化操作
    # normalized_page_ranks=normalized_page_ranks(page_ranks)
    # update_page_ranks_in_db(normalized_page_ranks)



    # print("The final page rank is")
    # for key, value in page_ranks.items():
    #     print(key + " : ", value[0])
