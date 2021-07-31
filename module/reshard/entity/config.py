import logging

from elasticsearch import Elasticsearch

from module.reshard.common.constant import avg_shade_num
from module.reshard.entity.cluster import Cluster

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class Config:
    def __init__(self, url, action, big_first, tolerance, p_r_strategy, waiting,if_see_log):
        self.action = action
        self.url = url
        self.balance_strategy = big_first
        self.avg_shade_num = avg_shade_num
        self.if_see_log = if_see_log
        self.should_p_r_shard_be_on_one_node = p_r_strategy
        self.tolerance = tolerance
        self.waiting = waiting
        self.es_client = self.validate(url)
        self.cluster = Cluster(url)
        res, node_set, index_list = self.get_basic_sets(self.es_client)
        self.res = res
        self.index_list = index_list
        self.node_set = node_set
        self.node_list = []
        self.avg_index_list = []
        self.sum_memory = 0.00
        self.avg_memory = 0.00

    @staticmethod
    def validate(url):
        es = Elasticsearch([url],
                           sniff_on_start=True,  # test before the connect
                           sniff_on_connection_fail=True,  # flush node when failed
                           sniff_timeout=60)  # set the timeout)
        if es.ping():
            logger.info("Successfully connected es cluster")
            return es
        logger.error("Fail to connector to es cluster. Will exit")
        exit(0)

    @staticmethod
    def get_basic_sets(es):

        res = es.cat.shards(format='json')
        nodes = es.cat.nodes(format='json')
        indexes = es.cat.indices(format='json')
        node_set = set()
        index_set = set()
        index_list = []
        for i in nodes:
            node_set.add(i['name'])
        for i in indexes:
            index_set.add(i['index'])
            index_list.append((i['index'], i['pri.store.size'], i['pri']))
        return res, node_set, index_list
