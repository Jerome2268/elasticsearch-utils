import logging

from module.reshard.common.measure import convert
from module.reshard.entity.abstracts import Node_Shade
from module.reshard.entity.config import Config

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def esCheck(node_name: str, config: Config):
    nod = Node_Shade(config, node_name)
    shards = config.es_client.cat.shards(format='json')
    for i in shards:
        if i['node'] == node_name:
            store_size = str(i['store']).lower()
            if i['prirep'] == "p":
                nod.addShade((i['index'], i['shard'], "p", i['store']))
            if i['prirep'] == "r":
                nod.addShade((i['index'], i['shard'], "r", i['store']))
    return nod.storeValue


# {'index': '.kibana_1', 'node': 'dorr-1', 'state': 'STARTED', 'docs': '21', 'shard': '0', 'prirep': 'p',
# 'ip': '192.168.58.12', 'store': '44.2kb'}
def computeStoreSize(shards, node, nod: Node_Shade):
    for i in shards:
        if i['node'] == node:
            store_size = str(i['store']).lower()
            if i['prirep'] == "p":
                nod.addShade((i['index'], i['shard'], "p", i['store']))
            if i['prirep'] == "r":
                nod.addShade((i['index'], i['shard'], "r", i['store']))
    return nod


def showCap(config: Config, backup=None):
    logger.info("sumMem" + str(config.sum_memory) + "kb   --balancedStore: " + str(
        config.avg_memory * (1.0 - config.tolerance)) + "kb~" + str(
        config.avg_memory) + "kb~" + str(config.avg_memory * (1.0 + config.tolerance)) + "kb")
    for i in sorted(config.node_list if (backup is None) else backup, key=lambda s: s.node):
        info = i.node + "  size:  " + i.storeValue
        logger.info(info)


def getNode(nodeList, num, reverse=False):
    # node list.reverse default asc
    if len(nodeList) and num < len(nodeList) > 0:
        return sorted(nodeList, key=lambda node: convert(node.storeValue), reverse=reverse)[num]


def showShardDistribute(NodeList):
    for node in sorted(NodeList, key=lambda s: s.node):
        logger.info(node.node + "shards count:" + str(len(node.index_shade_list)) + " pri :" + str(
            len(node.index_shade_p_list)) + " replicas : " + str(len(node.index_shade_r_list)))
        pNodelistInfo = ""
        rNodeListInfo = ""
        for i in node.index_shade_p_list:
            pNodelistInfo += str(i) + "   "
            logger.debug(pNodelistInfo)
        for i in node.index_shade_r_list:
            rNodeListInfo += str(i) + "   "
            logger.debug(rNodeListInfo)


def get_mandontory_sets(config: Config):
    node_list = []
    for m in config.node_set:
        nod = Node_Shade(config, m)
        computeStoreSize(shards=config.res, node=m, nod=nod)
        node_list.append(nod)

    # compute the avg shard per index
    avg_index_list = []
    for i in config.index_list:
        shard_num = int(int(i[2]) / len(node_list)) + 1
        avg_index_list.append((i[0], (str(convert(i[1]) / len(node_list))) + "kb", shard_num))
    sum_memory = 0.0
    for node in node_list:
        sum_memory += convert(node.storeValue)
    avg_memory = sum_memory / len(node_list)
    return node_list, avg_index_list, sum_memory, avg_memory
