import logging
import os

from module.reshard.common.check import checkIfDiskBalance
from module.reshard.common.constant import show_disk_status, show_plan, run_disk_balance_plan
from module.reshard.common.es_utils import showCap, showShardDistribute, computeStoreSize, get_mandontory_sets
from module.reshard.common.logic import balanceDisk
from module.reshard.common.measure import convert
from module.reshard.entity.abstracts import Node_Shade
from module.reshard.entity.config import Config

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def run(configs: Config):
    node_list, avg_index_list, sum_memory, avg_memory = get_mandontory_sets(configs)
    configs.node_list = node_list
    configs.avg_index_list = avg_index_list
    configs.sum_memory = sum_memory
    configs.avg_memory = avg_memory
    if show_disk_status.__eq__(configs.action):
        showCap(configs)
        checkIfDiskBalance(configs)
        showShardDistribute(configs.node_list)
    if show_plan.__eq__(configs.action):
        showCap(configs)
        showShardDistribute(configs.node_list)
        logger.info("-------------------------balance planning started------------------------------")
        balanceDisk(configs)
        logger.info("moving count :" + str(
            configs.cluster.count) + " network_transmission : " + configs.cluster.network_transmission + "will take time :" + str(
            convert(configs.cluster.network_transmission) / configs.waiting) + "s")
        logger.info("-------------------------balanced planning completed------------------------------")
        showCap(configs)
        checkIfDiskBalance(configs)
        logger.info("------------------------shard distribution after the moving-----------------------------")
        showShardDistribute(configs.node_list)
    if run_disk_balance_plan.__eq__(configs.action):
        # isWantToExecute this scripts , set cluster.routing.allocation.enable transient none
        os.system(
            command="curl -H \"Content-Type: application/json\"  -X PUT http://" + configs.url + ":9200/_cluster/settings?pretty -d '{\"transient\": {\"cluster.routing.allocation.enable\": \"none\"}}'")
        logger.info(
            "=====================set cluster.routing.allocation.enable transient none========================")
        showCap(configs)
        logger.info("-------------------------executing the balanced schedule--------------------------------")
        balanceDisk(configs)

        logger.info("---------------------------finsh the balanced schedule----------------------------------")
        logger.info("-----------------------------checking the execution----------------------------------")
        res = configs.es_client.cat.shards(format='json')
        node_list2 = []
        for m in configs.node_set:
            nod = Node_Shade(configs, m)
            computeStoreSize(shards=res, node=m, nod=nod)
            node_list2.append(nod)
        logger.info("---------------------------calculated volume----------------------------------")
        showCap(configs)
        # ensure the counting result
        logger.info("----------------------------actually volume-----------------------------------")
        showCap(configs, backup=node_list2)
        logger.info(
            "-----------------------datenode shard count distribution after the execution-----------------------------")
        showShardDistribute(node_list2)
        os.system(
            command="curl -H \"Content-Type: application/json\"  -X PUT http://" + configs.url + ":9200/_cluster/settings?pretty -d '{\"transient\": {\"cluster.routing.allocation.enable\": \"all\"}}'")


if __name__ == '__main__':
    config = Config("hadoop102", 's', False, 0.07, True, 0.1, False)
    run(config)
