import logging
import os
import time

from module.reshard.common.check import checkIfDiskBalance
from module.reshard.common.constant import run_disk_balance_plan
from module.reshard.common.es_utils import esCheck, getNode
from module.reshard.common.measure import convert
from module.reshard.entity.config import Config

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def move(index, shard, fromnode, tonode, storeValue, config: Config):
    # sleep for a time
    logger.info("start to execute the moving process")
    command = "curl -H \"Content-Type: application/json\" -XPOST \"" + config.url + ":9200/_cluster/reroute\" -d  '{\"commands\" : [{\"move\" : {\"index\" : \"" + index + "\",\"shard\" : " + str(
        shard) + ",\"from_node\" : \"" + fromnode.node + "\",\"to_node\" : \"" + tonode.node + "\"}}]}' "
    flag = 1
    while not flag == 0:
        # post is idmpotency so we can use while
        flag = os.system(command=command)
        file = open("execute.log", "a+")
        t = float(convert(storeValue) / config.waiting)

        time.sleep(t)
        if flag == 0:
            toNodeCheckValue = esCheck(node_name=tonode.node, config=config)
            fromNodeCheckValue = esCheck(node_name=fromnode.node, config=config)
            file.write(
                "move from" + fromnode.node + "  size:" + fromnode.storeValue + "to" + tonode.node + " :size" + tonode.storeValue + "  index:" + index + "  shard :" + shard + "successfully!" + " shard size: " + storeValue + "   node :" + tonode.node + "compute size :   " + str(
                    convert(tonode.storeValue) + convert(
                        storeValue)) + "kb  actual toNode size:     " + toNodeCheckValue + "Variance:" + str(
                    convert(toNodeCheckValue) - convert(
                        tonode.storeValue)) + "kb  actual fromNode size：  " + fromNodeCheckValue + "Variance" + str(
                    convert(fromNodeCheckValue) - convert(fromnode.storeValue)) + "kb\n")
        else:
            file.write(
                "----------------------------------------------------------------------------------------------------------\n")
            file.write(command)
            file.write(
                "from" + fromnode.node + "to" + tonode.node + ":index:" + index + "  shard :" + shard + "failed!  " + "shard size : " + storeValue + "\n")
            file.write(
                "----------------------------------------------------------------------------------------------------------\n")
    logger.debug("follow command:" + command)

    logger.debug("finshed  the moving process")


def getIndexShade(node, num, reverse=False):
    # default asc
    return sorted(node.index_shade_list, reverse=reverse, key=lambda s: convert(s[3]))[num]


def change_node_list(toNodeList, fromNodeList, config: Config, f=move):
    # the node that has the least storeVal
    toNode = getNode(toNodeList, 0)
    for m in range(len(fromNodeList) - 1, -1, -1):
        fromNode = getNode(fromNodeList, m)
        # advancing on the little
        for i in range(len(fromNode.index_shade_list) - 1):
            indexShade = getIndexShade(fromNode, i, config.balance_strategy)

            toNodeValue = convert(toNode.storeValue)
            indexShadeValue = convert(indexShade[3])
            upperLimit = config.avg_memory * (1 + config.tolerance)
            # not toNode.checkIfConflict(indexShade) and
            condition = (toNodeValue + indexShadeValue) < upperLimit if config.should_p_r_shard_be_on_one_node else (
                    not toNode.checkIfConflict(indexShade) and (toNodeValue + indexShadeValue) < upperLimit)
            if condition:
                # showing it is convient to move
                logger.info("from " + fromNode.node + " to " + toNode.node + " moving index:" + indexShade[
                    0] + "    shade :" +
                            indexShade[1] + " moving size :" + indexShade[3] +
                            "  volume :" + toNode.storeValue + " volume after the moving :  " +
                            str(convert(toNode.storeValue) + convert(indexShade[3])) +
                            "kb")

                if run_disk_balance_plan.__eq__(config.action):
                    f(index=indexShade[0], shard=indexShade[1], fromnode=fromNode, tonode=toNode,
                      storeValue=indexShade[3], config=config)
                # after the moving
                config.cluster.move(indexShade[3])
                fromNode.removeShade(indexShade)
                toNode.addShade(indexShade)
                return True
    return False


def captureSize(a: list):
    s = 0
    for node in a:
        s += convert(node.storeValue)
        print(node.node + "  :  " + str(convert(node.storeValue)) + "-->  " + str(node.index_shade_list))
    return s


def balanceDisk(config: Config):
    count = 0
    res = True
    lRes = True
    hRes = True
    # divid the datanode to 3 list :
    #   avgList : the storeValue is between  avgMem( 1 - percent) and avgMem( 1 + percent)
    while not checkIfDiskBalance(config)[0]:
        avgList = []
        lUnBalanceList = []
        hUnBalanceList = []
        for node in config.node_list:
            store_value = convert(node.storeValue)
            if abs(store_value - config.avg_memory) / config.avg_memory < config.tolerance:
                avgList.append(node)
            if (config.avg_memory - store_value) / config.avg_memory > config.tolerance:
                lUnBalanceList.append(node)

            if (store_value - config.avg_memory) / config.avg_memory > config.tolerance:
                hUnBalanceList.append(node)
        if res and not len(lUnBalanceList) == 0 and not len(hUnBalanceList) == 0:
            logger.debug("start balance between higher list and lower list  --------------- ")
            # showCap(NodeList,percent)
            res = change_node_list(lUnBalanceList, hUnBalanceList, config=config)
            # showCap(NodeList,percent)
        elif not res and not len(lUnBalanceList) == 0 and not len(avgList) == 0 and not len(hUnBalanceList) == 0:
            if len(lUnBalanceList) >= len(hUnBalanceList):
                logger.debug("start balance between lowList and  avgList ########--------------- ")
                result1 = change_node_list(lUnBalanceList, avgList, config=config)
                # if once succeed u show give the avglist a chance
                if result1:
                    res = True
                else:
                    logger.debug("start balance highlist between avglist ########----------------")
                    result2 = change_node_list(avgList, hUnBalanceList, config=config)
                    if result2:
                        res = True
                    else:
                        logger.info("moving completed  , can not be fully balanced")
                        return

            else:
                # balance with the higher list and avglist
                logger.debug("start balance highlist between avglist @@@@@@@@@----------------")
                result1 = change_node_list(avgList, hUnBalanceList, config=config)
                if result1:
                    res = True
                else:
                    logger.info("start balance between lowList and  avgList @@@@@@@@@@--------------- ")
                    result2 = change_node_list(lUnBalanceList, avgList, config=config)
                    if result2:
                        res = True
                    else:
                        logger.debug("moving completed  , can not be fully balanced")
                        return

        elif hRes and not len(avgList) == 0 and len(lUnBalanceList) == 0 and not len(hUnBalanceList) == 0:
            logger.debug("start balance highlist between avglist ****************----------------")
            hRes = change_node_list(avgList, hUnBalanceList, config=config)

        elif lRes and not len(avgList) == 0 and not len(lUnBalanceList) == 0 and len(hUnBalanceList) == 0:
            logger.debug("start balance between lowList and  avgList *****************--------------- ")
            lRes = change_node_list(lUnBalanceList, avgList, config=config)
        else:
            count += 1
            if count > 2:
                logger.info("We can not provide a better balance option. Will exit the program")
                return
