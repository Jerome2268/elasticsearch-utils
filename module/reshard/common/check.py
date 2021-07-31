import logging

from module.reshard.common.measure import convert
from module.reshard.entity.config import Config

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def checkIfShardNumBalance(config: Config):
    for i in config.avg_index_list:
        size = convert(i[1])
        for m in config.node_list:
            PNum, PVal = m.getIndexPShardNumAndVal(i[0])
            RNum, RVal = m.getIndexRShadeNumAndVal(i[0])
            if abs(PNum - i[2]) > 1:
                logger.debug(i[0] + m.node + "pri  unbalanced")
            if abs(RNum - i[2]) > 1:
                logger.debug(i[0] + m.node + "rpli unbalanced")
            # 一般是均衡算法的问题  此处不做讨论
            if abs(convert(PVal) - size) / size > config.tolerance:
                logger.debug(i[0] + m.node + "pri value unbalanced")
            if abs(convert(RVal) - size) / size > config.tolerance:
                logger.debug(i[0] + m.node + "r value unbalanced")


def checkIfDiskBalance(config: Config):
    # check if the disk is balanced
    flag = True
    count = 0
    newSumMem = 0
    for node in config.node_list:
        per = (convert(node.storeValue) - config.avg_memory) / config.avg_memory
        newSumMem += convert(node.storeValue)
        if abs(per) > config.tolerance:
            flag = False
            count += 1
            logger.info(node.node +
                        ": disk unbalance D-value with avgValue  " + str(per * 100) + "%" + "standard D-value" + str(
                config.tolerance * 100) + "%")
    if flag:
        logger.info("=========================")
        logger.info("disk balanced")
        logger.info("=========================")
    else:
        if config.if_see_log:
            logger.fatal("disk unbalanced ， datanode count:" + str(count))
    return flag, count
