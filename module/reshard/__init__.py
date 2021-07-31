import click

from module.reshard.common.constant import big_shard_first, actions, p_r_strategy, tolerance, waiting, if_see_log
from module.reshard.entity.config import Config
from module.reshard.process.balance import run


@click.command()
@click.option('-a', '--action-type', 'action', required=True,
              help='Action type. Why u use this tool? must be in %s. If not specified, will exit immediately.' % actions.keys())
@click.option('-u', '--url', 'url', required=True,
              help='Query es url')
@click.option('-b', '--big_shard_first', 'big_first', required=False, default=big_shard_first,
              help='Define whether bigger shard first more or not')
@click.option('-p', '--p_r_strategy', 'shard_balanced_strategy', required=False, default=p_r_strategy,
              help='Define whether p shard and r shard can be located on the same node')
@click.option('-t', '--tolerance', 'tolerance', required=False, default=tolerance,
              help='Determine on which condition the disk is balanced.')
@click.option('-w', '--waiting', 'wait_factor', required=False, default=waiting,
              help='Determine on which condition the disk is balanced.')
@click.option('-i', '--if_see_log', 'if_see_log', required=False, default=if_see_log,
              help='Determine on which condition the disk is balanced.')
def cli(action, url, big_first, tolerance, shard_balanced_strategy, wait_factor, if_see_log):
    config = Config(url=url,
                    action=action,
                    big_first=big_first,
                    tolerance=tolerance,
                    p_r_strategy=shard_balanced_strategy,
                    waiting=wait_factor,
                    if_see_log=if_see_log)
    run(config)

