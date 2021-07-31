#
# import time
# import json
# import logging
# from module.utils import http_client, constants
# from module.process.writer import FileWriter
# from module.pseudonymize import pseudonymize
#
# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
# logger = logging.getLogger(__name__)
#
#
# def query_imsi_list_with_alert_type(parser, identifier):
#     response = http_client.query_es(constants.sa_index,
#                                     constants.sa_query_imsi_body_with_alert_type(identifier,
#                                                                                  parser.get_start(),
#                                                                                  parser.get_end()))
#     results = json.loads(response)["aggregations"][constants.report_name]["buckets"]
#     return list(map(lambda a: a["key"], results))
#
#
# def query_imsi_list_without_alert_type(parser):
#     response = http_client.query_es(constants.sa_index,
#                                     constants.sa_query_imsi_body_without_alert_type(
#                                         parser.get_start(),
#                                         parser.get_end()))
#     results = json.loads(response)["aggregations"][constants.report_name]["buckets"]
#     return list(map(lambda a: a["key"], results))
#
#
# def get_file_name(prefix, parser, imsi):
#     file = "pseu_%s.csv" % (pseudonymize.pseudonymize(imsi)) if parser.get_pseudonymize() else "ori_%s.csv" % (
#         str(imsi))
#     return "/%s/%s" % (prefix, file)
#
#
# def query_rta(parser, imsi_list):
#     index = parser.get_dcp_index_list()
#     prefix = "%s/%s/%s" % (constants.data_root,
#                            parser.get_start() + "-" + parser.get_end(),
#                            constants.rta)
#     rta_writer = FileWriter(parser)
#     rta_writer.set_pseudonymized_field(constants.rta_pseudonymized_field)
#     rta_writer.create_dir(prefix)
#     logger.info("Start file write process. Alert type: %s. Write_mode: overwrite. Path: %s" % (
#         parser.get_alert_type(),
#         prefix))
#     results = map(lambda imsi:
#                   rta_writer.write_to_csv(
#                       http_client.query_es_by_scroll_api(index,
#                                                          constants.query_rta_source(
#                                                              imsi,
#                                                              constants.es_page_size,
#                                                              parser.get_start(),
#                                                              parser.get_end()
#                                                          )),
#                       get_file_name(prefix, parser, imsi),
#                       constants.overwrite_mode),
#                   imsi_list)
#     logger.info(
#         "Successfully created %s file. Alert type: %s. Path: %s" % (
#             sum(list(results)), constants.rta, prefix))
#
#
# def query_sa(parser, imsi_list, identifier=None):
#     writer = FileWriter(parser)
#     prefix = "%s/%s/%s" % (constants.data_root,
#                            parser.get_start() + "-" + parser.get_end(),
#                            parser.get_alert_type())
#     writer.create_dir(prefix)
#     logger.info("Start file write process. Alert type: %s. Write_mode: overwrite. Path: %s" % (
#         parser.get_alert_type(),
#         prefix))
#
#     results = map(lambda imsi:
#                   writer.write_to_csv(
#                       http_client.query_es_by_scroll_api(constants.sa_index,
#                                                          constants.query_sa_source_without_alert_type(imsi,
#                                                                                                       constants.es_page_size,
#                                                                                                       parser.get_start(),
#                                                                                                       parser.get_end())
#                                                          if constants.usecase_all.__eq__(parser.get_alert_type()) else
#                                                          constants.query_sa_source(identifier,
#                                                                                    imsi,
#                                                                                    constants.es_page_size,
#                                                                                    parser.get_start(),
#                                                                                    parser.get_end()
#                                                                                    )),
#                       get_file_name(prefix, parser, imsi),
#                       constants.overwrite_mode,
#                       True),
#                   imsi_list)
#     logger.info(
#         "Successfully created %s file. Write_mode: overwrite. Alert type: %s in path: %s" % (
#             sum(list(results)), parser.get_alert_type(), prefix))
#
#
# def run(parser):
#     logger.info("Start process. Switch alert_type: %s, Start time: %s, End time: %s" % (
#         parser.get_alert_type(), parser.get_start(), parser.get_end()))
#     time_start = time.time()
#     if constants.usecase_all.__eq__(parser.get_alert_type()):
#         imsi_list = query_imsi_list_without_alert_type(parser)
#         query_sa(parser, imsi_list)
#         query_rta(parser, imsi_list)
#     elif constants.sa_dic.keys().__contains__(parser.get_alert_type()):
#         imsi_list = query_imsi_list_with_alert_type(parser, constants.sa_dic.get(parser.get_alert_type()))
#         query_sa(parser, imsi_list, constants.sa_dic.get(parser.get_alert_type()))
#         query_rta(parser, imsi_list)
#     else:
#         valid_alert_type = list(constants.sa_dic.keys())
#         valid_alert_type.append(constants.usecase_all)
#         logger.error("Alert_type must be in %s" % valid_alert_type)
#         exit(0)
#     time_end = time.time()
#     logger.info("End process, time cost: %ss. Switch alert_type: %s. Start time: %s. End time: %s" % (
#         format(time_end - time_start, '.3f'), parser.get_alert_type(), parser.get_start(), parser.get_end()
#     ))