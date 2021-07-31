import os

from module.decopy.utils import pseudonymize

es_scheme = "https"
hosts = os.getenv("ES_HOST")
user = os.getenv("SA_QUERY_USER_NAME")
password = os.getenv("SA_QUERY_USER_PASSWORD")
sa_index = os.getenv("SA_INDEX")
data_root = os.getenv("SA_DATA_PATH")
application_json = {'Content-Type': 'application/json'}
time_pattern = "%Y-%m-%dT%H:%M:%S.%fZ"
rta = "rta"
usecase_all = "all"
report_name = "subscription_anomalies_reports"
overwrite_mode = "w"
support_start = 8
support_period = 3
empty = ""
sa_pseudonymized_field = ["imsi"]
rta_pseudonymized_field = ["imsi"]
salt = pseudonymize.get_random_str(6)
init_scroll_id = "1m"
delimiter = ","
sleep_interval = 0.2
es_max_size = 10000
es_page_size = 1000
max_bucket_size = 200
error_info = "Error response code : %s"
sa_dic = {
    "us": "Unknown Subscription",
    "elu": "Excessive Location Update",
    "hsfr": "High SMS Failure Rate"
}
rta_source = ["country_code", "enterprise_name", "enterprise_id", "event", "imsi", "map_errorcode", "map_status", "mcc",
              "mnc", "operator_name", "operator_id", "event_raw", "result_code", "timestamp_event", "msg_number",
              "exp_resultcode"]
query_dcp_index_body = {
    "_source": "{}"
}
sa_index_total_schema = ("imsi", "timestamp_event", "timestamp_event_last", "event_raw", "msg_number", "operator_id",
                         "operator_name", "enterprise_id", "enterprise_name", "report_type", "timestamp_processing",
                         "message_count", "sms_mo_message_count", "sms_mt_message_count", "message_error_count",
                         "sms_mo_error_message_count", "sms_mt_error_message_count", "failure_rate", "country_code",
                         "visited_nw", "mcc", "mnc")


def sa_query_imsi_body_with_alert_type(identifier, start, end):
    return {
        "size": 0,
        "query": {
            "bool": {
                "must": [
                    {
                        "match_phrase": {
                            "report_type": {
                                "query": identifier,
                                "slop": 0,
                                "zero_terms_query": "NONE",
                                "boost": 1.0
                            }
                        }
                    }
                ],
                "filter": [
                    {
                        "range": {
                            "timestamp_event": {
                                "from": start,
                                "to": end,
                                "include_lower": True,
                                "include_upper": True,
                                "boost": 1.0
                            }
                        }
                    }
                ],
                "adjust_pure_negative": True,
                "boost": 1.0
            }
        },
        "aggregations": {
            report_name: {
                "terms": {
                    "size": max_bucket_size,
                    "field": "imsi",
                    "min_doc_count": 1,
                    "shard_min_doc_count": 0,
                    "execution_hint": "map",
                    "order": [
                        {
                            "_count": "desc"
                        },
                        {
                            "_key": "asc"
                        }
                    ]
                }
            }
        }
    }


def sa_query_imsi_body_without_alert_type(start, end):
    return {
        "size": 0,
        "query": {
            "bool": {
                "filter": [
                    {
                        "range": {
                            "timestamp_event": {
                                "from": start,
                                "to": end,
                                "include_lower": True,
                                "include_upper": True,
                                "boost": 1.0
                            }
                        }
                    }
                ],
                "adjust_pure_negative": True,
                "boost": 1.0
            }
        },
        "aggregations": {
            report_name: {
                "terms": {
                    "field": "imsi",
                    "size": max_bucket_size,
                    "min_doc_count": 1,
                    "shard_min_doc_count": 0,
                    "execution_hint": "map",
                    "order": [
                        {
                            "_count": "desc"
                        },
                        {
                            "_key": "asc"
                        }
                    ]
                }
            }
        }
    }


def query_sa_source(identifier, imsi, size, start, end):
    return {
        "size": size,
        "query": {
            "bool": {
                "must": [
                    {
                        "match_phrase": {
                            "report_type": {
                                "query": identifier,
                                "slop": 0,
                                "zero_terms_query": "NONE",
                                "boost": 1.0
                            }
                        }
                    },
                    {
                        "match_phrase": {
                            "imsi": {
                                "query": imsi,
                                "slop": 0,
                                "zero_terms_query": "NONE",
                                "boost": 1.0
                            }
                        }
                    }
                ],
                "filter": [
                    {
                        "range": {
                            "timestamp_event": {
                                "from": start,
                                "to": end,
                                "include_lower": True,
                                "include_upper": True,
                                "boost": 1.0
                            }
                        }
                    }
                ],
                "adjust_pure_negative": True,
                "boost": 1.0
            }
        }
    }


def query_sa_source_without_alert_type(imsi, size, start, end):
    return {
        "size": size,
        "query": {
            "bool": {
                "must": [
                    {
                        "match_phrase": {
                            "imsi": {
                                "query": imsi,
                                "slop": 0,
                                "zero_terms_query": "NONE",
                                "boost": 1.0
                            }
                        }
                    }
                ],
                "filter": [
                    {
                        "range": {
                            "timestamp_event": {
                                "from": start,
                                "to": end,
                                "include_lower": True,
                                "include_upper": True,
                                "boost": 1.0
                            }
                        }
                    }
                ],
                "adjust_pure_negative": True,
                "boost": 1.0
            }
        }
    }


def query_rta_source(imsi, size, start, end):
    return {
        "size": size,
        "_source": rta_source,
        "query": {
            "bool": {
                "must": [
                    {
                        "match_phrase": {
                            "imsi": {
                                "query": imsi,
                                "slop": 0,
                                "zero_terms_query": "NONE",
                                "boost": 1.0
                            }
                        }
                    }
                ],
                "filter": [
                    {
                        "range": {
                            "timestamp_event": {
                                "from": start,
                                "to": end,
                                "include_lower": True,
                                "include_upper": True,
                                "boost": 1.0
                            }
                        }
                    }
                ],
                "adjust_pure_negative": True,
                "boost": 1.0
            }
        }
    }


def scroll_body(page_id, scroll_id):
    return {
        "scroll": page_id,
        "scroll_id": scroll_id
    }


def delete_scroll_body(scroll_id):
    return {
        "scroll_id": scroll_id
    }
