from utils import kafka_utils, elastic_search_utils


def prepare_env():
    elastic_search_used_method = ['group_by_agg_streaming', 'distinct_agg_streaming']

    topics = kafka_utils.list_topics()
    if 'user' not in topics:
        kafka_utils.create_topic('user')
        msgs = [{'a': 'a', 'b': 1, 'c': 1, 'time': '2013-01-01T00:14:13Z'},
                {'a': 'b', 'b': 2, 'c': 2, 'time': '2013-01-01T00:24:13Z'},
                {'a': 'a', 'b': 3, 'c': 3, 'time': '2013-01-01T00:34:13Z'},
                {'a': 'a', 'b': 4, 'c': 4, 'time': '2013-01-01T01:14:13Z'},
                {'a': 'b', 'b': 4, 'c': 5, 'time': '2013-01-01T01:24:13Z'},
                {'a': 'a', 'b': 5, 'c': 2, 'time': '2013-01-01T01:34:13Z'}]
        for msg in msgs:
            kafka_utils.send_msg('user', msg)

    mapping = '''
    {
        "mappings" : {
              "pyflink" : {
                "properties" : {
                  "a" : {
                    "type" : "text",
                    "fields" : {
                      "keyword" : {
                        "type" : "keyword",
                        "ignore_above" : 256
                      }
                    }
                  },
                  "b" : {
                    "type" : "text",
                    "fields" : {
                      "keyword" : {
                        "type" : "keyword",
                        "ignore_above" : 256
                      }
                    }
                  }
                }
              }
        }
    }
    '''
    for method in elastic_search_used_method:
        elastic_search_utils.delete_index(method)
        elastic_search_utils.create_index(method, mapping)
