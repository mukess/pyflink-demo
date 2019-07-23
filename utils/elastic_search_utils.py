from elasticsearch import Elasticsearch


def create_index(index='test', body=''):
    es = Elasticsearch()
    es.indices.create(index=index, ignore=400, body=body)


def add_update_data(index, doc_type, id, body):
    es = Elasticsearch()
    es.index(index=index, doc_type=doc_type, id=id, body=body)


def get_data(index, doc_type, id):
    es = Elasticsearch()
    return es.get(index=index, doc_type=doc_type, id=id)['_source']


def get_all_data(index, doc_type='_all'):
    es = Elasticsearch()
    if index is not None:
        data = es.search(index=index, doc_type=doc_type)
    return data


def delete_index(index):
    es = Elasticsearch()
    if index is not None:
        es.indices.delete(index=index, ignore=[400, 404])


if __name__ == '__main__':
    create_index('user')
    # from datetime import datetime
    #
    # body = {"any": "data", "timestamp": datetime.now()}
    # add_update_data(index='test', doc_type='person', id=1, body=body)
    # import time
    # time.sleep(1)
    # print(get_all_data('test', 'person'))
    # delete_index('user')
