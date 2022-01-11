from elasticsearch import helpers

from es.es_helper import EsHelper


class BaseEs:
    _index = None
    _index_config = None
    _es_instance = None

    def __init__(self):
        es_helper = EsHelper()
        self._es_instance = es_helper.es_instance
        self._index_config = es_helper.get_korean_analyzer_index_config()

    @staticmethod
    def _get_single_match_query(key, value):
        return {
            'query': {
                'match': {
                    key: value
                }
            }
        }

    def _set_index(self, index):
        self._index = index

    def _create_index(self):
        self._es_instance.indices.create(index=self._index, body=self._index_config)

    def _delete_index(self):
        self._es_instance.indices.delete(index=self._index, ignore=[400, 404])

    def _bulk_insert(self, data_list):
        bulk_data_list = [{'_index': self._index, '_source': data} for data in data_list]
        helpers.bulk(self._es_instance, bulk_data_list)

    def _analyze(self, body):
        return self._es_instance.indices.analyze(index=self._index, body=body)

    def _analyze_korean(self, text):
        return self._analyze(
            body={
                'analyzer': 'analyzer_korean',
                'text': text
            }
        )

    def _count(self, body=None):
        return self._es_instance.count(index=self._index, body=body)

    def _find(self, body=None, size=None):
        return self._es_instance.search(index=self._index, body=body, size=size)

    # https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-update.html#_update_part_of_a_document
    def _update_by_query(self, query, script):
        body = {}
        if query:
            body.update(query)
        body.update(script)
        self._es_instance.update_by_query(
            index=self._index,
            body=body
        )

    def _delete_by_query(self, query):
        self._es_instance.delete_by_query(index=self._index, body=query)

    def _parse_find_response(self, response):
        hits = response['hits']['hits']
        return [hit['_source'] for hit in hits]

    def _parse_count_response(self, response):
        return response['count']
