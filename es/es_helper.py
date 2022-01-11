import boto3
from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth

import constant
import settings


class EsHelper:

    _instance = None
    _es_instance = None

    def __new__(cls):
        if not cls._instance:
            cls._instance = super().__new__(cls)
        return cls._instance

    @property
    def es_instance(self):
        if not self._es_instance:
            if settings.ES_AUTH_TYPE == constant.ES_AUTH_TYPE_MASTER:
                self._set_master_es_instance()
            else:
                self._set_iam_es_instance()
        return self._es_instance

    def _set_master_es_instance(self):
        variables = {
            'hosts': [settings.ES_HOST_MASTER],
            'http_auth': ('es_admin', settings.ES_PASSWORD),
            'use_ssl': True,
            'scheme': 'https',
            'port': 443
        }
        self._set_es_instance(**variables)

    def _set_iam_es_instance(self):
        credential = boto3.session.Session().get_credentials()
        aws_auth = AWS4Auth(
            credential.access_key,
            credential.secret_key,
            settings.ES_REGION,
            'es',
            session_token=credential.token
        )
        variables = {
            'hosts': [settings.ES_HOST_IAM],
            'http_auth': aws_auth,
            'use_ssl': True,
            'port': 443,
            'connection_class': RequestsHttpConnection
        }
        self._set_es_instance(**variables)

    def _set_es_instance(self, **kwargs):
        self._es_instance = Elasticsearch(**kwargs)

    def get_korean_analyzer_index_config(self):
        return {
            'settings': {
                'index': {
                    'analysis': {
                        'analyzer': {
                            'analyzer_korean_default': {
                                'type': 'custom',
                                'tokenizer': 'seunjeon',
                            },
                            'analyzer_korean': {
                                'type': 'custom',
                                'tokenizer': 'seunjeon',
                                'filter': ['filter_synonym']
                            }
                        },
                        'filter': {
                            'filter_synonym': {
                                'type': 'synonym',
                                'synonyms_path': 'analyzers/F33341521',
                                'updateable': 'true'
                            }
                        },
                        'tokenizer': {
                            'seunjeon': {
                                'type': 'seunjeon_tokenizer',
                                'user_dict_path': 'analyzers/F86218362',
                                'index_eojeol': 'true',
                                'index_poses': ['UNK', 'EP', 'I', 'J', 'M', 'N', 'SL', 'SH', 'SN', 'VCP', 'XP', 'XS', 'XR'],
                                'decompound': 'true',
                            }
                        }
                    }
                }
            },
            'mappings': {
                'properties': {
                    'product_id': {
                        'type': 'integer'
                    },
                    'product_name': {
                        'type': 'text',
                        'analyzer': 'analyzer_korean_default',
                        'search_analyzer': 'analyzer_korean'
                    },
                    'categories': {
                        'type': 'text',
                        'analyzer': 'analyzer_korean_default',
                        'search_analyzer': 'analyzer_korean'
                    },
                    'pet_type': {
                        'type': 'keyword'
                    },
                    'created_at': {
                        'type': 'date',
                    }
                }
            }
        }