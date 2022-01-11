import os
from pathlib import Path

from dotenv import load_dotenv

import constant

env_path = Path('.env')
load_dotenv(dotenv_path=env_path)
SLEEP_TIME = 3

ES_HOST_MASTER = os.getenv('ES_HOST_MASTER')
ES_HOST_IAM = os.getenv('ES_HOST_IAM')
ES_PASSWORD = os.getenv('ES_PASSWORD')
ES_AUTH_TYPE = constant.ES_AUTH_TYPE_MASTER
ES_REGION = 'ap-northeast-2'
