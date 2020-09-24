import logging
import sys
import argparse


def setup_custom_logger(name):
    formatter = logging.Formatter(fmt='%(asctime)s %(levelname)-8s %(message)s',
                                  datefmt='%Y-%m-%d %H:%M:%S')
    handler = logging.FileHandler('log.txt', mode='w')
    handler.setFormatter(formatter)
    screen_handler = logging.StreamHandler(stream=sys.stdout)
    screen_handler.setFormatter(formatter)
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    logger.addHandler(handler)
    logger.addHandler(screen_handler)
    return logger

logger = setup_custom_logger('KafkaSender')    


parser = argparse.ArgumentParser()

parser.add_argument('-e', action='store', help='Set kafka env', required=True)
parser.add_argument('-db', action='store', help='Set source database', required=True)
parser.add_argument('-phase', action='store', help='Send appropriate data set', required=True)
parser.add_argument('-s', action='store', dest='server', default='.', help='Set sql server')
parser.add_argument('-u', action='store', dest='user', help='Set sql user')
parser.add_argument('-p', action='store', dest='password', help='Set sql user password')
parser.add_argument('-pf', action='store', default='', dest='prefix', help='Set a topic prefix')
parser.add_argument('-sf', action='store', default='', dest='suffix', help='Set a topic suffix')
parser.add_argument('-d', action='store_true', default=False, dest='is_delete', help='Delete topics only')

results = parser.parse_args()



class GeneralConfig:
    ROWCOUNT_LOG_FILE = 'rowcount_log.txt'
    PHASE = results.phase
    ENV = results.e
    TOPIC_PREFIX = results.prefix
    TOPIC_SUFFIX = results.suffix
    DELETE_ONLY = results.is_delete
    CHUNK = 10000

class DBConfig:
    SERVER = results.server
    # DB = 'ModasTransform100' if GeneralConfig.TOPIC_PREFIX == '100.' else 'ModasTransform10k'
    DB = results.db
    USER = results.user
    PASSWORD = results.password


class ConfluentKafkaConfig:
    BAT_LOCATION = r'C:\WORK\GIT\KafkaViewer\confluent-5.0.0\bin\windows'

class KafkaConfigDev:
    BROKER = 'kafka1.dev.modas.cliffordthames.com:9092,kafka2.dev.modas.cliffordthames.com:9092,kafka3.dev.modas.cliffordthames.com:9092'
    ZOOKEEPER = 'kafka1.dev.modas.cliffordthames.com:2181'

class KafkaConfigQc:
    BROKER = 'kafka1.qc.modas.cliffordthames.com:9092,kafka2.qc.modas.cliffordthames.com:9092,kafka3.qc.modas.cliffordthames.com:9092'
    ZOOKEEPER = 'kafka1.qc.modas.cliffordthames.com:2181'

class KafkaConfigUat:
    BROKER = 'kafka1.jlr.ds.uat.oec.local:9092,kafka2.jlr.ds.uat.oec.local:9092,kafka3.jlr.ds.uat.oec.local:9092'
    ZOOKEEPER = 'kafka1.jlr.ds.uat.oec.local:2181'

class KafkaConfigPrd:
    BROKER = 'kafka1.jlr.ds.oec.local:9092,kafka2.jlr.ds.oec.local:9092,kafka3.jlr.ds.oec.local:9092'
    ZOOKEEPER = 'kafka1.jlr.ds.oec.local:2181'

class KafkaConfigInt:
    BROKER = 'dev1.jlr.ds.cliffordthames.com:9092'
    ZOOKEEPER = 'dev1.jlr.ds.cliffordthames.com:2181'

class KafkaConfigDevRS:
    BROKER = 'kafka1.jlr.ds.dev.oec.local:9092,kafka2.jlr.ds.dev.oec.local:9092,kafka3.jlr.ds.dev.oec.local:9092'
    ZOOKEEPER = 'kafka1.jlr.ds.dev.oec.local:2181'



if GeneralConfig.ENV == 'DEV':
    KafkaConfig = KafkaConfigDev
elif GeneralConfig.ENV == 'UAT':
    KafkaConfig = KafkaConfigUat
elif GeneralConfig.ENV == 'PRD':
    KafkaConfig = KafkaConfigPrd
elif GeneralConfig.ENV == 'QC':
    KafkaConfig = KafkaConfigQc
elif GeneralConfig.ENV == 'INT':
    KafkaConfig = KafkaConfigInt
elif GeneralConfig.ENV == 'DEV_RS':
    KafkaConfig = KafkaConfigDevRS     
else:
    KafkaConfig = KafkaConfigDev





class JsonConfig:
    WRAPPER_DICT = {
        'meta':{
            'uuid':'uuid',
            'src':'MoDAS_Downstream'
        },
        'msg':{
        }
    }
    ACTION_DICT = {
        'cd': 'A',
        'tp': 'ADDED'
    }



class VinConfig:
    TOPIC = GeneralConfig.TOPIC_PREFIX + 'mock.push.upstream.vin' + GeneralConfig.TOPIC_SUFFIX
    KAFKA_KEY = ('msg','vin')
    MSG_TO_DROP = 0
    DB = 'Vin100' if GeneralConfig.TOPIC_PREFIX == '100.' else 'Vin10k'



class AftermarketPartConfig:
    TOPIC = GeneralConfig.TOPIC_PREFIX + 'mock.push.modas.aftermarket_part' + GeneralConfig.TOPIC_SUFFIX
    KAFKA_KEY = None #('msg','apn')
    MSG_TO_DROP = 0
    DEFAULT_DATE = '1900-01-01'
    DEFAULT_BOOL = False
    IS_DEFAULT_ACTIVE = False

class SupersessionConfig:
    TOPIC = GeneralConfig.TOPIC_PREFIX + 'mock.push.modas.supersession' + GeneralConfig.TOPIC_SUFFIX
    KAFKA_KEY = None #('msg','apn')
    MSG_TO_DROP = 0

class EngineeringPartConfig:
    TOPIC = GeneralConfig.TOPIC_PREFIX + 'mock.push.modas.engineering_part' + GeneralConfig.TOPIC_SUFFIX
    KAFKA_KEY = None #('msg','apn')
    MSG_TO_DROP = 0

class EngineeringPartFunctionConfig:
    TOPIC = GeneralConfig.TOPIC_PREFIX + 'mock.push.modas.engineering_part_function' + GeneralConfig.TOPIC_SUFFIX
    KAFKA_KEY = None #('msg','apn')
    MSG_TO_DROP = 0

class EngineeringPartUsageConfig:
    TOPIC = GeneralConfig.TOPIC_PREFIX + 'mock.push.modas.engineering_part_usage' + GeneralConfig.TOPIC_SUFFIX
    KAFKA_KEY = None #('msg','apn')
    MSG_TO_DROP = 0

class DescriptionConfig:
    TOPIC = GeneralConfig.TOPIC_PREFIX + 'mock.push.modas.description' + GeneralConfig.TOPIC_SUFFIX
    KAFKA_KEY = None #('msg','cd')
    MSG_TO_DROP = 0

class FeatureConfig:
    TOPIC = GeneralConfig.TOPIC_PREFIX + 'mock.push.modas.feature' + GeneralConfig.TOPIC_SUFFIX
    KAFKA_KEY = None #('msg','cd')
    MSG_TO_DROP = 0

class FeatureFamilyConfig:
    TOPIC = GeneralConfig.TOPIC_PREFIX + 'mock.push.modas.feature_family' + GeneralConfig.TOPIC_SUFFIX
    KAFKA_KEY = None #('msg','cd')
    MSG_TO_DROP = 0

class HierarchyConfig:
    TOPIC = GeneralConfig.TOPIC_PREFIX + 'mock.push.modas.hierarchy' + GeneralConfig.TOPIC_SUFFIX
    KAFKA_KEY = None #('msg','cd')
    MSG_TO_DROP = 0

class HierarchyUsageConfig:
    TOPIC = GeneralConfig.TOPIC_PREFIX + 'mock.push.modas.hierarchy_usage' + GeneralConfig.TOPIC_SUFFIX
    KAFKA_KEY = None #('msg','cd')
    MSG_TO_DROP = 0    

class SectionCalloutConfig:
    TOPIC = GeneralConfig.TOPIC_PREFIX + 'mock.push.modas.section_callout' + GeneralConfig.TOPIC_SUFFIX
    KAFKA_KEY = None #('msg','cd')
    MSG_TO_DROP = 0    

class SectionPartUsageConfig:
    TOPIC = GeneralConfig.TOPIC_PREFIX + 'mock.push.modas.section_part_usage' + GeneralConfig.TOPIC_SUFFIX
    KAFKA_KEY = None #('msg','cd')
    MSG_TO_DROP = 0    

class HierarchyIllustrationConfig:
    TOPIC = GeneralConfig.TOPIC_PREFIX + 'mock.push.modas.hierarchy_illustration' + GeneralConfig.TOPIC_SUFFIX
    KAFKA_KEY = None #('msg','cd')
    MSG_TO_DROP = 0    

class PartMetaConfig:
    TOPIC = GeneralConfig.TOPIC_PREFIX + 'mock.streaming.modas.part_meta' + GeneralConfig.TOPIC_SUFFIX
    KAFKA_KEY = None #('msg','cd')
    MSG_TO_DROP = 0    

class IntrayConfig:
    TOPIC = GeneralConfig.TOPIC_PREFIX + 'mock.push.modas.intray' + GeneralConfig.TOPIC_SUFFIX
    KAFKA_KEY = None #('msg','cd')
    MSG_TO_DROP = 0    
