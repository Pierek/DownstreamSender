# -*- coding: utf-8 -*-
"""
Created on Thu Apr  4 11:26:20 2019

@author: ppurwin
"""

import pyodbc
import os
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient
import json
import copy
from config import *
import inspect
from functools import reduce
import random
from config import logger
import subprocess


def delete_topics(topics):
    """ delete topics """

    a = AdminClient({'bootstrap.servers': KafkaConfig.BROKER})
    fs = a.delete_topics([topics], operation_timeout=30)

    # Wait for operation to finish.
    for topic, f in fs.items():
        try:
            f.result()  # The result itself is None
            #print("Topic {} deleted".format(topic))
        except Exception as e:
            print("Failed to delete topic {}: {}".format(topic, e))


def entity_wrapper(func):
	def wrapper(*args):
		logger.info('(' + func.__name__ + ') Deleting topic ' + args[0] + '...')

		delete_topics(args[0])
		logger.info('(' + func.__name__ + ') Topic deleted ' + args[0])

		if not GeneralConfig.DELETE_ONLY:
			func(*args)
			logger.info('(' + func.__name__ + ') Data successfully sent to kafka (' + args[0] + ')')

	return wrapper
  




def get_data(query, server=DBConfig.SERVER, db=DBConfig.DB, user=DBConfig.USER, password=DBConfig.PASSWORD):
	if user is not None:
		conn = pyodbc.connect('DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={0};DATABASE={1};UID={2};PWD={3}'.format(server, db, user, password))
	else:
		conn = pyodbc.connect('DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={0};DATABASE={1};Trusted_Connection=yes'.format(server, db))
	with conn.cursor() as cursor:
		logger.info('(' + inspect.stack()[1].function + ') Getting DB data...')
		cursor.execute(query)
		column_names = [column[0] for column in cursor.description]
		while True:
			results = cursor.fetchmany(5000)
			if not results:
				break
			for result in results:
				yield dict(zip(column_names, result))

def get_scalar(query, server=DBConfig.SERVER, db=DBConfig.DB, user=DBConfig.USER, password=DBConfig.PASSWORD):
	if user is not None:
		conn = pyodbc.connect('DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={0};DATABASE={1};UID={2};PWD={3}'.format(server, db, user, password))
	else:
		conn = pyodbc.connect('DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={0};DATABASE={1};Trusted_Connection=yes'.format(server, db))
	with conn.cursor() as cursor:
		logger.info('(' + inspect.stack()[1].function + ') Getting rowcount...')
		rowcount = cursor.execute(query).fetchval()

	with open('log\\' + GeneralConfig.ENV + '_' + GeneralConfig.TOPIC_PREFIX + '_' + GeneralConfig.ROWCOUNT_LOG_FILE, 'a',) as f:
		f.write(inspect.stack()[1].function + ' ' + str(rowcount) + '\n')

	return rowcount



def send_to_kafka(topic, key_path, events):
	producer = Producer({'bootstrap.servers':KafkaConfig.BROKER})

	logger.info('(' + inspect.stack()[1].function + ') Sending ' + str(len(events)) + ' events to ' + topic + '...')

	for msg in events:
		while True:
			try:
				producer.produce(topic, key=reduce((lambda d, key: d[key]), key_path, msg).encode('utf-8') if key_path is not None else None,
								value=json.dumps(msg).encode('utf-8'))
				break
			except BufferError:
				producer.poll(1)
	producer.flush()


@entity_wrapper
def send_description(topic, kafka_key):

	rowcount = get_scalar("SELECT COUNT(*) FROM export.tblLEX_Term")

	term_query = """
	SELECT
		 [DescriptionCode] = CONVERT(varchar(10),A.TermId)
		,A.TermId
		,A.Description
		,[ShortDescription] = B.Description
		,A.IsTranslatable
		,[Status] = 'Completed'
		,A.AddDate
		,[Action] = 'A'
		,[Origin] = 'MIG'
	FROM export.tblLEX_Term A
	LEFT JOIN export.tblLEX_Term B
		ON B.TermId = A.ShortTermId
	ORDER BY A.TermId
	"""

	trans_query = """
	SELECT
		 TR.TermId
		,[LangCode] = L.ModasCoreLanguageCode
		,[LangDesc] = L.Description
		,TR.Description
		,[ShortDescription] = TRS.Description
	FROM export.tblLEX_Translation TR
	INNER JOIN config.tblLanguage L
		ON L.LanguageId = TR.LanguageId
		AND L.ModasCoreLanguageCode IS NOT NULL
	LEFT JOIN export.tblLEX_Term T
		LEFT JOIN export.tblLEX_Term TS
			INNER JOIN export.tblLEX_Translation TRS
				ON TRS.TermId = TS.TermId
				AND TRS.IsPrimaryTranslation = 1
			ON TS.TermId = T.ShortTermId
		ON T.TermId = TR.TermId
		AND TR.LanguageId = TRS.LanguageId
	WHERE TR.IsPrimaryTranslation = 1
	ORDER BY TR.TermId, LangCode
	"""

	desc_type_query = """
	SELECT
		 T.TermId
		,LT.TargetCategory
	FROM export.tblLEX_Term T
	INNER JOIN export.tblLEX_TermLexicon TL
		INNER JOIN export.tblLEX_Lexicon L
			INNER JOIN config.tblLexiconType LT
				ON LT.LexiconTypeId = L.LexiconTypeId
				AND LT.TargetCategory IS NOT NULL				
			ON L.LexiconId = TL.LexiconId
		ON TL.TermId = T.TermId
	GROUP BY T.TermId, LT.TargetCategory
	ORDER BY T.TermId, LT.TargetCategory
	"""

	rows = get_data(term_query)
	trans = get_data(trans_query)
	types = get_data(desc_type_query)		

	trans_list = []
	types_list = []
	combined = []
	runs = []
	chunk = GeneralConfig.CHUNK


	if rowcount <= chunk:
		runs.append(rowcount)
	else:
		while rowcount != 0:
			if rowcount <= chunk:
				runs.append(rowcount)
				break
			else:
				runs.append(chunk)
				rowcount-=chunk
			

	for run in runs:
		for _ in range(run):
			row = next(rows)
			final = copy.deepcopy(JsonConfig.WRAPPER_DICT)
			final['msg']['cd'] = row['DescriptionCode']
			final['msg']['desc'] = row['Description']
			final['msg']['sht_desc'] = row['ShortDescription']				
			final['msg']['is_tran'] = row['IsTranslatable']		
			final['msg']['stat'] = 'Completed'
			final['msg']['upd_dt'] = '{:%Y-%m-%dT%H:%M:%S.%f}'.format(row['AddDate'])
			final['msg']['actn'] = copy.deepcopy(JsonConfig.ACTION_DICT)
			final['msg']['orig'] = 'MIG'
			final['msg']['tp'] = []
			final['msg']['tran'] = []


			for tran in trans:
				if tran['TermId'] > row['TermId']:
					trans_list.append(tran)
					break
				elif tran['TermId'] == row['TermId']:
					final['msg']['tran'].append({
						'lang': {
							'cd': tran['LangCode'],
							'desc': tran['LangDesc']
						},
						'desc': tran['Description'],
						'sht_desc': tran['ShortDescription']
					})
			
			for id1, tran in enumerate(trans_list):
				if tran['TermId'] > row['TermId']:
					break
				elif tran['TermId'] == row['TermId']:
					final['msg']['tran'].append({
						'lang': {
							'cd': tran['LangCode'],
							'desc': tran['LangDesc']
						},
						'desc': tran['Description'],
						'sht_desc': tran['ShortDescription']
					})
			del trans_list[:id1]

			for typ in types:
				if typ['TermId'] > row['TermId']:
					types_list.append(typ)
					break
				elif typ['TermId'] == row['TermId']:
					final['msg']['tp'].append({
						'cd': typ['TargetCategory'],
						'desc': typ['TargetCategory']
					})
			
			for id2, typ in enumerate(types_list):
				if typ['TermId'] > row['TermId']:
					break
				elif typ['TermId'] == row['TermId']:
					final['msg']['tp'].append({
						'cd': typ['TargetCategory'],
						'desc': typ['TargetCategory']
					})

			del types_list[:id2]
			combined.append(final)


		send_to_kafka(topic=topic, key_path=kafka_key, events=combined)
		combined = []

	# #simulate inconsistency by dropping some random msgs
	# random.shuffle(combined)
	# del combined[:DescriptionConfig.MSG_TO_DROP]

@entity_wrapper
def send_feature(topic, kafka_key):

	rowcount = get_scalar("""
	;WITH CTE2 AS (
	SELECT DISTINCT F.JaguarAVSCode
	FROM export.tblAPL_Feature F
	WHERE F.JaguarAVSCode IS NOT NULL
	)
	SELECT (SELECT COUNT(*) FROM CTE2)"""
	)


	feature_query = """
	;WITH CTE2 AS (
	SELECT
		F.FeatureId
		,[FeatureCode] = F.JaguarAVSCode
		,FT.FeatureTypeCode
		,[DescriptionCode] = CONVERT(varchar(10),F.TermId)
		,FF.FeatureFamilyCode
		,F.AddDate
		,[rnb] = ROW_NUMBER() OVER (PARTITION BY F.JaguarAVSCode ORDER BY F.FeatureId)
	FROM export.tblAPL_Feature F
	INNER JOIN config.tblFeatureType FT
		ON FT.FeatureTypeId = F.FeatureTypeId
	INNER JOIN export.tblAPL_FeatureFamily FF
		ON FF.FeatureFamilyId = F.FeatureFamilyId
	WHERE F.JaguarAVSCode IS NOT NULL
	)
	SELECT
		FeatureId
		,FeatureCode
		,FeatureTypeCode
		,DescriptionCode
		,FeatureFamilyCode
		,AddDate
	FROM CTE2
	WHERE rnb = 1
	ORDER BY FeatureId
	"""

	child_feature_query = """
	SELECT
		FG.ParentFeatureId
		,[ChildFeatureCode] = COALESCE(F2.MIPSCode,F2.GCATCode)
	FROM export.tblAPL_FeatureGroup FG
	INNER JOIN export.tblAPL_Feature F2
		ON F2.FeatureId = FG.ChildFeatureId
	ORDER BY FG.ParentFeatureId
	"""


	rows = get_data(feature_query)
	child_features = get_data(child_feature_query)
	child_features_list = []
	runs = []
	chunk = GeneralConfig.CHUNK


	if rowcount <= chunk:
		runs.append(rowcount)
	else:
		while rowcount != 0:
			if rowcount <= chunk:
				runs.append(rowcount)
				break
			else:
				runs.append(chunk)
				rowcount-=chunk
			


	combined = []
	for run in runs:
		for _ in range(run):
			row = next(rows)
			final = copy.deepcopy(JsonConfig.WRAPPER_DICT)
			final['msg']['cd'] = row['FeatureCode']
			final['msg']['tp'] = row['FeatureTypeCode']
			final['msg']['stat'] = 'Approved'
			final['msg']['desc_cd'] = row['DescriptionCode']
			final['msg']['fml_cd'] = row['FeatureFamilyCode']
			final['msg']['chld_cd'] = []
			final['msg']['not_in_epc'] = False
			final['msg']['upd_dt'] = '{:%Y-%m-%dT%H:%M:%S.%f}'.format(row['AddDate'])
			final['msg']['actn'] = copy.deepcopy(JsonConfig.ACTION_DICT)
			final['msg']['orig'] = 'MIG'

			for child_feature in child_features:
				if child_feature['ParentFeatureId'] > row['FeatureId']:
					child_features_list.append(child_feature)
					break
				elif child_feature['ParentFeatureId'] == row['FeatureId']:
					final['msg']['chld_cd'].append(child_feature['ChildFeatureCode'])

			for id1, child_feature in enumerate(child_features_list):
				if child_feature['ParentFeatureId'] > row['FeatureId']:
					break
				elif child_feature['ParentFeatureId'] == row['FeatureId']:
					final['msg']['chld_cd'].append(child_feature['ChildFeatureCode'])
				
			del child_features_list[:id1]

			combined.append(final)


		send_to_kafka(topic=topic, key_path=kafka_key, events=combined)
		combined = []


@entity_wrapper
def send_feature_family(topic, kafka_key):

	rowcount = get_scalar("SELECT COUNT(*) FROM export.tblAPL_FeatureFamily")

	ff_query = """
	SELECT
		 FF.FeatureFamilyCode
		,FFT.TargetModasFeatureFamilyTypeCode
		,[DescriptionCode] = CONVERT(varchar(10), FF.TermId)
		,FF.AddDate
	FROM export.tblAPL_FeatureFamily FF
	INNER JOIN config.tblFeatureFamilyType FFT
		ON FFT.FeatureFamilyTypeId = FF.FeatureFamilyTypeId
	ORDER BY FF.FeatureFamilyCode
	"""

	rows = get_data(ff_query)	
	runs = []
	chunk = GeneralConfig.CHUNK


	if rowcount <= chunk:
		runs.append(rowcount)
	else:
		while rowcount != 0:
			if rowcount <= chunk:
				runs.append(rowcount)
				break
			else:
				runs.append(chunk)
				rowcount-=chunk
			

	combined = []
	for run in runs:
		for _ in range(run):
			row = next(rows)
			final = copy.deepcopy(JsonConfig.WRAPPER_DICT)
			final['msg']['cd'] = row['FeatureFamilyCode']
			final['msg']['tp'] = row['TargetModasFeatureFamilyTypeCode']
			final['msg']['desc_cd'] = row['DescriptionCode']					
			final['msg']['stat'] = 'Approved'
			final['msg']['upd_dt'] = '{:%Y-%m-%dT%H:%M:%S.%f}'.format(row['AddDate'])
			final['msg']['actn'] = copy.deepcopy(JsonConfig.ACTION_DICT)
			final['msg']['orig'] = 'MIG'

			combined.append(final)
	
		send_to_kafka(topic=topic, key_path=kafka_key, events=combined)
		combined = []


@entity_wrapper
def send_aftermarket_part(topic, kafka_key):

	rowcount = get_scalar("SELECT COUNT(*) FROM export.tblPRT_AftermarketPart")

	apn_query = """
	SELECT
		AP.AftermarketPartId
		,[APNPrefixCode] = AP.PrefixCode
		,[APNBaseCode] = AP.BaseCode
		,[APNSuffixCode] = AP.SuffixCode
		,[EPNPrefixCode] = EP.PrefixCode
		,[EPNBaseCode] = ISNULL(EP.BaseCode,'TEST')
		,[EPNSuffixCode] = EP.SuffixCode
		,[EPNCode] = ISNULL(EP.EPNCode,'TEST')
		,B.BrandCode
		,[EPNPartServiceStatusCode] = PS1.PartServiceStatusCode
		,[EPNPartServiceStatusDescription] = PS1.Description
		,[APNPartServiceStatusCode] = PS.PartServiceStatusCode
		,[APNPartServiceStatusDescription] = PS.Description		
		--,[DispositionTypeCode] = ISNULL(DT.DispositionTypeCode,'N')
		--,[DispositionTypeDescription] = ISNULL(DT.Description,'Normal')
		,[DescriptionCode] = CONVERT(varchar(10), AP.TermId)
		,[ServiceComment] = LEFT(RD.Description,4000)
		,AP.JAGServiceDecisionDate
		,AP.JAGServiceDecisionUser
		,[FirstEffInDate] = EPU.EffectiveInDate
		,[FirstEffInPoint] = BR.BreakPointCode
		,[EffInDate] = PL.EffectiveDateIn
		,[EffOutDate] = PL.EffectiveDateOut
		,AP.JAGRequiresCataloguing
		,AP.JAGAccessoryFlag
		,[ComparableEPN] = EP1.EPNCode
		,AP.AddDate
	FROM export.tblPRT_AftermarketPart AP
	INNER JOIN config.tblBrand B
		ON B.BrandId = AP.BrandId
	LEFT JOIN export.tblPRT_PartLink PL
		INNER JOIN export.tblPRT_EngineeringPart EP
			LEFT JOIN (
						SELECT EngineeringPartId, EffectiveInDate, EffectiveInPointId, [rnb] = ROW_NUMBER() OVER (PARTITION BY EngineeringPartId ORDER BY EffectiveInDate DESC, EffectiveInPointId ASC)
						FROM (
								SELECT EngineeringPartId, EffectiveInDate, EffectiveInPointId FROM export.tblPRT_EngineeringPartUsage WHERE EffectiveInDate IS NOT NULL
								UNION
								SELECT FinalEngineeringPartId, EffectiveInDate, EffectiveInPointId FROM export.tblPRT_EngineeringPartUsage WHERE EffectiveInDate IS NOT NULL
							) A
					) EPU
				INNER JOIN export.tblAPL_BreakPoint BR
					ON BR.BreakPointId = EPU.EffectiveInPointId
				ON EPU.EngineeringPartId = EP.EngineeringPartId
				AND EPU.rnb = 1
			LEFT JOIN export.tblPRT_EngineeringPartComparable EPNC
				INNER JOIN export.tblPRT_EngineeringPart EP1
					ON EP1.EngineeringPartId = EPNC.ComparableEngineeringPartId
				ON EPNC.EngineeringPartId = EP.EngineeringPartId
			LEFT JOIN config.tblPartServiceStatus PS1
				ON PS1.PartServiceStatusId = EP.JAGServiceStatusId
			ON EP.EngineeringPartId = PL.EngineeringPartId
		ON PL.AftermarketPartId = AP.AftermarketPartId
		AND PL.EffectiveDateOut IS NULL
	LEFT JOIN config.tblPartServiceStatus PS
		ON PS.PartServiceStatusId = AP.JAGServiceStatusId
	LEFT JOIN config.tblDispositionType DT
		ON DT.DispositionTypeId = AP.OPTDispositionTypeId
	LEFT JOIN export.tblREF_ReferenceDescription RD
		ON RD.ReferenceDescriptionId = AP.ServiceCommentId
	ORDER BY AP.AftermarketPartId
	"""


	qualifier_query = """
	SELECT
		 AftermarketPartId
		,[DescriptionCode] = CONVERT(varchar(10), QualifierTermId)
	FROM export.tblPRT_AftermarketPartQualifier
	ORDER BY AftermarketPartId, DescriptionCode
	"""

	rows = get_data(apn_query)
	qualifiers = get_data(qualifier_query)
	qualifiers_list = []
	runs = []
	chunk = GeneralConfig.CHUNK


	if rowcount <= chunk:
		runs.append(rowcount)
	else:
		while rowcount != 0:
			if rowcount <= chunk:
				runs.append(rowcount)
				break
			else:
				runs.append(chunk)
				rowcount-=chunk
			


	combined = []
	for run in runs:
		for _ in range(run):
			row = next(rows)
			final = copy.deepcopy(JsonConfig.WRAPPER_DICT)
			final['msg']['apn'] = {
				'pf': row['APNPrefixCode'],
				'bs': row['APNBaseCode'],
				'sf': row['APNSuffixCode']
			}
			final['msg']['epn'] = {
				'pf': row['EPNPrefixCode'],
				'bs': row['EPNBaseCode'],
				'sf': row['EPNSuffixCode']
			} if row['EPNCode'] is not None else None
			final['msg']['brd'] = 'JAG'
			final['msg']['srv_stat'] = {
				'cd': row['EPNPartServiceStatusCode'],
				'desc': row['EPNPartServiceStatusDescription']
			} if row['EPNPartServiceStatusCode'] is not None else None
			# final['msg']['stk_disp'] = {
			# 	'cd': row['DispositionTypeCode'],
			# 	'desc': row['DispositionTypeDescription']
			# } if row['DispositionTypeCode'] is not None else None
			final['msg']['long_desc_cd'] = row['DescriptionCode']
			final['msg']['cmnt_desc_cd'] = []
			final['msg']['srv_cmnt'] = row['ServiceComment']

			if row['JAGServiceDecisionDate'] is not None:
				final['msg']['srv_dcsn_dt'] = '{:%Y-%m-%d}'.format(row['JAGServiceDecisionDate'])
			elif AftermarketPartConfig.IS_DEFAULT_ACTIVE:
				final['msg']['srv_dcsn_dt'] = AftermarketPartConfig.DEFAULT_DATE
			else:
				final['msg']['srv_dcsn_dt'] = None

			final['msg']['srv_dcsn_usr'] = row['JAGServiceDecisionUser']

			if row['FirstEffInDate'] is not None:
				final['msg']['fst_eff_in_dt'] = '{:%Y-%m-%d}'.format(row['FirstEffInDate'])
			elif AftermarketPartConfig.IS_DEFAULT_ACTIVE:
				final['msg']['fst_eff_in_dt'] = AftermarketPartConfig.DEFAULT_DATE
			else:
				final['msg']['fst_eff_in_dt'] = None

			final['msg']['fst_eff_in_pt'] = row['FirstEffInPoint']
			final['msg']['brd_frst_int'] = 'JAG'

			if row['EffInDate'] is not None:
				final['msg']['eff_in_dt'] = '{:%Y-%m-%d}'.format(row['EffInDate'])
			elif AftermarketPartConfig.IS_DEFAULT_ACTIVE:
				final['msg']['eff_in_dt'] = AftermarketPartConfig.DEFAULT_DATE
			else:
				final['msg']['eff_in_dt'] = None

			if row['EffOutDate'] is not None:
				final['msg']['eff_out_dt'] = '{:%Y-%m-%d}'.format(row['EffOutDate'])
			elif AftermarketPartConfig.IS_DEFAULT_ACTIVE:
				final['msg']['eff_out_dt'] = AftermarketPartConfig.DEFAULT_DATE
			else:
				final['msg']['eff_out_dt'] = None

			if row['JAGRequiresCataloguing'] is not None:
				final['msg']['req_cat_flg'] = row['JAGRequiresCataloguing']
			elif AftermarketPartConfig.IS_DEFAULT_ACTIVE:
				final['msg']['req_cat_flg'] = AftermarketPartConfig.DEFAULT_BOOL
			else:
				final['msg']['req_cat_flg'] = None

			final['msg']['upsf_flg'] = False

			final['msg']['apn_stat'] = {
				'cd': row['APNPartServiceStatusCode'],
				'desc': row['APNPartServiceStatusDescription']
			} if row['APNPartServiceStatusCode'] is not None else None

			final['msg']['acc_flg'] = row['JAGAccessoryFlag']
			final['msg']['cmprbl_prt'] = row['ComparableEPN']
			final['msg']['spo_spr_flg'] = False
			final['msg']['mlt_flg'] = False
			final['msg']['brd_frst_int'] = 'JAG'
			final['msg']['upd_dt'] = '{:%Y-%m-%dT%H:%M:%S.%f}'.format(row['AddDate'])
			final['msg']['actn'] = copy.deepcopy(JsonConfig.ACTION_DICT)
			final['msg']['orig'] = 'MIG'


			for qual in qualifiers:
				if qual['AftermarketPartId'] > row['AftermarketPartId']:
					qualifiers_list.append(qual)
					break
				elif qual['AftermarketPartId'] == row['AftermarketPartId']:
					final['msg']['cmnt_desc_cd'].append(qual['DescriptionCode'])
				
			for id1, qual in enumerate(qualifiers_list):
				if qual['AftermarketPartId'] > row['AftermarketPartId']:
					break
				elif qual['AftermarketPartId'] == row['AftermarketPartId']:
					final['msg']['cmnt_desc_cd'].append(qual['DescriptionCode'])
				
			del qualifiers_list[:id1]


			combined.append(final)

		send_to_kafka(topic=topic, key_path=kafka_key, events=combined)
		combined = []


@entity_wrapper
def send_supersession(topic, kafka_key):

	rowcount = get_scalar("""
	SELECT COUNT(DISTINCT A.ReplacedAftermarketPartId)
	FROM export.tblPRT_AftermarketSupersession A
	INNER JOIN export.tblPRT_AftermarketPart B
		ON B.AftermarketPartId = A.ReplacedAftermarketPartId
		AND B.APNCodePacked NOT IN ('AAU8405','BAC2972','BEC21696','HJA7700AAANR','JLM10885','LMB6220AA','MHC5173AAAFW','NJE5954AA')
	""")

	supersession_query = """
	SELECT
		SS.ReplacedAftermarketPartId
		,[APNPrefixCode] = AP.PrefixCode
		,[APNBaseCode] = AP.BaseCode
		,[APNSuffixCode] = AP.SuffixCode
		,B.BrandCode
		,[DecisionDate] = MIN(SS.DecisionDate)
		,[SupersessionTypeCode] = CASE WHEN ST.SupersessionTypeCode = 'ON2N' THEN ISNULL(DT.DispositionTypeCode,'R1')
										WHEN ST.SupersessionTypeCode = 'COND' THEN 'V2'
										WHEN ST.SupersessionTypeCode = 'GRPL' THEN 'V1' ELSE NULL END
		,[SupersessionTypeDescription] = ST.Description
		,[DispositionTypeCode] = ISNULL(DT.DispositionTypeCode,'R1')
		,[DispositionTypeDescription] = ISNULL(DT.Description,'R1')
		,SS.AddDate
	FROM export.tblPRT_AftermarketSupersession SS
	INNER JOIN export.tblPRT_AftermarketPart AP
		INNER JOIN config.tblBrand B
			ON B.BrandId = AP.BrandId
		ON AP.AftermarketPartId = SS.ReplacedAftermarketPartId
		AND AP.APNCodePacked NOT IN ('AAU8405','BAC2972','BEC21696','HJA7700AAANR','JLM10885','LMB6220AA','MHC5173AAAFW','NJE5954AA')
	INNER JOIN config.tblSupersessionType ST
		ON ST.SupersessionTypeId = SS.SupersessionTypeId
	LEFT JOIN config.tblDispositionType DT
		ON DT.DispositionTypeId = SS.DispositionTypeId
	GROUP BY SS.ReplacedAftermarketPartId, AP.PrefixCode, AP.BaseCode, AP.SuffixCode, B.BrandCode, ST.SupersessionTypeCode, ST.Description, DT.DispositionTypeCode, DT.Description, SS.AddDate
	ORDER BY SS.ReplacedAftermarketPartId
	"""

	replacing_query = """
	SELECT
		SS.ReplacedAftermarketPartId
		,[APNPrefixCode] = AP.PrefixCode
		,[APNBaseCode] = AP.BaseCode
		,[APNSuffixCode] = AP.SuffixCode
		,SS.GroupNumber
		,SS.Sequence
		,SS.Quantity
		,[DescriptionCode] = CONVERT(varchar(10),SS.TermId)
	FROM export.tblPRT_AftermarketSupersession SS
	INNER JOIN export.tblPRT_AftermarketPart A1
		ON A1.AftermarketPartId = SS.ReplacedAftermarketPartId
		AND A1.APNCodePacked NOT IN ('AAU8405','BAC2972','BEC21696','HJA7700AAANR','JLM10885','LMB6220AA','MHC5173AAAFW','NJE5954AA')
	INNER JOIN export.tblPRT_AftermarketPart AP
		ON AP.AftermarketPartId = SS.ReplacingAftermarketPartId
	ORDER BY SS.ReplacedAftermarketPartId, AP.APNCodePacked
	"""


	rows = get_data(supersession_query)
	replacings = get_data(replacing_query)
	runs = []
	chunk = GeneralConfig.CHUNK
	replacing = None

	if rowcount <= chunk:
		runs.append(rowcount)
	else:
		while rowcount != 0:
			if rowcount <= chunk:
				runs.append(rowcount)
				break
			else:
				runs.append(chunk)
				rowcount-=chunk

	combined = []
	for run in runs:
		for _ in range(run):
			row = next(rows)
			final = copy.deepcopy(JsonConfig.WRAPPER_DICT)
			final['msg']['apn'] = {
				'pf': row['APNPrefixCode'],
				'bs': row['APNBaseCode'],
				'sf': row['APNSuffixCode']
			}
			final['msg']['brd'] = row['BrandCode']
			final['msg']['dec_dt'] = '{:%Y-%m-%d}'.format(row['DecisionDate']) if row['DecisionDate'] is not None else None
			final['msg']['bkn_dt'] = None
			final['msg']['dcsn_usr'] = None
			final['msg']['tp'] = {
				'cd': row['SupersessionTypeCode'],
				'desc': row['SupersessionTypeDescription']
			}
			final['msg']['stk_disp'] = {
				'cd': row['DispositionTypeCode'],
				'desc': row['DispositionTypeDescription']
			}
			final['msg']['upd_dt'] = '{:%Y-%m-%dT%H:%M:%S.%f}'.format(row['AddDate'])
			final['msg']['actn'] = copy.deepcopy(JsonConfig.ACTION_DICT)
			final['msg']['orig'] = 'MIG'
			final['msg']['repl'] = []

			if not replacing:
				replacing = next(replacings)
			while replacing['ReplacedAftermarketPartId'] == row['ReplacedAftermarketPartId']:
				final['msg']['repl'].append({
					'apn': {
						'pf': replacing['APNPrefixCode'],
						'bs': replacing['APNBaseCode'],
						'sf': replacing['APNSuffixCode']
					},
					'grp': replacing['GroupNumber'],
					'seq': replacing['Sequence'],
					'qty': replacing['Quantity'],
					'desc_cd': replacing['DescriptionCode']
				})
				try:
					replacing = next(replacings)
				except StopIteration:
					break


			combined.append(final)


		send_to_kafka(topic=topic, key_path=kafka_key, events=combined)
		combined = []

@entity_wrapper
def send_engineering_part(topic, kafka_key):

	rowcount = get_scalar("SELECT COUNT(*) FROM export.tblPRT_EngineeringPart") + 1

	epn_query = """
	SELECT
		[EPNPrefixCode] = EP.PrefixCode
		,[EPNBaseCode] = EP.BaseCode
		,[EPNSuffixCode] = EP.SuffixCode
		,[EPNPartServiceStatusCode] = PS.PartServiceStatusCode
		,[EPNPartServiceStatusDescription] = PS.Description
		,[WersComment] = RD.Description
		,[ColourInd] = ISNULL(EP.ColourFlag,'')
		,[IsGenerated] = CONVERT(bit,0)
		,EP.AddDate
	FROM export.tblPRT_EngineeringPart EP
	LEFT JOIN config.tblPartServiceStatus PS
		ON PS.PartServiceStatusId = EP.JAGServiceStatusId
	LEFT JOIN export.tblREF_ReferenceDescription RD
		ON RD.ReferenceDescriptionId = EP.ReferenceDescriptionId
	
	UNION ALL

	SELECT
		[EPNPrefixCode] = NULL
		,[EPNBaseCode] = 'TEST'
		,[EPNSuffixCode] = NULL
		,[EPNPartServiceStatusCode] = 'NS'
		,[EPNPartServiceStatusDescription] = 'Not serviced'
		,[WersComment] = 'TEST EPN'
		,[ColourInd] = ''
		,[IsGenerated] = CONVERT(bit,1)		
		,[AddDate] = '2010-01-01 00:00:00.000'
	"""

	rows = get_data(epn_query)
	runs = []
	chunk = GeneralConfig.CHUNK


	if rowcount <= chunk:
		runs.append(rowcount)
	else:
		while rowcount != 0:
			if rowcount <= chunk:
				runs.append(rowcount)
				break
			else:
				runs.append(chunk)
				rowcount-=chunk
			


	combined = []
	for run in runs:
		for _ in range(run):
			row = next(rows)
			final = copy.deepcopy(JsonConfig.WRAPPER_DICT)
			final['msg']['epn'] = {
				'pf': row['EPNPrefixCode'],
				'bs': row['EPNBaseCode'],
				'sf': row['EPNSuffixCode']
			}
			final['msg']['srv_stat'] = {
				'cd': row['EPNPartServiceStatusCode'],
				'desc': row['EPNPartServiceStatusDescription']
			} if row['EPNPartServiceStatusCode'] is not None else None
			final['msg']['wers_cmnt'] = row['WersComment']
			final['msg']['clrd_ind'] = row['ColourInd']
			final['msg']['rat_epn'] = None
			final['msg']['brd_appl'] = 'JAG'
			final['msg']['is_gen'] = row['IsGenerated']
			final['msg']['upd_dt'] = '{:%Y-%m-%dT%H:%M:%S.%f}'.format(row['AddDate'])
			final['msg']['actn'] = copy.deepcopy(JsonConfig.ACTION_DICT)
			final['msg']['orig'] = 'MIG'

			combined.append(final)

		send_to_kafka(topic=topic, key_path=kafka_key, events=combined)
		combined = []

@entity_wrapper
def send_engineering_part_function(topic, kafka_key):

	rowcount = get_scalar("SELECT COUNT(*) FROM export.tblAPL_EngineeringPartFunction")

	epn_func_query = """
	SELECT
		[EPNPrefixCode] = EP.PrefixCode
		,[EPNBaseCode] = EP.BaseCode
		,[EPNSuffixCode] = EP.SuffixCode
		,F.FunctionCode
		,[ReplacingEPNPrefixCode] = EP1.PrefixCode
		,[ReplacingEPNBaseCode] = EP1.BaseCode
		,[ReplacingEPNSuffixCode] = EP1.SuffixCode
		,[ReplacingEPNCode] = EP1.EPNCode
		,[SSD] = EPF.ServiceDispositionCode
		,[SI] = EPF.SICode
		,[CFS] = EPF.ContinueForServiceCode
		,[CFIndicator] = EPF.ComponentFinalIndicator
		,EPF.AddDate
	FROM export.tblAPL_EngineeringPartFunction EPF
	INNER JOIN export.tblPRT_EngineeringPart EP
		ON EP.EngineeringPartId = EPF.EngineeringPartId
	INNER JOIN export.tblAPL_Function F
		ON F.FunctionId = EPF.FunctionId
	LEFT JOIN (
				SELECT ReplacedEngineeringPartId, ReplacedFunctionId, [ReplacingEngineeringPartId] = MAX(ReplacingEngineeringPartId)
				FROM export.tblPRT_EngineeringSupersession
				GROUP BY ReplacedEngineeringPartId, ReplacedFunctionId
			) A
		INNER JOIN export.tblPRT_EngineeringPart EP1
			ON EP1.EngineeringPartId = A.ReplacingEngineeringPartId
		ON A.ReplacedEngineeringPartId = EPF.EngineeringPartId
		AND A.ReplacedFunctionId = EPF.FunctionId
	"""

	rows = get_data(epn_func_query)
	runs = []
	chunk = GeneralConfig.CHUNK


	if rowcount <= chunk:
		runs.append(rowcount)
	else:
		while rowcount != 0:
			if rowcount <= chunk:
				runs.append(rowcount)
				break
			else:
				runs.append(chunk)
				rowcount-=chunk
			


	combined = []
	for run in runs:
		for _ in range(run):
			row = next(rows)
			final = copy.deepcopy(JsonConfig.WRAPPER_DICT)
			final['msg']['epn'] = {
				'pf': row['EPNPrefixCode'],
				'bs': row['EPNBaseCode'],
				'sf': row['EPNSuffixCode']
			}
			final['msg']['func'] = row['FunctionCode']
			final['msg']['repl_epn'] = {
				'pf': row['ReplacingEPNPrefixCode'],
				'bs': row['ReplacingEPNBaseCode'],
				'sf': row['ReplacingEPNSuffixCode']
			} if row['ReplacingEPNCode'] is not None else None
			final['msg']['SSD'] = {
				'cd': row['SSD'],
				'desc': row['SSD']
			} if row['SSD'] is not None else None
			final['msg']['SI'] = {
				'cd': row['SI'],
				'desc': row['SI']
			} if row['SI'] is not None else None
			final['msg']['CFS'] = row['CFS']
			final['msg']['cmpnt_fnl'] = row['CFIndicator']
			final['msg']['upd_dt'] = '{:%Y-%m-%dT%H:%M:%S.%f}'.format(row['AddDate'])
			final['msg']['actn'] = copy.deepcopy(JsonConfig.ACTION_DICT)
			final['msg']['orig'] = 'MIG'

			combined.append(final)

		send_to_kafka(topic=topic, key_path=kafka_key, events=combined)
		combined = []

@entity_wrapper
def send_engineering_part_usage(topic, kafka_key):

	rowcount = get_scalar("SELECT COUNT(*) FROM export.tblPRT_EngineeringPartUsage")

	epn_usage_query = """
	SELECT
		 EPU.EngineeringPartUsageId
		,[EPNPrefixCode] = EP.PrefixCode
		,[EPNBaseCode] = EP.BaseCode
		,[EPNSuffixCode] = EP.SuffixCode
		,M.ProductType
		,M.VehicleLine
		,C1.RangeCode
		,[RangeDescription] = C1.Description
		,F.FunctionCode
		,C.CPSCCode
		,[CPSCDescription] = RD.Description
		,[EffInPoint] = B1.BreakPointCode
		,[EffInDate] = EPU.EffectiveInDate
		,[EffInSource] = EPU.EffectiveInDateSource
		,[EffInNotice] = N1.NoticeCode
		,[EffOutPoint] = B2.BreakPointCode
		,[EffOutDate] = EPU.EffectiveOutDate
		,[EffOutSource] = EPU.EffectiveOutDateSource
		,[EffOutNotice] = N2.NoticeCode
		,EPU.Quantity
		,[ActiveFlag] = CASE WHEN EPU.ActiveFlag = 1 THEN 'A' WHEN EPU.ActiveFlag = 0 THEN 'I' ELSE NULL END
		,EPU.AddDate
		,EPU.FeatureSetId
		,[IsDerivedFromParent] = CASE WHEN EPU.UsageType = 'C' THEN 'T' WHEN EPU.UsageType = 'F' THEN 'F' ELSE NULL END
	FROM export.tblPRT_EngineeringPartUsage EPU
	INNER JOIN export.tblPRT_EngineeringPart EP
		ON EP.EngineeringPartId = EPU.EngineeringPartId
	INNER JOIN export.tblREF_Model M
		LEFT JOIN (
					SELECT
						ModelId
						,RangeCode
						,Description
					FROM (
							SELECT
								CA.ModelId
								,CA.RangeCode
								,T.Description
								,[rnb] = ROW_NUMBER() OVER (PARTITION BY CA.ModelId ORDER BY CA.RangeCode ASC)
							FROM export.tblCAT_Catalogue CA
							INNER JOIN export.tblLEX_Term T
								ON T.TermId = CA.TermId
							) A
					WHERE A.rnb = 1
				) C1
			ON C1.ModelId = M.ModelId
		ON M.ModelId = EPU.ModelId
	INNER JOIN export.tblAPL_Function F
		INNER JOIN export.tblAPL_CPSC C
			LEFT JOIN export.tblREF_ReferenceDescription RD
				ON RD.ReferenceDescriptionId = C.ReferenceDescriptionId
			ON C.CPSCId = F.CPSCId
		ON F.FunctionId = EPU.FunctionId
	LEFT JOIN export.tblAPL_BreakPoint B1
		ON B1.BreakPointId = EPU.EffectiveInPointId
	LEFT JOIN export.tblPRT_Notice N1
		ON N1.NoticeId = EPU.EffectiveInNoticeId
	LEFT JOIN export.tblAPL_BreakPoint B2
		ON B2.BreakPointId = EPU.EffectiveOutPointId
	LEFT JOIN export.tblPRT_Notice N2
		ON N2.NoticeId = EPU.EffectiveOutNoticeId
	ORDER BY EPU.FeatureSetId
	"""

	fs_query = """
	SELECT
		FS.FeatureSetId
		,FS.FeatureSet
	FROM export.tblAPL_FeatureSet FS
	WHERE EXISTS (
					SELECT 1
					FROM export.tblPRT_EngineeringPartUsage EPU
					WHERE EPU.FeatureSetId = FS.FeatureSetId
				)
	ORDER BY FS.FeatureSetId
	"""

	rows = get_data(epn_usage_query)
	featuresets = get_data(fs_query)
	featureset = {}
	runs = []
	chunk = GeneralConfig.CHUNK


	if rowcount <= chunk:
		runs.append(rowcount)
	else:
		while rowcount != 0:
			if rowcount <= chunk:
				runs.append(rowcount)
				break
			else:
				runs.append(chunk)
				rowcount-=chunk
			

	combined = []
	for run in runs:
		for _ in range(run):
			row = next(rows)
			final = copy.deepcopy(JsonConfig.WRAPPER_DICT)
			final['msg']['id'] = row['EngineeringPartUsageId']
			final['msg']['epn'] = {
				'pf': row['EPNPrefixCode'],
				'bs': row['EPNBaseCode'],
				'sf': row['EPNSuffixCode']
			}
			final['msg']['epn_orig'] = 'WERS'
			final['msg']['mdl'] = {
				'prd_tp': row['ProductType'],
				'vhl_ln': row['VehicleLine'],
				'rng': row['RangeCode'],
				'rng_desc': row['RangeDescription']
			}
			final['msg']['func'] = row['FunctionCode']
			final['msg']['cpsc'] = {
				'cd': row['CPSCCode'],
				'desc': row['CPSCDescription']
			}
			final['msg']['eff_in_pt'] = row['EffInPoint']
			final['msg']['eff_in_dt'] = '{:%Y-%m-%d}'.format(row['EffInDate']) if row['EffInDate'] is not None else None
			final['msg']['eff_in_dt_src'] = row['EffInSource']
			final['msg']['eff_in_ntc'] = row['EffInNotice']
			final['msg']['eff_out_pt'] = row['EffOutPoint']
			final['msg']['eff_out_dt'] = '{:%Y-%m-%d}'.format(row['EffOutDate']) if row['EffOutDate'] is not None else None
			final['msg']['eff_out_dt_src'] = row['EffOutSource']
			final['msg']['eff_out_ntc'] = row['EffOutNotice']
			final['msg']['qty'] = '{:.2f}'.format(row['Quantity'])
			final['msg']['usg_act_ind'] = row['ActiveFlag']
			final['msg']['usg_df_prnt_ind'] = row['IsDerivedFromParent']
			final['msg']['upd_dt'] = '{:%Y-%m-%dT%H:%M:%S.%f}'.format(row['AddDate'])
			final['msg']['actn'] = copy.deepcopy(JsonConfig.ACTION_DICT)
			final['msg']['orig'] = 'MIG'

			if row['FeatureSetId'] is None:
				final['msg']['feat'] = []
			else:
				if not featureset:
					featureset = next(featuresets)
				if row['FeatureSetId'] == featureset['FeatureSetId']:
					final['msg']['feat'] = featureset['FeatureSet'].split(',')
				else:
					featureset = next(featuresets)
					final['msg']['feat'] = featureset['FeatureSet'].split(',')


			combined.append(final)

		send_to_kafka(topic=topic, key_path=kafka_key, events=combined)
		combined = []

@entity_wrapper
def send_hierarchy(topic, kafka_key):

	rowcount = get_scalar("SELECT COUNT(*) + 1 FROM export.tblCAT_Hierarchy WHERE AssemblyLevel BETWEEN 1 AND 4")

	hierarchy_query = """
	WITH CTE AS (

	SELECT
		[HierarchyPath] = ISNULL(CONVERT(varchar(100),H1.ParentHierarchyId),0)
		,H1.HierarchyId
		,H1.HierarchyCode
		,H1.HierarchyTypeId
		,H1.TermId
		,H1.SuppressionId
		,H1.BrandId
		,H1.AddDate
	FROM export.tblCAT_Hierarchy H1
	WHERE H1.AssemblyLevel = 1

	UNION ALL

	SELECT
		[HierarchyPath] = CONVERT(varchar(100),H2.ParentHierarchyId)
		,H2.HierarchyId
		,H2.HierarchyCode
		,H2.HierarchyTypeId
		,H2.TermId
		,H2.SuppressionId
		,H2.BrandId
		,H2.AddDate
	FROM export.tblCAT_Hierarchy H2
	WHERE H2.AssemblyLevel = 2

	UNION ALL

	SELECT
		[HierarchyPath] = CONCAT(H2.ParentHierarchyId,'.',H3.ParentHierarchyId)
		,H3.HierarchyId
		,H3.HierarchyCode
		,H3.HierarchyTypeId
		,H3.TermId
		,H3.SuppressionId
		,H3.BrandId
		,H3.AddDate
	FROM export.tblCAT_Hierarchy H3
	INNER JOIN export.tblCAT_Hierarchy H2
		ON H2.HierarchyId = H3.ParentHierarchyId
	WHERE H3.AssemblyLevel = 3

	UNION ALL

	SELECT
		[HierarchyPath] = CONCAT(H2.ParentHierarchyId,'.',H3.ParentHierarchyId,'.',H4.ParentHierarchyId)
		,H4.HierarchyId
		,H4.HierarchyCode
		,H4.HierarchyTypeId
		,H4.TermId
		,H4.SuppressionId
		,H4.BrandId
		,H4.AddDate
	FROM export.tblCAT_Hierarchy H4 --13873
	INNER JOIN export.tblCAT_Hierarchy H3
		INNER JOIN export.tblCAT_Hierarchy H2
			ON H2.HierarchyId = H3.ParentHierarchyId
		ON H3.HierarchyId = H4.ParentHierarchyId
	WHERE H4.AssemblyLevel = 4
	)
	SELECT
		[HierarchyPath] = C.HierarchyPath
		,C.HierarchyId
		,C.HierarchyCode
		,[HierarchyTypeId] = CONVERT(varchar(10),HT.HierarchyTypeId)
		,[HierarchyTypeDescription] = HT.Description
		,[DescriptionCode] = CONVERT(varchar(10),C.TermId)
		,[StatusCode] = CONVERT(varchar(10),ISNULL(S.SuppressionId,0))
		,[StatusDescription] = ISNULL(S.Annotation,'Published')
		,[IsPublished] = CASE WHEN C.SuppressionId IS NULL THEN CONVERT(bit,1) ELSE CONVERT(bit,0) END
		,B.BrandCode
		,C.AddDate
	FROM CTE C
	INNER JOIN config.tblHierarchyType HT
		ON HT.HierarchyTypeId = C.HierarchyTypeId
	INNER JOIN config.tblBrand B
		ON B.BrandId = C.BrandId
	LEFT JOIN export.tblCAT_Suppression S
		ON S.SuppressionId = C.SuppressionId

	UNION ALL

	SELECT
		 [HierarchyPath] = NULL
		,[HierarchyId] = 0
		,[HierarchyCode] = 'JG0'
		,[HierarchyTypeId] = '1'
		,[HierarchyTypeDescription] = 'Brand'
		,[DescriptionCode] = (SELECT TOP 1 CONVERT(varchar(100),TermId) FROM export.tblLEX_Term)
		,[StatusCode] = '0'
		,[StatusDescription] = 'Published'
		,[IsPublished] = CONVERT(bit,1)
		,[BrandCode] = 'JAG'
		,[AddDate] = GETDATE() - 1000
	
		"""


	rows = get_data(hierarchy_query)
	runs = []
	chunk = GeneralConfig.CHUNK


	if rowcount <= chunk:
		runs.append(rowcount)
	else:
		while rowcount != 0:
			if rowcount <= chunk:
				runs.append(rowcount)
				break
			else:
				runs.append(chunk)
				rowcount-=chunk
			


	combined = []
	for run in runs:
		for _ in range(run):
			row = next(rows)
			final = copy.deepcopy(JsonConfig.WRAPPER_DICT)
			final['msg']['hier'] = {
				'path': row['HierarchyPath'],
				'id': row['HierarchyId']
			}
			final['msg']['cd'] = row['HierarchyCode']
			final['msg']['tp'] = {
				'cd': row['HierarchyTypeId'],
				'desc': row['HierarchyTypeDescription']
			}
			final['msg']['desc_cd'] = row['DescriptionCode']
			final['msg']['stat'] = {
				'cd': row['StatusCode'],
				'desc': row['StatusDescription']
			}
			final['msg']['publ'] = row['IsPublished']
			final['msg']['brd'] = row['BrandCode']
			final['msg']['upd_dt'] = '{:%Y-%m-%dT%H:%M:%S.%f}'.format(row['AddDate'])
			final['msg']['actn'] = copy.deepcopy(JsonConfig.ACTION_DICT)
			final['msg']['orig'] = 'MIG'
			final['msg']['cat_tp'] = '1'

			combined.append(final)

		send_to_kafka(topic=topic, key_path=kafka_key, events=combined)
		combined = []


@entity_wrapper
def send_hierarchy_usage(topic, kafka_key):

	rowcount = get_scalar("""
	WITH CTE AS (
	SELECT [HierarchyId] = COALESCE(HU.HierarchyId,HC.HierarchyId)
	FROM export.tblCAT_HierarchyUsage HU
	FULL JOIN (SELECT DISTINCT HierarchyId FROM export.tblCAT_HierarchyComment) HC
		ON HC.HierarchyId = HU.HierarchyId
	), CTE2 AS (
	SELECT C.*
	FROM CTE C
	INNER JOIN export.tblCAT_Hierarchy H
		ON H.HierarchyId = C.HierarchyId
		AND H.AssemblyLevel = 4
	)
	SELECT COUNT(*)
	FROM CTE2 C
	LEFT JOIN export.tblCAT_Hierarchy H
		INNER JOIN (SELECT [AssemblyLevel] = 4) A
			ON A.AssemblyLevel = H.AssemblyLevel
		ON H.HierarchyId = C.HierarchyId
	""")

	hierarchy_usage_query = """
	WITH CTE AS (
	SELECT
		[HierarchyId] = COALESCE(HU.HierarchyId,HC.HierarchyId)
		,[EffInPoint] = CP1.ChangePointCode
		,[EffOutPoint] = CP2.ChangePointCode
	FROM export.tblCAT_HierarchyUsage HU
		LEFT JOIN export.tblAPL_ChangePoint CP1
			ON CP1.ChangePointId = HU.FromChangePointId
		LEFT JOIN export.tblAPL_ChangePoint CP2
			ON CP2.ChangePointId = HU.ToChangePointId
	FULL JOIN (SELECT DISTINCT HierarchyId FROM export.tblCAT_HierarchyComment) HC
		ON HC.HierarchyId = HU.HierarchyId
	), CTE2 AS (
	SELECT C.*
	FROM CTE C
	INNER JOIN export.tblCAT_Hierarchy H
		ON H.HierarchyId = C.HierarchyId
		AND H.AssemblyLevel = 4
	)
	SELECT
		H.HierarchyId
		,C.[EffInPoint]
		,C.[EffOutPoint]
		,[StatusCode] = CONVERT(varchar(10),ISNULL(S.SuppressionId,0))
		,[StatusDescription] = ISNULL(S.Annotation,'Published')
		,[IsPublished] = CASE WHEN H.SuppressionId IS NULL THEN CONVERT(bit,1) ELSE CONVERT(bit,0) END
		,[AddDate] = GETUTCDATE()
		,[id] = ROW_NUMBER() OVER (ORDER BY H.HierarchyId)
	FROM CTE2 C
	LEFT JOIN export.tblCAT_Hierarchy H
		INNER JOIN (SELECT [AssemblyLevel] = 4) A
			ON A.AssemblyLevel = H.AssemblyLevel
		LEFT JOIN export.tblCAT_Suppression S
			ON S.SuppressionId = H.SuppressionId
		ON H.HierarchyId = C.HierarchyId
	ORDER BY H.HierarchyId
	"""

	hier_comment_query = """
	SELECT
		HC.HierarchyId
		,[DescriptionCode] = CONVERT(varchar(10),T.TermId)
	FROM export.tblCAT_HierarchyComment HC
	INNER JOIN export.tblAPL_Comment C
		INNER JOIN export.tblLEX_Term T
			ON T.TermId = C.TermId
		ON C.CommentId = HC.CommentId
	INNER JOIN export.tblCAT_Hierarchy H
		ON H.HierarchyId = HC.HierarchyId
		AND H.AssemblyLevel = 4
	ORDER BY HC.HierarchyId, T.TermId
	"""

	fs_query = """
	SELECT
		HU.HierarchyId
		,[FeatureSet] = FS.JaguarAVSFeatureSet
	FROM export.tblCAT_HierarchyUsage HU
	INNER JOIN export.tblAPL_FeatureSet FS
		ON FS.FeatureSetId = HU.FeatureSetId
	INNER JOIN export.tblCAT_Hierarchy H
		ON H.HierarchyId = HU.HierarchyId
		AND H.AssemblyLevel = 4
	ORDER BY HU.HierarchyId
	"""

	rows = get_data(hierarchy_usage_query)
	hier_comments = get_data(hier_comment_query)
	hier_comments_list = []
	featuresets = get_data(fs_query)
	featureset = {}
	runs = []
	chunk = GeneralConfig.CHUNK


	if rowcount <= chunk:
		runs.append(rowcount)
	else:
		while rowcount != 0:
			if rowcount <= chunk:
				runs.append(rowcount)
				break
			else:
				runs.append(chunk)
				rowcount-=chunk
			

	combined = []
	for run in runs:
		for _ in range(run):
			row = next(rows)
			final = copy.deepcopy(JsonConfig.WRAPPER_DICT)
			final['msg']['id'] = row['id']
			final['msg']['sec_hier_id'] = row['HierarchyId']
			final['msg']['eff_in_pt'] = row['EffInPoint']
			final['msg']['eff_in_dt'] = None
			final['msg']['eff_out_pt'] = row['EffOutPoint']
			final['msg']['eff_out_dt'] = None
			final['msg']['cmnt_desc_cd'] = []
			final['msg']['stat'] = {
				'cd': row['StatusCode'],
				'desc': row['StatusDescription']
			}
			final['msg']['publ'] = row['IsPublished']
			final['msg']['upd_dt'] = '{:%Y-%m-%dT%H:%M:%S.%f}'.format(row['AddDate'])
			final['msg']['actn'] = copy.deepcopy(JsonConfig.ACTION_DICT)
			final['msg']['orig'] = 'MIG'


			for comment in hier_comments:
				if comment['HierarchyId'] > row['HierarchyId']:
					hier_comments_list.append(comment)
					break
				elif comment['HierarchyId'] == row['HierarchyId']:
					final['msg']['cmnt_desc_cd'].append(comment['DescriptionCode'])

			for id1, comment in enumerate(hier_comments_list):
				if comment['HierarchyId'] > row['HierarchyId']:
					break
				elif comment['HierarchyId'] == row['HierarchyId']:
					final['msg']['cmnt_desc_cd'].append(comment['DescriptionCode'])

			del hier_comments_list[:id1]


			if not featureset:
				try:
					featureset = next(featuresets)
					if row['HierarchyId'] == featureset['HierarchyId']:
						final['msg']['feat'] = featureset['FeatureSet'].split(',')
						featureset = {}
					else:
						final['msg']['feat'] = []
				except StopIteration:
					final['msg']['feat'] = []
					pass
			else:
				if row['HierarchyId'] == featureset['HierarchyId']:
					final['msg']['feat'] = featureset['FeatureSet'].split(',')
					featureset = {}
				else:
					final['msg']['feat'] = []		


			combined.append(final)

		send_to_kafka(topic=topic, key_path=kafka_key, events=combined)
		combined = []


@entity_wrapper
def send_section_callout(topic, kafka_key):

	rowcount = get_scalar("SELECT COUNT(*) FROM export.tblCAT_Hierarchy WHERE AssemblyLevel = 5")

	callout_query = """
	SELECT
		HierarchyId
		,HierarchyCode
		,[TermId] = CONVERT(varchar(10),TermId)
		,AddDate
	FROM export.tblCAT_Hierarchy
	WHERE AssemblyLevel = 5
	"""

	rows = get_data(callout_query)
	runs = []
	chunk = GeneralConfig.CHUNK


	if rowcount <= chunk:
		runs.append(rowcount)
	else:
		while rowcount != 0:
			if rowcount <= chunk:
				runs.append(rowcount)
				break
			else:
				runs.append(chunk)
				rowcount-=chunk
			



	combined = []
	for run in runs:
		for _ in range(run):
			row = next(rows)
			final = copy.deepcopy(JsonConfig.WRAPPER_DICT)
			final['msg']['id'] = row['HierarchyId']
			final['msg']['cd'] = row['HierarchyCode']
			final['msg']['desc_cd'] = row['TermId']
			final['msg']['upd_dt'] = '{:%Y-%m-%dT%H:%M:%S.%f}'.format(row['AddDate'])
			final['msg']['actn'] = copy.deepcopy(JsonConfig.ACTION_DICT)
			final['msg']['orig'] = 'MIG'

			combined.append(final)

		send_to_kafka(topic=topic, key_path=kafka_key, events=combined)
		combined = []


@entity_wrapper
def send_section_part_usage(topic, kafka_key):

	rowcount = get_scalar("""
	WITH CTE AS (
	SELECT [HierarchyId] = COALESCE(HU.HierarchyId,HC.HierarchyId)
	FROM export.tblCAT_HierarchyUsage HU
	FULL JOIN (SELECT DISTINCT HierarchyId FROM export.tblCAT_HierarchyComment) HC
		ON HC.HierarchyId = HU.HierarchyId
	), CTE2 AS (
	SELECT C.*
	FROM CTE C
	INNER JOIN export.tblCAT_Hierarchy H
		ON H.HierarchyId = C.HierarchyId
		AND H.AssemblyLevel = 6
		AND H.AftermarketPartId IS NOT NULL
	)
	SELECT COUNT(*)
	FROM CTE2 C
	FULL JOIN export.tblCAT_Hierarchy H
		INNER JOIN export.tblCAT_Hierarchy HC
			INNER JOIN export.tblCAT_Hierarchy HS
				ON HS.HierarchyId = HC.ParentHierarchyId
			ON HC.HierarchyId = H.ParentHierarchyId
		INNER JOIN export.tblPRT_AftermarketPart AP
			ON AP.AftermarketPartId = H.AftermarketPartId
		LEFT JOIN export.tblCAT_Suppression S
			ON S.SuppressionId = H.SuppressionId
		ON H.HierarchyId = C.HierarchyId
	""")

	section_part_usage_query = """
	WITH CTE AS (
	SELECT
		[HierarchyId] = COALESCE(HU.HierarchyId,HC.HierarchyId)
		,[EffInPoint] = CP1.ChangePointCode
		,[EffOutPoint] = CP2.ChangePointCode
	FROM export.tblCAT_HierarchyUsage HU
		LEFT JOIN export.tblAPL_ChangePoint CP1
			ON CP1.ChangePointId = HU.FromChangePointId
		LEFT JOIN export.tblAPL_ChangePoint CP2
			ON CP2.ChangePointId = HU.ToChangePointId
	FULL JOIN (SELECT DISTINCT HierarchyId FROM export.tblCAT_HierarchyComment) HC
		ON HC.HierarchyId = HU.HierarchyId
	), CTE2 AS (
	SELECT C.*
	FROM CTE C
	INNER JOIN export.tblCAT_Hierarchy H
		ON H.HierarchyId = C.HierarchyId
		AND H.AssemblyLevel = 6
		AND H.AftermarketPartId IS NOT NULL
	)
	SELECT
		H.HierarchyId
		,[CalloutHierarchyId] = HC.HierarchyId
		,[SectionHierarchyId] = HS.HierarchyId
		,[APNPrefixCode] = AP.PrefixCode
		,[APNBaseCode] = AP.BaseCode
		,[APNSuffixCode] = AP.SuffixCode
		,C.[EffInPoint]
		,C.[EffOutPoint]
		,[Quantity] = ISNULL(H.Quantity,-1)
		,[StatusCode] = CONVERT(varchar(10),ISNULL(S.SuppressionId,0))
		,[StatusDescription] = ISNULL(S.Annotation,'Published')
		,[IsPublished] = CASE WHEN H.SuppressionId IS NULL THEN CONVERT(bit,1) ELSE CONVERT(bit,0) END
		,[AddDate] = GETUTCDATE()
		,[id] = ROW_NUMBER() OVER (ORDER BY H.HierarchyId)
	FROM CTE2 C
	FULL JOIN export.tblCAT_Hierarchy H
		INNER JOIN export.tblCAT_Hierarchy HC
			INNER JOIN export.tblCAT_Hierarchy HS
				ON HS.HierarchyId = HC.ParentHierarchyId
			ON HC.HierarchyId = H.ParentHierarchyId
		INNER JOIN export.tblPRT_AftermarketPart AP
			ON AP.AftermarketPartId = H.AftermarketPartId
		LEFT JOIN export.tblCAT_Suppression S
			ON S.SuppressionId = H.SuppressionId
		ON H.HierarchyId = C.HierarchyId
	ORDER BY H.HierarchyId
	"""

	section_part_comment_query = """
	SELECT
		HC.HierarchyId
		,[DescriptionCode] = CONVERT(varchar(10),T.TermId)
	FROM export.tblCAT_HierarchyComment HC
	INNER JOIN export.tblAPL_Comment C
		INNER JOIN export.tblLEX_Term T
			ON T.TermId = C.TermId
		ON C.CommentId = HC.CommentId
	INNER JOIN export.tblCAT_Hierarchy H
		ON H.HierarchyId = HC.HierarchyId
		AND H.AssemblyLevel = 6
	ORDER BY HC.HierarchyId, T.TermId
	"""

	fs_query = """
	SELECT
		HU.HierarchyId
		,[FeatureSet] = FS.JaguarAVSFeatureSet
	FROM export.tblCAT_HierarchyUsage HU
	INNER JOIN export.tblAPL_FeatureSet FS
		ON FS.FeatureSetId = HU.FeatureSetId
	INNER JOIN export.tblCAT_Hierarchy H
		ON H.HierarchyId = HU.HierarchyId
		AND H.AssemblyLevel = 6
	ORDER BY HU.HierarchyId
	"""

	rows = get_data(section_part_usage_query)
	section_part_hier_comments = get_data(section_part_comment_query)
	section_part_hier_comments_list = []
	featuresets = get_data(fs_query)
	featureset = {}
	runs = []
	chunk = GeneralConfig.CHUNK


	if rowcount <= chunk:
		runs.append(rowcount)
	else:
		while rowcount != 0:
			if rowcount <= chunk:
				runs.append(rowcount)
				break
			else:
				runs.append(chunk)
				rowcount-=chunk
			

	combined = []
	for run in runs:
		for _ in range(run):
			row = next(rows)
			final = copy.deepcopy(JsonConfig.WRAPPER_DICT)
			final['msg']['id'] = row['id']
			final['msg']['clt_id'] = row['CalloutHierarchyId']
			final['msg']['sec_hier_id'] = row['SectionHierarchyId']
			final['msg']['apn'] = {
				'pf': row['APNPrefixCode'],
				'bs': row['APNBaseCode'],
				'sf': row['APNSuffixCode']
			}
			final['msg']['eff_in_pt'] = row['EffInPoint']
			final['msg']['eff_in_dt'] = None
			final['msg']['eff_out_pt'] = row['EffOutPoint']
			final['msg']['eff_out_dt'] = None
			final['msg']['cmnt_desc_cd'] = []
			final['msg']['qty'] = row['Quantity']
			final['msg']['stat'] = {
				'cd': row['StatusCode'],
				'desc': row['StatusDescription']
			}
			final['msg']['publ'] = row['IsPublished']
			final['msg']['upd_dt'] = '{:%Y-%m-%dT%H:%M:%S.%f}'.format(row['AddDate'])
			final['msg']['actn'] = copy.deepcopy(JsonConfig.ACTION_DICT)
			final['msg']['orig'] = 'MIG'

			for comment in section_part_hier_comments:
				if comment['HierarchyId'] > row['HierarchyId']:
					section_part_hier_comments_list.append(comment)
					break
				elif comment['HierarchyId'] == row['HierarchyId']:
					final['msg']['cmnt_desc_cd'].append(comment['DescriptionCode'])

			for id1, comment in enumerate(section_part_hier_comments_list):
				if comment['HierarchyId'] > row['HierarchyId']:
					break
				elif comment['HierarchyId'] == row['HierarchyId']:
					final['msg']['cmnt_desc_cd'].append(comment['DescriptionCode'])

			del section_part_hier_comments_list[:id1]


			if not featureset:
				try:
					featureset = next(featuresets)
					if row['HierarchyId'] == featureset['HierarchyId']:
						final['msg']['feat'] = featureset['FeatureSet'].split(',')
						featureset = {}
					else:
						final['msg']['feat'] = []
				except StopIteration:
					final['msg']['feat'] = []
					pass
			else:
				if row['HierarchyId'] == featureset['HierarchyId']:
					final['msg']['feat'] = featureset['FeatureSet'].split(',')
					featureset = {}
				else:
					final['msg']['feat'] = []


			combined.append(final)

		send_to_kafka(topic=topic, key_path=kafka_key, events=combined)
		combined = []


@entity_wrapper
def send_hierarchy_illustration(topic, kafka_key):

	rowcount = get_scalar("""SELECT COUNT(*) FROM export.tblCAT_HierarchyIllustration
	WHERE IsActive = 1 AND IllustrationCode IS NOT NULL""")

	hierarchy_ill_query = """
	WITH CTE AS (

	SELECT
		[HierarchyPath] = CONVERT(varchar(100),H1.ParentHierarchyId)
		,H1.HierarchyId
	FROM export.tblCAT_Hierarchy H1
	WHERE H1.AssemblyLevel = 1

	UNION ALL

	SELECT
		[HierarchyPath] = CONVERT(varchar(100),H2.ParentHierarchyId)
		,H2.HierarchyId
	FROM export.tblCAT_Hierarchy H2
	WHERE H2.AssemblyLevel = 2

	UNION ALL

	SELECT
		[HierarchyPath] = CONCAT(H2.ParentHierarchyId,'.',H3.ParentHierarchyId)
		,H3.HierarchyId
	FROM export.tblCAT_Hierarchy H3
	INNER JOIN export.tblCAT_Hierarchy H2
		ON H2.HierarchyId = H3.ParentHierarchyId
	WHERE H3.AssemblyLevel = 3

	UNION ALL

	SELECT
		[HierarchyPath] = CONCAT(H2.ParentHierarchyId,'.',H3.ParentHierarchyId,'.',H3.ParentHierarchyId)
		,H4.HierarchyId
	FROM export.tblCAT_Hierarchy H4 --13873
	INNER JOIN export.tblCAT_Hierarchy H3
		INNER JOIN export.tblCAT_Hierarchy H2
			ON H2.HierarchyId = H3.ParentHierarchyId
		ON H3.HierarchyId = H4.ParentHierarchyId
	WHERE H4.AssemblyLevel = 4
	)
	SELECT
		[HierarchyPath] = ISNULL(C.HierarchyPath,'')
		,C.HierarchyId
		,HI.IllustrationCode
		,HI.AddDate
	FROM CTE C
	INNER JOIN export.tblCAT_HierarchyIllustration HI 
		ON HI.HierarchyId = C.HierarchyId
		AND HI.IsActive = 1
		AND HI.IllustrationCode IS NOT NULL
	"""

	rows = get_data(hierarchy_ill_query)
	runs = []
	chunk = GeneralConfig.CHUNK


	if rowcount <= chunk:
		runs.append(rowcount)
	else:
		while rowcount != 0:
			if rowcount <= chunk:
				runs.append(rowcount)
				break
			else:
				runs.append(chunk)
				rowcount-=chunk
			



	combined = []
	for run in runs:
		for _ in range(run):
			row = next(rows)
			final = copy.deepcopy(JsonConfig.WRAPPER_DICT)
			final['msg']['hier'] = {
				'path': row['HierarchyPath'],
				'id': row['HierarchyId']
			}
			final['msg']['flnm'] = row['IllustrationCode']
			final['msg']['upd_dt'] = '{:%Y-%m-%dT%H:%M:%S.%f}'.format(row['AddDate'])
			final['msg']['actn'] = copy.deepcopy(JsonConfig.ACTION_DICT)
			final['msg']['orig'] = 'MIG'

			combined.append(final)


		send_to_kafka(topic=topic, key_path=kafka_key, events=combined)
		combined = []


@entity_wrapper
def send_vin(topic, kafka_key):

	rowcount = get_scalar("SELECT COUNT(*) FROM dbo.Vin", db=VinConfig.DB)

	vin_query = """
	SELECT
		Vin
		,BuildDate = CONVERT(varchar(8), BuildDate,112)
		,Plant
		,Brand
		,Features
		,[AddDate] = GETUTCDATE()
	FROM dbo.Vin
	"""

	rows = get_data(vin_query, db=VinConfig.DB)
	runs = []
	chunk = GeneralConfig.CHUNK


	if rowcount <= chunk:
		runs.append(rowcount)
	else:
		while rowcount != 0:
			if rowcount <= chunk:
				runs.append(rowcount)
				break
			else:
				runs.append(chunk)
				rowcount-=chunk
			

	combined = []
	for run in runs:
		for _ in range(run):
			row = next(rows)
			final = copy.deepcopy(JsonConfig.WRAPPER_DICT)
			final['msg']['vin'] = row['Vin']
			final['msg']['bld_dt'] = row['BuildDate']
			final['msg']['plt'] = row['Plant']
			final['msg']['brd'] = row['Brand']
			final['msg']['feat'] = row['Features'].split(',')
			final['msg']['upd_dt'] = '{:%Y-%m-%dT%H:%M:%S.%f}'.format(row['AddDate'])
			final['msg']['orig'] = 'MIG'

			combined.append(final)


		send_to_kafka(topic=topic, key_path=kafka_key, events=combined)
		combined = []


@entity_wrapper
def send_part_meta(topic, kafka_key):

	rowcount = get_scalar("""
	;WITH CTE AS (
	SELECT DISTINCT
		 AftermarketPartId
	FROM export.tblCAT_Hierarchy
	WHERE AftermarketPartId IS NOT NULL
	)
	SELECT COUNT(*)
	FROM export.tblPRT_AftermarketPart AP
	LEFT JOIN CTE H
		ON H.AftermarketPartId = AP.AftermarketPartId
	LEFT JOIN config.tblPartServiceStatus PS
		ON PS.PartServiceStatusId = AP.JAGServiceStatusId
		AND PS.PartServiceStatusCode IN ('RL', 'RS')
	WHERE PS.PartServiceStatusCode IS NOT NULL
		OR H.AftermarketPartId IS NOT NULL
	""")

	apn_query = """
	;WITH CTE AS (
		SELECT DISTINCT
			AftermarketPartId
		FROM export.tblCAT_Hierarchy
		WHERE AftermarketPartId IS NOT NULL
	)
	SELECT
		 AP.AftermarketPartId
		,[APNPrefixCode] = ISNULL(AP.PrefixCode,'')
		,[APNBaseCode] = AP.BaseCode
		,[APNSuffixCode] = ISNULL(AP.SuffixCode,'')
		,[EPNPrefixCode] = ISNULL(EP.PrefixCode,'')
		,[EPNBaseCode] = ISNULL(EP.BaseCode,'TESTEPN')
		,[EPNSuffixCode] = ISNULL(EP.SuffixCode,'')
		,[DescriptionCode] = CASE WHEN AP.TermId IS NOT NULL THEN CONVERT(varchar(10),AP.TermId) ELSE (SELECT TOP 1 CONVERT(varchar(10),TermId) FROM export.tblPRT_AftermarketPart WHERE TermId IS NOT NULL) END
		,[ReleaseDate] = ISNULL(CONVERT(varchar(10),AP.JAGServiceDecisionDate),'1900-01-01')
		,AP.AddDate
	FROM export.tblPRT_AftermarketPart AP
	LEFT JOIN CTE H
		ON H.AftermarketPartId = AP.AftermarketPartId
	LEFT JOIN config.tblPartServiceStatus PS
		ON PS.PartServiceStatusId = AP.JAGServiceStatusId
		AND PS.PartServiceStatusCode IN ('RL', 'RS')
	LEFT JOIN export.tblPRT_PartLink PL
		INNER JOIN export.tblPRT_EngineeringPart EP
			ON EP.EngineeringPartId = PL.EngineeringPartId
		ON PL.AftermarketPartId = AP.AftermarketPartId
		AND PL.EffectiveDateOut IS NULL
	WHERE PS.PartServiceStatusCode IS NOT NULL
		OR H.AftermarketPartId IS NOT NULL
	ORDER BY AP.AftermarketPartId
	"""


	qualifier_query = """
	SELECT
		 AftermarketPartId
		,[DescriptionCode] = CONVERT(varchar(10), QualifierTermId)
	FROM export.tblPRT_AftermarketPartQualifier
	ORDER BY AftermarketPartId, DescriptionCode
	"""

	# qualifier_query = """
	# ;WITH CTE AS (
	# 	SELECT DISTINCT
	# 		AftermarketPartId
	# 	FROM export.tblCAT_Hierarchy
	# 	WHERE AftermarketPartId IS NOT NULL
	# ), CTE2 AS (
	# 	SELECT
	# 		 AP.AftermarketPartId
	# 		,[QualifierTermId] = 109242
	# 	FROM export.tblPRT_AftermarketPart AP
	# 	LEFT JOIN CTE H
	# 		ON H.AftermarketPartId = AP.AftermarketPartId
	# 	LEFT JOIN config.tblPartServiceStatus PS
	# 		ON PS.PartServiceStatusId = AP.JAGServiceStatusId
	# 		AND PS.PartServiceStatusCode IN ('RL', 'RS')
	# 	WHERE PS.PartServiceStatusCode IS NOT NULL
	# 		OR H.AftermarketPartId IS NOT NULL
	# )
	# SELECT
	# 	 AftermarketPartId
	# 	,[DescriptionCode] = CONVERT(varchar(10), QualifierTermId)
	# FROM export.tblPRT_AftermarketPartQualifier

	# UNION

	# SELECT
	# 	 AftermarketPartId
	# 	,[DescriptionCode] = CONVERT(varchar(10), QualifierTermId)
	# FROM CTE2
	# ORDER BY AftermarketPartId, DescriptionCode
	# """





	rows = get_data(apn_query)
	qualifiers = get_data(qualifier_query)
	qualifiers_list = []
	runs = []
	chunk = GeneralConfig.CHUNK


	if rowcount <= chunk:
		runs.append(rowcount)
	else:
		while rowcount != 0:
			if rowcount <= chunk:
				runs.append(rowcount)
				break
			else:
				runs.append(chunk)
				rowcount-=chunk
			


	combined = []
	for run in runs:
		for _ in range(run):
			row = next(rows)
			final = copy.deepcopy(JsonConfig.WRAPPER_DICT)
			final['msg']['apn'] = {
				'pf': row['APNPrefixCode'],
				'bs': row['APNBaseCode'],
				'sf': row['APNSuffixCode']
			}
			final['msg']['epn'] = {
				'pf': row['EPNPrefixCode'],
				'bs': row['EPNBaseCode'],
				'sf': row['EPNSuffixCode']
			}
			final['msg']['brd'] = 'JAG'
			final['msg']['desc_cd'] = row['DescriptionCode']
			final['msg']['qual_cds'] = []
			final['msg']['rl_dt'] = row['ReleaseDate']
			final['msg']['orig'] = 'MIG'
			final['msg']['upd_dt'] = '{:%Y-%m-%dT%H:%M:%S.%f}'.format(row['AddDate'])
			final['msg']['actn'] = copy.deepcopy(JsonConfig.ACTION_DICT)



			for qual in qualifiers:
				if qual['AftermarketPartId'] > row['AftermarketPartId']:
					qualifiers_list.append(qual)
					break
				elif qual['AftermarketPartId'] == row['AftermarketPartId']:
					final['msg']['qual_cds'].append(qual['DescriptionCode'])
				
			for id1, qual in enumerate(qualifiers_list):
				if qual['AftermarketPartId'] > row['AftermarketPartId']:
					break
				elif qual['AftermarketPartId'] == row['AftermarketPartId']:
					final['msg']['qual_cds'].append(qual['DescriptionCode'])
				
			del qualifiers_list[:id1]


			combined.append(final)

		send_to_kafka(topic=topic, key_path=kafka_key, events=combined)
		combined = []


@entity_wrapper
def send_intray(topic, kafka_key):

	rowcount = get_scalar("SELECT COUNT(*) FROM export.tblCAT_Hierarchy WHERE AssemblyLevel = 6 AND IsInIntray = 1 AND AftermarketPartId IS NOT NULL")

	intray_query = """
	SELECT
		 [SectionHierarchyId] = HS.HierarchyId
		,[APNPrefixCode] = AP.PrefixCode
		,[APNBaseCode] = AP.BaseCode
		,[APNSuffixCode] = AP.SuffixCode
		,[AddDate] = H.AddDate
		,[id] = ROW_NUMBER() OVER (ORDER BY H.HierarchyId)
	FROM export.tblCAT_Hierarchy H
	INNER JOIN export.tblCAT_Hierarchy HC
		INNER JOIN export.tblCAT_Hierarchy HS
			ON HS.HierarchyId = HC.ParentHierarchyId
		ON HC.HierarchyId = H.ParentHierarchyId
	INNER JOIN export.tblPRT_AftermarketPart AP
		ON AP.AftermarketPartId = H.AftermarketPartId
	LEFT JOIN export.tblCAT_Suppression S
		ON S.SuppressionId = H.SuppressionId
	WHERE H.AssemblyLevel = 6
		AND H.IsInIntray = 1
	ORDER BY H.HierarchyId
	"""


	rows = get_data(intray_query)
	runs = []
	chunk = GeneralConfig.CHUNK


	if rowcount <= chunk:
		runs.append(rowcount)
	else:
		while rowcount != 0:
			if rowcount <= chunk:
				runs.append(rowcount)
				break
			else:
				runs.append(chunk)
				rowcount-=chunk
			


	combined = []
	for run in runs:
		for _ in range(run):
			row = next(rows)
			final = copy.deepcopy(JsonConfig.WRAPPER_DICT)
			final['msg']['apn'] = {
				'pf': row['APNPrefixCode'],
				'bs': row['APNBaseCode'],
				'sf': row['APNSuffixCode']
			}
			final['msg']['orig'] = 'MIG'
			final['msg']['sec_hier_id'] = row['SectionHierarchyId']
			final['msg']['id'] = row['id']
			final['msg']['upd_dt'] = '{:%Y-%m-%dT%H:%M:%S.%f}'.format(row['AddDate'])
			final['msg']['actn'] = copy.deepcopy(JsonConfig.ACTION_DICT)


			combined.append(final)

		send_to_kafka(topic=topic, key_path=kafka_key, events=combined)
		combined = []

