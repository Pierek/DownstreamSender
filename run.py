from pathos.multiprocessing import ProcessPool
from pathos.helpers import cpu_count
from downstream import *
import time


def maintest():

	# pool = ProcessPool(nodes=cpu_count() - 1 or 1)

	# pool.amap(send_section_part_usage, [SectionPartUsageConfig.TOPIC], [SectionPartUsageConfig.KAFKA_KEY])	

	# pool.close()
	# pool.join()

	# send_engineering_part_function()
	# send_aftermarket_part(AftermarketPartConfig.TOPIC,AftermarketPartConfig.KAFKA_KEY)
	# send_hierarchy(HierarchyConfig.TOPIC,HierarchyConfig.KAFKA_KEY)
	# send_part_meta(PartMetaConfig.TOPIC,PartMetaConfig.KAFKA_KEY)
	# send_intray(IntrayConfig.TOPIC,IntrayConfig.KAFKA_KEY)	
	# send_hierarchy_usage(HierarchyUsageConfig.TOPIC, HierarchyUsageConfig.KAFKA_KEY)
	# send_section_callout()
	# send_hierarchy_illustration()
	# send_section_part_usage(SectionPartUsageConfig.TOPIC, SectionPartUsageConfig.KAFKA_KEY)
	# send_feature(FeatureConfig.TOPIC, FeatureConfig.KAFKA_KEY)
	# send_supersession(topic=SupersessionConfig.TOPIC, kafka_key=SupersessionConfig.KAFKA_KEY)
	# send_engineering_part(EngineeringPartConfig.TOPIC, EngineeringPartConfig.KAFKA_KEY)
	# send_description(DescriptionConfig.TOPIC, DescriptionConfig.KAFKA_KEY)
	# send_feature_family(FeatureFamilyConfig.TOPIC, FeatureFamilyConfig.KAFKA_KEY)
	# send_engineering_part_usage(EngineeringPartUsageConfig.TOPIC,EngineeringPartUsageConfig.KAFKA_KEY)
	send_vin(VinConfig.TOPIC,VinConfig.KAFKA_KEY)


def main():
	if os.path.exists('log\\' + GeneralConfig.ENV + '_' + GeneralConfig.TOPIC_PREFIX + '_' + GeneralConfig.ROWCOUNT_LOG_FILE):
		os.remove('log\\' + GeneralConfig.ENV + '_' + GeneralConfig.TOPIC_PREFIX + '_' + GeneralConfig.ROWCOUNT_LOG_FILE)

	pool = ProcessPool(nodes=cpu_count() - 1 or 1)

	# pool.amap(send_aftermarket_part, [AftermarketPartConfig.TOPIC], [AftermarketPartConfig.KAFKA_KEY])
	# pool.amap(send_description, [DescriptionConfig.TOPIC], [DescriptionConfig.KAFKA_KEY])
	# pool.amap(send_engineering_part, [EngineeringPartConfig.TOPIC], [EngineeringPartConfig.KAFKA_KEY])
	# pool.amap(send_engineering_part_function, [EngineeringPartFunctionConfig.TOPIC], [EngineeringPartFunctionConfig.KAFKA_KEY])
	# pool.amap(send_engineering_part_usage, [EngineeringPartUsageConfig.TOPIC], [EngineeringPartUsageConfig.KAFKA_KEY])
	# pool.amap(send_feature, [FeatureConfig.TOPIC], [FeatureConfig.KAFKA_KEY])
	# pool.amap(send_feature_family, [FeatureFamilyConfig.TOPIC], [FeatureFamilyConfig.KAFKA_KEY])
	pool.amap(send_hierarchy, [HierarchyConfig.TOPIC], [HierarchyConfig.KAFKA_KEY])
	# pool.amap(send_hierarchy_illustration, [HierarchyIllustrationConfig.TOPIC], [HierarchyIllustrationConfig.KAFKA_KEY])
	# pool.amap(send_hierarchy_usage, [HierarchyUsageConfig.TOPIC], [HierarchyUsageConfig.KAFKA_KEY])
	# pool.amap(send_section_callout, [SectionCalloutConfig.TOPIC], [SectionCalloutConfig.KAFKA_KEY])
	# pool.amap(send_section_part_usage, [SectionPartUsageConfig.TOPIC], [SectionPartUsageConfig.KAFKA_KEY])
	# pool.amap(send_supersession, [SupersessionConfig.TOPIC], [SupersessionConfig.KAFKA_KEY])
	# pool.amap(send_intray, [IntrayConfig.TOPIC], [IntrayConfig.KAFKA_KEY])		
	# pool.amap(send_vin, [VinConfig.TOPIC], [VinConfig.KAFKA_KEY])	

	pool.close()
	pool.join()



def main_jagcat():
	if os.path.exists('log\\' + GeneralConfig.ENV + '_' + GeneralConfig.TOPIC_PREFIX + '_' + GeneralConfig.ROWCOUNT_LOG_FILE):
		os.remove('log\\' + GeneralConfig.ENV + '_' + GeneralConfig.TOPIC_PREFIX + '_' + GeneralConfig.ROWCOUNT_LOG_FILE)

	pool = ProcessPool(nodes=cpu_count() - 1 or 1)

	pool.amap(send_part_meta, [PartMetaConfig.TOPIC], [PartMetaConfig.KAFKA_KEY])
	pool.amap(send_intray, [IntrayConfig.TOPIC], [IntrayConfig.KAFKA_KEY])	
	pool.amap(send_description, [DescriptionConfig.TOPIC], [DescriptionConfig.KAFKA_KEY])
	pool.amap(send_feature, [FeatureConfig.TOPIC], [FeatureConfig.KAFKA_KEY])
	pool.amap(send_feature_family, [FeatureFamilyConfig.TOPIC], [FeatureFamilyConfig.KAFKA_KEY])
	pool.amap(send_hierarchy, [HierarchyConfig.TOPIC], [HierarchyConfig.KAFKA_KEY])
	pool.amap(send_hierarchy_illustration, [HierarchyIllustrationConfig.TOPIC], [HierarchyIllustrationConfig.KAFKA_KEY])
	pool.amap(send_hierarchy_usage, [HierarchyUsageConfig.TOPIC], [HierarchyUsageConfig.KAFKA_KEY])
	pool.amap(send_section_callout, [SectionCalloutConfig.TOPIC], [SectionCalloutConfig.KAFKA_KEY])
	pool.amap(send_section_part_usage, [SectionPartUsageConfig.TOPIC], [SectionPartUsageConfig.KAFKA_KEY])
	pool.amap(send_vin, [VinConfig.TOPIC], [VinConfig.KAFKA_KEY])	

	pool.close()
	pool.join()

	# send_feature(FeatureConfig.TOPIC, FeatureConfig.KAFKA_KEY)


if __name__ == '__main__':
	start_time = time.time()
	# maintest()
	if GeneralConfig.PHASE == 'phase1':
		main()
	if GeneralConfig.PHASE == 'jagcat':
		main_jagcat()
	elapsed_time = time.time() - start_time
	print(time.strftime("%H:%M:%S", time.gmtime(elapsed_time)))
