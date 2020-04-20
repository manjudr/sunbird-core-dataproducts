package org.ekstep.analytics.job

import java.util.concurrent.CountDownLatch

import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.ekstep.analytics.framework.FrameworkContext
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.kafka.consumer.{JobConsumerV2, JobConsumerV2Config}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

class TestJobManager extends FlatSpec with Matchers with BeforeAndAfterAll with EmbeddedKafka {

    ignore should "start job manager and consume 1 message" in {

        val userDefinedConfig = EmbeddedKafkaConfig(kafkaPort = 9092, zooKeeperPort = 2181)
        withRunningKafkaOnFoundPort(userDefinedConfig) { implicit actualConfig =>

            Console.println("Kafka started...");
            createCustomTopic("test", Map(), 1, 1)
            createCustomTopic("output", Map(), 1, 1)
            implicit val serializer = new StringSerializer()
            implicit val deserializer = new StringDeserializer()

            publishStringMessageToKafka("test", """{"model":"wfs","config":{"search":{"type":"none"},"model":"org.ekstep.analytics.model.WorkflowSummary","modelParams":{"apiVersion":"v2"},"output":[{"to":"console","params":{"printEvent": true}},{"to":"kafka","params":{"brokerList":"localhost:9092","topic":"output"}}],"parallelization":8,"appName":"Workflow Summarizer","deviceMapping":true}}""")
            println(consumeFirstStringMessageFrom("test"))

            val config = """{"jobsCount":1,"topic":"test","bootStrapServer":"localhost:9092","zookeeperConnect":"localhost:2181","consumerGroup":"jobmanager","slackChannel":"#test_channel","slackUserName":"JobManager","tempBucket":"dev-data-store","tempFolder":"transient-data-test"}""";
            JobManager.main(config);

            // shutdown explicitly
            JobManager.close();

        }
    }

    it should "start job manager and consume 2 messages and auto shutdown" in {

        val userDefinedConfig = EmbeddedKafkaConfig(kafkaPort = 9092, zooKeeperPort = 2181)
        withRunningKafkaOnFoundPort(userDefinedConfig) { implicit actualConfig =>

            Console.println("Kafka started...");
            createCustomTopic("test", Map(), 1, 1)
            createCustomTopic("output", Map(), 1, 1)
            implicit val serializer = new StringSerializer()
            implicit val deserializer = new StringDeserializer()

            publishStringMessageToKafka("test", """{"model":"wfs","config":{"search":{"type":"none"},"model":"org.ekstep.analytics.model.WorkflowSummary","modelParams":{"apiVersion":"v2"},"output":[{"to":"console","params":{"printEvent": true}},{"to":"kafka","params":{"brokerList":"localhost:9092","topic":"output"}}],"parallelization":8,"appName":"Workflow Summarizer","deviceMapping":true}}""")
            publishStringMessageToKafka("test", """{"model":"monitor-job-summ","config":{"search":{"type":"none"},"model":"org.ekstep.analytics.model.MonitorSummaryModel","modelParams":{"model":[{"model":"ItemSummaryModel","category":"consumption","input_dependency":"LearnerSessionSummaryModel"},{"model":"GenieUsageSummaryModel","category":"consumption","input_dependency":"GenieLaunchSummaryModel"},{"model":"GenieStageSummaryModel","category":"consumption","input_dependency":"GenieLaunchSummaryModel"},{"model":"ItemUsageSummaryModel","category":"consumption","input_dependency":"ItemSummaryModel"},{"model":"DeviceContentUsageSummaryModel","category":"consumption","input_dependency":"LearnerSessionSummaryModel"},{"model":"DeviceUsageSummaryModel","category":"consumption","input_dependency":"GenieLaunchSummaryModel"},{"model":"UpdateGenieUsageDB","category":"consumption","input_dependency":"GenieUsageSummaryModel"},{"model":"UpdateItemSummaryDB","category":"consumption","input_dependency":"ItemUsageSummaryModel"},{"model":"UpdateContentUsageDB","category":"consumption","input_dependency":"ContentUsageSummaryModel"},{"model":"UpdateContentPopularityDB","category":"consumption","input_dependency":"ContentPopularitySummaryModel"},{"model":"ContentUsageSummaryModel","category":"consumption","input_dependency":"LearnerSessionSummaryModel"},{"model":"UpdateLearnerProfileDB","category":"consumption","input_dependency":"None"},{"model":"UpdateDeviceSpecificationDB","category":"consumption","input_dependency":"None"},{"model":"LearnerSessionSummaryModel","category":"consumption","input_dependency":"None"},{"model":"GenieSessionSummaryModel","category":"consumption","input_dependency":"None"},{"model":"GenieLaunchSummaryModel","category":"consumption","input_dependency":"None"},{"model":"ContentPopularitySummaryModel","category":"consumption","input_dependency":"None"},{"model":"EOCRecommendationFunnelModel","category":"consumption","input_dependency":"None"},{"model":"StageSummaryModel","category":"consumption","input_dependency":"None"},{"model":"GenieFunnelAggregatorModel","category":"consumption","input_dependency":"None"},{"model":"ContentSideloadingSummaryModel","category":"consumption","input_dependency":"None"},{"model":"GenieFunnelModel","category":"consumption","input_dependency":"None"},{"model":"UpdateContentModel","category":"consumption","input_dependency":"None"},{"model":"ConsumptionMetricsUpdater","category":"consumption","input_dependency":"None"},{"model":"PrecomputedViews","category":"consumption","input_dependency":"None"},{"model":"DataExhaustJob","category":"consumption","input_dependency":"None"},{"model":"AppUsageSummaryModel","category":"creation","input_dependency":"AppSessionSummaryModel"},{"model":"ContentEditorUsageSummaryModel","category":"creation","input_dependency":"ContentEditorSessionSummaryModel"},{"model":"TextbookUsageSummaryModel","category":"creation","input_dependency":"TextbookSessionSummaryModel"},{"model":"UpdateAppUsageDB","category":"creation","input_dependency":"AppUsageSummaryModel"},{"model":"UpdateContentEditorUsageDB","category":"creation","input_dependency":"ContentEditorUsageSummaryModel"},{"model":"UpdateTextbookUsageDB","category":"creation","input_dependency":"TextbookUsageSummaryModel"},{"model":"UpdateAuthorSummaryDB","category":"creation","input_dependency":"AuthorUsageSummaryModel"},{"model":"UpdatePublishPipelineSummarycreation","category":"creation","input_dependency":"PublishPipelineSummaryModel"},{"model":"AppSessionSummaryModel","category":"creation","input_dependency":"None"},{"model":"ContentEditorSessionSummaryModel","category":"creation","input_dependency":"None"},{"model":"PublishPipelineSummaryModel","category":"creation","input_dependency":"None"},{"model":"UpdateObjectLifecycleDB","category":"creation","input_dependency":"None"},{"model":"UpdateContentCreationMetricsDB","category":"creation","input_dependency":"None"},{"model":"AuthorUsageSummaryModel","category":"creation","input_dependency":"None"},{"model":"TextbookSessionSummaryModel","category":"creation","input_dependency":"None"},{"model":"UpdateCreationMetricsDB","category":"creation","input_dependency":"None"},{"model":"UpdateConceptSnapshotDB","category":"creation","input_dependency":"None"},{"model":"ContentSnapshotSummaryModel","category":"creation","input_dependency":"None"},{"model":"ConceptSnapshotSummaryModel","category":"creation","input_dependency":"None"},{"model":"UpdateContentSnapshotDB","category":"creation","input_dependency":"None"},{"model":"AssetSnapshotSummaryModel","category":"creation","input_dependency":"None"},{"model":"UpdateTextbookSnapshotDB","category":"creation","input_dependency":"None"},{"model":"UpdateAssetSnapshotDB","category":"creation","input_dependency":"None"},{"model":"ContentLanguageRelationModel","category":"creation","input_dependency":"None"},{"model":"ConceptLanguageRelationModel","category":"creation","input_dependency":"None"},{"model":"AuthorRelationsModel","category":"creation","input_dependency":"None"},{"model":"CreationRecommendationEnrichmentModel","category":"creation","input_dependency":"None"},{"model":"ContentAssetRelationModel","category":"creation","input_dependency":"None"},{"model":"DeviceRecommendationTrainingModel","category":"recommendation","input_dependency":"None"},{"model":"DeviceRecommendationScoringModel","category":"recommendation","input_dependency":"None"},{"model":"ContentVectorsModel","category":"recommendation","input_dependency":"None"},{"model":"EndOfContentRecommendationModel","category":"recommendation","input_dependency":"None"},{"model":"CreationRecommendationModel","category":"recommendation","input_dependency":"None"}]},"output":[{"to":"console","params":{"printEvent":false}}],"appName":"TestMonitorSummarizer","deviceMapping":true,"pushMetrics":false}}""")
            println(consumeFirstStringMessageFrom("test"))

            val config = """{"jobsCount":1,"topic":"test","bootStrapServer":"localhost:9092","zookeeperConnect":"localhost:2181","consumerGroup":"jobmanager","slackChannel":"#test_channel","slackUserName":"JobManager","tempBucket":"dev-data-store","tempFolder":"transient-data-test"}""";
            JobManager.main(config);

        }
    }

    it should "test JobRunner" in {

        val userDefinedConfig = EmbeddedKafkaConfig(kafkaPort = 9092, zooKeeperPort = 2181)
        withRunningKafkaOnFoundPort(userDefinedConfig) { implicit actualConfig =>

            Console.println("Kafka started...");
            createCustomTopic("test", Map(), 1, 1)
            implicit val serializer = new StringSerializer()
            implicit val deserializer = new StringDeserializer()

            publishStringMessageToKafka("test", """{"model":"abc","config":{"search":{"type":"none"},"model":"org.ekstep.analytics.model.WorkflowSummary","modelParams":{"apiVersion":"v2"},"output":[{"to":"console","params":{"printEvent": true}},{"to":"kafka","params":{"brokerList":"localhost:9092","topic":"output"}}],"parallelization":8,"appName":"Workflow Summarizer","deviceMapping":true}}""")
            publishStringMessageToKafka("test", """{"model":"monitor-job-summ","config":{"search":{"type":"none"},"model":"org.ekstep.analytics.model.MonitorSummaryModel","modelParams":{"model":[{"model":"ItemSummaryModel","category":"consumption","input_dependency":"LearnerSessionSummaryModel"},{"model":"GenieUsageSummaryModel","category":"consumption","input_dependency":"GenieLaunchSummaryModel"},{"model":"GenieStageSummaryModel","category":"consumption","input_dependency":"GenieLaunchSummaryModel"},{"model":"ItemUsageSummaryModel","category":"consumption","input_dependency":"ItemSummaryModel"},{"model":"DeviceContentUsageSummaryModel","category":"consumption","input_dependency":"LearnerSessionSummaryModel"},{"model":"DeviceUsageSummaryModel","category":"consumption","input_dependency":"GenieLaunchSummaryModel"},{"model":"UpdateGenieUsageDB","category":"consumption","input_dependency":"GenieUsageSummaryModel"},{"model":"UpdateItemSummaryDB","category":"consumption","input_dependency":"ItemUsageSummaryModel"},{"model":"UpdateContentUsageDB","category":"consumption","input_dependency":"ContentUsageSummaryModel"},{"model":"UpdateContentPopularityDB","category":"consumption","input_dependency":"ContentPopularitySummaryModel"},{"model":"ContentUsageSummaryModel","category":"consumption","input_dependency":"LearnerSessionSummaryModel"},{"model":"UpdateLearnerProfileDB","category":"consumption","input_dependency":"None"},{"model":"UpdateDeviceSpecificationDB","category":"consumption","input_dependency":"None"},{"model":"LearnerSessionSummaryModel","category":"consumption","input_dependency":"None"},{"model":"GenieSessionSummaryModel","category":"consumption","input_dependency":"None"},{"model":"GenieLaunchSummaryModel","category":"consumption","input_dependency":"None"},{"model":"ContentPopularitySummaryModel","category":"consumption","input_dependency":"None"},{"model":"EOCRecommendationFunnelModel","category":"consumption","input_dependency":"None"},{"model":"StageSummaryModel","category":"consumption","input_dependency":"None"},{"model":"GenieFunnelAggregatorModel","category":"consumption","input_dependency":"None"},{"model":"ContentSideloadingSummaryModel","category":"consumption","input_dependency":"None"},{"model":"GenieFunnelModel","category":"consumption","input_dependency":"None"},{"model":"UpdateContentModel","category":"consumption","input_dependency":"None"},{"model":"ConsumptionMetricsUpdater","category":"consumption","input_dependency":"None"},{"model":"PrecomputedViews","category":"consumption","input_dependency":"None"},{"model":"DataExhaustJob","category":"consumption","input_dependency":"None"},{"model":"AppUsageSummaryModel","category":"creation","input_dependency":"AppSessionSummaryModel"},{"model":"ContentEditorUsageSummaryModel","category":"creation","input_dependency":"ContentEditorSessionSummaryModel"},{"model":"TextbookUsageSummaryModel","category":"creation","input_dependency":"TextbookSessionSummaryModel"},{"model":"UpdateAppUsageDB","category":"creation","input_dependency":"AppUsageSummaryModel"},{"model":"UpdateContentEditorUsageDB","category":"creation","input_dependency":"ContentEditorUsageSummaryModel"},{"model":"UpdateTextbookUsageDB","category":"creation","input_dependency":"TextbookUsageSummaryModel"},{"model":"UpdateAuthorSummaryDB","category":"creation","input_dependency":"AuthorUsageSummaryModel"},{"model":"UpdatePublishPipelineSummarycreation","category":"creation","input_dependency":"PublishPipelineSummaryModel"},{"model":"AppSessionSummaryModel","category":"creation","input_dependency":"None"},{"model":"ContentEditorSessionSummaryModel","category":"creation","input_dependency":"None"},{"model":"PublishPipelineSummaryModel","category":"creation","input_dependency":"None"},{"model":"UpdateObjectLifecycleDB","category":"creation","input_dependency":"None"},{"model":"UpdateContentCreationMetricsDB","category":"creation","input_dependency":"None"},{"model":"AuthorUsageSummaryModel","category":"creation","input_dependency":"None"},{"model":"TextbookSessionSummaryModel","category":"creation","input_dependency":"None"},{"model":"UpdateCreationMetricsDB","category":"creation","input_dependency":"None"},{"model":"UpdateConceptSnapshotDB","category":"creation","input_dependency":"None"},{"model":"ContentSnapshotSummaryModel","category":"creation","input_dependency":"None"},{"model":"ConceptSnapshotSummaryModel","category":"creation","input_dependency":"None"},{"model":"UpdateContentSnapshotDB","category":"creation","input_dependency":"None"},{"model":"AssetSnapshotSummaryModel","category":"creation","input_dependency":"None"},{"model":"UpdateTextbookSnapshotDB","category":"creation","input_dependency":"None"},{"model":"UpdateAssetSnapshotDB","category":"creation","input_dependency":"None"},{"model":"ContentLanguageRelationModel","category":"creation","input_dependency":"None"},{"model":"ConceptLanguageRelationModel","category":"creation","input_dependency":"None"},{"model":"AuthorRelationsModel","category":"creation","input_dependency":"None"},{"model":"CreationRecommendationEnrichmentModel","category":"creation","input_dependency":"None"},{"model":"ContentAssetRelationModel","category":"creation","input_dependency":"None"},{"model":"DeviceRecommendationTrainingModel","category":"recommendation","input_dependency":"None"},{"model":"DeviceRecommendationScoringModel","category":"recommendation","input_dependency":"None"},{"model":"ContentVectorsModel","category":"recommendation","input_dependency":"None"},{"model":"EndOfContentRecommendationModel","category":"recommendation","input_dependency":"None"},{"model":"CreationRecommendationModel","category":"recommendation","input_dependency":"None"}]},"output":[{"to":"console","params":{"printEvent":false}}],"appName":"TestMonitorSummarizer","deviceMapping":true,"pushMetrics":false}}""")

            val strConfig = """{"jobsCount":1,"topic":"test","bootStrapServer":"localhost:9092","zookeeperConnect":"localhost:2181","consumerGroup":"jobmanager","slackChannel":"#test_channel","slackUserName":"JobManager","tempBucket":"dev-data-store","tempFolder":"transient-data-test"}""";
            val config = JSONUtils.deserialize[JobManagerConfig](strConfig)

            val props = JobConsumerV2Config.makeProps("localhost:2181", "test-jobmanager")
            val consumer = new JobConsumerV2("test", props);
            val doneSignal = new CountDownLatch(1)
            val runner = new JobRunner(config, consumer, doneSignal)
            runner.run()
            consumer.close()
//            Thread.sleep(10 * 1000);

//            publishStringMessageToKafka("test", """{"model":"abc","config":{"search":{"type":"none"},"model":"org.ekstep.analytics.model.WorkflowSummary","modelParams":{"apiVersion":"v2"},"output":[{"to":"console","params":{"printEvent": true}},{"to":"kafka","params":{"brokerList":"localhost:9092","topic":"output"}}],"parallelization":8,"appName":"Workflow Summarizer","deviceMapping":true}}""")

        }
    }

}
