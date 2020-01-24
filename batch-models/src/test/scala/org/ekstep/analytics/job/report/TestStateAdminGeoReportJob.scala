package org.ekstep.analytics.job

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.ekstep.analytics.model.SparkSpec
import org.scalamock.scalatest.MockFactory
import org.ekstep.analytics.job.report.{BaseReportsJob, StateAdminGeoReportJob}
import org.ekstep.analytics.util.EmbeddedCassandra
import org.sunbird.cloud.storage.conf.AppConf
import java.io.File

import org.ekstep.analytics.framework.FrameworkContext

class TestStateAdminGeoReportJob extends SparkSpec(null) with MockFactory {

 implicit var spark: SparkSession = _
 var map: Map[String, String] = _
 var shadowUserDF: DataFrame = _
 var orgDF: DataFrame = _
 var reporterMock: BaseReportsJob = mock[BaseReportsJob]
 val sunbirdKeyspace = "sunbird"

 override def beforeAll(): Unit = {
   super.beforeAll()
   spark = getSparkSession()
   EmbeddedCassandra.loadData("src/test/resources/reports/reports_test_data.cql") // Load test data in embedded cassandra server
 }

 "StateAdminGeoReportJob" should "generate reports" in {
   implicit val fc = new FrameworkContext()
   val tempDir = AppConf.getConfig("admin.metrics.temp.dir")
   val reportDF = StateAdminGeoReportJob.generateGeoReport()(spark, fc)
   reportDF.count() should be(6)
   //for geo report we expect these columns
   reportDF.columns.contains("index") should be(true)
   reportDF.columns.contains("School id") should be(true)
   reportDF.columns.contains("School name") should be(true)
   reportDF.columns.contains("District id") should be(true)
   reportDF.columns.contains("District name") should be(true)
   reportDF.columns.contains("Block id") should be(true)
   reportDF.columns.contains("Block name") should be(true)
   reportDF.columns.contains("slug") should be(true)
   reportDF.columns.contains("externalid") should be(true)
   val apslug = reportDF.where(col("slug") === "ApSlug")
   val school_name = apslug.select("School name").collect().map(_ (0)).toList
   school_name(0) should be("MPPS SIMHACHALNAGAR")
   school_name(1) should be("Another school")
   reportDF.select("District id").distinct().count should be(4)
   //checking reports were created under slug folder
   val slugName = apslug.select("slug").collect().map(_ (0)).toList
   val apslugDirPath = tempDir+"/renamed/"+slugName(0)+"/"
   val geoDetail = new File(apslugDirPath+"geo-detail.csv")
   val geoSummary = new File(apslugDirPath+"geo-summary.json")
   val geoSummaryDistrict = new File(apslugDirPath+"geo-summary-district.json")
   geoDetail.exists() should be(true)
   geoSummary.exists() should be(true)
   geoSummaryDistrict.exists() should be(true)
 }
}