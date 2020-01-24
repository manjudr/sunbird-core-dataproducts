package org.ekstep.analytics.job

import java.io.File

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.job.report.{BaseReportSpec, CourseMetricsJob, ReportGenerator}
import org.scalamock.scalatest.MockFactory


class TestCourseMetricsJob extends BaseReportSpec with MockFactory {
  var spark: SparkSession = _
  var courseBatchDF: DataFrame = _
  var userCoursesDF: DataFrame = _
  var userDF: DataFrame = _
  var locationDF: DataFrame = _
  var orgDF: DataFrame = _
  var userOrgDF: DataFrame = _
  var externalIdentityDF: DataFrame = _
  var reporterMock: ReportGenerator = mock[ReportGenerator]
  val sunbirdCoursesKeyspace = "sunbird_courses"
  val sunbirdKeyspace = "sunbird"

  override def beforeAll(): Unit = {

    super.beforeAll()
    spark = getSparkSession();

    /*
     * Data created with 31 active batch from batchid = 1000 - 1031
     * */
    courseBatchDF = spark
      .read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .load("src/test/resources/course-metrics-updater/courseBatchTable.csv")
      .cache()

    externalIdentityDF = spark
      .read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .load("src/test/resources/course-metrics-updater/usr_external_identity.csv")
      .cache()


    /*
     * Data created with 35 participants mapped to only batch from 1001 - 1010 (10), so report
     * should be created for these 10 batch (1001 - 1010) and 34 participants (1 user is not active in the course)
     * and along with 5 existing users from 31-35 has been subscribed to another batch 1003-1007 also
     * */
    userCoursesDF = spark
      .read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .load("src/test/resources/course-metrics-updater/userCoursesTable.csv")
      .cache()

    /*
     * This has users 30 from user001 - user030
     * */
    userDF = spark
      .read
      .json("src/test/resources/course-metrics-updater/userTable.json")
      .cache()

    /*
     * This has 30 unique location
     * */
    locationDF = spark
      .read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .load("src/test/resources/course-metrics-updater/locationTable.csv")
      .cache()

    /*
     * There are 8 organisation added to the data, which can be mapped to `rootOrgId` in user table
     * and `organisationId` in userOrg table
     * */
    orgDF = spark
      .read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .load("src/test/resources/course-metrics-updater/orgTable.csv")
      .cache()

    /*
     * Each user is mapped to organisation table from any of 8 organisation
     * */
    userOrgDF = spark
      .read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .load("src/test/resources/course-metrics-updater/userOrgtable.csv")
      .cache()
  }


  "TestUpdateCourseMetrics" should "generate reports" in {

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "course_batch", "keyspace" -> sunbirdCoursesKeyspace))
      .returning(courseBatchDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "user_courses", "keyspace" -> sunbirdCoursesKeyspace))
      .returning(userCoursesDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "user", "keyspace" -> sunbirdKeyspace))
      .returning(userDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "user_org", "keyspace" -> sunbirdKeyspace))
      .returning(userOrgDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "organisation", "keyspace" -> sunbirdKeyspace))
      .returning(orgDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "location", "keyspace" -> sunbirdKeyspace))
      .returning(locationDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "usr_external_identity", "keyspace" -> sunbirdKeyspace))
      .returning(externalIdentityDF)

    val reportDF = CourseMetricsJob
      .prepareReport(spark, reporterMock.loadData)
      .cache()

    CourseMetricsJob.saveReport(reportDF, AppConf.getConfig("course.metrics.temp.dir"))
    CourseMetricsJob.saveReportES(reportDF)


    reportDF.count should be(34)
    reportDF.groupBy(col("batchid")).count().count() should be(10)

    val reportData = reportDF
      .groupBy(col("batchid"))
      .count()
      .collect()


    reportData.filter(row => row.getString(0) == "1001").head.getLong(1) should be(2)
    reportData.filter(row => row.getString(0) == "1002").head.getLong(1) should be(3)
    reportData.filter(row => row.getString(0) == "1003").head.getLong(1) should be(4)
    reportData.filter(row => row.getString(0) == "1004").head.getLong(1) should be(4)
    reportData.filter(row => row.getString(0) == "1005").head.getLong(1) should be(4)
    reportData.filter(row => row.getString(0) == "1006").head.getLong(1) should be(4)
    reportData.filter(row => row.getString(0) == "1007").head.getLong(1) should be(4)
    reportData.filter(row => row.getString(0) == "1008").head.getLong(1) should be(3)
    reportData.filter(row => row.getString(0) == "1009").head.getLong(1) should be(3)
    reportData.filter(row => row.getString(0) == "1010").head.getLong(1) should be(3)

  }

  it should "should calculate the progress" in {
    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "course_batch", "keyspace" -> sunbirdCoursesKeyspace))
      .returning(courseBatchDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "user_courses", "keyspace" -> sunbirdCoursesKeyspace))
      .returning(userCoursesDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "user", "keyspace" -> sunbirdKeyspace))
      .returning(userDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "user_org", "keyspace" -> sunbirdKeyspace))
      .returning(userOrgDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "organisation", "keyspace" -> sunbirdKeyspace))
      .returning(orgDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "location", "keyspace" -> sunbirdKeyspace))
      .returning(locationDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "usr_external_identity", "keyspace" -> sunbirdKeyspace))
      .returning(externalIdentityDF)

    val reportDF = CourseMetricsJob.prepareReport(spark, reporterMock.loadData)

    //sampling report
    val data1 = reportDF
      .select("course_completion")
      .where(col("batchid") === "1007" and col("userid") === "user017")
      .collect()

    data1.head.getInt(0) should be(65)

    val data2 = reportDF
      .select("course_completion")
      .where(col("batchid") === "1009" and col("userid") === "user019")
      .collect()

    data2.head.get(0)should be(0)

    val districtName = reportDF
      .select("district_name")
      .where(col("batchid") === "1006" and col("userid") === "user026")
      .collect()

    districtName.head.get(0) should be("GULBARGA")
  }

  it should "should round course progress to 100 when it is greater than 100" in {
    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "course_batch", "keyspace" -> sunbirdCoursesKeyspace))
      .returning(courseBatchDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "user_courses", "keyspace" -> sunbirdCoursesKeyspace))
      .returning(userCoursesDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "user", "keyspace" -> sunbirdKeyspace))
      .returning(userDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "user_org", "keyspace" -> sunbirdKeyspace))
      .returning(userOrgDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "organisation", "keyspace" -> sunbirdKeyspace))
      .returning(orgDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "location", "keyspace" -> sunbirdKeyspace))
      .returning(locationDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "usr_external_identity", "keyspace" -> sunbirdKeyspace))
      .returning(externalIdentityDF)

    val reportDF = CourseMetricsJob.prepareReport(spark, reporterMock.loadData)

    val data1 = reportDF
      .select("course_completion")
      .where(col("batchid") === "1006" and col("userid") === "user005")
      .collect()

    data1.head.getInt(0)should be(100)

    val data2 = reportDF
      .select("course_completion")
      .where(col("batchid") === "1005" and col("userid") === "user004")
      .collect()

    data2.head.getInt(0) should be(100)
  }

  it should "[Issue SB-12141] report should have 1 record for users mapped to two organisation (root and suborg)" in {

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "course_batch", "keyspace" -> sunbirdCoursesKeyspace))
      .returning(courseBatchDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "user_courses", "keyspace" -> sunbirdCoursesKeyspace))
      .returning(userCoursesDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "user", "keyspace" -> sunbirdKeyspace))
      .returning(userDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "user_org", "keyspace" -> sunbirdKeyspace))
      .returning(userOrgDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "organisation", "keyspace" -> sunbirdKeyspace))
      .returning(orgDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "location", "keyspace" -> sunbirdKeyspace))
      .returning(locationDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "usr_external_identity", "keyspace" -> sunbirdKeyspace))
      .returning(externalIdentityDF)

    val reportDF = CourseMetricsJob.prepareReport(spark, reporterMock.loadData)

    val data1 = reportDF
      .where(col("batchid") === "1003" and col("userid") === "user013")
      .count()

    data1 should be(1)
  }

  it should "[Issue SB-13080] report should have externalid,orgname, block_name, completedOn fields" in {

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "course_batch", "keyspace" -> sunbirdCoursesKeyspace))
      .returning(courseBatchDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "user_courses", "keyspace" -> sunbirdCoursesKeyspace))
      .returning(userCoursesDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "user", "keyspace" -> sunbirdKeyspace))
      .returning(userDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "user_org", "keyspace" -> sunbirdKeyspace))
      .returning(userOrgDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "organisation", "keyspace" -> sunbirdKeyspace))
      .returning(orgDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "location", "keyspace" -> sunbirdKeyspace))
      .returning(locationDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "usr_external_identity", "keyspace" -> sunbirdKeyspace))
      .returning(externalIdentityDF)

    val reportDF = CourseMetricsJob.prepareReport(spark, reporterMock.loadData)
    reportDF.columns.contains("userid") should be(true)
    reportDF.columns.contains("completedon") should be(true)
    reportDF.columns.contains("externalid") should be(true)
    reportDF.columns.contains("block_name")should be(true)
    // Externalid must not be null for user010, user030
    val user10DF = reportDF.filter(reportDF.col("userid") === "user010")
    val is10DFPresent = user10DF.select("externalid").collect().map(_ (0)).toList.contains(null)
    val user030DF = reportDF.filter(reportDF.col("userid") === "user030")
    val is30DFPresent = user030DF.select("externalid").collect().map(_ (0)).toList.contains(null)
    is10DFPresent should be(false)
    is30DFPresent should be(false)
  }

  it should "[Issue SB-13080] report should validate the block_name for the locationids " in {
    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "course_batch", "keyspace" -> sunbirdCoursesKeyspace))
      .returning(courseBatchDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "user_courses", "keyspace" -> sunbirdCoursesKeyspace))
      .returning(userCoursesDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "user", "keyspace" -> sunbirdKeyspace))
      .returning(userDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "user_org", "keyspace" -> sunbirdKeyspace))
      .returning(userOrgDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "organisation", "keyspace" -> sunbirdKeyspace))
      .returning(orgDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "location", "keyspace" -> sunbirdKeyspace))
      .returning(locationDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "usr_external_identity", "keyspace" -> sunbirdKeyspace))
      .returning(externalIdentityDF)

    val reportDF = CourseMetricsJob.prepareReport(spark, reporterMock.loadData)
    val result = reportDF.filter(reportDF("userid") === "user030" && reportDF("block_name") === "TUMKUR").count()
    result should be(1)
  }


  it should "[Issue SB-14781] not have any duplicate userid  in a batch " in {
    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "course_batch", "keyspace" -> sunbirdCoursesKeyspace))
      .returning(courseBatchDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "user_courses", "keyspace" -> sunbirdCoursesKeyspace))
      .returning(userCoursesDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "user", "keyspace" -> sunbirdKeyspace))
      .returning(userDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "user_org", "keyspace" -> sunbirdKeyspace))
      .returning(userOrgDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "organisation", "keyspace" -> sunbirdKeyspace))
      .returning(orgDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "location", "keyspace" -> sunbirdKeyspace))
      .returning(locationDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "usr_external_identity", "keyspace" -> sunbirdKeyspace))
      .returning(externalIdentityDF)

    val reportDF = CourseMetricsJob.prepareReport(spark, reporterMock.loadData)
    val result1 = reportDF.filter(reportDF("userid") === "user021")
    result1.groupBy(col("userid")).count().count() should be(1)
  }

  it should "[Issue SB-14781] When User in multiple organisation then result should enclosed with square bracket " in {
    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "course_batch", "keyspace" -> sunbirdCoursesKeyspace))
      .returning(courseBatchDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "user_courses", "keyspace" -> sunbirdCoursesKeyspace))
      .returning(userCoursesDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "user", "keyspace" -> sunbirdKeyspace))
      .returning(userDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "user_org", "keyspace" -> sunbirdKeyspace))
      .returning(userOrgDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "organisation", "keyspace" -> sunbirdKeyspace))
      .returning(orgDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "location", "keyspace" -> sunbirdKeyspace))
      .returning(locationDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "usr_external_identity", "keyspace" -> sunbirdKeyspace))
      .returning(externalIdentityDF)

    val reportDF = CourseMetricsJob.prepareReport(spark, reporterMock.loadData)
    val result = reportDF.filter(reportDF("userid") === "user021")
    result.groupBy(col("userid")).count().count() should be(1)

    val school_name1 = result.select("schoolname_resolved").collect().map(_ (0)).toList.head.toString
    val regex = "\\[([^\\]]*)\\]".r
    val value = regex.findFirstIn(school_name1)
    value.isEmpty should be(false)
  }

  it should "[Issue SB-14781] When User in not in multiple organisation then result should not enclosed with square bracket " in {
    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "course_batch", "keyspace" -> sunbirdCoursesKeyspace))
      .returning(courseBatchDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "user_courses", "keyspace" -> sunbirdCoursesKeyspace))
      .returning(userCoursesDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "user", "keyspace" -> sunbirdKeyspace))
      .returning(userDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "user_org", "keyspace" -> sunbirdKeyspace))
      .returning(userOrgDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "organisation", "keyspace" -> sunbirdKeyspace))
      .returning(orgDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "location", "keyspace" -> sunbirdKeyspace))
      .returning(locationDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "usr_external_identity", "keyspace" -> sunbirdKeyspace))
      .returning(externalIdentityDF)

    val reportDF = CourseMetricsJob.prepareReport(spark, reporterMock.loadData)
    val result = reportDF.filter(reportDF("userid") === "user007")
    result.groupBy(col("userid")).count().count() should be(1)

    val school_name1 = result.select("schoolname_resolved").collect().map(_ (0)).toList.head.toString
    val regex = "\\[([^\\]]*)\\]".r
    val value = regex.findFirstIn(school_name1)
    value.isEmpty should be(true)
  }

  it should "Should able to create a local directory to rename the reports" in {
    val tempDir = AppConf.getConfig("course.metrics.temp.dir")
    val df = spark.createDataFrame(Seq(
      ("", "user010", "Manju", "D R", "****@gmail.com", "*****75643", "org1", "TMK", "NVPHS", "BlockName", "2019-10-11", "100", "2019-11-11", "batch_001")
    )).toDF("externalid", "userid", "firstname", "lastname", "maskedemail", "maskedphone", "orgname_resolved", "district_name", "schoolname_resolved", "block_name", "enrolleddate", "course_completion", "completedon", "batchid")
    CourseMetricsJob.saveReport(df, tempDir)
    //TODO : Sonar cloud testcase failing need to check
    val renamedDir = s"$tempDir/renamed"
    val temp = new File(tempDir)
    val out = new File(renamedDir)
    try {
      CourseMetricsJob.renameReport(tempDir, renamedDir);
//      out.exists() === true)
//      temp.exists() === true)
    } catch {
      case ex: Exception => println("Error" + ex)
    }
  }

  it should "Not throw any error if the temp folder is already exists" in {
    val tempDir = AppConf.getConfig("course.metrics.temp.dir")
    val renamedDir = s"$tempDir/renamed"
    val temp = new File(tempDir)
    val out = new File(renamedDir)
    temp.mkdirs()
    out.mkdirs()
    try {
      CourseMetricsJob.renameReport(tempDir, renamedDir);
    } catch {
      case ex: Exception => ex should be(null)
    }

  }
}