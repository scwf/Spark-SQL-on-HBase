package org.apache.spark.sql.hbase

import java.io.File

import org.apache.hadoop.fs._
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.spark.sql.{SQLConf, DataFrame}
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.execution.Exchange
import org.apache.spark.sql.hbase.util.HBaseKVHelper
import org.apache.spark.sql.types.{IntegerType, StringType}
import org.apache.spark.{Logging, SparkConf, SparkContext}
import org.scalatest.{Ignore, BeforeAndAfterAll, FunSuite}

/**
* Not really a unit test, but the FunSuite is used here to
* bulkload to an external cluster.
*
* To do this, hbase-site has to be installed in
* sql/hbase/src/test/resources/hbase-site.xml (and built).
*
* We have not found a way to make this work from CLI.
*/


class ClusterTestSuite extends FunSuite with BeforeAndAfterAll with Logging {

  val sparkHome = System.getenv("SPARK_HOME")
  logInfo("SPARK_HOME: " + sparkHome)

  val sparkConf = new SparkConf(true)
    .setMaster("spark://xinyunh-OptiPlex-755:7077") //"spark://swlab-server-r03-16L:7079")
    .setAppName("ClusterTest")
    .set("spark.executor.memory", "4g")
  val sc: SparkContext = new SparkContext(sparkConf)
  sc.hadoopConfiguration.set("fs.defaultFS", "hdfs://localhost:9000")
  //  sc.hadoopConfiguration.set("hbase.zookeeper.quorum", "swlab-server-r03-16L:2181")
  val hbaseContext = new org.apache.spark.sql.hbase.HBaseSQLContext(sc)

  val hbaseAdmin: HBaseAdmin = new HBaseAdmin(sc.hadoopConfiguration)

  val catalog:HBaseCatalog = hbaseContext.catalog

  val hbaseHome = {
    val loader = this.getClass.getClassLoader
    val url = loader.getResource("loadData.txt")
    val file = new File(url.getPath)
    val parent = file.getParentFile
    parent.getAbsolutePath
  }

  test("create presplit table") {
    creatTable
  }

  test("drop presplit table") {
    dropTable
  }

  test("recreate presplit table") {
    dropTable
    creatTable
  }

  test("query test for presplit table") {
    queryTest
  }

  test("query test") {
    hbaseContext.sql("select * from tb").show
    hbaseContext.sql("select * from tb where col1>600").show
    hbaseContext.sql("select col1, count(1) from tb where col1>600 group by col1 order by col1").show
  }

  test("query test for presplit table with codegen") {
    val originalValue = hbaseContext.conf.codegenEnabled
    hbaseContext.setConf(SQLConf.CODEGEN_ENABLED, "true")
    queryTest
    hbaseContext.setConf(SQLConf.CODEGEN_ENABLED, originalValue.toString)
  }

  def queryTest = {
    var sql = "select col1,col3 from testblk where col1 < 4096 group by col3,col1"
    checkResult(hbaseContext.sql(sql), containExchange = true, 3)

    sql = "select col1,col2,col3 from testblk group by col1,col2,col3"
    checkResult(hbaseContext.sql(sql), containExchange = false, 7)

    sql = "select col1,col2,col3,count(*) from testblk group by col1,col2,col3,col1+col3"
    checkResult(hbaseContext.sql(sql), containExchange = false, 7) //Sprecial case

    sql = "select count(*) from testblk group by col1+col3"
    checkResult(hbaseContext.sql(sql), containExchange = true, 5)

    sql = "select col1,col2 from testblk where col1 < 4096 group by col1,col2"
    checkResult(hbaseContext.sql(sql), containExchange = false, 3)

    sql = "select col1,col2 from testblk where (col1 = 4096 and col2 < 'cc') group by col1,col2"
    checkResult(hbaseContext.sql(sql), containExchange = true, 2)

    sql = "select col1 from testblk where col1 < 4096 group by col1"
    checkResult(hbaseContext.sql(sql), containExchange = false, 3)
  }

  def creatTable = {
    val types = Seq(IntegerType, StringType, IntegerType)

    def generateRowKey(keys: Array[Any], length: Int = -1) = {
      val completeRowKey = HBaseKVHelper.makeRowKey(new GenericRow(keys), types)
      if (length < 0) completeRowKey
      else completeRowKey.take(length)
    }

    val splitKeys: Array[HBaseRawType] = Array(
//      generateRowKey(Array(1024, "0b", 0), 3),
//      generateRowKey(Array(2048, "cc", 1024), 4),
//      generateRowKey(Array(4096, "0a", 0), 4) ++ Array[Byte](0x00),
//      generateRowKey(Array(4096, "0b", 1024), 7),
//      generateRowKey(Array(4096, "cc", 0), 7) ++ Array[Byte](0x00),
      generateRowKey(Array(4096, "cc", 1000))
    )
    catalog.createHBaseUserTable("presplit_table", Set("cf"), splitKeys)

    val sql1 =
      s"""CREATE TABLE testblk(col1 INT, col2 STRING, col3 INT, col4 STRING,
          PRIMARY KEY(col1, col2, col3))
          MAPPED BY (presplit_table, COLS=[col4=cf.a])"""
        .stripMargin
    hbaseContext.sql(sql1).collect()

    val inputFile = "'" + hbaseHome + "/splitLoadData1.txt'"
    // then load parall data into table
    val loadSql = "LOAD PARALL DATA LOCAL INPATH " + inputFile + " INTO TABLE testblk"
    hbaseContext.sql(loadSql).collect()
  }

  def dropTable = {
    // cleanup
    hbaseContext.sql("drop table testblk").collect()
    dropNativeHbaseTable("presplit_table")
  }

  def dropNativeHbaseTable(tableName: String) = {
    try {
      hbaseAdmin.disableTable(tableName)
      hbaseAdmin.deleteTable(tableName)
    } catch {
      case e: TableExistsException =>
        logError(s"Table already exists $tableName", e)
    }
  }

  def checkResult(df: DataFrame, containExchange: Boolean, size: Int) = {
    df.queryExecution.executedPlan match {
      case a: org.apache.spark.sql.execution.Aggregate =>
        assert(a.child.isInstanceOf[Exchange] == containExchange)
      case a: org.apache.spark.sql.execution.GeneratedAggregate =>
        assert(a.child.isInstanceOf[Exchange] == containExchange)
      case _ => Nil
    }
    assert(df.collect().size == size)
  }
}

