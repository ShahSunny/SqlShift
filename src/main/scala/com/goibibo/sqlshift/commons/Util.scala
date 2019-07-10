package com.goibibo.sqlshift.commons

import java.io.{File, InputStream}
import java.sql.ResultSet

import com.goibibo.sqlshift.models.Configurations._
import com.goibibo.sqlshift.models.InternalConfs.{IncrementalSettings, InternalConfig}
import com.goibibo.sqlshift.models._
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.json4s.jackson.JsonMethods._
import org.json4s.{DefaultFormats, _}
import org.slf4j.{Logger, LoggerFactory}
import java.sql.Timestamp
import org.apache.spark.sql.types._

import scala.util.Try

/**
  * Project: mysql-redshift-loader
  * Author: shivamsharma
  * Date: 10/11/16.
  */
object Util {
    private val logger: Logger = LoggerFactory.getLogger(Util.getClass)

    val KB: Long = 1024
    val MB: Long = KB * KB
    val GB: Long = KB * MB

    /**
      * Get spark executor memory when provided. Default: 512m
      *
      * @return Executor memory
      */
    //TODO: Replace this function with conf.getSizeAsBytes("spark.executor.memory")
    def getExecutorMemory(conf: SparkConf): Long = {
        val defaultExecutorMemorySize = 512 * MB
        val executorMemorySize = Try {
            conf.getSizeAsBytes("spark.executor.memory")
        }.getOrElse {
            logger.warn("Wrong format of executor memory, Taking default {}", defaultExecutorMemorySize)
            defaultExecutorMemorySize
        }
        logger.info("executorMemorySize = {}", executorMemorySize)
        executorMemorySize
    }

    /**
      * Get average row size of table.
      *
      * @param mysqlDBConf mysql configuration
      * @return average size of record(row) in bytes
      */
    def getAvgRowSize(mysqlDBConf: DBConfiguration): Long = {
        logger.info("Calculating average row size: {}", mysqlDBConf.toString)
        val query = s"SELECT avg_row_length FROM information_schema.tables WHERE table_schema = " +
                s"'${mysqlDBConf.db}' AND table_name = '${mysqlDBConf.tableName}'"
        val connection = RedshiftUtil.getConnection(mysqlDBConf)
        val result: ResultSet = connection.createStatement().executeQuery(query)
        try {
            result.next()
            result.getLong(1)
        } catch {
            case e: Exception =>
                logger.error("Failed in finding average row size of table from source")
                logger.error("Stack Trace: ", e.fillInStackTrace())
                0
        } finally {
            result.close()
            connection.close()
        }
    }

    /**
      * Get minimum, maximum of primary key if primary key is integer and total records with given where condition
      *
      * @param mysqlDBConf    mysql configuration
      * @param whereCondition filter condition(without where clause)
      * @return tuple: (min, max)
      */
    def getMinMax(mysqlDBConf: DBConfiguration, distKey: String, 
            distKeyType:Option[DataType], 
            whereCondition: Option[String] = None): (Any, Any) = {
        val connection = RedshiftUtil.getConnection(mysqlDBConf)

        var query = s"SELECT min($distKey), max($distKey) FROM ${mysqlDBConf.db}.${mysqlDBConf.tableName}"
        if (whereCondition.nonEmpty) {
            query += " WHERE " + whereCondition.get
        }
        logger.info("Running Query: \n{}", query)
        val result: ResultSet = connection.createStatement().executeQuery(query)

        try {
            result.next()

            val res = if(distKeyType == TimestampType) {
                val min: Timestamp = result.getTimestamp(1)
                val max: Timestamp = result.getTimestamp(2)    
                (min,max)

            } else if (distKeyType == IntegerType){
                val min:Int = result.getInt(1)
                val max:Int = result.getInt(2)
                (min,max)
            } else {
                throw new IllegalArgumentException("distKeyType is neither IntegerType nor TimestampType");
            }
            
            logger.info(s"Minimum $distKey: $min :: Maximum $distKey: $max")
        } finally {
            result.close()
            connection.close()
        }
    }

    /**
      * Get optimum number of partitions on the basis of auto incremental and executor size.
      * If fails then return 1
      *
      * @param mysqlDBConf mysql configuration
      * @return no of partitions
      */
    def getPartitions(sqlContext: SQLContext, mysqlDBConf: DBConfiguration, minMaxAndRows: (Long, Long)): Int = {
        val memory: Long = getExecutorMemory(sqlContext.sparkContext.getConf)
        logger.info("Calculating number of partitions with each executor has memory: {}", memory)
        val minMaxDiff: Long = minMaxAndRows._2 - minMaxAndRows._1 + 1
        val avgRowSize: Long = getAvgRowSize(mysqlDBConf)
        if (avgRowSize == 0) {
            return 0
        }
        logger.info("Average Row size: {}, difference b/w min-max primary key: {}", avgRowSize, minMaxDiff)
        val expectedNumberOfRows = (memory / avgRowSize).toDouble * 0.2
        logger.info("Expected number of rows: {}", expectedNumberOfRows)

        var partitions: Int = Math.ceil(minMaxDiff / expectedNumberOfRows).toInt
        logger.info("Total number partitions are {}", partitions)
        partitions
    }

    def getSparkContext: (SparkContext, SQLContext) = {
        logger.info("Starting spark context...")
        val sparkConf: SparkConf = new SparkConf().setAppName("RDS to Redshift DataPipeline")
        val sc: SparkContext = new SparkContext(sparkConf)
        val sqlContext: SQLContext = new SQLContext(sc)

        System.setProperty("com.amazonaws.services.s3.enableV4", "true")
        sc.hadoopConfiguration.set("fs.s3a.endpoint", "s3.ap-south-1.amazonaws.com")
        sc.hadoopConfiguration.set("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
        sc.getConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        sc.hadoopConfiguration.set("fs.s3a.fast.upload", "true")
        sc.hadoopConfiguration.set("fs.s3a.fast.upload.buffer", "disk")
        sc.hadoopConfiguration.set("fs.s3a.connection.maximum","1000")
        sc.hadoopConfiguration.set("fs.s3a.attempts.maximum","30")
        (sc, sqlContext)
    }

    def closeSparkContext(sparkContext: SparkContext): Unit = {
        sparkContext.stop()
    }

    private def getDBsConf(mysqlJson: JValue, redshiftJson: JValue, s3Json: JValue, table: JValue):
    (DBConfiguration, DBConfiguration, S3Config) = {
        implicit val formats = DefaultFormats

        val mysqlConf: DBConfiguration = DBConfiguration("mysql", (mysqlJson \ "db").extract[String], null,
            (table \ "name").extract[String], (mysqlJson \ "hostname").extract[String],
            (mysqlJson \ "portno").extract[Int], (mysqlJson \ "username").extract[String],
            (mysqlJson \ "password").extract[String])

        val redshiftConf: DBConfiguration = DBConfiguration("redshift", "goibibo",
            (redshiftJson \ "schema").extract[String], (table \ "name").extract[String],
            (redshiftJson \ "hostname").extract[String], (redshiftJson \ "portno").extract[Int],
            (redshiftJson \ "username").extract[String], (redshiftJson \ "password").extract[String])

        val s3Conf: S3Config = S3Config((s3Json \ "location").extract[String],
            Try {
                Some((s3Json \ "accessKey").extract[String])
            }.getOrElse(None),
            Try {
                Some((s3Json \ "secretKey").extract[String])
            }.getOrElse(None))
        (mysqlConf, redshiftConf, s3Conf)
    }

    private def getAppConfiguration(mysqlConf: DBConfiguration, redshiftConf: DBConfiguration, s3Conf: S3Config, table: JValue): AppConfiguration = {
        implicit val formats = DefaultFormats

        logger.info("\n------------- Start :: table: {} -------------", (table \ "name").extract[String])
        val preLoadCmdValue: JValue = table \ "preLoadCmd"
        val preLoadCmd = if (preLoadCmdValue != JNothing && preLoadCmdValue != JNull) {
            val strVal = preLoadCmdValue.extract[String]
            if (strVal.trim == "") None else Some(strVal.trim)
        } else {
            None
        }

        val postLoadCmdValue: JValue = table \ "postLoadCmd"
        val postLoadCmd = if (postLoadCmdValue != JNothing && postLoadCmdValue != JNull) {
            val strVal = postLoadCmdValue.extract[String]
            if (strVal.trim == "") None else Some(strVal.trim)
        } else {
            None
        }
        val redshiftConfU = redshiftConf.copy(preLoadCmd = preLoadCmd, postLoadCmd = postLoadCmd)
        var internalConfig: InternalConfig = null

        val isAppendOnlyValue = table \ "isAppendOnly"
        val isAppendOnly = if (isAppendOnlyValue != JNothing && isAppendOnlyValue != JNull) {
            isAppendOnlyValue.extract[Boolean]
        } else {
            false
        }
        logger.info("Whether incremental update is append only: {}", isAppendOnly)

        val isSplittableValue = table \ "isSplittable"
        val isSplittable: Boolean = if (isSplittableValue != JNothing && isSplittableValue != JNull) {
            isSplittableValue.extract[Boolean]
        } else {
            false //Splitting of the incremental table load is usually degrades the performance.
        }
        logger.info("Whether table is splittable: {}", isSplittable)

        val distKeyValue = table \ "distkey"
        val distKey: Option[String] = if (distKeyValue != JNothing && distKeyValue != JNull) {
            logger.info("Found distribution key:- {}", distKeyValue.extract[String])
            Some(distKeyValue.extract[String])
        } else {
            logger.info("No distribution key found in configuration")
            None
        }

        val partitionsValue = table \ "partitions"
        var partitions: Option[Int] = {
            if (isSplittable && partitionsValue != JNothing && partitionsValue != JNull)
                Some(partitionsValue.extract[Int])
            else
                None
        }
        logger.info("Number of partitions: {}", partitions)


        val mergeKeyValue: JValue = table \ "mergeKey"
        val mergeKey: Option[String] = if (mergeKeyValue == JNothing || mergeKeyValue == JNull)
            None
        else
            Some(mergeKeyValue.extract[String])
        logger.info("Merge Key: {}", mergeKey.orNull)

        val addColumnValue: JValue = table \ "addColumn"
        val addColumn: Option[String] = if (addColumnValue == JNothing || addColumnValue == JNull)
            None
        else
            Some(addColumnValue.extract[String])
        logger.info("Add Column: {}", addColumn.orNull)

        val shallVacuumAfterLoadValue: JValue = table \ "deleteOnlyVacuum"
        val shallVacuumAfterLoad: Boolean = if (shallVacuumAfterLoadValue == JNothing || shallVacuumAfterLoadValue == JNull)
            false
        else
            shallVacuumAfterLoadValue.extract[Boolean]
        logger.info("Delete only vacuum is {}", shallVacuumAfterLoad)

        val incrementalColumnJValue: JValue = table \ "incrementalColumn"
        val incrementalColumn: Option[String] = if (incrementalColumnJValue == JNothing || incrementalColumnJValue == JNull)
            None
        else
            Some(incrementalColumnJValue.extract[String])

        val fromOffsetJValue: JValue = table \ "fromOffset"
        val fromOffset: Option[String] = if (fromOffsetJValue == JNothing || fromOffsetJValue == JNull)
            None
        else
            Some(fromOffsetJValue.extract[String])

        val toOffsetJValue: JValue = table \ "toOffset"
        val toOffset: Option[String] = if (toOffsetJValue == JNothing || toOffsetJValue == JNull)
            None
        else
            Some(toOffsetJValue.extract[String])

        val isSnapshotValue: JValue = table \ "isSnapshot"
        val isSnapshot: Boolean = if (isSnapshotValue != JNothing && isSnapshotValue != JNull) {
            isSnapshotValue.extract[Boolean]
        } else {
            false
        }
        logger.info("Whether merge type is snapshot: {}", isSnapshot)

        val fieldsToDeduplicateOnValue: JValue = table \ "fieldsToDeduplicateOn"
        val fieldsToDeduplicateOn: Option[Seq[String]] = if (fieldsToDeduplicateOnValue != JNothing && fieldsToDeduplicateOnValue != JNull) {
            val fieldsToDeduplicateOnSeq = fieldsToDeduplicateOnValue.extract[Seq[String]]
            logger.info("Found deduplication fields:- {}", fieldsToDeduplicateOnSeq)
            Some(fieldsToDeduplicateOnSeq)
        } else {
            logger.info("No deduplication fields found in configuration")
            None
        }

        val shallMergeJValue: JValue = table \ "shallMerge"
        val shallMerge: Boolean = if (shallMergeJValue == JNothing || shallMergeJValue == JNull)
            false
        else
            shallMergeJValue.extract[Boolean]

        val autoIncrementalJValue: JValue = table \ "autoIncremental"
        val autoIncremental: Option[Boolean] = if (autoIncrementalJValue == JNothing || autoIncrementalJValue == JNull)
            Some(false)
        else
            Some(autoIncrementalJValue.extract[Boolean])

        val snapshotOptimizingFilterValue: JValue = table \ "snapshotOptimizingFilter"
        val snapshotOptimizingFilter: Option[String] = if (snapshotOptimizingFilterValue == JNothing || snapshotOptimizingFilterValue == JNull)
            None
        else
            Some(snapshotOptimizingFilterValue.extract[String])
        logger.info("Business logic: {}", snapshotOptimizingFilter.orNull)
       
        val deltaTimeJValue: JValue = table \ "deltaTime"
        val deltaTime: Option[Int] = if (deltaTimeJValue == JNothing || deltaTimeJValue == JNull)
            Some(10) 
        else
            Some(deltaTimeJValue.extract[Int])

        val incrementalSettings: IncrementalSettings = IncrementalSettings(shallMerge = shallMerge, mergeKey = mergeKey,
            shallVacuumAfterLoad = shallVacuumAfterLoad, customSelectFromStaging = addColumn, isAppendOnly = isAppendOnly,
            incrementalColumn = incrementalColumn, fromOffset = fromOffset, toOffset = toOffset, isSnapshot = isSnapshot,
            fieldsToDeduplicateOn = fieldsToDeduplicateOn, snapshotOptimizingFilter = snapshotOptimizingFilter, autoIncremental = autoIncremental, deltaTime = deltaTime)


        val settings: Some[IncrementalSettings] = Some(incrementalSettings)
        internalConfig = InternalConfig(shallSplit = Some(isSplittable), distKey = distKey, incrementalSettings = settings,
            mapPartitions = partitions, reducePartitions = partitions)

        AppConfiguration(mysqlConf, redshiftConfU, s3Conf, internalConfig)
    }

    def getAppConfigurations(jsonPath: String): PAppConfiguration = {
        var configurations: Seq[AppConfiguration] = Seq[AppConfiguration]()
        implicit val formats = DefaultFormats

        val jsonInputStream: InputStream = new File(jsonPath).toURI.toURL.openStream()
        try {
            val json: JValue = parse(jsonInputStream)
            val offsetManager = (json \ "offsetManager").extract[OffsetManagerConf]

            val details: List[JValue] = (json \ "configuration").extract[List[JValue]]
            for (detail <- details) {
                val mysqlJson: JValue = (detail \ "mysql").extract[JValue]
                val redshiftJson: JValue = (detail \ "redshift").extract[JValue]
                val s3Json: JValue = (detail \ "s3").extract[JValue]
                val tables: List[JValue] = (detail \ "tables").extract[List[JValue]]
                for (table <- tables) {
                    val (mysqlConf: DBConfiguration, redshiftConf: DBConfiguration, s3Conf: S3Config) =
                        getDBsConf(mysqlJson, redshiftJson, s3Json, table)
                    var configuration: AppConfiguration = AppConfiguration(mysqlConf, redshiftConf, s3Conf, null)
                    try {
                        configuration = getAppConfiguration(mysqlConf, redshiftConf, s3Conf, table)
                    } catch {
                        case e: Exception =>
                            configuration = configuration.copy(status = Some(Status(isSuccessful = false, e)))
                    }
                    configurations = configurations :+ configuration
                    logger.info("\n------------- End :: table: {} -------------", (table \ "name").extract[String])
                }
            }
            PAppConfiguration(Some(offsetManager), configurations)
        } finally {
            jsonInputStream.close()
        }
    }

    /**
      * Return list of failed and success configurations.
      *
      * @param configurations table configurations
      * @return list of failed and success configurations
      */
    def failedAndSuccessConfigurations(configurations: Seq[AppConfiguration]): (Seq[AppConfiguration], Seq[AppConfiguration]) = {
        var failedConfigurations: Seq[AppConfiguration] = Seq[AppConfiguration]()
        var successConfigurations: Seq[AppConfiguration] = Seq[AppConfiguration]()

        for (configuration <- configurations) {
            if (configuration.status.isEmpty || !configuration.status.get.isSuccessful)
                failedConfigurations :+= configuration
            else
                successConfigurations :+= configuration
        }
        (failedConfigurations, successConfigurations)
    }

    /**
      * True if configurations has atleast one failed configuration otherwise false.
      *
      * @param pAppConfiguration table configurations
      * @return
      */
    def anyFailures(pAppConfiguration: PAppConfiguration): Boolean = {
        failedAndSuccessConfigurations(pAppConfiguration.configuration)._1.nonEmpty
    }

    def formattedInfoSection(pAppConfiguration: PAppConfiguration): String = {
        val tableSpaceFormatString = "|%4s| %20s| %40s| %20s| %12s| %13s|"
        val header = String.format(tableSpaceFormatString, "SNo", "MySQL DB", "Table Name",
            "Redshift Schema", "isSuccessful", "MigrationTime")

        var formattedString = "-" * header.length + "\n"
        formattedString += header + "\n"
        formattedString += "-" * header.length + "\n"
        var sno = 1
        for (appConf <- pAppConfiguration.configuration) {
            formattedString += String.format(tableSpaceFormatString + "\n", sno.toString,
                appConf.mysqlConf.db, appConf.mysqlConf.tableName, appConf.redshiftConf.schema,
                appConf.status.get.isSuccessful.toString, appConf.migrationTime.get.toString)
            formattedString += "-" * header.length + "\n"
            sno += 1
        }
        formattedString
    }

    /**
      * Pause the application for 2 ^^ MIN(retryCount, 9) - 1 seconds
      *
      * @param retryCount retry count
      * @return
      */
    def exponentialPause(retryCount: Int): Unit = {
        val MAX_RETRIES_COUNT: Double = 9.0
        val exponent: Double = if (retryCount > MAX_RETRIES_COUNT)
            MAX_RETRIES_COUNT
        else retryCount.toDouble
        val timeToSleep = Math.pow(2, exponent).toLong - 1
        logger.info(s"Pausing for $timeToSleep seconds")
        Thread.sleep(timeToSleep * 1000)
    }
}
