package com.goibibo.sqlshift.commons

import java.util.Properties
import java.util.regex._

import com.goibibo.sqlshift.models.Configurations.{DBConfiguration, S3Config}
import com.goibibo.sqlshift.models.InternalConfs.{IncrementalSettings, InternalConfig, TableDetails, DBField}
import org.apache.spark.sql._
import org.apache.spark.sql.types.DataType._
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.immutable.Seq

/*
--packages "org.apache.hadoop:hadoop-aws:2.7.2,com.databricks:spark-redshift_2.10:1.1.0,com.amazonaws:aws-java-sdk:1.7.4,mysql:mysql-connector-java:5.1.39"
--jars=<Some-location>/RedshiftJDBC4-1.1.17.1017.jar
*/

object MySQLToRedshiftMigrator {
    private val logger: Logger = LoggerFactory.getLogger(MySQLToRedshiftMigrator.getClass)

    private def getFromAndToOffset(incrementalSettings: Option[IncrementalSettings], 
        distKeyType:Option[DataType], minMax:(Any,Any)): Option[(Any, Any)] = {
        
        distKeyType.isDefined match {
            case Some(a:IntegerType) => {
                // If it's not incremental then take min time from minMax
                // If it's incremental then use fromOffset
            }
            case Some(a:TimestampType) => {
                // If it's not incremental then take min time from minMax
                // If it's incremental then use fromOffset, If delta is definited then 
                //      subtract delta from fromOffset
                val fromOffset  = incrementalSettings.map.deltaTime.
                                    .flatMap()
                                    map(d => s"date_sub('${fromOffset.get}' , INTERVAL '${d}' MINUTE )").
                                    getOrElse(incrementalSettings.fromOffset)
                val toOffset    = minMax._2
                Some(fromOffset, toOffset)
            }
        } else None
    }

    private def getWhereCondition(incrementalSettings: IncrementalSettings): Option[String] = {

        val deltaTime = incrementalSettings.deltaTime
        val column = incrementalSettings.incrementalColumn
        val fromOffset = incrementalSettings.fromOffset
        val toOffset = incrementalSettings.toOffset
        logger.info(s"Found incremental condition. Column: ${column.orNull}, fromOffset: " +
                s"${fromOffset.orNull}, toOffset: ${toOffset.orNull}, deltaTime : ${deltaTime.orNull}")
        if (column.isDefined && fromOffset.isDefined && toOffset.isDefined) {
            Some(s"${column.get} BETWEEN date_sub('${fromOffset.get}' , INTERVAL '${deltaTime.get}' MINUTE ) AND '${toOffset.get}'")
        } else if (column.isDefined && fromOffset.isDefined) {
            Some(s"${column.get} >= date_sub('${fromOffset.get}' , INTERVAL '${deltaTime.get}' MINUTE )")
        } else if (column.isDefined && toOffset.isDefined) {
            Some(s"${column.get} <= date_sub('${fromOffset.get}' , INTERVAL '${deltaTime.get}' MINUTE )")
        } else {
            logger.info("Either of column or (fromOffset/toOffset) is not provided")
            None
        }
    }

    /**
      * Load table in spark.
      *
      * @param mysqlConfig        Mysql connection configuration
      * @param sqlContext         Spark SQLContext
      * @param crashOnInvalidType ToDo: What is this?
      * @return
      */
    def loadToSpark(mysqlConfig: DBConfiguration, sqlContext: SQLContext, internalConfig: InternalConfig = new InternalConfig)
                   (implicit crashOnInvalidType: Boolean): (DataFrame, TableDetails) = {

        logger.info("Loading table to Spark from MySQL")
        logger.info("MySQL details: \n{}", mysqlConfig.toString)
        val tableDetails: TableDetails = RedshiftUtil.getValidFieldNames(mysqlConfig, internalConfig)
        logger.info("Table details: \n{}", tableDetails.toString)
        SqlShiftMySQLDialect.registerDialect()
        val partitionDetails: Option[Seq[String]] = internalConfig.shallSplit match {
            case Some(false) =>
                logger.info("shallSplit is false")
                None
            case _ =>
                logger.info("shallSplit either not set or true")
                tableDetails.distributionKey match {
                    case Some(distKey) =>
                        val typeOfDistKey = tableDetails.validFields.filter(_.fieldName == distKey).head.fieldType
                        
                        val distKeyType:Option[DataType] = {
                            if(typeOfDistKey == "TIMESTAMP") Some(TimestampType)
                            else if typeOfDistKey.startsWith("INT") Some(IntegerType)
                            else None
                        }

                        if ( distKeyType.isDefined && 
                                (distKeyType.get == TimestampType || distKeyType.get == IntegerType)) {
                            //minMax needs return minmax in specifc datatype
                            val minMaxTmp: (Any, Any) = Util.getMinMax(mysqlConfig, distKey, distKeyType.get, whereCondition)


                            //Below implementation will stay for datatype Int
                            val minMax: (Long, Long) = (minMaxTmp._1.toLong, minMaxTmp._2.toLong)
                            val nr: Long = minMax._2 - minMax._1 + 1

                            val mapPartitions = internalConfig.mapPartitions match {
                                case Some(partitions) => partitions
                                case None => Util.getPartitions(sqlContext, mysqlConfig, minMax)
                            }
                            if (mapPartitions == 0) {
                                None
                            } else {
                                val inc: Long = Math.ceil(nr.toDouble / mapPartitions).toLong
                                val predicates = (0 until mapPartitions).toList.
                                        map { n =>
                                            s"$distKey BETWEEN ${minMax._1 + n * inc} AND ${minMax._1 - 1 + (n + 1) * inc} "
                                        }.
                                        map(c => if (whereCondition.isDefined) c + s"AND (${whereCondition.get})" else c)
                                Some(predicates)
                            }
                        }
                        else {
                            logger.warn(s"primary keys is non INT $typeOfPrimaryKey")
                            None
                        }
                    case None =>
                        logger.warn("No Distribution key found!!!")
                        None
                }
        }

        val partitionedReader: DataFrame = partitionDetails match {
            case Some(predicates) =>
                logger.info("Using partitionedRead {}", predicates)
                val properties = new Properties()
                properties.setProperty("user", mysqlConfig.userName)
                properties.setProperty("password", mysqlConfig.password)

                sqlContext.read.
                        option("driver", "com.mysql.jdbc.Driver").
                        option("fetchSize", Integer.MIN_VALUE.toString).
                        option("fetchsize", Integer.MIN_VALUE.toString).
                        option("user", mysqlConfig.userName).
                        option("password", mysqlConfig.password).
                        jdbc(RedshiftUtil.getJDBCUrl(mysqlConfig), mysqlConfig.tableName, predicates.toArray, properties)
            case None =>
                val tableQuery = internalConfig.incrementalSettings match {
                    case Some(incrementalSettings) =>
                        val whereCondition = getWhereCondition(incrementalSettings)
                        s"""(SELECT * from ${mysqlConfig.tableName}${if (whereCondition.isDefined) " WHERE " + whereCondition.get else ""}) AS A"""
                    case None => mysqlConfig.tableName
                }
                logger.info("Using single partition read query = {}", tableQuery)
                val dataReader = RedshiftUtil.getDataFrameReader(mysqlConfig, tableQuery, sqlContext)
                dataReader.load
        }
        val data = partitionedReader.selectExpr(tableDetails.validFields.map(_.fieldName): _*)
        val dataWithTypesFixed = tableDetails.validFields.filter(_.javaType.isDefined).foldLeft(data) {
            (df, dbField) => {
                val modifiedCol = df.col(dbField.fieldName).cast(dbField.javaType.get)
                df.withColumn(dbField.fieldName, modifiedCol)
            }
        }
        logger.info("Table load in spark is finished!!!")
        (dataWithTypesFixed, tableDetails)
    }

    /**
      * Queries for identifying changed records and updating the endtime of corresponding records in original table
      *
      * @param redshiftTableName        Original table
      * @param redshiftStagingTableName Staging table containing new records
      * @param mergeKey                 Key on which table will be merged
      * @param fieldsToDeduplicateOn    A change in any of these fields should create a new record in original table
      * @param incrementalColumn        Column containing the timestamp on which it is updated
      * @param tableDetails             Contains all the table details like fieldnames, distkey and sortkey.
      *
      */
    def getSnapshotCreationSql(redshiftTableName: String, redshiftStagingTableName:String, mergeKey:String,
                               fieldsToDeduplicateOn:Seq[String], incrementalColumn:String, tableDetails: TableDetails, snapshotOptimizingFilter: String): String = {

        val tableColumns = "\"" + tableDetails.validFields.map(_.fieldName).mkString("\", \"") + "\""
        val deDuplicateFieldNames = "\"" + fieldsToDeduplicateOn.mkString("\", \"") + "\""
        val deDuplicateCondition = fieldsToDeduplicateOn.map(x => "nvl(s.\""+ x +"\"::varchar,'') = nvl(o.\""+ x +"\"::varchar,'')").mkString(" and ")
        val changedFilter = snapshotOptimizingFilter.replace("{{o}}",redshiftTableName).replace("{{s}}",redshiftStagingTableName)
        val optimiserCondition = if (snapshotOptimizingFilter.nonEmpty) s" and $changedFilter " else ""


        s"""create temp table changed_records
           |diststyle key
           |distkey("$mergeKey")
           |sortkey("$mergeKey",$deDuplicateFieldNames) as
           |(
           |   select s.* from $redshiftStagingTableName s
           |   left join (select * from $redshiftTableName where endtime is null $optimiserCondition) o
           |   on (s."$mergeKey" = o."$mergeKey" and $deDuplicateCondition)
           |   where o."$mergeKey" is null
           |);
           |update $redshiftTableName set endtime = c."$incrementalColumn"
           |from changed_records c
           |where $redshiftTableName."$mergeKey" = c."$mergeKey" and $redshiftTableName.endtime is null $optimiserCondition;
           |insert into $redshiftTableName($tableColumns)
           |   select *, "$incrementalColumn" as starttime, null::timestamp as endtime
           |   from changed_records;""".stripMargin
    }


    /**
      * Store Dataframe to Redshift table. Drop table if exists.
      * It table doesn't exist it will create table.
      *
      * @param df             dataframe
      * @param tableDetails   valid and not valid field details
      * @param redshiftConf   redshift configuration
      * @param s3Conf         s3 configuration
      * @param sqlContext     spark SQL context
      * @param internalConfig Information about incremental and partitions
      */
    def storeToRedshift(df: DataFrame, tableDetails: TableDetails, redshiftConf: DBConfiguration, s3Conf: S3Config,
                        sqlContext: SQLContext, internalConfig: InternalConfig = new InternalConfig): Unit = {

        logger.info("Storing to Redshift")
        logger.info("Redshift Details: \n{}", redshiftConf.toString)
        if (s3Conf.accessKey.isDefined && s3Conf.secretKey.isDefined) {
            sqlContext.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", s3Conf.accessKey.get)
            sqlContext.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", s3Conf.secretKey.get)
        }

        val redshiftTableName = RedshiftUtil.getTableNameWithSchema(redshiftConf)
        val stagingPrepend = "_staging" + {
            val r = scala.util.Random
            r.nextInt(10000)
        }
        val redshiftStagingTableName = redshiftTableName + stagingPrepend
        val dropTableString = RedshiftUtil.getDropCommand(redshiftConf)
        logger.info("dropTableString {}", dropTableString)

        val shallOverwrite = internalConfig.shallOverwrite match {
            case None =>
                internalConfig.incrementalSettings.get.incrementalColumn match {
                    case None =>
                        logger.info("internalConfig.shallOverwrite is None and internalConfig.incrementalSettings is None")
                        true
                    case Some(_) =>
                        logger.info("internalConfig.shallOverwrite is None and internalConfig.incrementalSettings is Some")
                        false
                }
            case Some(sow) =>
                logger.info("internalConfig.shallOverwrite is {}", sow)
                sow
        }


        val (dropStagingTableString: String, mergeKey: String, shallVacuumAfterLoad: Boolean,
            customFields: Seq[String],incrementalColumn: String, isSnapshot: Boolean, fieldsToDeduplicateOn: Option[Seq[String]], snapshotOptimizingFilter: Option[String]) = {
            internalConfig.incrementalSettings match {
                case None =>
                    logger.info("No dropStagingTableString and No vacuum, internalConfig.incrementalSettings is None")
                    ("", "", false, Seq[String](),"", false, None, None)
                case Some(IncrementalSettings(shallMerge, stagingTableMergeKey, vaccumAfterLoad, cs, true, incrementalColumn, fromOffset, toOffset, isSnapshot, fieldsToDeduplicateOn, snapshotOptimizingFilter, deltaTime, _)) =>
                    logger.info("Incremental update is append only")
                    ("", "", false, Seq[String](),incrementalColumn, false, None, None)
                case Some(IncrementalSettings(shallMerge, stagingTableMergeKey, vaccumAfterLoad, cs, false, incrementalColumn, fromOffset, toOffset, isSnapshot, fieldsToDeduplicateOn, snapshotOptimizingFilter, deltaTime, _)) =>
                    val dropStatingTableStr = if (shallMerge || isSnapshot) s"DROP TABLE IF EXISTS $redshiftStagingTableName;" else ""

                    logger.info(s"dropStatingTableStr = {}", dropStatingTableStr)
                    val mKey: String = {
                        if (shallMerge || isSnapshot) {
                            stagingTableMergeKey match {
                                case None =>
                                    logger.info("mergeKey is also not provided, we use primary key in this case {}",
                                        tableDetails.distributionKey.get)
                                    //If primaryKey is not available and mergeKey is also not provided
                                    //This means wrong input, get will crash if tableDetails.distributionKey is None
                                    tableDetails.distributionKey.get
                                case Some(mk) =>
                                    logger.info(s"Found mergeKey = {}", mk)
                                    mk
                            }
                        } else {
                            logger.info(s"Shall merge is not specified passing mergeKey as empty")
                            ""
                        }
                    }
                    val customFieldsI = cs match {
                        case Some(customSelect) =>
                            val pattern = Pattern.compile("(?:AS|as)\\s*(\\w+)\\s*(?:,|$)")
                            val matcher = pattern.matcher(customSelect)
                            val cf = scala.collection.mutable.ListBuffer.empty[String]
                            while (matcher.find()) {
                                val matched = matcher.group(1)
                                cf += matched
                                logger.info("matched => {}", matched)
                            }
                            cf.toSeq
                        case None => Seq[String]()
                    }
                    (dropStatingTableStr, mKey, vaccumAfterLoad, customFieldsI, incrementalColumn.getOrElse(""),
                      isSnapshot, fieldsToDeduplicateOn, snapshotOptimizingFilter)
            }
        }

        val extraFields = tableDetails.validFields ++ Seq(DBField("starttime","timestamp"), DBField("endtime","timestamp"))
        val extraSortKeys = Seq(mergeKey, "starttime", "endtime")
        val tableDetailsExtra = tableDetails.copy(validFields = extraFields, sortKeys = extraSortKeys)

        val createTableString: String = if(isSnapshot){
            RedshiftUtil.getCreateTableString(tableDetailsExtra, redshiftConf, None, true)
        }
        else {
            RedshiftUtil.getCreateTableString(tableDetails, redshiftConf)
        }
        //val redshiftStagingConf = redshiftConf.copy(tableName = redshiftConf.tableName + stagingPrepend
        val createStagingTableString = RedshiftUtil.getCreateTableString(tableDetails, redshiftConf, Some(redshiftStagingTableName))
        logger.info("createTableString {}", createTableString)

        val dropAndCreateTableString = if (shallOverwrite) dropTableString + "\n" + createTableString else createTableString

        val preActions: String = {
            redshiftConf.preLoadCmd.map(_.trim).map{ cmd =>
                cmd + (if (cmd.endsWith(";")) "" else ";") + "\n"
            }.getOrElse("") +
                    dropAndCreateTableString + {
                if (dropStagingTableString.nonEmpty && !isSnapshot) {
                    dropStagingTableString +
                            alterTableQuery(tableDetails, redshiftConf, customFields) +
                            "\n" + createStagingTableString
                } else if (dropStagingTableString.nonEmpty && isSnapshot) {
                    dropStagingTableString + alterTableQuery(tableDetailsExtra, redshiftConf, customFields) +
                      "\n" + createStagingTableString
                } else {
                    ""
                }
            }
        }

        val stagingTablePostActions = if (dropStagingTableString.nonEmpty && !isSnapshot) {
            val tableColumns = "\"" + tableDetails.validFields.map(_.fieldName).mkString("\", \"") + "\""

            s"""DELETE FROM $redshiftTableName USING $redshiftStagingTableName
               |    WHERE $redshiftTableName.$mergeKey = $redshiftStagingTableName.$mergeKey; """.stripMargin +
                    "\n" + {
                if (customFields.isEmpty) {
                    // Handling columns order mismatch
                    s"""INSERT INTO $redshiftTableName ($tableColumns)
                       |SELECT $tableColumns FROM $redshiftStagingTableName;""".stripMargin
                } else {
                    val customFieldsStr = "\"" + customFields.mkString("\", \"") + "\""
                    val allColumnsPlusCustomOnes = s"( $tableColumns, $customFieldsStr )"
                    logger.info("allColumnsPlusCustomOnes => {}", allColumnsPlusCustomOnes)
                    val customSelect: String = internalConfig.incrementalSettings.get.customSelectFromStaging.get
                    s"""INSERT INTO $redshiftTableName $allColumnsPlusCustomOnes
                       |SELECT *, $customSelect FROM $redshiftStagingTableName;""".stripMargin
                }
            }
        } else if (dropStagingTableString.nonEmpty && isSnapshot){
            if (fieldsToDeduplicateOn.isEmpty)
                throw new RequiredFieldNotPresentException("fieldsToDeduplicateOn is not present")
            getSnapshotCreationSql(redshiftTableName, redshiftStagingTableName, mergeKey, fieldsToDeduplicateOn.get, incrementalColumn, tableDetailsExtra, snapshotOptimizingFilter.getOrElse(""))
        } else {
            ""
        }

        val postActions: String = Seq[String](stagingTablePostActions,
            redshiftConf.postLoadCmd.map(_.trim).map { cmd =>
                val changedCmd = cmd.replace("{{s}}", redshiftStagingTableName)
                changedCmd + (if (changedCmd.endsWith(";")) "" else ";") + "\n"
            }.getOrElse(""),
            dropStagingTableString
        ).filter(_.trim != "").mkString("\n")

        logger.info("Redshift PreActions = {}", preActions)
        logger.info("Redshift PostActions = {}", postActions)

        val redshiftWriteMode = if (dropStagingTableString == "") "append" else "overwrite"
        logger.info("Redshift write mode: {}", redshiftWriteMode)


        val redshiftTableNameForIngestion = if (dropStagingTableString != "") redshiftStagingTableName
        else redshiftTableName

        logger.info("redshiftTableNameForIngestion: {}", redshiftTableNameForIngestion)

        val redshiftWriterPartitioned: DataFrame = internalConfig.reducePartitions match {
            case Some(reducePartitions) =>
                if (df.rdd.getNumPartitions == reducePartitions)
                    df.repartition(reducePartitions)
                else
                    df
            case None => df
        }
        val extracopyoptions = if (dropStagingTableString != "") {
            "TRUNCATECOLUMNS COMPUPDATE OFF STATUPDATE OFF"
        } else "TRUNCATECOLUMNS COMPUPDATE OFF "

        val redshiftWriter = redshiftWriterPartitioned.write.
                format("com.databricks.spark.redshift").
                option("url", RedshiftUtil.getJDBCUrl(redshiftConf)).
                option("user", redshiftConf.userName).
                option("password", redshiftConf.password).
                option("jdbcdriver", "com.amazon.redshift.jdbc4.Driver").
                option("dbtable", redshiftTableNameForIngestion).
                option("tempdir", s3Conf.s3Location).
                option("extracopyoptions", extracopyoptions).
                mode(redshiftWriteMode)

        val redshiftWriterWithPreActions = {
            if (preActions != "") redshiftWriter.option("preactions", preActions)
            else redshiftWriter
        }

        val redshiftWriterWithPostActions = {
            if (postActions != "") redshiftWriterWithPreActions.option("postactions", postActions)
            else redshiftWriterWithPreActions
        }

        redshiftWriterWithPostActions.save()
        try {
            if (shallVacuumAfterLoad) {
                RedshiftUtil.performVacuum(redshiftConf)
            } else {
                logger.info("Not opting for Vacuum, shallVacuumAfterLoad is false")
            }
        } catch {
            case e: Exception => logger.warn("Vacuum failed for reason", e)
        }
    }

    /**
      * Alter table to add or delete columns in redshift table if any changes occurs in sql table
      *
      * @param tableDetails sql table details
      * @param redshiftConf redshift configuration
      * @return Query of add and delete columns from redshift table
      */
    private def alterTableQuery(tableDetails: TableDetails, redshiftConf: DBConfiguration, customFields: Seq[String]): String = {

        val redshiftTableName: String = RedshiftUtil.getTableNameWithSchema(redshiftConf)
        try {
            val mainTableColumnNames: Set[String] = RedshiftUtil.getColumnNamesAndTypes(redshiftConf).keys.toSet

            // All columns name must be distinct other wise redshift load will fail
            val stagingTableColumnAndTypes: Map[String, String] = tableDetails
                    .validFields
                    .map { td => td.fieldName.toLowerCase -> td.fieldType }
                    .toMap

            val stagingTableColumnNames: Set[String] = (stagingTableColumnAndTypes.keys ++ customFields).toSet
            val addedColumns: Set[String] = stagingTableColumnNames -- mainTableColumnNames
            val deletedColumns: Set[String] = mainTableColumnNames -- stagingTableColumnNames

            val addColumnsQuery = addedColumns.foldLeft("\n") { (query, columnName) =>
                query + s"""ALTER TABLE $redshiftTableName ADD COLUMN "$columnName" """ +
                        stagingTableColumnAndTypes.getOrElse(columnName, "") + ";\n"
            }

            val deleteColumnQuery = deletedColumns.foldLeft("\n") { (query, columnName) =>
                query + s"""ALTER TABLE $redshiftTableName DROP COLUMN "$columnName" ;\n"""
            }

            addColumnsQuery + deleteColumnQuery
        } catch {
            case e: Exception =>
                logger.warn("Error occurred while altering table", e)
                ""
        }
    }
}
