package com.goibibo.sqlshift.commons

import java.sql.{Connection, DriverManager, ResultSet}
import java.util.Properties

import com.goibibo.sqlshift.models.Configurations.DBConfiguration
import com.goibibo.sqlshift.models.InternalConfs.{DBField, InternalConfig, TableDetails}
import org.apache.spark.sql.{DataFrameReader, SQLContext}
import org.slf4j.LoggerFactory

import scala.collection.immutable.{IndexedSeq, Set}

/**
  * Project: mysql-redshift-loader
  * Author: shivamsharma
  * Date: 12/22/16.
  */
object RedshiftUtil {
    private val logger = LoggerFactory.getLogger(RedshiftUtil.getClass)

    case class RedshiftType(typeName: String,
                            hasPrecision: Boolean = false,
                            hasScale: Boolean = false,
                            precisionMultiplier: Int = 1)

    def getJDBCUrl(conf: DBConfiguration): String = {
        val jdbcUrl = s"jdbc:${conf.database}://${conf.hostname}:${conf.portNo}/${conf.db}"
        if (conf.database.toLowerCase == "mysql")
            jdbcUrl + "?zeroDateTimeBehavior=convertToNull"
        else jdbcUrl
    }

    def getConnection(conf: DBConfiguration): Connection = {
        val connectionProps = new Properties()
        connectionProps.put("user", conf.userName)
        connectionProps.put("password", conf.password)
        val connectionString = getJDBCUrl(conf)
        Class.forName("com.mysql.jdbc.Driver")
        Class.forName("com.amazon.redshift.jdbc4.Driver")
        DriverManager.getConnection(connectionString, connectionProps)
    }

    /**
      * Get all column names and data types from redshift
      *
      * @param dbConf redshift configuration
      * @return
      */
    def getColumnNamesAndTypes(dbConf: DBConfiguration): Map[String, String] = {
        logger.info("Getting all column names for {}", dbConf.toString)
        val query = s"SELECT * FROM ${dbConf.schema}.${dbConf.tableName} WHERE 1 < 0;"
        val connection = RedshiftUtil.getConnection(dbConf)
        val result: ResultSet = connection.createStatement().executeQuery(query)
        try {
            val resultSetMetaData = result.getMetaData
            val count = resultSetMetaData.getColumnCount

            val columnMap = (1 to count).foldLeft(Map[String, String]()) { (set, i) =>

                set + (resultSetMetaData.getColumnName(i).toLowerCase -> resultSetMetaData.getColumnTypeName(i))
            }
            columnMap
        } finally {
            result.close()
            connection.close()
        }
    }

    def getVacuumString(shallVacuumAfterLoad: Boolean, redshiftConf: DBConfiguration): String = {
        if (shallVacuumAfterLoad)
            s"VACUUM DELETE ONLY ${getTableNameWithSchema(redshiftConf)};"
        else ""
    }

    def performVacuum(conf: DBConfiguration): Unit = {
        logger.info("Initiating the connection for vacuum")
        val con = getConnection(conf)
        logger.info("Creating statement for Connection")
        val stmt = con.createStatement()
        val vacuumString = getVacuumString(shallVacuumAfterLoad = true, conf)
        logger.info("Running command {}", vacuumString)
        stmt.executeUpdate(vacuumString)
        stmt.close()
        con.close()
    }

    def getDropCommand(conf: DBConfiguration): String = {
        val tableNameWithSchema =
            if (conf.schema != null && conf.schema != "")
                s"${conf.schema}.${conf.tableName}"
            else
                conf.tableName
        s"DROP TABLE IF EXISTS $tableNameWithSchema CASCADE;"
    }

    def createRedshiftTable(con: Connection, conf: DBConfiguration, createTableQuery: String, overwrite: Boolean = true): Unit = {
        val stmt = con.createStatement()
        if (overwrite) {
            stmt.executeUpdate(getDropCommand(conf))
        }
        stmt.executeUpdate(createTableQuery)
        stmt.close()
    }

    def getTableDetails(con: Connection, conf: DBConfiguration, internalConfig: InternalConfig)
                       (implicit crashOnInvalidType: Boolean): TableDetails = {
        val stmt = con.createStatement()
        val query = s"SELECT * from ${conf.db}.${conf.tableName} where 1 < 0"
        val rs = stmt.executeQuery(query)
        val rsmd = rs.getMetaData
        val validFieldTypes = mysqlToRedshiftTypeConverter.keys.toSet
        var validFields = Seq[DBField]()
        var invalidFields = Seq[DBField]()

        var setColumns = scala.collection.immutable.Set[String]()

        for (i <- 1 to rsmd.getColumnCount) {
            val columnType = rsmd.getColumnTypeName(i)
            val precision = rsmd.getPrecision(i)
            val scale = rsmd.getScale(i)

            if (validFieldTypes.contains(columnType.toUpperCase)) {
                val redshiftColumnType = convertMySqlTypeToRedshiftType(columnType, precision, scale)
                val javaTypeMapping = {
                    if (redshiftColumnType == "TIMESTAMP" || redshiftColumnType == "DATE") Some("String")
                    else if (columnType == "TINYINT UNSIGNED") Some("Integer")
                    else None
                }
                validFields = validFields :+ DBField(rsmd.getColumnName(i), redshiftColumnType, javaTypeMapping)

            } else {
                if (crashOnInvalidType)
                    throw new IllegalArgumentException(s"Invalid type $columnType")
                invalidFields = invalidFields :+ DBField(rsmd.getColumnName(i), columnType)
            }
            setColumns = setColumns + rsmd.getColumnName(i).toLowerCase
            logger.info(s" column: ${rsmd.getColumnName(i)}, type: ${rsmd.getColumnTypeName(i)}," +
                    s" precision: ${rsmd.getPrecision(i)}, scale:${rsmd.getScale(i)}\n")
        }
        rs.close()
        stmt.close()
        val sortKeys = getIndexes(con, setColumns, conf)
        val primaryKey = getPrimaryKey(con, conf)
        val distKey = getDistStyleAndKey(setColumns, internalConfig, primaryKey)
        
        TableDetails(validFields, invalidFields, sortKeys, distKey, primaryKey)
    }

    def getTableNameWithSchema(rc: DBConfiguration): String = {
        if (rc.schema != null && rc.schema != "") s"${rc.schema}.${rc.tableName}"
        else s"${rc.tableName}"
    }

    def getCreateTableString(td: TableDetails, rc: DBConfiguration, stagingTableName: Option[String] = None, isSnapshot: Boolean = false): String = {
        val tableNameWithSchema = getTableNameWithSchema(rc)
        if (stagingTableName.isDefined) {
            val fieldNames = td.validFields.map(r => s"""\t"${r.fieldName}" """).mkString(",\n")
            s"""
               | CREATE TABLE ${stagingTableName.get} AS
               | SELECT $fieldNames FROM $tableNameWithSchema LIMIT 0
            """.stripMargin
        } else if (isSnapshot){
            val fieldNames = td.validFields.map(r => s"""\t"${r.fieldName}" ${r.fieldType} ENCODE ZSTD """).mkString(",\n")
            val distributionKey = td.distributionKey match {
                case None => "DISTSTYLE EVEN"
                case Some(key) => s"""DISTSTYLE KEY \nDISTKEY ( "$key" ) """
            }
            val sortKeys = if (td.sortKeys.nonEmpty) "SORTKEY ( \"" + td.sortKeys.mkString("\", \"") + "\" )" else ""

            s"""CREATE TABLE IF NOT EXISTS $tableNameWithSchema (
               |    $fieldNames
               |)
               |$distributionKey
               |$sortKeys ;""".stripMargin
        } else {
            val fieldNames = td.validFields.map(r => s"""\t"${r.fieldName}" ${r.fieldType} ENCODE ZSTD """).mkString(",\n")
            val distributionKey = td.distributionKey match {
                case None => "DISTSTYLE EVEN"
                case Some(key) => s"""DISTSTYLE KEY \nDISTKEY ( "$key" ) """
            }
            val sortKeys = if (td.sortKeys.nonEmpty) "INTERLEAVED SORTKEY ( \"" + td.sortKeys.mkString("\", \"") + "\" )" else ""

            s"""CREATE TABLE IF NOT EXISTS $tableNameWithSchema (
               |    $fieldNames
               |)
               |$distributionKey
               |$sortKeys ;""".stripMargin
        }
    }

    //Use this method to get the columns to extract
    //Use sqoop 's --columns option to only request the valid columns
    //Use sqoop 's --map-column-java option to request for the fields that needs typeChange
    def getValidFieldNames(mysqlConfig: DBConfiguration, internalConfig: InternalConfig)(implicit crashOnInvalidType: Boolean): TableDetails = {
        val con = getConnection(mysqlConfig)
        val tableDetails = getTableDetails(con, mysqlConfig, internalConfig)(crashOnInvalidType)
        con.close()
        tableDetails
    }

    def getPrimaryKey(con:Connection, conf: DBConfiguration):Option[String] = {
        //TOOD: Make get primary keys a function
        val meta = con.getMetaData
        val resPrimaryKeys = meta.getPrimaryKeys(conf.db, null, conf.tableName)
        var primaryKeys = scala.collection.immutable.Set[String]()

        while (resPrimaryKeys.next) {
            val columnName = resPrimaryKeys.getString(4)
            if (setColumns.contains(columnName.toLowerCase)) {
                primaryKeys = primaryKeys + columnName
            } else {
                logger.warn(s"Rejected $columnName")
            }
        }
        resPrimaryKeys.close()

        if (primaryKeys.isEmpty) {
            logger.error(s"Found no primary keys...")
            None
        } else if(primaryKeys.size > 1) {
            logger.error(s"Found multiple primary keys therefore not picking up any: ${primaryKeys.mkString(",")}")
            None
        } else {
            logger.info(s"Found primary keys, distribution key is. ${primaryKeys.toSeq.head}")
            Some(primaryKeys.toSeq.head)
        }
    }

    def getDistStyleAndKey(setColumns: Set[String], internalConfig: InternalConfig,
                            primaryKey:Option[String]): Option[String] = {
        internalConfig.distKey match {
            case Some(key) => {
                logger.info("Found distKey in configuration {}", key)
                Some(key)
            }
            case None => {
                logger.info("Found no distKey in configuration")

                if(internalConfig.incrementalSettings.isDefined) {
                    val incrementalFieldName = internalConfig.incrementalSettings.get.incrementalColumn.get
                    if(!primaryKeys.contains(incrementalFieldName)) {
                        logger.error("Incremental field is not indexed")
                        //TODO: Crash the code and run only if sudo mode is on
                    }
                    Some(incrementalFieldName)
                } else {
                    primaryKey
                }
            }
        }
    }

    def getIndexes(con: Connection, setColumns: Set[String], conf: DBConfiguration): IndexedSeq[String] = {
        val meta = con.getMetaData
        val resIndexes = meta.getIndexInfo(conf.db, null, conf.tableName, false, false)
        var setIndexedColumns = scala.collection.immutable.Set[String]()
        while (resIndexes.next) {
            val columnName = resIndexes.getString(9)
            if (setColumns.contains(columnName.toLowerCase)) {
                setIndexedColumns = setIndexedColumns + columnName
            } else {
                System.err.println(s"Rejected $columnName")
            }
        }
        resIndexes.close()
        // Redshift can only have 8 interleaved sort keys
        setIndexedColumns.toIndexedSeq.take(8)
    }

    def convertMySqlTypeToRedshiftType(columnType: String, precision: Int, scale: Int): String = {
        val redshiftType: RedshiftType = mysqlToRedshiftTypeConverter(columnType.toUpperCase)
        //typeName:String, hasPrecision:Boolean = false, hasScale:Boolean = false, precisionMultiplier:Int
        val result = if (redshiftType.hasPrecision && redshiftType.hasScale) {
            s"${redshiftType.typeName}( ${precision * redshiftType.precisionMultiplier}, $scale )"
        } else if (redshiftType.hasPrecision) {
            var redshiftPrecision = precision * redshiftType.precisionMultiplier
            if (redshiftPrecision < 0 || redshiftPrecision > 65535)
                redshiftPrecision = 65535
            s"${redshiftType.typeName}( $redshiftPrecision )"
        } else {
            redshiftType.typeName
        }
        logger.info(s"Converted type: $columnType, precision: $precision, scale:$scale to $result")
        result
    }

    val mysqlToRedshiftTypeConverter: Map[String, RedshiftType] = {
        val maxVarcharSize = 65535
        Map(
            "TINYINT" -> RedshiftType("INT2"),
            "TINYINT UNSIGNED" -> RedshiftType("INT2"),
            "SMALLINT" -> RedshiftType("INT2"),
            "SMALLINT UNSIGNED" -> RedshiftType("INT4"),
            "MEDIUMINT" -> RedshiftType("INT4"),
            "MEDIUMINT UNSIGNED" -> RedshiftType("INT4"),
            "INT" -> RedshiftType("INT4"),
            "INT UNSIGNED" -> RedshiftType("INT8"),
            "BIGINT" -> RedshiftType("INT8"),
            "BIGINT UNSIGNED" -> RedshiftType("INT8"), //Corner case indeed makes this buggy, Just hoping that it does not occure!
            "FLOAT" -> RedshiftType("FLOAT4"),
            "FLOAT UNSIGNED" -> RedshiftType("FLOAT4"),
            "DOUBLE" -> RedshiftType("FLOAT8"),
            "DOUBLE UNSIGNED" -> RedshiftType("FLOAT8"),
            "DECIMAL" -> RedshiftType("FLOAT8"),
            "CHAR" -> RedshiftType("VARCHAR", hasPrecision = true, hasScale = false, 4),
            "VARCHAR" -> RedshiftType("VARCHAR", hasPrecision = true, hasScale = false, 4),
            "TINYTEXT" -> RedshiftType("VARCHAR(1024)"),
            "TEXT" -> RedshiftType(s"VARCHAR($maxVarcharSize)"),
            "MEDIUMTEXT" -> RedshiftType(s"VARCHAR($maxVarcharSize)"),
            "LONGTEXT" -> RedshiftType(s"VARCHAR($maxVarcharSize)"),
            "JSON" -> RedshiftType(s"VARCHAR($maxVarcharSize)"),
            "BOOLEAN" -> RedshiftType("BOOLEAN"),
            "BOOL" -> RedshiftType("BOOLEAN"),
            "ENUM" -> RedshiftType("VARCHAR(255)"),
            "SET" -> RedshiftType("VARCHAR(255)"),
            "DATE" -> RedshiftType("DATE"),
            "TIME" -> RedshiftType("TIMESTAMP"),
            "DATETIME" -> RedshiftType("TIMESTAMP"),
            "TIMESTAMP" -> RedshiftType("TIMESTAMP"),
            "YEAR" -> RedshiftType("INT")
        )
    }

    def getDataFrameReader(mysqlConfig: DBConfiguration, sqlQuery: String, sqlContext: SQLContext): DataFrameReader = {
        sqlContext.read.format("jdbc").
                option("url", getJDBCUrl(mysqlConfig)).
                option("dbtable", sqlQuery).
                option("driver", "com.mysql.jdbc.Driver").
                option("user", mysqlConfig.userName).
                option("password", mysqlConfig.password)
    }
}
