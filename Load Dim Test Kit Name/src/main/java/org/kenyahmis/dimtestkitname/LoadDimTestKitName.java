package org.kenyahmis.dimtestkitname;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.spark.sql.functions.row_number;

public class LoadDimTestKitName {
    private static final Logger logger = LoggerFactory.getLogger(LoadDimTestKitName.class);

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("Load Dim Test Kit Name");
        SparkSession session = SparkSession.builder()
                .config(conf)
                .getOrCreate();
        RuntimeConfig rtConfig = session.conf();

        // Test Kit Names source
        logger.info("Loading Test Kit Name source");
        Dataset<Row> testKitNameSourceDataDf = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("query", "select distinct TestKitName1 as TestKitName from ODS.dbo.HTS_TestKits\n" +
                        "    where TestKitName1 is not null and TestKitName1 <> '' and TestKitName1 <> 'null'\n" +
                        "        union\n" +
                        "    select distinct TestKitName2 as TestKitName from dbo.HTS_TestKits\n" +
                        "    where TestKitName2 is not null and TestKitName2 <> '' and TestKitName2 <> 'null'")
                .load();
        testKitNameSourceDataDf.persist(StorageLevel.DISK_ONLY());
        testKitNameSourceDataDf.createOrReplaceTempView("source_data");
        testKitNameSourceDataDf.printSchema();

        // load dim patients
        Dataset<Row> dimTestKitNames = session.sql("select source_data.*,current_date() as LoadDate from source_data");

        dimTestKitNames
                .write()
                .format("jdbc")
                .option("url", rtConfig.get("spark.edw.url"))
                .option("driver", rtConfig.get("spark.edw.driver"))
                .option("user", rtConfig.get("spark.edw.user"))
                .option("password", rtConfig.get("spark.edw.password"))
                .option("truncate", "true")
                .option("dbtable", "dbo.DimTestKitName")
                .mode(SaveMode.Overwrite)
                .save();
    }
}
