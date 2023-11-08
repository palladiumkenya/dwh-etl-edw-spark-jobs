package org.kenyahmis.loaddimvaccinationstatus;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.spark.sql.functions.row_number;

public class LoadDimVaccinationStatus {
    private static final Logger logger = LoggerFactory.getLogger(LoadDimVaccinationStatus.class);

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("Load Vaccination Status Dimension");

        SparkSession session = SparkSession.builder()
                .config(conf)
                .getOrCreate();

        RuntimeConfig rtConfig = session.conf();

        // Vaccination status source
        logger.info("Loading Vaccination Status source");
        Dataset<Row> vaccinationStatusSourceDataDf = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("query", "select\n" +
                        "distinct VaccinationStatus\n" +
                        "from ODS.dbo.CT_Covid\n" +
                        "where VaccinationStatus <> '' and VaccinationStatus is not null")
                .load();
        vaccinationStatusSourceDataDf.persist(StorageLevel.DISK_ONLY());
        vaccinationStatusSourceDataDf.createOrReplaceTempView("source_vaccination_status");
        vaccinationStatusSourceDataDf.printSchema();

        // load Vaccination status Dim
        Dataset<Row> dimVaccinationStatus = session.sql("select source_vaccination_status.*, current_date() as LoadDate " +
                "from source_vaccination_status");
        dimVaccinationStatus
                .write()
                .format("jdbc")
                .option("url", rtConfig.get("spark.edw.url"))
                .option("driver", rtConfig.get("spark.edw.driver"))
                .option("user", rtConfig.get("spark.edw.user"))
                .option("password", rtConfig.get("spark.edw.password"))
                .option("truncate", "true")
                .option("dbtable", "dbo.DimVaccinationStatus")
                .mode(SaveMode.Overwrite)
                .save();
    }
}