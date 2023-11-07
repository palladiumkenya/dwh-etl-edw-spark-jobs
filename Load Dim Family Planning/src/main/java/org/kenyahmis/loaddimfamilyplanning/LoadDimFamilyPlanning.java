package org.kenyahmis.loaddimfamilyplanning;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.spark.sql.functions.row_number;

public class LoadDimFamilyPlanning {
    private static final Logger logger = LoggerFactory.getLogger(LoadDimFamilyPlanning.class);

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("Load Family Planning Dimension");

        SparkSession session = SparkSession.builder()
                .config(conf)
                .getOrCreate();

        RuntimeConfig rtConfig = session.conf();

        // Family Planning source
        logger.info("Loading Family Planning source");
        Dataset<Row> familyPlanningSourceDataDf = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("query", "select \n" +
                        "distinct FamilyPlanningMethod as FamilyPlanning\n" +
                        "from ODS.dbo.CT_PatientVisits\n" +
                        "where FamilyPlanningMethod <> 'NULL' and FamilyPlanningMethod <>''")
                .load();
        familyPlanningSourceDataDf.persist(StorageLevel.DISK_ONLY());
        familyPlanningSourceDataDf.createOrReplaceTempView("source_FamilyPlanning");
        familyPlanningSourceDataDf.printSchema();

        // load Family Planning Dim
        Dataset<Row> dimFamilyPlanning = session.sql("select source_FamilyPlanning.*, current_date() as LoadDate " +
                "from source_FamilyPlanning");

        dimFamilyPlanning
                .write()
                .format("jdbc")
                .option("url", rtConfig.get("spark.edw.url"))
                .option("driver", rtConfig.get("spark.edw.driver"))
                .option("user", rtConfig.get("spark.edw.user"))
                .option("password", rtConfig.get("spark.edw.password"))
                .option("dbtable", "dbo.DimFamilyPlanning")
                .mode(SaveMode.Overwrite)
                .save();
    }
}