package org.kenyahmis.dimtreatmenttype;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.spark.sql.functions.row_number;

public class LoadDimTreatmentType {
    private static final Logger logger = LoggerFactory.getLogger(LoadDimTreatmentType.class);

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("Load Dim Treatment Type");
        SparkSession session = SparkSession.builder()
                .config(conf)
                .getOrCreate();
        RuntimeConfig rtConfig = session.conf();

        // Treatment type source
        logger.info("Loading Treatment Type source");
        Dataset<Row> treatmentTypeSourceDataDf = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("query", "select \n" +
                        "distinct TreatmentType as TreatmentType,\n" +
                        "case when TreatmentType in ('ARV','HIV Treatment') Then 'ART'\n" +
                        "when TreatmentType='Hepatitis B' Then 'Non-ART'\n" +
                        "Else TreatmentType End As TreatmentType_Cleaned\n" +
                        "from dbo.CT_PatientPharmacy\n" +
                        "where TreatmentType <> 'NULL' and TreatmentType <>''")
                .load();
        treatmentTypeSourceDataDf.persist(StorageLevel.DISK_ONLY());
        treatmentTypeSourceDataDf.createOrReplaceTempView("source_TreatmentType");
        treatmentTypeSourceDataDf.printSchema();

        // load dim treatment types
        Dataset<Row> dimTreatmentTypes = session.sql("select source_TreatmentType.*, current_date() as LoadDate " +
                "from source_TreatmentType");

        WindowSpec window = Window.orderBy("TreatmentType");
        dimTreatmentTypes = dimTreatmentTypes.withColumn("TreatmentTypeKey",  row_number().over(window));
        dimTreatmentTypes
                .write()
                .format("jdbc")
                .option("url", rtConfig.get("spark.edw.url"))
                .option("driver", rtConfig.get("spark.edw.driver"))
                .option("user", rtConfig.get("spark.edw.user"))
                .option("password", rtConfig.get("spark.edw.password"))
                .option("dbtable", "dbo.DimTreatmentType")
                .mode(SaveMode.Overwrite)
                .save();
    }
}
