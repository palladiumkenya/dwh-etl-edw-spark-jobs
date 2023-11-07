package org.kenyahmis.loaddrugdimension;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.spark.sql.functions.row_number;

public class LoadDrugDimension {
    private static final Logger logger = LoggerFactory.getLogger(LoadDrugDimension.class);

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("Load Drug Dimension");

        SparkSession session = SparkSession.builder()
                .config(conf)
                .getOrCreate();

        RuntimeConfig rtConfig = session.conf();
        String query = "select \n" +
                "distinct Drug as Drug\n" +
                "from dbo.CT_PatientPharmacy\n" +
                "where Drug <> 'NULL' and Drug <>'' and TreatmentType='ARV'";
        Dataset<Row> dimDrugDataFrame = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("query", query)
                .load();

        dimDrugDataFrame.persist(StorageLevel.DISK_ONLY());
        dimDrugDataFrame.createOrReplaceTempView("source_drug");
        Dataset<Row> dimDrugFinalDf = session
                .sql("select source_drug.*,current_date() as LoadDate from source_drug");

        dimDrugFinalDf.printSchema();
        final int writePartitions = 20;
        dimDrugFinalDf
                .repartition(writePartitions)
                .write()
                .format("jdbc")
                .option("url", rtConfig.get("spark.edw.url"))
                .option("driver", rtConfig.get("spark.edw.driver"))
                .option("user", rtConfig.get("spark.edw.user"))
                .option("password", rtConfig.get("spark.edw.password"))
                .option("dbtable", "dbo.DimDrug")
                .option("truncate", "true")
                .mode(SaveMode.Overwrite)
                .save();

    }
}
