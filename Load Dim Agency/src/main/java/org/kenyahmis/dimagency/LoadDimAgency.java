package org.kenyahmis.dimagency;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;

import static org.apache.spark.sql.functions.current_date;
import static org.apache.spark.sql.functions.row_number;

public class LoadDimAgency {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf();
        conf.setAppName("Load Dim Agency");
        SparkSession session = SparkSession.builder()
                .config(conf)
                .getOrCreate();
        RuntimeConfig rtConfig = session.conf();

        final String sourceAgenciesQuery = "SELECT DISTINCT [SDP_Agency] as Agency \n" +
                "FROM dbo.ALL_EMRSites WHERE [SDP_Agency] <>'NULL' AND [SDP_Agency] IS NOT NULL AND [SDP_Agency]<>''";
        Dataset<Row> sourceAgencyDataframe = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("query", sourceAgenciesQuery)
                .load();
        sourceAgencyDataframe.createOrReplaceTempView("source_agency");
        Dataset<Row> dimAgencyDf = session.sql("SELECT upper(Agency) AS AgencyName,current_date() as LoadDate FROM source_agency");
        WindowSpec window = Window.orderBy("AgencyName");
        dimAgencyDf = dimAgencyDf.withColumn("AgencyKey",  row_number().over(window));
        dimAgencyDf = dimAgencyDf.withColumn("LoadDate", current_date());

        dimAgencyDf.write()
                .format("jdbc")
                .option("url", rtConfig.get("spark.edw.url"))
                .option("driver", rtConfig.get("spark.edw.driver"))
                .option("user", rtConfig.get("spark.edw.user"))
                .option("password", rtConfig.get("spark.edw.password"))
                .option("dbtable", "dbo.DimAgency")
                .mode(SaveMode.Overwrite)
                .save();
    }
}
