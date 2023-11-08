package org.kenyahmis.dimpartners;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;

import static org.apache.spark.sql.functions.row_number;

public class LoadDimPartners {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("Load Dim Partner");
        SparkSession session = SparkSession.builder()
                .config(conf)
                .getOrCreate();
        RuntimeConfig rtConfig = session.conf();

        final String sourcePartnersQuery = "SELECT \n" +
                "DISTINCT SDP AS PartnerName\n" +
                " FROM dbo.ALL_EMRSites";
        Dataset<Row> sourcePartnersDataframe = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("query", sourcePartnersQuery)
                .load();
        sourcePartnersDataframe.createOrReplaceTempView("source_partner");
        Dataset<Row> dimPartnerDf = session.sql("SELECT upper(PartnerName) AS PartnerName, current_date() AS LoadDate FROM source_partner");

        dimPartnerDf.printSchema();
        dimPartnerDf.write()
                .format("jdbc")
                .option("url", rtConfig.get("spark.edw.url"))
                .option("driver", rtConfig.get("spark.edw.driver"))
                .option("user", rtConfig.get("spark.edw.user"))
                .option("password", rtConfig.get("spark.edw.password"))
                .option("truncate", "true")
                .option("dbtable", "dbo.DimPartner")
                .mode(SaveMode.Overwrite)
                .save();
    }
}
