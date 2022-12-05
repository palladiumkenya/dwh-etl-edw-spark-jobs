package org.kenyahmis.relationshiptopatient;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;

public class LoadRelationshipToPatientDim {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("Load Relationship with patient Dimension");

        SparkSession session = SparkSession.builder()
                .config(conf)
                .getOrCreate();
        RuntimeConfig rtConfig = session.conf();

        final String sourceRelationshipsQuery = "SELECT DISTINCT RelationshipWithPatient FROM dbo.CT_ContactListing";

        Dataset<Row> sourceRelationshipsDataframe = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("query", sourceRelationshipsQuery)
                .load();

        sourceRelationshipsDataframe.createOrReplaceTempView("source_relationship");
        Dataset<Row> dimRelationshipDf = session.sql("SELECT source_relationship.*,current_date() as LoadDate" +
                " FROM source_relationship");

        dimRelationshipDf.printSchema();
        dimRelationshipDf.show();
        dimRelationshipDf.write()
                .format("jdbc")
                .option("url", rtConfig.get("spark.edw.url"))
                .option("driver", rtConfig.get("spark.edw.driver"))
                .option("user", rtConfig.get("spark.edw.user"))
                .option("password", rtConfig.get("spark.edw.password"))
                .option("dbtable", rtConfig.get("spark.dimRelationshipWithAgent.dbtable"))
                .option("truncate", "true")
                .mode(SaveMode.Overwrite)
                .save();
    }
}
