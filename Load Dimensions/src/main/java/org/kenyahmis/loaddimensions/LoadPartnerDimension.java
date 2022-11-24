package org.kenyahmis.loaddimensions;

import org.apache.spark.sql.*;

public class LoadPartnerDimension {

    public void loadPartners(SparkSession session) {
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
        Dataset<Row> dimPartnerDf = session.sql("SELECT upper(PartnerName),current_date() AS LoadDate FROM source_partner");

        dimPartnerDf.printSchema();
        dimPartnerDf.show();
        dimPartnerDf.write()
                .format("jdbc")
                .option("url", rtConfig.get("spark.edw.url"))
                .option("driver", rtConfig.get("spark.edw.driver"))
                .option("user", rtConfig.get("spark.edw.user"))
                .option("password", rtConfig.get("spark.edw.password"))
                .option("dbtable", rtConfig.get("spark.dimPartner.dbtable"))
                .option("truncate", "true")
                .mode(SaveMode.Overwrite)
                .save();
    }
}
