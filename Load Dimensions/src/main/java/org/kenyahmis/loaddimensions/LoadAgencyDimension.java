package org.kenyahmis.loaddimensions;

import org.apache.spark.sql.*;

public class LoadAgencyDimension {
    public void loadAgencies(SparkSession session){
        RuntimeConfig rtConfig = session.conf();
        final String sourceAgenciesQuery = "SELECT DISTINCT [SDP Agency] as Agency \n" +
                "FROM [ODS].[dbo].[ALL_EMRSites] WHERE [SDP Agency] <>'NULL' AND [SDP Agency] IS NOT NULL  AND [SDP Agency]<>''" +
                " FROM dbo.ALL_EMRSites";
        Dataset<Row> sourceAgencyDataframe = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("query", sourceAgenciesQuery)
                .load();
        sourceAgencyDataframe.createOrReplaceTempView("source_agency");
        Dataset<Row> dimAgencyDf = session.sql("SELECT upper(AgencyName) FROM source_agency");

        dimAgencyDf.printSchema();
        dimAgencyDf.show();
        dimAgencyDf.write()
                .format("jdbc")
                .option("url", rtConfig.get("spark.edw.url"))
                .option("driver", rtConfig.get("spark.edw.driver"))
                .option("user", rtConfig.get("spark.edw.user"))
                .option("password", rtConfig.get("spark.edw.password"))
                .option("dbtable", rtConfig.get("spark.dimAgency.dbtable"))
                .option("truncate", "true")
                .mode(SaveMode.Overwrite)
                .save();
    }
}
