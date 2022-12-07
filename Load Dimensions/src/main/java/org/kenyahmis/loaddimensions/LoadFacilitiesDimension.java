package org.kenyahmis.loaddimensions;

import org.apache.spark.sql.*;

public class LoadFacilitiesDimension {

    public void loadFacilities(SparkSession session) {
        RuntimeConfig rtConfig = session.conf();
        final String sourceFacilitiesQuery = "select MFL_Code as MFLCode,[Facility Name] as FacilityName,SubCounty,County,EMR," +
                "Project,Longitude,Latitude from dbo.ALL_EMRSites";
        Dataset<Row> sourceFacilitiesDataframe = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("query", sourceFacilitiesQuery)
                .load();
        sourceFacilitiesDataframe.createOrReplaceTempView("source_facility");

        final String siteAbstractionQuery = "select SiteCode,max(VisitDate) as DateSiteAbstraction from dbo.CT_PatientVisits" +
                " group by SiteCode";
        Dataset<Row> siteAbstractionDataframe = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("query", siteAbstractionQuery)
                .load();
        siteAbstractionDataframe.createOrReplaceTempView("site_abstraction");

        String latestUploadQuery = "select \n" +
                "SiteCode,\n" +
                "max(cast([DateRecieved] as date)) as LatestDateUploaded\n" +
                "from [dbo].[FacilityManifest](NoLock) \n" +
                "group by SiteCode";
        Dataset<Row> latestUploadDataframe = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.dwapicentral.url"))
                .option("driver", rtConfig.get("spark.dwapicentral.driver"))
                .option("user", rtConfig.get("spark.dwapicentral.user"))
                .option("password", rtConfig.get("spark.dwapicentral.password"))
                .option("query", latestUploadQuery)
                .load();
        latestUploadDataframe.createOrReplaceTempView("latest_upload");

        Dataset<Row> dimFacilityDf = session.sql("select \n" +
                "source_facility.*,\n" +
                "cast(date_format(site_abstraction.DateSiteAbstraction,'yyyyMMdd') as int) as DateSiteAbstractionKey,\n" +
                "cast(date_format(latest_upload.LatestDateUploaded, 'yyyyMMdd') as int) as LatestDateUploadedKey,\n" +
                "current_date() as LoadDate \n" +
                "from source_facility\n" +
                " left join site_abstraction on site_abstraction.SiteCode <=> source_facility.MFLCode\n" +
                " left join latest_upload on latest_upload.SiteCode <=> source_facility.MFLCode");

        dimFacilityDf.printSchema();
        dimFacilityDf.show();
        dimFacilityDf.write()
                .format("jdbc")
                .option("url", rtConfig.get("spark.edw.url"))
                .option("driver", rtConfig.get("spark.edw.driver"))
                .option("user", rtConfig.get("spark.edw.user"))
                .option("password", rtConfig.get("spark.edw.password"))
                .option("dbtable", rtConfig.get("spark.dimFacility.dbtable"))
                .option("truncate", "true")
                .mode(SaveMode.Overwrite)
                .save();
    }
}
