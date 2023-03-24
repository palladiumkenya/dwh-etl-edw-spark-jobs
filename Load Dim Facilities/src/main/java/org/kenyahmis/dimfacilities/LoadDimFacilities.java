package org.kenyahmis.dimfacilities;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;

import static org.apache.spark.sql.functions.row_number;

public class LoadDimFacilities {
    public static void main(String[] args) {

        SparkConf conf = new SparkConf();
        conf.setAppName("Load Dim Facilities");
        SparkSession session = SparkSession.builder()
                .config(conf)
                .getOrCreate();

        RuntimeConfig rtConfig = session.conf();

        final String sourceFacilitiesQuery = "select MFL_Code as MFLCode,[Facility_Name] as FacilityName,SubCounty,County,EMR," +
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

        Dataset<Row> siteAbstractionDataframe = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("query", "select SiteCode,max(VisitDate) as DateSiteAbstraction from dbo.CT_PatientVisits" +
                        " group by SiteCode")
                .load();
        siteAbstractionDataframe.createOrReplaceTempView("site_abstraction");

        Dataset<Row> latestUploadDataframe = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("query", "select  \n" +
                        "SiteCode,\n" +
                        "max(cast([DateRecieved] as date)) as LatestDateUploaded\n" +
                        "from [dbo].[CT_FacilityManifest](NoLock) \n" +
                        "group by SiteCode")
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

        WindowSpec window = Window.orderBy("MFLCode");
        dimFacilityDf = dimFacilityDf.withColumn("FacilityKey", row_number().over(window));

        dimFacilityDf.printSchema();
        dimFacilityDf.write()
                .format("jdbc")
                .option("url", rtConfig.get("spark.edw.url"))
                .option("driver", rtConfig.get("spark.edw.driver"))
                .option("user", rtConfig.get("spark.edw.user"))
                .option("password", rtConfig.get("spark.edw.password"))
                .option("dbtable", "dbo.DimFacility")
                .mode(SaveMode.Overwrite)
                .save();
    }
}
