package org.kenyahmis.loadfacttxcurrconcordance;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;

public class LoadFactTxCurrConcordance {
    private static final Logger logger = LoggerFactory.getLogger(LoadFactTxCurrConcordance.class);
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("Load TxCurr Concordance Fact");

        SparkSession session = SparkSession.builder()
                .config(conf)
                .getOrCreate();

        RuntimeConfig rtConfig = session.conf();

        LoadFactTxCurrConcordance loadFactTxCurrConcordance = new LoadFactTxCurrConcordance();

        Dataset<Row> dimFacilityDataFrame = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.edw.url"))
                .option("driver", rtConfig.get("spark.edw.driver"))
                .option("user", rtConfig.get("spark.edw.user"))
                .option("password", rtConfig.get("spark.edw.password"))
                .option("dbtable", "dbo.DimFacility")
                .load();
        dimFacilityDataFrame.persist(StorageLevel.DISK_ONLY());
        dimFacilityDataFrame.createOrReplaceTempView("facility");

        Dataset<Row> dimPartnerDataFrame = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.edw.url"))
                .option("driver", rtConfig.get("spark.edw.driver"))
                .option("user", rtConfig.get("spark.edw.user"))
                .option("password", rtConfig.get("spark.edw.password"))
                .option("dbtable", "dbo.DimPartner")
                .load();
        dimPartnerDataFrame.persist(StorageLevel.DISK_ONLY());
        dimPartnerDataFrame.createOrReplaceTempView("partner");

        Dataset<Row> dimAgencyDataFrame = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.edw.url"))
                .option("driver", rtConfig.get("spark.edw.driver"))
                .option("user", rtConfig.get("spark.edw.user"))
                .option("password", rtConfig.get("spark.edw.password"))
                .option("dbtable", "dbo.DimAgency")
                .load();
        dimAgencyDataFrame.persist(StorageLevel.DISK_ONLY());
        dimAgencyDataFrame.createOrReplaceTempView("agency");

        Dataset<Row> dimMFLPartnerAgencyCombinationDataFrame = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("query", "select distinct MFL_Code , Facility_Name, County, SDP as PartnerName, SDP_Agency as Agency, EMR from dbo.All_EMRSites")
                .load();
        dimMFLPartnerAgencyCombinationDataFrame.persist(StorageLevel.DISK_ONLY());
        dimMFLPartnerAgencyCombinationDataFrame.createOrReplaceTempView("Facilityinfo");

        Dataset<Row> ndwTxCurrDataFrame = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.edw.url"))
                .option("driver", rtConfig.get("spark.edw.driver"))
                .option("user", rtConfig.get("spark.edw.user"))
                .option("password", rtConfig.get("spark.edw.password"))
                .option("query", "SELECT Sitecode, Count(*) AS CurTx_total FROM dbo.DimPatient as Patient WHERE isTXcurr =1 GROUP BY SiteCode")
                .load();
        ndwTxCurrDataFrame.persist(StorageLevel.DISK_ONLY());
        ndwTxCurrDataFrame.createOrReplaceTempView("NDW_CurTx");

        Dataset<Row> allUploadDataFrame = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("query", "SELECT DateRecieved as DateUploaded, SiteCode, ROW_NUMBER()OVER(Partition by Sitecode Order by DateRecieved Desc) as Num FROM dbo.CT_FacilityManifest m")
                .load();
        allUploadDataFrame.persist(StorageLevel.DISK_ONLY());
        allUploadDataFrame.createOrReplaceTempView("AllUpload");

        session.sql("SELECT SiteCode, DateUploaded FROM AllUpload WHERE Num = 1")
                .createOrReplaceTempView("Upload");

        String dhisTxCurrQuery = loadFactTxCurrConcordance.loadQuery("DHISTxCurr.sql");
        Dataset<Row> dhisTxCurrDataFrame = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("query", dhisTxCurrQuery)
                .load();
        dhisTxCurrDataFrame.persist(StorageLevel.DISK_ONLY());
        dhisTxCurrDataFrame.createOrReplaceTempView("DHIS2_CurTx");

        String emrQuery = loadFactTxCurrConcordance.loadQuery("EMR.sql");
        Dataset<Row> emrDataFrame = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("query", emrQuery)
                .load();
        emrDataFrame.persist(StorageLevel.DISK_ONLY());
        emrDataFrame.createOrReplaceTempView("EMR");

        session.sql("Select Emr.facilityCode,Emr.facilityName, Emr.value As EMRValue,Emr.statusDate,Emr.indicatorDate FROM EMR WHERE Num=1").createOrReplaceTempView("LatestEMR");

        Dataset<Row> dwapiDataFrame = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("query", "SELECT Sitecode, DwapiVersion, Docket From dbo.CT_FacilityManifestCargo ")
                .load();
        dwapiDataFrame.persist(StorageLevel.DISK_ONLY());
        dwapiDataFrame.createOrReplaceTempView("DWAPI");

        String uploadsQuery = loadFactTxCurrConcordance.loadQuery("Uploads.sql");
        Dataset<Row> uploadsDataFrame = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("query", uploadsQuery)
                .load();
        uploadsDataFrame.persist(StorageLevel.DISK_ONLY());
        uploadsDataFrame.createOrReplaceTempView("Uploads");

        String latestUploadsQuery = loadFactTxCurrConcordance.loadQuery("LatestUploads.sql");
        session.sql(latestUploadsQuery).createOrReplaceTempView("LatestUploads");

        String receivedQuery = loadFactTxCurrConcordance.loadQuery("Received.sql");
        Dataset<Row> receivedDataFrame = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("query", receivedQuery)
                .load();
        receivedDataFrame.persist(StorageLevel.DISK_ONLY());
        receivedDataFrame.createOrReplaceTempView("Received");

        String combinedQuery = loadFactTxCurrConcordance.loadQuery("Combined.sql");
        session.sql(combinedQuery).createOrReplaceTempView("Combined");

        String uploadDataQuery = loadFactTxCurrConcordance.loadQuery("Uploaddata.sql");
        session.sql(uploadDataQuery).createOrReplaceTempView("Uploaddata");


        String summaryQuery = loadFactTxCurrConcordance.loadQuery("Summary.sql");
        session.sql(summaryQuery).createOrReplaceTempView("Summary");

        String query = loadFactTxCurrConcordance.loadQuery("LoadFactTxCurrConcordance.sql");
        Dataset<Row> concordanceDf = session.sql(query);
        concordanceDf.printSchema();
        int numberOfPartitionsBeforeRepartition = concordanceDf.rdd().getNumPartitions();
        logger.info("Number of partitions before repartition: "+ numberOfPartitionsBeforeRepartition);
        final int writePartitions = 100;
        concordanceDf
                .repartition(writePartitions)
                .write()
                .format("jdbc")
                .option("url", rtConfig.get("spark.edw.url"))
                .option("driver", rtConfig.get("spark.edw.driver"))
                .option("user", rtConfig.get("spark.edw.user"))
                .option("password", rtConfig.get("spark.edw.password"))
                .option("dbtable", "dbo.FactTxCurrConcordance")
                .option("truncate", "true")
                .mode(SaveMode.Overwrite)
                .save();
    }

    private String loadQuery(String fileName) {
        String query;
        InputStream inputStream = LoadFactTxCurrConcordance.class.getClassLoader().getResourceAsStream(fileName);
        if (inputStream == null) {
            logger.error(fileName + " not found");
            return null;
        }
        try {
            query = IOUtils.toString(inputStream, Charset.defaultCharset());
        } catch (IOException e) {
            logger.error("Failed to load query from file", e);
            return null;
        }
        return query;
    }
}