package org.kenyahmis.loadfacthtsposconcordance;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;

public class LoadFactHTSPOSConcordance {
    private static final Logger logger = LoggerFactory.getLogger(LoadFactHTSPOSConcordance.class);
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("Load HTS POS Concordance Fact");

        SparkSession session = SparkSession.builder()
                .config(conf)
                .getOrCreate();

        RuntimeConfig rtConfig = session.conf();

        LoadFactHTSPOSConcordance loadFactHTSPOSConcordance= new LoadFactHTSPOSConcordance();

        String htsPosQuery = loadFactHTSPOSConcordance.loadQuery("HTSPos.sql");
        Dataset<Row> htsPosDataFrame = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.edw.url"))
                .option("driver", rtConfig.get("spark.edw.driver"))
                .option("user", rtConfig.get("spark.edw.user"))
                .option("password", rtConfig.get("spark.edw.password"))
                .option("query", htsPosQuery)
                .load();
        htsPosDataFrame.createOrReplaceTempView("HTSPos");
        htsPosDataFrame.persist(StorageLevel.DISK_ONLY());

        String dwhHtsPosQuery = loadFactHTSPOSConcordance.loadQuery("NDWHHTSPos.sql");
        Dataset<Row> dwhHtsPosDataFrame = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("query", dwhHtsPosQuery)
                .load();
        dwhHtsPosDataFrame.createOrReplaceTempView("NDW_HTSPos");
        dwhHtsPosDataFrame.persist(StorageLevel.DISK_ONLY());

        String allUploadQuery = loadFactHTSPOSConcordance.loadQuery("AllUpload.sql");
        Dataset<Row> allUploadDataFrame = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("query", allUploadQuery)
                .load();
        allUploadDataFrame.createOrReplaceTempView("AllUpload");
        allUploadDataFrame.persist(StorageLevel.DISK_ONLY());

        String uploadQuery = loadFactHTSPOSConcordance.loadQuery("Upload.sql");
        session.sql(uploadQuery).createOrReplaceTempView("Upload");

        String emrQuery = loadFactHTSPOSConcordance.loadQuery("EMR.sql");
        Dataset<Row> emrDataFrame = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("query", emrQuery)
                .load();
        emrDataFrame.createOrReplaceTempView("EMR");
        emrDataFrame.persist(StorageLevel.DISK_ONLY());

        Dataset<Row> facilityInfoDataFrame = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("query", "Select MFL_Code, County, SDP, EMR from dbo.All_EMRSites")
                .load();
        facilityInfoDataFrame.createOrReplaceTempView("Facilityinfo");
        facilityInfoDataFrame.persist(StorageLevel.DISK_ONLY());

        String dhis2HtsPosQuery = loadFactHTSPOSConcordance.loadQuery("Dhis2HTSPos.sql");
        Dataset<Row> dhis2HtsPosDataFrame = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("query", dhis2HtsPosQuery)
                .load();
        dhis2HtsPosDataFrame.createOrReplaceTempView("DHIS2_HTSPos");
        dhis2HtsPosDataFrame.persist(StorageLevel.DISK_ONLY());

        String latestEmrQuery = loadFactHTSPOSConcordance.loadQuery("LatestEMR.sql");
        session.sql(latestEmrQuery).createOrReplaceTempView("LatestEMR");

        String dwapiQuery = loadFactHTSPOSConcordance.loadQuery("Dwapi.sql");
        Dataset<Row> dwapiDataFrame = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("query", dwapiQuery)
                .load();
        dwapiDataFrame.createOrReplaceTempView("DWAPI");
        dwapiDataFrame.persist(StorageLevel.DISK_ONLY());

        String summaryQuery = loadFactHTSPOSConcordance.loadQuery("Summary.sql");
        session.sql(summaryQuery).createOrReplaceTempView("Summary");


        Dataset<Row> dimFacilityDataFrame = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.edw.url"))
                .option("driver", rtConfig.get("spark.edw.driver"))
                .option("user", rtConfig.get("spark.edw.user"))
                .option("password", rtConfig.get("spark.edw.password"))
                .option("dbtable", "dbo.DimFacility")
                .load();
        dimFacilityDataFrame.persist(StorageLevel.DISK_ONLY());
        dimFacilityDataFrame.createOrReplaceTempView("DimFacility");

        Dataset<Row> dimPartnerDataFrame = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.edw.url"))
                .option("driver", rtConfig.get("spark.edw.driver"))
                .option("user", rtConfig.get("spark.edw.user"))
                .option("password", rtConfig.get("spark.edw.password"))
                .option("dbtable", "dbo.DimPartner")
                .load();
        dimPartnerDataFrame.persist(StorageLevel.DISK_ONLY());
        dimPartnerDataFrame.createOrReplaceTempView("DimPartner");

        Dataset<Row> dimAgencyDataFrame = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.edw.url"))
                .option("driver", rtConfig.get("spark.edw.driver"))
                .option("user", rtConfig.get("spark.edw.user"))
                .option("password", rtConfig.get("spark.edw.password"))
                .option("dbtable", "dbo.DimAgency")
                .load();
        dimAgencyDataFrame.persist(StorageLevel.DISK_ONLY());
        dimAgencyDataFrame.createOrReplaceTempView("DimAgency");

        Dataset<Row> dimMFLPartnerAgencyCombinationDataFrame = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("query", "select distinct MFL_Code, SDP, SDP_Agency as Agency from dbo.All_EMRSites")
                .load();
        dimMFLPartnerAgencyCombinationDataFrame.persist(StorageLevel.DISK_ONLY());
        dimMFLPartnerAgencyCombinationDataFrame.createOrReplaceTempView("mfl_partner_agency_combination");

        String htsConcordanceQuery = loadFactHTSPOSConcordance.loadQuery("LoadFactHTSPOSConcordance.sql");
        Dataset<Row> htsConcordanceDf = session.sql(htsConcordanceQuery);

        htsConcordanceDf.printSchema();
        int numberOfPartitionsBeforeRepartition = htsConcordanceDf.rdd().getNumPartitions();
        logger.info("Number of partitions before repartition: "+ numberOfPartitionsBeforeRepartition);
        final int writePartitions = 100;
        htsConcordanceDf
                .repartition(writePartitions)
                .write()
                .format("jdbc")
                .option("url", rtConfig.get("spark.edw.url"))
                .option("driver", rtConfig.get("spark.edw.driver"))
                .option("user", rtConfig.get("spark.edw.user"))
                .option("password", rtConfig.get("spark.edw.password"))
                .option("dbtable", "dbo.FactHTSPosConcordance")
                .option("truncate", "true")
                .mode(SaveMode.Overwrite)
                .save();
    }

    private String loadQuery(String fileName) {
        String query;
        InputStream inputStream = LoadFactHTSPOSConcordance.class.getClassLoader().getResourceAsStream(fileName);
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
