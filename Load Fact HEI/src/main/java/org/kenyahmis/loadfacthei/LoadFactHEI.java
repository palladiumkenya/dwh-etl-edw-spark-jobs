package org.kenyahmis.loadfacthei;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;

public class LoadFactHEI {
    private static final Logger logger = LoggerFactory.getLogger(LoadFactHEI.class);

    public static void main(String[] args) {

        SparkConf conf = new SparkConf();
        conf.setAppName("Load HEI Fact");

        SparkSession session = SparkSession.builder()
                .config(conf)
                .getOrCreate();

        RuntimeConfig rtConfig = session.conf();

        LoadFactHEI loadFactHEI = new LoadFactHEI();

        Dataset<Row> pmtctDemographicsDataFrame = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("query", "select PatientPk, SiteCode, DOB, Gender from dbo.MNCH_Patient as patient")
                .load();
        pmtctDemographicsDataFrame.persist(StorageLevel.DISK_ONLY());
        pmtctDemographicsDataFrame.createOrReplaceTempView("pmtct_client_demographics");

        Dataset<Row> mnchHEIsDataFrame = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("query", "select * from dbo.MNCH_HEIs")
                .load();
        mnchHEIsDataFrame.persist(StorageLevel.DISK_ONLY());
        mnchHEIsDataFrame.createOrReplaceTempView("MNCH_HEIs");

        String tested6wksQuery = loadFactHEI.loadQuery("tested_at_6wks_first_contact.sql");
        Dataset<Row> tested6wksDataFrame = session.sql(tested6wksQuery);
        tested6wksDataFrame.createOrReplaceTempView("tested_at_6wks_first_contact");
        tested6wksDataFrame.persist(StorageLevel.DISK_ONLY());
        tested6wksDataFrame.printSchema();
        tested6wksDataFrame.show();

        String tested6mntsQuery = loadFactHEI.loadQuery("tested_at_6_months.sql");
        Dataset<Row> tested6mntsDataFrame = session.sql(tested6mntsQuery);
        tested6mntsDataFrame.createOrReplaceTempView("tested_at_6_months");
        tested6mntsDataFrame.persist(StorageLevel.DISK_ONLY());
        tested6mntsDataFrame.printSchema();
        tested6mntsDataFrame.show();

        String tested12mntsQuery = loadFactHEI.loadQuery("tested_at_12_months.sql");
        Dataset<Row> tested12mntsDataFrame = session.sql(tested12mntsQuery);
        tested12mntsDataFrame.createOrReplaceTempView("tested_at_12_months");
        tested12mntsDataFrame.persist(StorageLevel.DISK_ONLY());
        tested12mntsDataFrame.printSchema();
        tested12mntsDataFrame.show();

        String pcr8wksQuery = loadFactHEI.loadQuery("initial_PCR_less_than_8wks.sql");
        Dataset<Row> pcr8wksDataFrame = session.sql(pcr8wksQuery);
        pcr8wksDataFrame.createOrReplaceTempView("initial_PCR_less_than_8wks");
        pcr8wksDataFrame.persist(StorageLevel.DISK_ONLY());
        pcr8wksDataFrame.printSchema();
        pcr8wksDataFrame.show();

        String pcr12mntsQuery = loadFactHEI.loadQuery("initial_PCR_btwn_8wks_12mnths.sql");
        Dataset<Row> pcr12mntsDataFrame = session.sql(pcr12mntsQuery);
        pcr12mntsDataFrame.createOrReplaceTempView("initial_PCR_btwn_8wks_12mnths");
        pcr12mntsDataFrame.persist(StorageLevel.DISK_ONLY());

        Dataset<Row> finalAntibodyDataFrame = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("query", "select heis.PatientPk, heis.SiteCode, FinalyAntibody, FinalyAntibodyDate " +
                        " from ODS.dbo.MNCH_HEIs as heis where FinalyAntibody is not null")
                .load();
        finalAntibodyDataFrame.persist(StorageLevel.DISK_ONLY());
        finalAntibodyDataFrame.createOrReplaceTempView("final_antibody_data");

        String cwcOrderingQuery = loadFactHEI.loadQuery("cwc_visits_ordering.sql");
        Dataset<Row> cwcOrderingDataFrame = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("query", cwcOrderingQuery)
                .load();
        cwcOrderingDataFrame.persist(StorageLevel.DISK_ONLY());
        cwcOrderingDataFrame.createOrReplaceTempView("cwc_visits_ordering");

        Dataset<Row> latestCwcDataFrame = session.sql("select * from cwc_visits_ordering where num = 1");
        latestCwcDataFrame.persist(StorageLevel.DISK_ONLY());
        latestCwcDataFrame.createOrReplaceTempView("latest_cwc_visit");

        String feedingDataQuery = loadFactHEI.loadQuery("feeding_data.sql");
        Dataset<Row> feedingDataFrame = session.sql(feedingDataQuery);
        feedingDataFrame.createOrReplaceTempView("feeding_data");
        feedingDataFrame.persist(StorageLevel.DISK_ONLY());

        String positiveHeisQuery = loadFactHEI.loadQuery("positive_heis.sql");
        Dataset<Row> positiveHeisDataFrame = session.sql(positiveHeisQuery);
        positiveHeisDataFrame.createOrReplaceTempView("positive_heis");
        positiveHeisDataFrame.persist(StorageLevel.DISK_ONLY());

        String unknownStatusQuery = loadFactHEI.loadQuery("unknown_status_24_months.sql");
        Dataset<Row> unknownStatusDataFrame = session.sql(unknownStatusQuery);
        unknownStatusDataFrame.createOrReplaceTempView("unknown_status_24_months");
        unknownStatusDataFrame.persist(StorageLevel.DISK_ONLY());

        String infectedARTQuery = loadFactHEI.loadQuery("infected_and_ART.sql");
        Dataset<Row> infectedARTDataFrame = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("query", infectedARTQuery)
                .load();
        infectedARTDataFrame.persist(StorageLevel.DISK_ONLY());
        infectedARTDataFrame.createOrReplaceTempView("infected_and_ART");

        String prophylaxisQuery = loadFactHEI.loadQuery("prophylaxis_data.sql");
        Dataset<Row> prophylaxisDataFrame = session.sql(prophylaxisQuery);
        prophylaxisDataFrame.createOrReplaceTempView("prophylaxis_data");
        prophylaxisDataFrame.persist(StorageLevel.DISK_ONLY());



        Dataset<Row> dimMFLPartnerAgencyCombinationDataFrame = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("query", "select MFL_Code,SDP,[SDP_Agency] as Agency from dbo.All_EMRSites")
                .load();
        dimMFLPartnerAgencyCombinationDataFrame.persist(StorageLevel.DISK_ONLY());
        dimMFLPartnerAgencyCombinationDataFrame.createOrReplaceTempView("MFL_partner_agency_combination");

        Dataset<Row> dimFacilityDataFrame = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.edw.url"))
                .option("driver", rtConfig.get("spark.edw.driver"))
                .option("user", rtConfig.get("spark.edw.user"))
                .option("password", rtConfig.get("spark.edw.password"))
                .option("dbtable", "dbo.DimFacility")
                .load();
        dimFacilityDataFrame.persist(StorageLevel.DISK_ONLY());
        dimFacilityDataFrame.createOrReplaceTempView("Dimfacility");

        Dataset<Row> dimPatientDataFrame = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.edw.url"))
                .option("driver", rtConfig.get("spark.edw.driver"))
                .option("user", rtConfig.get("spark.edw.user"))
                .option("password", rtConfig.get("spark.edw.password"))
                .option("dbtable", "dbo.DimPatient")
                .load();
        dimPatientDataFrame.persist(StorageLevel.DISK_ONLY());
        dimPatientDataFrame.createOrReplaceTempView("DimPatient");

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

        Dataset<Row> dimAgeGroupDataFrame = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.edw.url"))
                .option("driver", rtConfig.get("spark.edw.driver"))
                .option("user", rtConfig.get("spark.edw.user"))
                .option("password", rtConfig.get("spark.edw.password"))
                .option("dbtable", "dbo.DimAgeGroup")
                .load();
        dimAgeGroupDataFrame.persist(StorageLevel.DISK_ONLY());
        dimAgeGroupDataFrame.createOrReplaceTempView("DimAgeGroup");

        Dataset<Row> dimDateDataFrame = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.edw.url"))
                .option("driver", rtConfig.get("spark.edw.driver"))
                .option("user", rtConfig.get("spark.edw.user"))
                .option("password", rtConfig.get("spark.edw.password"))
                .option("dbtable", "dbo.DimDate")
                .load();
        dimDateDataFrame.persist(StorageLevel.DISK_ONLY());
        dimDateDataFrame.createOrReplaceTempView("DimDate");


        String factHEIQuery = loadFactHEI.loadQuery("LoadFactHEI.sql");
        Dataset<Row> factHEIDf = session.sql(factHEIQuery);
        factHEIDf.printSchema();
        long factHEICount = factHEIDf.count();
        logger.info("Fact HEI Count is: " + factHEICount);
        final int writePartitions = 50;
        factHEIDf
                .repartition(writePartitions)
                .write()
                .format("jdbc")
                .option("url", rtConfig.get("spark.edw.url"))
                .option("driver", rtConfig.get("spark.edw.driver"))
                .option("user", rtConfig.get("spark.edw.user"))
                .option("password", rtConfig.get("spark.edw.password"))
                .option("truncate", "true")
                .option("dbtable", "dbo.FactHEI")
                .mode(SaveMode.Overwrite)
                .save();
    }

    private String loadQuery(String fileName) {
        String query;
        InputStream inputStream = LoadFactHEI.class.getClassLoader().getResourceAsStream(fileName);
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