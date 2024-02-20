package org.kenyahmis.loadfactviralload;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;

import static org.apache.spark.sql.functions.*;

public class FactViralLoad {
    private static final Logger logger = LoggerFactory.getLogger(FactViralLoad.class);
    public static void main(String[] args) {

        SparkConf conf = new SparkConf();
        conf.setAppName("Load Viral Load Fact");

        SparkSession session = SparkSession.builder()
                .config(conf)
                .getOrCreate();
        RuntimeConfig rtConfig = session.conf();

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

        FactViralLoad loadViralLoad = new FactViralLoad();
        final String loadEligibleForVLQueryFileName = "LoadEligibleForVL.sql";
        String loadEligibleForVLQuery = loadViralLoad.loadQuery(loadEligibleForVLQueryFileName);

        Dataset<Row> loadEligibleForVLDataFrame = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("query", loadEligibleForVLQuery)
                .load();
        loadEligibleForVLDataFrame.createOrReplaceTempView("eligible_for_VL");
        loadEligibleForVLDataFrame.persist(StorageLevel.DISK_ONLY());
        loadEligibleForVLDataFrame.printSchema();

        final String loadValidVLQueryFileName = "LoadValidVL.sql";
        String loadValidVLQuery = loadViralLoad.loadQuery(loadValidVLQueryFileName);

        Dataset<Row> validVLDataFrame = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("query", loadValidVLQuery)
                .load();
        validVLDataFrame.createOrReplaceTempView("valid_vl");
        validVLDataFrame.persist(StorageLevel.DISK_ONLY());
        validVLDataFrame.printSchema();

        final String loadPBFValidVLQueryFileName = "LoadPBFWValidVL.sql";
        String loadPBFValidVLDataQuery = loadViralLoad.loadQuery(loadPBFValidVLQueryFileName);

        Dataset<Row> PBFValidVLDataFrame = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("query", loadPBFValidVLDataQuery)
                .load();
        PBFValidVLDataFrame.createOrReplaceTempView("PBFW_valid_vl");
        PBFValidVLDataFrame.persist(StorageLevel.DISK_ONLY());
        PBFValidVLDataFrame.printSchema();

        final String loadPBFValidVLIndicatorsQueryFileName = "LoadPBFWValidVLIndicators.sql";
        String loadPBFValidVLIndicatorsDataQuery = loadViralLoad.loadQuery(loadPBFValidVLIndicatorsQueryFileName);
        session.sql(loadPBFValidVLIndicatorsDataQuery).createOrReplaceTempView("PBFW_valid_vl_indicators");

        final String loadValidVLIndicatorsQueryFileName = "LoadValidVLIndicators.sql";
        String loadValidVLIndicatorsDataQuery = loadViralLoad.loadQuery(loadValidVLIndicatorsQueryFileName);
        session.sql(loadValidVLIndicatorsDataQuery).createOrReplaceTempView("valid_VL_indicators");

        final String loadPatientViralLoadIntervalsQueryFileName = "LoadPatientViralLoadIntervals.sql";
        String loadPatientViralLoadIntervalsDataQuery = loadViralLoad.loadQuery(loadPatientViralLoadIntervalsQueryFileName);
        Dataset<Row> patientViralLoadIntervalsDataFrame = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("query", loadPatientViralLoadIntervalsDataQuery)
                .load();
        patientViralLoadIntervalsDataFrame.createOrReplaceTempView("patient_viral_load_intervals");
        patientViralLoadIntervalsDataFrame.persist(StorageLevel.DISK_ONLY());
        patientViralLoadIntervalsDataFrame.printSchema();

        final String loadFirstVLQueryFileName = "LoadFirstVL.sql";
        String loadFirstVLDataQuery = loadViralLoad.loadQuery(loadFirstVLQueryFileName);

        Dataset<Row> FirstVLDataFrame = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("query", loadFirstVLDataQuery)
                .load();
        FirstVLDataFrame.createOrReplaceTempView("first_vl");
        FirstVLDataFrame.persist(StorageLevel.DISK_ONLY());
        FirstVLDataFrame.printSchema();

        final String loadLastVLQueryFileName = "LoadLastVL.sql";
        String loadLastVLDataQuery = loadViralLoad.loadQuery(loadLastVLQueryFileName);

        Dataset<Row> lastVLDataFrame = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("query", loadLastVLDataQuery)
                .load();
        lastVLDataFrame.createOrReplaceTempView("last_vl");
        lastVLDataFrame.persist(StorageLevel.DISK_ONLY());
        lastVLDataFrame.printSchema();

        final String loadTimeToFirstVLQueryFileName = "LoadTimeToFirstVL.sql";
        String loadTimeToFirstVLDataQuery = loadViralLoad.loadQuery(loadTimeToFirstVLQueryFileName);

        Dataset<Row> timeToFirstVLDataFrame = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("query", loadTimeToFirstVLDataQuery)
                .load();
        timeToFirstVLDataFrame.createOrReplaceTempView("time_to_first_vl");
        timeToFirstVLDataFrame.persist(StorageLevel.DISK_ONLY());
        timeToFirstVLDataFrame.printSchema();

        final String loadTimeToFirstVLGroupQueryFileName = "LoadTimeToFirstVLGroup.sql";
        String loadTimeToFirstVLGroupDataQuery = loadViralLoad.loadQuery(loadTimeToFirstVLGroupQueryFileName);

        session.sql(loadTimeToFirstVLGroupDataQuery).createOrReplaceTempView("time_to_first_vl_group");

        final String loadLatestVL1QueryFileName = "LoadLatestVL1.sql";
        String loadLatestVL1DataQuery = loadViralLoad.loadQuery(loadLatestVL1QueryFileName);

        Dataset<Row> latestVL1DataFrame = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("query", loadLatestVL1DataQuery)
                .load();
        latestVL1DataFrame.createOrReplaceTempView("latest_VL_1");
        latestVL1DataFrame.persist(StorageLevel.DISK_ONLY());
        latestVL1DataFrame.printSchema();

        final String loadLatestVL2QueryFileName = "LoadLatestVL2.sql";
        String loadLatestVL2DataQuery = loadViralLoad.loadQuery(loadLatestVL2QueryFileName);

        Dataset<Row> latestVL2DataFrame = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("query", loadLatestVL2DataQuery)
                .load();
        latestVL2DataFrame.createOrReplaceTempView("latest_VL_2");
        latestVL2DataFrame.persist(StorageLevel.DISK_ONLY());
        latestVL2DataFrame.printSchema();

        final String loadLatestVL3QueryFileName = "LoadLatestVL3.sql";
        String loadLatestVL3DataQuery = loadViralLoad.loadQuery(loadLatestVL3QueryFileName);

        Dataset<Row> latestVL3DataFrame = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("query", loadLatestVL3DataQuery)
                .load();
        latestVL3DataFrame.createOrReplaceTempView("latest_VL_3");
        latestVL3DataFrame.persist(StorageLevel.DISK_ONLY());
        latestVL3DataFrame.printSchema();

        Dataset<Row> IntermediateOrderedViralLoadsDataFrame = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("dbtable", "dbo.Intermediate_OrderedViralLoads")
                .load();
        IntermediateOrderedViralLoadsDataFrame.createOrReplaceTempView("Intermediate_OrderedViralLoads");
        IntermediateOrderedViralLoadsDataFrame.persist(StorageLevel.DISK_ONLY());
        IntermediateOrderedViralLoadsDataFrame.printSchema();

        final String loadSecondLatestVLFileName = "LoadSecondLatestVL.sql";
        String loadSecondLatestVLDataQuery = loadViralLoad.loadQuery(loadSecondLatestVLFileName);

        session.sql(loadSecondLatestVLDataQuery).createOrReplaceTempView("SecondLatestVL");

        final String loadRepeatVLFileName = "LoadRepeatVL.sql";
        String loadRepeatVLDataQuery = loadViralLoad.loadQuery(loadRepeatVLFileName);

        session.sql(loadRepeatVLDataQuery).createOrReplaceTempView("RepeatVL");

        final String loadRepeatVLSuppFileName = "LoadRepeatVlSupp.sql";
        String loadRepeatVLSuppDataQuery = loadViralLoad.loadQuery(loadRepeatVLSuppFileName);

        session.sql(loadRepeatVLSuppDataQuery).createOrReplaceTempView("RepeatVlSupp");

        final String loadRepeatVLUnSuppFileName = "LoadRepeatVlUnSupp.sql";
        String loadRepeatVLUnSuppDataQuery = loadViralLoad.loadQuery(loadRepeatVLUnSuppFileName);

        session.sql(loadRepeatVLUnSuppDataQuery).createOrReplaceTempView("RepeatVlUnSupp");

        final String loadPbfwClientFileName = "LoadPbfwClients.sql";
        String loadPbfwClientsDataQuery = loadViralLoad.loadQuery(loadPbfwClientFileName);

        Dataset<Row> PbfwClientsDataFrame = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("query", loadPbfwClientsDataQuery)
                .load();
        PbfwClientsDataFrame.createOrReplaceTempView("pbfw_clients");
        PbfwClientsDataFrame.persist(StorageLevel.DISK_ONLY());
        PbfwClientsDataFrame.printSchema();

        Dataset<Row> ctARTPatientDataFrame = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("dbtable", "dbo.CT_ARTPatients")
                .load();
        ctARTPatientDataFrame.createOrReplaceTempView("CT_ARTPatients");
        ctARTPatientDataFrame.persist(StorageLevel.DISK_ONLY());
        ctARTPatientDataFrame.printSchema();

        Dataset<Row> ctPatientDataFrame = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("dbtable", "dbo.CT_Patient")
                .load();
        ctPatientDataFrame.createOrReplaceTempView("CT_Patient");
        ctPatientDataFrame.persist(StorageLevel.DISK_ONLY());
        latestVL3DataFrame.printSchema();

        Dataset<Row> lastPatientEncounterDataFrame = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("dbtable", "dbo.Intermediate_LastPatientEncounter")
                .load();
        lastPatientEncounterDataFrame.createOrReplaceTempView("Intermediate_LastPatientEncounter");
        lastPatientEncounterDataFrame.persist(StorageLevel.DISK_ONLY());
        lastPatientEncounterDataFrame.printSchema();

        final String loadCombinedViralLoadDatasetQueryFileName = "CombinedViralLoadDataset.sql";
        String loadCombinedViralLoadDatasetDataQuery = loadViralLoad.loadQuery(loadCombinedViralLoadDatasetQueryFileName);

        session.sql(loadCombinedViralLoadDatasetDataQuery).createOrReplaceTempView("combined_viral_load_dataset");

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


        String factVLQuery = loadViralLoad.loadQuery("LoadFactViralLoad.sql");
        Dataset<Row> factVLDf = session.sql(factVLQuery);
//        factVLDf = factVLDf.withColumn("FactKey", monotonically_increasing_id().plus(1));
        factVLDf.printSchema();
        final int writePartitions = 20;
        factVLDf
                .repartition(writePartitions)
                .write()
                .format("jdbc")
                .option("url", rtConfig.get("spark.edw.url"))
                .option("driver", rtConfig.get("spark.edw.driver"))
                .option("user", rtConfig.get("spark.edw.user"))
                .option("password", rtConfig.get("spark.edw.password"))
                .option("dbtable", "dbo.FactViralLoads")
                .option("truncate", "true")
                .mode(SaveMode.Overwrite)
                .save();
    }

    private String loadQuery(String fileName) {
        String query;
        InputStream inputStream = FactViralLoad.class.getClassLoader().getResourceAsStream(fileName);
        if (inputStream == null) {
            logger.error(fileName + " not found");
            throw new RuntimeException(fileName + " not found");
        }
        try {
            query = IOUtils.toString(inputStream, Charset.defaultCharset());
        } catch (IOException e) {
            logger.error("Failed to load query from file", e);
            throw new RuntimeException("Failed to load query from file");
        }
        return query;
    }
}
