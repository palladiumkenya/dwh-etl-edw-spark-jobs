package org.kenyahmis.loadfactpbfw;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;

public class LoadFactPBFW {
    private static final Logger logger = LoggerFactory.getLogger(LoadFactPBFW.class);

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("Load PBFW Fact");


        SparkSession session = SparkSession.builder()
                .config(conf)
                .getOrCreate();

        RuntimeConfig rtConfig = session.conf();


        LoadFactPBFW loadFactPBFW = new LoadFactPBFW();

        // Anc_from_mnch
        String loadAncMNCHQuery = loadFactPBFW.loadQuery("Anc_from_mnch.sql");

        Dataset<Row> ancMNCHDataFrame = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("query", loadAncMNCHQuery)
                .load();
        ancMNCHDataFrame.createOrReplaceTempView("Anc_from_mnch");
        ancMNCHDataFrame.persist(StorageLevel.DISK_ONLY());
        ancMNCHDataFrame.printSchema();

        // Pbfw_patient
        Dataset<Row> pbfwPatientDataFrame = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("dbtable", "dbo.Intermediate_Pbfw")
                .load();
        pbfwPatientDataFrame.persist(StorageLevel.DISK_ONLY());
        pbfwPatientDataFrame.createOrReplaceTempView("Pbfw_patient");

        // Ancdate1
        Dataset<Row> ancDate1DataFrame = session.sql("SELECT Anc.Patientpkhash, Anc.Sitecode, Anc.Patientpk, Anc.Visitdate AS ANCDate1 FROM   Anc_from_mnch as anc WHERE  Num = 1");
        ancDate1DataFrame.persist(StorageLevel.DISK_ONLY());
        ancDate1DataFrame.createOrReplaceTempView("Ancdate1");

        // Ancdate2
        Dataset<Row> ancDate2DataFrame = session.sql("SELECT Anc.Patientpkhash, Anc.Sitecode, Anc.Patientpk, Anc.Visitdate AS ANCDate2 FROM   Anc_from_mnch as anc WHERE  Num = 2");
        ancDate2DataFrame.persist(StorageLevel.DISK_ONLY());
        ancDate2DataFrame.createOrReplaceTempView("Ancdate2");

        // Ancdate3
        Dataset<Row> ancDate3DataFrame = session.sql("SELECT Anc.Patientpkhash, Anc.Sitecode, Anc.Patientpk, Anc.Visitdate AS ANCDate3 FROM   Anc_from_mnch as anc WHERE  Num = 3");
        ancDate3DataFrame.persist(StorageLevel.DISK_ONLY());
        ancDate3DataFrame.createOrReplaceTempView("Ancdate3");

        // Ancdate4
        Dataset<Row> ancDate4DataFrame = session.sql("SELECT Anc.Patientpkhash, Anc.Sitecode, Anc.Patientpk, Anc.Visitdate AS ANCDate4 FROM   Anc_from_mnch as anc WHERE  Num = 4");
        ancDate4DataFrame.persist(StorageLevel.DISK_ONLY());
        ancDate4DataFrame.createOrReplaceTempView("Ancdate4");

        // Testsatanc
        String loadTestsAtAncQuery = loadFactPBFW.loadQuery("Testsatanc.sql");

        Dataset<Row> testsAtAncDataFrame = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("query", loadTestsAtAncQuery)
                .load();
        testsAtAncDataFrame.createOrReplaceTempView("Testsatanc");
        testsAtAncDataFrame.persist(StorageLevel.DISK_ONLY());
        testsAtAncDataFrame.printSchema();

        // Testedatanc
        Dataset<Row> testedAtAncDataFrame = session.sql("SELECT Pat.Patientpkhash, Pat.Sitecode, Pat.Patientpk FROM   Testsatanc Pat WHERE  Num = 1");
        testedAtAncDataFrame.persist(StorageLevel.DISK_ONLY());
        testedAtAncDataFrame.createOrReplaceTempView("Testedatanc");

        // Testsatlandd
        String loadTestsAtLanddQuery = loadFactPBFW.loadQuery("Testsatlandd.sql");

        Dataset<Row> testsAtLanddDataFrame = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("query", loadTestsAtLanddQuery)
                .load();
        testsAtLanddDataFrame.createOrReplaceTempView("Testsatlandd");
        testsAtLanddDataFrame.persist(StorageLevel.DISK_ONLY());
        testsAtLanddDataFrame.printSchema();

        // Testedatlandd
        Dataset<Row> testedAtLanddDataFrame = session.sql("SELECT Pat.Patientpkhash, Pat.Sitecode, Pat.Patientpk FROM Testsatlandd AS Pat WHERE  Num = 1");
        testedAtLanddDataFrame.persist(StorageLevel.DISK_ONLY());
        testedAtLanddDataFrame.createOrReplaceTempView("Testedatlandd");

        // Testsatpnc
        String loadTestsAtPncQuery = loadFactPBFW.loadQuery("Testsatpnc.sql");

        Dataset<Row> testsAtPncDataFrame = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("query", loadTestsAtPncQuery)
                .load();
        testsAtPncDataFrame.createOrReplaceTempView("Testsatpnc");
        testsAtPncDataFrame.persist(StorageLevel.DISK_ONLY());
        testsAtPncDataFrame.printSchema();

        // Testedatpnc
        Dataset<Row> testedAtPncDataFrame = session.sql("SELECT Pat.Patientpkhash, Pat.Sitecode, Pat.Patientpk FROM Testsatpnc AS Pat WHERE  Num = 1");
        testedAtPncDataFrame.persist(StorageLevel.DISK_ONLY());
        testedAtPncDataFrame.createOrReplaceTempView("Testedatpnc");

        // Eac
        String loadEACQuery = loadFactPBFW.loadQuery("Eac.sql");

        Dataset<Row> eacDataFrame = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("query", loadEACQuery)
                .load();
        eacDataFrame.createOrReplaceTempView("Eac");
        eacDataFrame.persist(StorageLevel.DISK_ONLY());
        eacDataFrame.printSchema();

        // Receivedeac1
        Dataset<Row> eac1DataFrame = session.sql("SELECT Eac1.Patientpkhash, Eac1.Sitecode, Eac1.Patientpk FROM Eac AS Eac1 WHERE  Num = 1");
        eac1DataFrame.persist(StorageLevel.DISK_ONLY());
        eac1DataFrame.createOrReplaceTempView("Receivedeac1");

        // Receivedeac2
        Dataset<Row> eac2DataFrame = session.sql("SELECT Eac2.Patientpkhash, Eac2.Sitecode, Eac2.Patientpk FROM Eac AS Eac2 WHERE  Num = 2");
        eac2DataFrame.persist(StorageLevel.DISK_ONLY());
        eac2DataFrame.createOrReplaceTempView("Receivedeac2");

        // Receivedeac3
        Dataset<Row> eac3DataFrame = session.sql("SELECT Eac3.Patientpkhash, Eac3.Sitecode, Eac3.Patientpk FROM Eac AS Eac3 WHERE  Num = 3");
        eac3DataFrame.persist(StorageLevel.DISK_ONLY());
        eac3DataFrame.createOrReplaceTempView("Receivedeac3");

        // Eac
        String loadSwitchesQuery = loadFactPBFW.loadQuery("Switches.sql");

        Dataset<Row> switchesDataFrame = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("query", loadSwitchesQuery)
                .load();
        switchesDataFrame.createOrReplaceTempView("Switches");
        switchesDataFrame.persist(StorageLevel.DISK_ONLY());
        switchesDataFrame.printSchema();

        // Pbfwreglineswitch
        Dataset<Row> pbfwReglineSwitchDataFrame = session.sql("SELECT * FROM Switches AS Eac3 WHERE  Num = 1");
        pbfwReglineSwitchDataFrame.persist(StorageLevel.DISK_ONLY());
        pbfwReglineSwitchDataFrame.createOrReplaceTempView("Pbfwreglineswitch");

        // Summary
        String loadSummaryQuery = loadFactPBFW.loadQuery("Summary.sql");

        Dataset<Row> summaryDataFrame = session.sql(loadSummaryQuery);
        summaryDataFrame.createOrReplaceTempView("Summary");
        summaryDataFrame.persist(StorageLevel.DISK_ONLY());
        summaryDataFrame.printSchema();



        Dataset<Row> dimDateDataFrame = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.edw.url"))
                .option("driver", rtConfig.get("spark.edw.driver"))
                .option("user", rtConfig.get("spark.edw.user"))
                .option("password", rtConfig.get("spark.edw.password"))
                .option("dbtable", "dbo.DimDate")
                .load();
        dimDateDataFrame.persist(StorageLevel.DISK_ONLY());
        dimDateDataFrame.createOrReplaceTempView("Dimdate");

        Dataset<Row> dimFacilityDataFrame = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.edw.url"))
                .option("driver", rtConfig.get("spark.edw.driver"))
                .option("user", rtConfig.get("spark.edw.user"))
                .option("password", rtConfig.get("spark.edw.password"))
                .option("dbtable", "dbo.DimFacility")
                .load();
        dimFacilityDataFrame.persist(StorageLevel.DISK_ONLY());
        dimFacilityDataFrame.createOrReplaceTempView("Facility");

        Dataset<Row> dimPatientDataFrame = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.edw.url"))
                .option("driver", rtConfig.get("spark.edw.driver"))
                .option("user", rtConfig.get("spark.edw.user"))
                .option("password", rtConfig.get("spark.edw.password"))
                .option("dbtable", "dbo.DimPatient")
                .load();
        dimPatientDataFrame.persist(StorageLevel.DISK_ONLY());
        dimPatientDataFrame.createOrReplaceTempView("Patient");

        Dataset<Row> dimPartnerDataFrame = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.edw.url"))
                .option("driver", rtConfig.get("spark.edw.driver"))
                .option("user", rtConfig.get("spark.edw.user"))
                .option("password", rtConfig.get("spark.edw.password"))
                .option("dbtable", "dbo.DimPartner")
                .load();
        dimPartnerDataFrame.persist(StorageLevel.DISK_ONLY());
        dimPartnerDataFrame.createOrReplaceTempView("Partner");

        Dataset<Row> dimAgencyDataFrame = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.edw.url"))
                .option("driver", rtConfig.get("spark.edw.driver"))
                .option("user", rtConfig.get("spark.edw.user"))
                .option("password", rtConfig.get("spark.edw.password"))
                .option("dbtable", "dbo.DimAgency")
                .load();
        dimAgencyDataFrame.persist(StorageLevel.DISK_ONLY());
        dimAgencyDataFrame.createOrReplaceTempView("Agency");

        Dataset<Row> dimAgeGroupDataFrame = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.edw.url"))
                .option("driver", rtConfig.get("spark.edw.driver"))
                .option("user", rtConfig.get("spark.edw.user"))
                .option("password", rtConfig.get("spark.edw.password"))
                .option("dbtable", "dbo.DimAgeGroup")
                .load();
        dimAgeGroupDataFrame.persist(StorageLevel.DISK_ONLY());
        dimAgeGroupDataFrame.createOrReplaceTempView("Age_group");

        Dataset<Row> dimMFLPartnerAgencyCombinationDataFrame = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("query", "select MFL_Code,SDP,[SDP_Agency] as Agency from dbo.All_EMRSites")
                .load();
        dimMFLPartnerAgencyCombinationDataFrame.persist(StorageLevel.DISK_ONLY());
        dimMFLPartnerAgencyCombinationDataFrame.createOrReplaceTempView("Mfl_partner_agency_combination");


        String loadFactPBFWQuery = loadFactPBFW.loadQuery("LoadFactPBFW.sql");
        Dataset<Row> factPBFWDf = session.sql(loadFactPBFWQuery);
        factPBFWDf.printSchema();
        long factPBFWCount = factPBFWDf.count();
        logger.info("Fact   Count is: " + factPBFWCount);
        final int writePartitions = 50;
        factPBFWDf
                .repartition(writePartitions)
                .write()
                .format("jdbc")
                .option("url", rtConfig.get("spark.edw.url"))
                .option("driver", rtConfig.get("spark.edw.driver"))
                .option("user", rtConfig.get("spark.edw.user"))
                .option("password", rtConfig.get("spark.edw.password"))
                .option("truncate", "true")
                .option("dbtable", "dbo.Factpbfw")
                .mode(SaveMode.Overwrite)
                .save();
    }

    private String loadQuery(String fileName) {
        String query;
        InputStream inputStream = LoadFactPBFW.class.getClassLoader().getResourceAsStream(fileName);
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