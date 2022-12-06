package org.kenyahmis.loadfactlatestobs;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;

public class LoadFactLatestObs {
    private static final Logger logger = LoggerFactory.getLogger(LoadFactLatestObs.class);
    public static void main(String[] args){

        SparkConf conf = new SparkConf();
        conf.setAppName("Load OVC Fact");
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
                .option("query", "select MFL_Code,SDP,[SDP Agency] as Agency from dbo.All_EMRSites")
                .load();
        dimMFLPartnerAgencyCombinationDataFrame.persist(StorageLevel.DISK_ONLY());
        dimMFLPartnerAgencyCombinationDataFrame.createOrReplaceTempView("MFL_partner_agency_combination");

        LoadFactLatestObs loadFactLatestObs = new LoadFactLatestObs();
        // latest weigh height
        String latestWeighHeightQuery = loadFactLatestObs.loadQuery("LatestWeightHeight.sql");
        Dataset<Row> latestWeightHeightDataFrame = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("query", latestWeighHeightQuery)
                .load();
        latestWeightHeightDataFrame.persist(StorageLevel.DISK_ONLY());
        latestWeightHeightDataFrame.createOrReplaceTempView("latest_weight_height");

        // Age of last visit
        String ageOfLastVisitQuery = loadFactLatestObs.loadQuery("AgeOfLastVisit.sql");
        Dataset<Row> ageOfLastVisitDataFrame = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("query", ageOfLastVisitQuery)
                .load();
        ageOfLastVisitDataFrame.persist(StorageLevel.DISK_ONLY());
        ageOfLastVisitDataFrame.createOrReplaceTempView("age_of_last_visit");

        // Latest adherence
        String latestAdherenceQuery = loadFactLatestObs.loadQuery("LatestAdherence.sql");
        Dataset<Row> latestAdherenceDataFrame = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("query", latestAdherenceQuery)
                .load();
        latestAdherenceDataFrame.persist(StorageLevel.DISK_ONLY());
        latestAdherenceDataFrame.createOrReplaceTempView("latest_adherence");

        // Latest differentiated care
        String latestDifferentiatedCareQuery = loadFactLatestObs.loadQuery("LatestDifferentiatedCare.sql");
        Dataset<Row> latestDifferentiatedCareDataFrame = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("query", latestDifferentiatedCareQuery)
                .load();
        latestDifferentiatedCareDataFrame.persist(StorageLevel.DISK_ONLY());
        latestDifferentiatedCareDataFrame.createOrReplaceTempView("latest_differentiated_care");

        // latest mmd
        String latestMMDQuery = loadFactLatestObs.loadQuery("LatestMmd.sql");
        Dataset<Row> latestMmdDataFrame = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("query", latestMMDQuery)
                .load();
        latestMmdDataFrame.persist(StorageLevel.DISK_ONLY());
        latestMmdDataFrame.createOrReplaceTempView("latest_mmd");

        // latest stability assessment
        String latestStabilityAssessmentQuery = loadFactLatestObs.loadQuery("LatestStabilityAssessment.sql");
        Dataset<Row> latestStabilityAssessmentDataFrame = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("query", latestStabilityAssessmentQuery)
                .load();
        latestStabilityAssessmentDataFrame.persist(StorageLevel.DISK_ONLY());
        latestStabilityAssessmentDataFrame.createOrReplaceTempView("lastest_stability_assessment");

        // latest pregnancy
        String latestPregnancyQuery = loadFactLatestObs.loadQuery("LatestPregnancy.sql");
        Dataset<Row> latestPregnancyDataFrame = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("query", latestPregnancyQuery)
                .load();
        latestPregnancyDataFrame.persist(StorageLevel.DISK_ONLY());
        latestPregnancyDataFrame.createOrReplaceTempView("latest_pregnancy");

        // latest fp method
        String latestFPMethodQuery = loadFactLatestObs.loadQuery("LatestFPMethod.sql");
        Dataset<Row> latestFPMethodDataFrame = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("query", latestFPMethodQuery)
                .load();
        latestFPMethodDataFrame.persist(StorageLevel.DISK_ONLY());
        latestFPMethodDataFrame.createOrReplaceTempView("latest_fp_method");

        // patient
        Dataset<Row> patientDataFrame = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("query", "select * from dbo.CT_Patient")
                .load();
        patientDataFrame.persist(StorageLevel.DISK_ONLY());
        patientDataFrame.createOrReplaceTempView("patient");

        // combined table
        String combinedTableQuery = loadFactLatestObs.loadQuery("CombinedTable.sql");
        session.sql(combinedTableQuery).createOrReplaceTempView("combined_table");

        Dataset<Row> dimPatientDataFrame = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.edw.url"))
                .option("driver", rtConfig.get("spark.edw.driver"))
                .option("user", rtConfig.get("spark.edw.user"))
                .option("password", rtConfig.get("spark.edw.password"))
                .option("dbtable", rtConfig.get("spark.dimPatient.dbtable"))
                .load();
        dimPatientDataFrame.persist(StorageLevel.DISK_ONLY());
        dimPatientDataFrame.createOrReplaceTempView("DimPatient");

        Dataset<Row> dimFacilityDataFrame = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.edw.url"))
                .option("driver", rtConfig.get("spark.edw.driver"))
                .option("user", rtConfig.get("spark.edw.user"))
                .option("password", rtConfig.get("spark.edw.password"))
                .option("dbtable", rtConfig.get("spark.dimFacility.dbtable"))
                .load();
        dimFacilityDataFrame.persist(StorageLevel.DISK_ONLY());
        dimFacilityDataFrame.createOrReplaceTempView("Dimfacility");

        Dataset<Row> dimPartnerDataFrame = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.edw.url"))
                .option("driver", rtConfig.get("spark.edw.driver"))
                .option("user", rtConfig.get("spark.edw.user"))
                .option("password", rtConfig.get("spark.edw.password"))
                .option("dbtable", rtConfig.get("spark.dimPartner.dbtable"))
                .load();
        dimPartnerDataFrame.persist(StorageLevel.DISK_ONLY());
        dimPartnerDataFrame.createOrReplaceTempView("DimPartner");

        Dataset<Row> dimAgencyDataFrame = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.edw.url"))
                .option("driver", rtConfig.get("spark.edw.driver"))
                .option("user", rtConfig.get("spark.edw.user"))
                .option("password", rtConfig.get("spark.edw.password"))
                .option("dbtable", rtConfig.get("spark.dimAgency.dbtable"))
                .load();
        dimAgencyDataFrame.persist(StorageLevel.DISK_ONLY());
        dimAgencyDataFrame.createOrReplaceTempView("DimAgency");

        Dataset<Row> dimAgeGroupDataFrame = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.edw.url"))
                .option("driver", rtConfig.get("spark.edw.driver"))
                .option("user", rtConfig.get("spark.edw.user"))
                .option("password", rtConfig.get("spark.edw.password"))
                .option("dbtable", rtConfig.get("spark.dimAgeGroup.dbtable"))
                .load();
        dimAgeGroupDataFrame.persist(StorageLevel.DISK_ONLY());
        dimAgeGroupDataFrame.createOrReplaceTempView("DimAgeGroup");

        Dataset<Row> dimDifferentiatedCareDataFrame = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.edw.url"))
                .option("driver", rtConfig.get("spark.edw.driver"))
                .option("user", rtConfig.get("spark.edw.user"))
                .option("password", rtConfig.get("spark.edw.password"))
                .option("dbtable", rtConfig.get("spark.dimDifferentiatedCare.dbtable"))
                .load();
        dimDifferentiatedCareDataFrame.persist(StorageLevel.DISK_ONLY());
        dimDifferentiatedCareDataFrame.createOrReplaceTempView("DimDifferentiatedCare");

        // latest Obs df
        String latestObsQuery = loadFactLatestObs.loadQuery("FactLatestObs.sql");
        Dataset<Row> latestObsDf = session.sql(latestObsQuery);

        latestObsDf.printSchema();
        latestObsDf.printSchema();
        final int writePartitions = 20;
        latestObsDf
                .repartition(writePartitions)
                .write()
                .format("jdbc")
                .option("url", rtConfig.get("spark.edw.url"))
                .option("driver", rtConfig.get("spark.edw.driver"))
                .option("user", rtConfig.get("spark.edw.user"))
                .option("password", rtConfig.get("spark.edw.password"))
                .option("dbtable", rtConfig.get("spark.factLatestObs.dbtable"))
                .option("truncate", "true")
                .mode(SaveMode.Overwrite)
                .save();

    }

    private String loadQuery(String fileName) {
        String query;
        InputStream inputStream = LoadFactLatestObs.class.getClassLoader().getResourceAsStream(fileName);
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
