package org.kenyahmis.loadfactprepassessments;

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

public class LoadFactPREPAssessments {
    private static final Logger logger = LoggerFactory.getLogger(LoadFactPREPAssessments.class);
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("Load PREP Assessment Fact");

        SparkSession session = SparkSession.builder()
                .config(conf)
                .getOrCreate();

        RuntimeConfig rtConfig = session.conf();


        LoadFactPREPAssessments loadPREPDiscontinuations = new LoadFactPREPAssessments();

        Dataset<Row> sourceDataFrame = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("query", "select\n" +
                        "patient.PatientPKHash,\n" +
                        "patient.SiteCode,\n" +
                        "VisitID,\n" +
                        "        SexPartnerHIVStatus,\n" +
                        "        IsHIVPositivePartnerCurrentonART,\n" +
                        "        IsPartnerHighrisk,\n" +
                        "        PartnerARTRisk,\n" +
                        "        ClientAssessments,\n" +
                        "        ClientWillingToTakePrep,\n" +
                        "        PrEPDeclineReason,\n" +
                        "        RiskReductionEducationOffered,\n" +
                        "        ReferralToOtherPrevServices,\n" +
                        "        FirstEstablishPartnerStatus,\n" +
                        "        PartnerEnrolledtoCCC,\n" +
                        "        HIVPartnerCCCnumber,\n" +
                        "        HIVPartnerARTStartDate,\n" +
                        "        MonthsknownHIVSerodiscordant,\n" +
                        "        SexWithoutCondom,\n" +
                        "        NumberofchildrenWithPartner,\n" +
                        "        ClientRisk,\n" +
                        "        case \n" +
                        "            when ClientRisk='Risk' then 1 else 0 end as EligiblePrep,\n" +
                        "        VisitDate As AssessmentVisitDate,\n" +
                        "        case \n" +
                        "            when VisitDate is not null then 1 else 0 end as ScreenedPrep\n" +
                        "    from dbo.PrEP_Patient as patient\n" +
                        "    left join dbo.PrEP_BehaviourRisk as risk on patient.PatientPk = risk.PatientPk\n" +
                        "        and patient.SiteCode = risk.SiteCode")
                .load();
        sourceDataFrame.createOrReplaceTempView("source_data");
        sourceDataFrame.persist(StorageLevel.DISK_ONLY());

        sourceDataFrame.printSchema();
        sourceDataFrame.show();

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
        dimFacilityDataFrame.createOrReplaceTempView("facility");

        Dataset<Row> dimPatientDataFrame = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.edw.url"))
                .option("driver", rtConfig.get("spark.edw.driver"))
                .option("user", rtConfig.get("spark.edw.user"))
                .option("password", rtConfig.get("spark.edw.password"))
                .option("dbtable", "dbo.DimPatient")
                .load();
        dimPatientDataFrame.persist(StorageLevel.DISK_ONLY());
        dimPatientDataFrame.createOrReplaceTempView("patient");

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

        Dataset<Row> dimAgeGroupDataFrame = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.edw.url"))
                .option("driver", rtConfig.get("spark.edw.driver"))
                .option("user", rtConfig.get("spark.edw.user"))
                .option("password", rtConfig.get("spark.edw.password"))
                .option("dbtable", "dbo.DimAgeGroup")
                .load();
        dimAgeGroupDataFrame.persist(StorageLevel.DISK_ONLY());
        dimAgeGroupDataFrame.createOrReplaceTempView("age_group");

        Dataset<Row> dimMFLPartnerAgencyCombinationDataFrame = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("query", "select MFL_Code,SDP,[SDP_Agency] as Agency from dbo.All_EMRSites")
                .load();
        dimMFLPartnerAgencyCombinationDataFrame.persist(StorageLevel.DISK_ONLY());
        dimMFLPartnerAgencyCombinationDataFrame.createOrReplaceTempView("mfl_partner_agency_combination");

        String factPrepAssQuery = loadPREPDiscontinuations.loadQuery("LoadFactPrepAssessments.sql");

        Dataset<Row> factPrepAssDf = session.sql(factPrepAssQuery);
        factPrepAssDf = factPrepAssDf.withColumn("FactKey", monotonically_increasing_id().plus(1));
        factPrepAssDf.printSchema();
        long factExitsCount = factPrepAssDf.count();
        logger.info("Fact Prep assessments Count is: " + factExitsCount);
        final int writePartitions = 50;
        factPrepAssDf
                .repartition(writePartitions)
                .write()
                .format("jdbc")
                .option("url", rtConfig.get("spark.edw.url"))
                .option("driver", rtConfig.get("spark.edw.driver"))
                .option("user", rtConfig.get("spark.edw.user"))
                .option("password", rtConfig.get("spark.edw.password"))
                .option("dbtable", "dbo.FactPrepAssessments")
                .mode(SaveMode.Overwrite)
                .save();
    }

    private String loadQuery(String fileName) {
        String query;
        InputStream inputStream = LoadFactPREPAssessments.class.getClassLoader().getResourceAsStream(fileName);
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