package org.kenyahmis.loadfactpreprefills;

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

public class LoadFactPREPRefills {
    private static final Logger logger = LoggerFactory.getLogger(LoadFactPREPRefills.class);
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("Load PREP Discontinuations Fact");

        SparkSession session = SparkSession.builder()
                .config(conf)
                .getOrCreate();

        RuntimeConfig rtConfig = session.conf();

        LoadFactPREPRefills loadPREPRefills = new LoadFactPREPRefills();

        // Prep Patients
        Dataset<Row> prepPatientDataDf = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("query", " select\n" +
                        "            distinct PatientPK," +
                        "            PrepNumber," +
                        "            PrepEnrollmentDate," +
                        "            SiteCode\n" +
                        "        from dbo.PrEP_Patient " +
                        "where PrepNumber is not null")
                .load();

        prepPatientDataDf.createOrReplaceTempView("PrepPatients");
        prepPatientDataDf.persist(StorageLevel.DISK_ONLY());

        // Prep Refills
        Dataset<Row> prepRefillsDataDf = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("query", "select" +
                        "        ROW_NUMBER () OVER (PARTITION BY PrepNumber, PatientPk, SiteCode ORDER BY DispenseDate Asc) As RowNumber,\n" +
                        "        PrepNumber" +
                        "        ,PatientPk" +
                        "        ,SiteCode\n" +
                        "        ,HtsNumber\n" +
                        "        ,RegimenPrescribed\n" +
                        "        ,DispenseDate\n" +
                        "    from dbo.PrEP_Pharmacy ")
                .load();

        prepRefillsDataDf.createOrReplaceTempView("prep_refills_ordered");
        prepRefillsDataDf.persist(StorageLevel.DISK_ONLY());

        // Prep Refill 1st Month
        session.sql( "select  \n" +
                        "        Refil.PrepNumber,\n" +
                        "        Refil.PatientPK,\n" +
                        "        Refil.SiteCode,\n" +
                        "        Refil.HtsNumber,\n" +
                        "        RegimenPrescribed,\n" +
                        "        Refil.DispenseDate,\n" +
                        "        Patients.PrepEnrollmentDate,\n" +
                        "        datediff(Refil.DispenseDate, Patients.PrepEnrollmentDate) as RefillFirstMonthDiffInDays\n" +
                        "    from prep_refills_ordered as  Refil\n" +
                        "    left join PrepPatients Patients on Refil.PrepNumber=Patients.PrepNumber and Refil.PatientPk=Patients.PatientPk \n" +
                        "        and Refil.SiteCode=Patients.SiteCode\n" +
                        "    where Refil.PrepNumber is not null \n" +
                        "        and datediff(Refil.DispenseDate, Patients.PrepEnrollmentDate) between 30 and 37\n" +
                        "        and Refil.RowNumber = 1")
                .createOrReplaceTempView("PrepRefil1stMonth");

        // Prep Refill 3rd Month
        session.sql("select  \n" +
                        "        Refil.PrepNumber,\n" +
                        "        Refil.PatientPK,\n" +
                        "        Refil.SiteCode,\n" +
                        "        Refil.HtsNumber,\n" +
                        "        RegimenPrescribed,\n" +
                        "        Refil.DispenseDate,\n" +
                        "        Patients.PrepEnrollmentDate,\n" +
                        "        datediff(Refil.DispenseDate, Patients.PrepEnrollmentDate)  as RefillThirdMonthDiffInDays\n" +
                        "    from prep_refills_ordered as  Refil\n" +
                        "    left join PrepPatients Patients on Refil.PrepNumber=Patients.PrepNumber and Refil.PatientPk=Patients.PatientPk \n" +
                        "        and Refil.SiteCode=Patients.SiteCode\n" +
                        "    where Refil.PrepNumber is not null \n" +
                        "        and datediff(Refil.DispenseDate, Patients.PrepEnrollmentDate) between 90 and 97")
                .createOrReplaceTempView("PrepRefil3rdMonth");


        // PREP tests
        Dataset<Row> prepTestsDataDf = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("query", "select  \n" +
                        "        distinct\n" +
                        "            PatientPK,\n" +
                        "            SiteCode,\n" +
                        "            TestDate,\n" +
                        "            FinalTestResult\n" +
                        "    from dbo.HTS_ClientTests as tests")
                .load();

        prepTestsDataDf.createOrReplaceTempView("tests");
        prepTestsDataDf.persist(StorageLevel.DISK_ONLY());

        // Prep Pharmacy
        Dataset<Row> patientDataFrame = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("dbtable", "dbo.PrEP_Pharmacy")
                .load();
        patientDataFrame.createOrReplaceTempView("refills");
        patientDataFrame.persist(StorageLevel.DISK_ONLY());


        // Combined source
        session.sql("select" +
                        "        distinct refills.PatientPKHash," +
                        "        refills.SiteCode," +
                        "        refills.PrepNumber," +
                        "        refills.DispenseDate," +
                        "        refill_month_1.RefillFirstMonthDiffInDays," +
                        "        refill_month_1.DispenseDate as DispenseDateMonth1," +
                        "        tests_month_1.TestDate as TestDateMonth1," +
                        "        tests_month_1.FinalTestResult as TestResultsMonth1," +
                        "        refill_month_3.RefillThirdMonthDiffInDays," +
                        "        refill_month_3.DispenseDate as DispenseDateMonth3," +
                        "        tests_month_3.TestDate as TestDateMonth3," +
                        "        tests_month_3.FinalTestResult as TestResultsMonth3" +
                        "    from refills" +
                        "    left join PrepRefil1stMonth as refill_month_1 on refill_month_1.PatientPK = refills.PatientPK" +
                        "        and refill_month_1.SiteCode = refills.Sitecode" +
                        "    left join PrepRefil3rdMonth as refill_month_3 on refill_month_3.PatientPK = refills.PatientPK" +
                        "        and refill_month_3.Sitecode = refills.Sitecode" +
                        "    left join tests as tests_month_1 on tests_month_1.PatientPK = refill_month_1.PatientPK" +
                        "        and tests_month_1.SiteCode = refill_month_1.SiteCode" +
                        "        and tests_month_1.TestDate = refill_month_1.DispenseDate" +
                        "    left join tests as tests_month_3 on tests_month_3.PatientPK = refill_month_3.PatientPK" +
                        "        and tests_month_3.SiteCode = refill_month_3.SiteCode" +
                        "        and tests_month_3.TestDate = refill_month_3.DispenseDate")
                .createOrReplaceTempView("source_data");


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

        String factPrepRefilQuery = loadPREPRefills.loadQuery("LoadFactPREPRefills.sql");

        Dataset<Row> factPrepRefilDf = session.sql(factPrepRefilQuery);
        factPrepRefilDf = factPrepRefilDf.withColumn("FactKey", monotonically_increasing_id().plus(1));
        factPrepRefilDf.printSchema();
        long factExitsCount = factPrepRefilDf.count();
        logger.info("Fact Prep Refills Count is: " + factExitsCount);
        final int writePartitions = 50;
        factPrepRefilDf
                .repartition(writePartitions)
                .write()
                .format("jdbc")
                .option("url", rtConfig.get("spark.edw.url"))
                .option("driver", rtConfig.get("spark.edw.driver"))
                .option("user", rtConfig.get("spark.edw.user"))
                .option("password", rtConfig.get("spark.edw.password"))
                .option("dbtable", "dbo.FactPrepRefills")
                .mode(SaveMode.Overwrite)
                .save();
    }

    private String loadQuery(String fileName) {
        String query;
        InputStream inputStream = LoadFactPREPRefills.class.getClassLoader().getResourceAsStream(fileName);
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