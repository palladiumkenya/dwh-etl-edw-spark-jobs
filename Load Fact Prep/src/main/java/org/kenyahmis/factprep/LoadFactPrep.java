package org.kenyahmis.factprep;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;

import static org.apache.spark.sql.functions.row_number;

public class LoadFactPrep {
    private static final Logger logger = LoggerFactory.getLogger(LoadFactPrep.class);
    public static void main(String[] args) {

        SparkConf conf = new SparkConf();
        conf.setAppName("Load Fact Prep");
        SparkSession session = SparkSession.builder()
                .config(conf)
                .getOrCreate();
        RuntimeConfig rtConfig = session.conf();
        LoadFactPrep loadFactPrep = new LoadFactPrep();

        Dataset<Row> dimMFLPartnerAgencyCombinationDataFrame = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("query", "select distinct MFL_Code,SDP,[SDP_Agency] as Agency from dbo.All_EMRSites")
                .load();
        dimMFLPartnerAgencyCombinationDataFrame.persist(StorageLevel.DISK_ONLY());
        dimMFLPartnerAgencyCombinationDataFrame.createOrReplaceTempView("MFL_partner_agency_combination");

        // Prep Patients
        Dataset<Row> prepPatientDataDf = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("query", " select\n" +
                        "            distinct PatientPKHash,\n" +
                        "            SiteCode\n" +
                        "        from dbo.PrEP_Patient")
                .load();

        prepPatientDataDf.createOrReplaceTempView("prep_patients");
        prepPatientDataDf.persist(StorageLevel.DISK_ONLY());

        // Exit ordering
        Dataset<Row> exitOrderingDataDf = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("query", "select  \n" +
                        "            row_number () over (partition by PrepNumber, PatientPKHash,SiteCode order by ExitDate desc) as num,\n" +
                        "            PatientPKHash,\n" +
                        "            SiteCode,\n" +
                        "            StatusDate,\n" +
                        "            ExitDate,\n" +
                        "            ExitReason,\n" +
                        "            DateOfLastPrepDose\n" +
                        "        from dbo.PrEP_CareTermination")
                .load();

        exitOrderingDataDf.createOrReplaceTempView("exits_ordering");
        exitOrderingDataDf.persist(StorageLevel.DISK_ONLY());

        // Latest exits
        Dataset<Row> latestExitsDataDf = session.sql(" select \n" +
                "            *\n" +
                "        from exits_ordering\n" +
                "        where num = 1");

        latestExitsDataDf.createOrReplaceTempView("latest_exits");
        latestExitsDataDf.persist(StorageLevel.DISK_ONLY());

        // Latest prep assessment
        Dataset<Row> latestPrepAssessmentDataDf = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("query", "select * from Intermediate_LastestPrepAssessments")
                .load();

        latestPrepAssessmentDataDf.createOrReplaceTempView("latest_prep_assessments");
        latestPrepAssessmentDataDf.persist(StorageLevel.DISK_ONLY());

        //  prep last visit
        Dataset<Row> prepLastVisitDataDf = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("dbtable", "dbo.Intermediate_PrepLastVisit")
                .load();

        prepLastVisitDataDf.createOrReplaceTempView("Intermediate_PrepLastVisit");
        prepLastVisitDataDf.persist(StorageLevel.DISK_ONLY());

        //  prep refills
        Dataset<Row> prepRefillsDataDf = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("dbtable", "dbo.Intermediate_PrepRefills")
                .load();

        prepRefillsDataDf.createOrReplaceTempView("Intermediate_PrepRefills");
        prepRefillsDataDf.persist(StorageLevel.DISK_ONLY());

        Dataset<Row> dimDateDataFrame = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.edw.url"))
                .option("driver", rtConfig.get("spark.edw.driver"))
                .option("user", rtConfig.get("spark.edw.user"))
                .option("password", rtConfig.get("spark.edw.password"))
                .option("dbtable", rtConfig.get("spark.dimDate.dbtable"))
                .load();
        dimDateDataFrame.persist(StorageLevel.DISK_ONLY());
        dimDateDataFrame.createOrReplaceTempView("DimDate");

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
                .option("dbtable", "dbo.DimAgeGroup")
                .load();
        dimAgeGroupDataFrame.persist(StorageLevel.DISK_ONLY());
        dimAgeGroupDataFrame.createOrReplaceTempView("DimAgeGroup");

        String factPrepQuery = loadFactPrep.loadQuery("LoadFactPrep.sql");
        Dataset<Row> factPrepDf = session.sql(factPrepQuery);

        // Add FactKey Column
        WindowSpec window = Window.orderBy("PatientKey");
        factPrepDf = factPrepDf.withColumn("FactKey",  row_number().over(window));
        factPrepDf.printSchema();
        factPrepDf
                .repartition(Integer.parseInt(rtConfig.get("spark.default.numpartitions")))
                .write()
                .format("jdbc")
                .option("url", rtConfig.get("spark.edw.url"))
                .option("driver", rtConfig.get("spark.edw.driver"))
                .option("user", rtConfig.get("spark.edw.user"))
                .option("password", rtConfig.get("spark.edw.password"))
                .option("dbtable", "dbo.FactPrep")
                .mode(SaveMode.Overwrite)
                .save();
        // TODO Create primary key
    }

    private String loadQuery(String fileName) {
        String query;
        InputStream inputStream = LoadFactPrep.class.getClassLoader().getResourceAsStream(fileName);
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
