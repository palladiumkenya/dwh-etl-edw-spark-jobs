package org.kenyahmis.dimpatients;

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

public class LoadDimPatients {
    private static final Logger logger = LoggerFactory.getLogger(LoadDimPatients.class);

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("Load Dim Patients");
        SparkSession session = SparkSession.builder()
                .config(conf)
                .getOrCreate();
        RuntimeConfig rtConfig = session.conf();

        LoadDimPatients loadDimPatients = new LoadDimPatients();
        final String loadCtPatientSourceDataQueryFileName = "CTPatientSource.sql";
        String loadCtPatientSourceQuery = loadDimPatients.loadQuery(loadCtPatientSourceDataQueryFileName);

        // C&T patient source
        logger.info("Loading C&T patient source");
        Dataset<Row> ctPatientSourceDataDf = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("query", loadCtPatientSourceQuery)
                .load();
        ctPatientSourceDataDf.persist(StorageLevel.DISK_ONLY());
        ctPatientSourceDataDf.createOrReplaceTempView("ct_patient_source");
        ctPatientSourceDataDf.printSchema();

        // hts patient source
        logger.info("Loading HTS Patient source");
        final String loadHtsPatientSourceDataQueryFileName = "HTSPatientSource.sql";
        String loadHtsPatientSourceQuery = loadDimPatients.loadQuery(loadHtsPatientSourceDataQueryFileName);
        Dataset<Row> htsPatientSourceDataDf = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("query", loadHtsPatientSourceQuery)
                .load();
        htsPatientSourceDataDf.persist(StorageLevel.DISK_ONLY());
        htsPatientSourceDataDf.createOrReplaceTempView("hts_patient_source");
        htsPatientSourceDataDf.printSchema();

        // prep patient source
        logger.info("Loading Prep Patient source");
        final String loadPrepPatientSourceDataQueryFileName = "PrepPatientSource.sql";
        String loadPrepPatientSourceQuery = loadDimPatients.loadQuery(loadPrepPatientSourceDataQueryFileName);
        Dataset<Row> prepPatientSourceDataDf = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("query", loadPrepPatientSourceQuery)
                .load();
        prepPatientSourceDataDf.persist(StorageLevel.DISK_ONLY());
        prepPatientSourceDataDf.createOrReplaceTempView("prep_patient_source");
        prepPatientSourceDataDf.printSchema();

        // combined data ct hts
        final String loadCombinedCtHtsDataQueryFileName = "CombinedDataCTHTS.sql";
        String loadCombinedCtHtsQuery = loadDimPatients.loadQuery(loadCombinedCtHtsDataQueryFileName);
        Dataset<Row> combinedCtHtsDf = session.sql(loadCombinedCtHtsQuery);
        combinedCtHtsDf.persist(StorageLevel.DISK_ONLY());
        combinedCtHtsDf.createOrReplaceTempView("combined_data_ct_hts");

        // combined data ct hts
        final String loadCombinedCtHtsPrepDataQueryFileName = "CombinedDataCTHTSPrep.sql";
        String loadCombinedCtHtsPrepQuery = loadDimPatients.loadQuery(loadCombinedCtHtsPrepDataQueryFileName);
        Dataset<Row> combinedCtHtsPrepDf = session.sql(loadCombinedCtHtsPrepQuery);
        combinedCtHtsPrepDf.persist(StorageLevel.DISK_ONLY());
        combinedCtHtsPrepDf.createOrReplaceTempView("combined_data_ct_hts_prep");

        // load dim patients
        String loadDimPatientsQuery = loadDimPatients.loadQuery("LoadDimPatients.sql");
        Dataset<Row> dimPatients = session.sql(loadDimPatientsQuery);

        WindowSpec window = Window.orderBy("DOB");
        dimPatients = dimPatients.withColumn("PatientKey",  row_number().over(window));
        dimPatients
                .repartition(Integer.parseInt(rtConfig.get("spark.default.numpartitions")))
                .write()
                .format("jdbc")
                .option("url", rtConfig.get("spark.edw.url"))
                .option("driver", rtConfig.get("spark.edw.driver"))
                .option("user", rtConfig.get("spark.edw.user"))
                .option("password", rtConfig.get("spark.edw.password"))
                .option("dbtable", rtConfig.get("spark.dimPatient.dbtable"))
                .mode(SaveMode.Overwrite)
                .save();
    }

    private String loadQuery(String fileName) {
        String query;
        InputStream inputStream = LoadDimPatients.class.getClassLoader().getResourceAsStream(fileName);
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
