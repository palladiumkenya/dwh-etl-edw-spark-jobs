package org.kenyahmis.loadregimenlinedim;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;

public class LoadRegimenLineDimension {
    private static final Logger logger = LoggerFactory.getLogger(LoadRegimenLineDimension.class);
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("Load Regimen Line Dimension");

        SparkSession session = SparkSession.builder()
                .config(conf)
                .getOrCreate();
        RuntimeConfig rtConfig = session.conf();

        final String sourceRegimenLineFileName = "SourceRegimenLine.sql";
        String sourceRegimenQuery;
        InputStream inputStream = LoadRegimenLineDimension.class.getClassLoader().getResourceAsStream(sourceRegimenLineFileName);
        if (inputStream == null) {
            logger.error(sourceRegimenLineFileName + " not found");
            return;
        }
        try {
            sourceRegimenQuery = IOUtils.toString(inputStream, Charset.defaultCharset());
        } catch (IOException e) {
            logger.error("Failed to load query from file", e);
            return;
        }

        Dataset<Row> sourceRegimenLineDataframe = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("numpartitions", rtConfig.get("spark.ods.numpartitions"))
                .option("query", sourceRegimenQuery)
                .load();

        sourceRegimenLineDataframe.persist(StorageLevel.DISK_ONLY());
        sourceRegimenLineDataframe.createOrReplaceTempView("source_regimen_line");

        final String enrichedSourceRegimenLineFileName = "EnrichedSourceRegimenLine.sql";
        String enrichedSourceRegimenQuery;
        InputStream enrichedSourceRegimentLineInputStream = LoadRegimenLineDimension.class.getClassLoader()
                .getResourceAsStream(enrichedSourceRegimenLineFileName);
        if (enrichedSourceRegimentLineInputStream == null) {
            logger.error(enrichedSourceRegimenLineFileName + " not found");
            return;
        }
        try {
            enrichedSourceRegimenQuery = IOUtils.toString(enrichedSourceRegimentLineInputStream, Charset.defaultCharset());
        } catch (IOException e) {
            logger.error("Failed to load query from file", e);
            return;
        }

        Dataset<Row> enrichedSourceRegimenDf = session.sql(enrichedSourceRegimenQuery);
        enrichedSourceRegimenDf.createOrReplaceTempView("enriched_source_regimen_line");

        Dataset<Row> dimeRegimenLineDf = session.sql("SELECT enriched_source_regimen_line.*," +
                "current_date() AS LoadDate FROM enriched_source_regimen_line");

        final int writePartitions = 20;
        dimeRegimenLineDf.printSchema();
        dimeRegimenLineDf
                .repartition(writePartitions)
                .write()
                .format("jdbc")
                .option("url", rtConfig.get("spark.edw.url"))
                .option("driver", rtConfig.get("spark.edw.driver"))
                .option("user", rtConfig.get("spark.edw.user"))
                .option("password", rtConfig.get("spark.edw.password"))
                .option("dbtable", rtConfig.get("spark.dimRegimenLine.dbtable"))
                .option("truncate", "true")
                .mode(SaveMode.Overwrite)
                .save();
    }
}
