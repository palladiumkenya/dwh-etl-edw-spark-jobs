package org.kenyahmis.loaddifferentiatedcaredim;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.upper;

public class LoadDifferentiatedCareDim {
    private static final Logger logger = LoggerFactory.getLogger(LoadDifferentiatedCareDim.class);

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("Load Differentiated Care Dimension");

        SparkSession session = SparkSession.builder()
                .config(conf)
                .getOrCreate();
        RuntimeConfig rtConfig = session.conf();

        final String queryFileName = "DifferentiatedCare.sql";
        String differentiatedCareQuery;
        InputStream inputStream = LoadDifferentiatedCareDim.class.getClassLoader().getResourceAsStream(queryFileName);
        if (inputStream == null) {
            logger.error(queryFileName + " not found");
            return;
        }
        try {
            differentiatedCareQuery = IOUtils.toString(inputStream, Charset.defaultCharset());
        } catch (IOException e) {
            logger.error("Failed to load query from file", e);
            return;
        }

        Dataset<Row> differentiatedCareDataframe = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("numpartitions", rtConfig.get("spark.ods.numpartitions"))
                .option("query", differentiatedCareQuery)
                .load();

        differentiatedCareDataframe.persist(StorageLevel.DISK_ONLY());
        differentiatedCareDataframe.createOrReplaceTempView("differentiated_care");

        differentiatedCareDataframe = differentiatedCareDataframe
                .withColumn("DifferentiatedCare", upper(col("DifferentiatedCare")));

        differentiatedCareDataframe.printSchema();
        final int writePartitions = 20;
        differentiatedCareDataframe
                .repartition(writePartitions)
                .write()
                .format("jdbc")
                .option("url", rtConfig.get("spark.edw.url"))
                .option("driver", rtConfig.get("spark.edw.driver"))
                .option("user", rtConfig.get("spark.edw.user"))
                .option("password", rtConfig.get("spark.edw.password"))
                .option("dbtable", rtConfig.get("spark.dimDifferentiatedCare.dbtable"))
                .option("truncate", "true")
                .mode(SaveMode.Overwrite)
                .save();

    }
}
