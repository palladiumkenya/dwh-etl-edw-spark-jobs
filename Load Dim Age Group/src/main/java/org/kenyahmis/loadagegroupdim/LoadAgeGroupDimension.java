package org.kenyahmis.loadagegroupdim;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.sql.Date;
import java.time.LocalDate;

import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.row_number;

public class LoadAgeGroupDimension {
    private static final Logger logger = LoggerFactory.getLogger(LoadAgeGroupDimension.class);

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("Load Age Group Dimension");

        SparkSession session = SparkSession.builder()
                .config(conf)
                .getOrCreate();
        RuntimeConfig rtConfig = session.conf();

        final String queryFileName = "LoadAgeGroups.sql";
        String query;
        InputStream inputStream = LoadAgeGroupDimension.class.getClassLoader().getResourceAsStream(queryFileName);
        if (inputStream == null) {
            logger.error(queryFileName + " not found");
            return;
        }
        try {
            query = IOUtils.toString(inputStream, Charset.defaultCharset());
        } catch (IOException e) {
            logger.error("Failed to load query from file", e);
            return;
        }
        Dataset<Row> ageGroups =  session.sql(query);
        ageGroups = ageGroups.withColumn("LoadDate", lit(Date.valueOf(LocalDate.now())));
        // Add DimensionKey Column
        WindowSpec window = Window.orderBy("Age");
        ageGroups = ageGroups.withColumn("AgeGroupKey",  row_number().over(window));
        ageGroups.printSchema();
        ageGroups.write()
                .format("jdbc")
                .option("url", rtConfig.get("spark.edw.url"))
                .option("driver", rtConfig.get("spark.edw.driver"))
                .option("user", rtConfig.get("spark.edw.user"))
                .option("password", rtConfig.get("spark.edw.password"))
                .option("dbtable", "dbo.DimAgeGroup")
                .option("truncate", "true")
                .mode(SaveMode.Overwrite)
                .save();
    }
}
