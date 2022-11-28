package org.kenyahmis.loaddatedimension;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import static org.apache.spark.sql.functions.*;

public class LoadDateDimension {
    private static final Logger logger = LoggerFactory.getLogger(LoadDateDimension.class);
    public static void main(String[] args) {
        final String sDate = "1900-01-01";
        final String eDate = "2100-12-31";

        SparkConf conf = new SparkConf();
        conf.setAppName("Load Date Dimension");

        SparkSession session = SparkSession.builder()
                .config(conf)
                .getOrCreate();
        RuntimeConfig rtConfig = session.conf();
        Dataset<Row> datesDataframe = session.sql("select explode(sequence(to_date('" + sDate + "'), to_date('" + eDate + "'),interval 1 day)) as Date");
        datesDataframe.createOrReplaceTempView("dates");
        long datesCount = datesDataframe.count();
        final String queryFileName = "LoadDateDimension.sql";
        String query;
        InputStream inputStream = LoadDateDimension.class.getClassLoader().getResourceAsStream(queryFileName);
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
        datesDataframe = session.sql(query);

        datesDataframe.printSchema();
        datesDataframe.write()
                .format("jdbc")
                .option("url", rtConfig.get("spark.edw.url"))
                .option("driver", rtConfig.get("spark.edw.driver"))
                .option("user", rtConfig.get("spark.edw.user"))
                .option("password", rtConfig.get("spark.edw.password"))
                .option("dbtable", rtConfig.get("spark.dimDate.dbtable"))
                .option("truncate", "true")
                .mode(SaveMode.Overwrite)
                .save();
    }
}
