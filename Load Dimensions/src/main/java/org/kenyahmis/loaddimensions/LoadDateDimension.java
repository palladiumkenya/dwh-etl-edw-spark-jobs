package org.kenyahmis.loaddimensions;

import org.apache.commons.io.IOUtils;
import org.apache.spark.sql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;

public class LoadDateDimension {
    private static final Logger logger = LoggerFactory.getLogger(LoadDateDimension.class);

    public void loadDates(SparkSession session) {
        final String sDate = "1900-01-01";
        final String eDate = "2100-12-31";

        Dataset<Row> dates = session.sql("select explode(sequence(to_date('" + sDate + "'), to_date('" + eDate + "'),interval 1 day)) as calendarDate");
        dates.createOrReplaceTempView("dates");

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
        Dataset<Row> dimDateDf = session.sql(query);
        RuntimeConfig rtConfig = session.conf();
        dimDateDf.write()
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
