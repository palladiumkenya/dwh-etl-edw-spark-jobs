package org.kenyahmis.htstracetype;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.spark.sql.functions.row_number;

public class LoadDimHtsTraceType {
    private static final Logger logger = LoggerFactory.getLogger(LoadDimHtsTraceType.class);

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("Load Dim HTS Trace Type");
        SparkSession session = SparkSession.builder()
                .config(conf)
                .getOrCreate();
        RuntimeConfig rtConfig = session.conf();

        logger.info("Loading HTS trace type source");
        Dataset<Row> traceTypeSourceDataDf = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("query", "select distinct TracingType as TraceType from dbo.HTS_ClientTracing\n" +
                        "        union\n" +
                        "        select distinct TraceType from ODS.dbo.HTS_PartnerTracings")
                .load();
        traceTypeSourceDataDf.persist(StorageLevel.DISK_ONLY());
        traceTypeSourceDataDf.createOrReplaceTempView("source_data");
        traceTypeSourceDataDf.printSchema();

        Dataset<Row> dimTraceType = session.sql("select\n" +
                "       distinct \n" +
                "       source_data.*,\n" +
                "       current_date() as LoadDate\n" +
                " from source_data");

        dimTraceType
                .write()
                .format("jdbc")
                .option("url", rtConfig.get("spark.edw.url"))
                .option("driver", rtConfig.get("spark.edw.driver"))
                .option("user", rtConfig.get("spark.edw.user"))
                .option("password", rtConfig.get("spark.edw.password"))
                .option("truncate", "true")
                .option("dbtable", "dbo.DimHTSTraceType")
                .mode(SaveMode.Overwrite)
                .save();
    }
}
