package org.kenyahmis.dimhtstraceoutcome;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.spark.sql.functions.row_number;

public class LoadDimHtsTraceOutcome {
    private static final Logger logger = LoggerFactory.getLogger(LoadDimHtsTraceOutcome.class);

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("Load Dim HTS Trace Outcome");
        SparkSession session = SparkSession.builder()
                .config(conf)
                .getOrCreate();
        RuntimeConfig rtConfig = session.conf();

        logger.info("Loading Trace outcome source");
        Dataset<Row> testKitNameSourceDataDf = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("query", "select distinct TracingOutcome as TraceOutcome from dbo.HTS_ClientTracing \n" +
                        "    where  TracingOutcome <> 'null' and TracingOutcome <> ''\n" +
                        "    union\n" +
                        "    select distinct TraceOutcome from ODS.dbo.HTS_PartnerTracings\n" +
                        "    where  TraceOutcome <> 'null' and TraceOutcome <> ''")
                .load();
        testKitNameSourceDataDf.persist(StorageLevel.DISK_ONLY());
        testKitNameSourceDataDf.createOrReplaceTempView("source_data");
        testKitNameSourceDataDf.printSchema();

        Dataset<Row> dimTraceOutcome = session.sql("select \n" +
                "    distinct\n" +
                "    case\n" +
                "        when source_data.TraceOutcome in ('Contact Not Reached', 'Contacted and not Reached') then 'Contact Not Reached'\n" +
                "        else source_data.TraceOutcome\n" +
                "    end as TraceOutcome,\n" +
                "    current_date() as LoadDate\n" +
                "from source_data");

        dimTraceOutcome
                .write()
                .format("jdbc")
                .option("url", rtConfig.get("spark.edw.url"))
                .option("driver", rtConfig.get("spark.edw.driver"))
                .option("user", rtConfig.get("spark.edw.user"))
                .option("password", rtConfig.get("spark.edw.password"))
                .option("truncate", "true")
                .option("dbtable", "dbo.DimHTSTraceOutcome")
                .mode(SaveMode.Overwrite)
                .save();
    }
}
