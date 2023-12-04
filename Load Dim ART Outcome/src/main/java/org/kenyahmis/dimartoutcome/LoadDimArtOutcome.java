package org.kenyahmis.dimartoutcome;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.sql.Date;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.*;

public class LoadDimArtOutcome {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("Load Dim ART Outcome");

        SparkSession session = SparkSession.builder()
                .config(conf)
                .getOrCreate();
        RuntimeConfig rtConfig = session.conf();
        StructType structType = new StructType();
        structType = structType.add("ARTOutcome", DataTypes.StringType, true);

        List<Row> distinctOutcomes = new ArrayList<>();
        distinctOutcomes.add(RowFactory.create("S"));
        distinctOutcomes.add(RowFactory.create("D"));
        distinctOutcomes.add(RowFactory.create("L"));
        distinctOutcomes.add(RowFactory.create("NV"));
        distinctOutcomes.add(RowFactory.create("T"));
        distinctOutcomes.add(RowFactory.create("V"));
        distinctOutcomes.add(RowFactory.create("NP"));
        distinctOutcomes.add(RowFactory.create("UL"));
        distinctOutcomes.add(RowFactory.create("FV"));

        Dataset<Row> outcomesDataframe = session.createDataFrame(distinctOutcomes, structType);
        outcomesDataframe = outcomesDataframe
                .withColumn("ARTOutcomeDescription", when(col("ARTOutcome").equalTo("S"), "Stopped")
                        .when(col("ARTOutcome").equalTo("D"), "DEAD")
                        .when(col("ARTOutcome").equalTo("L"), "LOSS TO FOLLOW UP")
                        .when(col("ARTOutcome").equalTo("NV"), "NO VISIT")
                        .when(col("ARTOutcome").equalTo("T"), "TRANSFERRED OUT")
                        .when(col("ARTOutcome").equalTo("V"), "ACTIVE")
                        .when(col("ARTOutcome").equalTo("NP"), "NEW PATIENT")
                        .when(col("ARTOutcome").equalTo("UL"), "UNDOCUMENTED LOSS")
                        .when(col("ARTOutcome").equalTo("FV"), "FUTURE VISIT"))
                .withColumn("LoadDate", lit(Date.valueOf(LocalDate.now())));

        outcomesDataframe.printSchema();
        outcomesDataframe.write()
                .format("jdbc")
                .option("url", rtConfig.get("spark.edw.url"))
                .option("driver", rtConfig.get("spark.edw.driver"))
                .option("user", rtConfig.get("spark.edw.user"))
                .option("password", rtConfig.get("spark.edw.password"))
                .option("truncate", "true")
                .option("dbtable", "dbo.DimARTOutcome")
                .mode(SaveMode.Overwrite)
                .save();
    }
}
