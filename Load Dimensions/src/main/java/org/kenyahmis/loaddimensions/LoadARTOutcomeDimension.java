package org.kenyahmis.loaddimensions;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.sql.Date;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.*;

public class LoadARTOutcomeDimension {
    public void loadARTOutcomes(SparkSession session) {
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
        distinctOutcomes.add(RowFactory.create("uL"));

        Dataset<Row> outcomesDataframe = session.createDataFrame(distinctOutcomes, structType);
        outcomesDataframe = outcomesDataframe
                .withColumn("ARTOutcomeDescription", when(col("ARTOutcome").equalTo("S"), "Stopped")
                        .when(col("ARTOutcome").equalTo("D"), "Dead")
                        .when(col("ARTOutcome").equalTo("L"), "Loss To Follow Up")
                        .when(col("ARTOutcome").equalTo("NV"), "No Visit")
                        .when(col("ARTOutcome").equalTo("T"), "Transferred Out")
                        .when(col("ARTOutcome").equalTo("V"), "Active")
                        .when(col("ARTOutcome").equalTo("NP"), "New Patient")
                        .when(col("ARTOutcome").equalTo("uL"), "Undocumented Loss"))
                .withColumn("LoadDate", lit(Date.valueOf(LocalDate.now())));

        RuntimeConfig rtConfig = session.conf();
        outcomesDataframe.printSchema();
        outcomesDataframe.show();
        outcomesDataframe.write()
                .format("jdbc")
                .option("url", rtConfig.get("spark.edw.url"))
                .option("driver", rtConfig.get("spark.edw.driver"))
                .option("user", rtConfig.get("spark.edw.user"))
                .option("password", rtConfig.get("spark.edw.password"))
                .option("dbtable", rtConfig.get("spark.dimARTOutcome.dbtable"))
                .option("truncate", "true")
                .mode(SaveMode.Overwrite)
                .save();
    }

}
