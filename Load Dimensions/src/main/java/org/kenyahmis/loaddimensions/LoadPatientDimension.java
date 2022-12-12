package org.kenyahmis.loaddimensions;

import org.apache.spark.sql.*;
import org.apache.spark.storage.StorageLevel;

public class LoadPatientDimension {

    public void loadPatients(SparkSession session) {
        RuntimeConfig rtConfig = session.conf();
        String sourcePatients = "SELECT DISTINCT patients.PatientID,patients.PatientPK,patients.SiteCode,Gender,DOB,MaritalStatus,NUPI,PatientType,PatientSource,eWHO,eWHODate,bWHO,bWHODate\n" +
                " FROM [dbo].[CT_Patient] patients\n" +
                " left join dbo.CT_PatientsWABWHOCD4 as wabwhocd4 on patients.PatientID = wabwhocd4.PatientID\n" +
                " WHERE patients.SiteCode >0";
        Dataset<Row> sourcePatientsDataframe = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("numpartitions", 50)
                .option("query", sourcePatients)
                .load();
        sourcePatientsDataframe.persist(StorageLevel.DISK_ONLY());
        sourcePatientsDataframe.createOrReplaceTempView("source_patient");
        sourcePatientsDataframe.count();

        Dataset<Row> dimPatient = session.sql("SELECT PatientID,PatientPK,SiteCode,Gender,DOB,MaritalStatus,NUPI," +
                "PatientType,PatientSource,eWHO,eWHODate,bWHO,bWHODate FROM source_patient");

        dimPatient.printSchema();
        dimPatient.show();
        dimPatient
                .repartition(50)
                .write()
                .format("jdbc")
                .option("url", rtConfig.get("spark.edw.url"))
                .option("driver", rtConfig.get("spark.edw.driver"))
                .option("user", rtConfig.get("spark.edw.user"))
                .option("password", rtConfig.get("spark.edw.password"))
                .option("dbtable", rtConfig.get("spark.dimPatient.dbtable"))
                .option("truncate", "true")
                .mode(SaveMode.Overwrite)
                .save();
    }
}
