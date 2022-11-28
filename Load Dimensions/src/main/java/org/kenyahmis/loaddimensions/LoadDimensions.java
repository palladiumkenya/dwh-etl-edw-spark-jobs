package org.kenyahmis.loaddimensions;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoadDimensions {
    private static final Logger logger = LoggerFactory.getLogger(LoadDimensions.class);

    public static void main(String[] args) {


        SparkConf conf = new SparkConf();
        conf.setAppName("Load Dimensions");
        SparkSession session = SparkSession.builder()
                .config(conf)
                .getOrCreate();

        // load dimensions
        new LoadFacilitiesDimension().loadFacilities(session);
        new LoadPatientDimension().loadPatients(session);
        new LoadPartnerDimension().loadPartners(session);
        new LoadAgencyDimension().loadAgencies(session);
        new LoadARTOutcomeDimension().loadARTOutcomes(session);

    }
}
