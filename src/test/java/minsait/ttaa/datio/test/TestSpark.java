/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package minsait.ttaa.datio.test;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;
import junit.framework.TestCase;
import static minsait.ttaa.datio.common.Common.*;
import static minsait.ttaa.datio.common.naming.PlayerInput.overall;
import static minsait.ttaa.datio.common.naming.PlayerInput.shortName;
import static minsait.ttaa.datio.common.naming.PlayerInput.teamPosition;
import static minsait.ttaa.datio.common.naming.PlayerOutput.*;
import org.junit.Test;

/**
 *
 * @author User
 */
public class TestSpark extends TestCase {

    static { System.setProperty(HADOOP_HOME_DIR, PATH_HADOPP);
    }
    static SparkSession spark = SparkSession
            .builder()
            .master(SPARK_MODE)
            .getOrCreate();
    private Dataset<Row> df = cleanData(readInput());
    @Test
    public void testValidateNotNull(){
        df = partitionNationTeam(df);
        assertTrue(df != null);
    }
    
    @Test
    public void testValidateContainData(){
        df = partitionNationTeam(df);
        assertTrue(df.count() > 0);
    }
    
    @Test
    public void testValidateAgeRange(){
        df = setAgeRange(df);
        df = df.where(col(COLUMN_AGE_RANGE).$eq$eq$eq(LETTER_UPPER_CASE_A).and(col(COLUMN_AGE).$greater(23)));
        assertTrue(df.count() == 0);
        
    }
    private Dataset<Row> readInput() {
        Dataset<Row> ds = spark.read()
                .option(HEADER, true)
                .option(INFER_SCHEMA, true)
                .csv(INPUT_PATH);
        return ds;
    }
    
    private Dataset<Row> cleanData(Dataset<Row> df) {
        df = df.filter(
                teamPosition.column().isNotNull().and(
                        shortName.column().isNotNull()
                ).and(
                        overall.column().isNotNull()
                )
        );

        return df;
    }
}

