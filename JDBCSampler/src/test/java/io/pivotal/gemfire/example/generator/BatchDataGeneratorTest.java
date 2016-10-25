package io.pivotal.gemfire.example.generator;


import org.junit.Before;
import org.junit.Test;

import java.util.logging.Logger;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class BatchDataGeneratorTest {

    private final static Logger LOG = Logger.getAnonymousLogger();
    final int MAX_ENTRIES = 1000;
//    private final BatchDataGenerator generator = new BatchDataGenerator();

    @Before
    public void setUp() throws Exception {

    }

    @Test
    public void GenerateRandomData_basic10Entries() throws Exception {
        final String input = "SMALLINT,10;INTEGER,10;BIGINT,10;DECIMAL,10;DOUBLE,10;FLOAT,10;REAL,10;CHAR,10;VARCHAR,100;DATE,10;TIMESTAMP,26;FIXED:10;INT,10;NUMERIC,10;";

        String[] results = BatchDataGenerator.generateRandomData(input, MAX_ENTRIES);
        LOG.info(String.format("Entry[0]: %s", results[0]));

        if (results != null)
            assertTrue(results.length == MAX_ENTRIES);
        else
            fail("Fail basic sanity test");
    }

    @Test
    public void GenerateRandomData_numberOfCols() throws Exception {
        final String input = "SMALLINT,10;INTEGER,10;BIGINT,10;DECIMAL,10;DOUBLE,10;FLOAT,10;REAL,10;CHAR,10;VARCHAR,100;DATE,10;TIMESTAMP,26;FIXED:Sampler;INTRANGE:10-200,10;INT,10;NUMERIC,10;";

        String[] results = BatchDataGenerator.generateRandomData(input, MAX_ENTRIES);
        LOG.info(String.format("Entry[0]: %s", results[0]));

        if (results != null)
            assertTrue(results[0].split(",").length == input.split(",").length);
         else
            fail("Incorrect number of columns");

    }

    @Test
    public void GenerateRandomData_allTypes() throws Exception {
        final String input = "SMALLINT,10;INTEGER,10;BIGINT,10;DECIMAL,10;DOUBLE,10;FLOAT,10;REAL,10;CHAR,10;VARCHAR,100;DATE,10;TIMESTAMP,26;FIXED:10;INT,10;NUMERIC,10;";

        String[] results = BatchDataGenerator.generateRandomData(input, MAX_ENTRIES);
        LOG.info(String.format("Entry[0]: %s", results[0]));

        if (results != null)
            assertTrue(results.length == MAX_ENTRIES);
        else
            assertTrue(false);
    }


}