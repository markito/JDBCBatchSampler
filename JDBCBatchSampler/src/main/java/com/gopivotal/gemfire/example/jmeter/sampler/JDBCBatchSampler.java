package com.gopivotal.gemfire.example.jmeter.sampler;

import com.gopivotal.gemfire.example.jmeter.generator.BatchDataGenerator;
import org.apache.jmeter.config.Arguments;
import org.apache.jmeter.protocol.java.sampler.AbstractJavaSamplerClient;
import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;

import org.apache.jmeter.protocol.jdbc.config.DataSourceElement;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.jorphan.logging.LoggingManager;
import org.apache.log.Logger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Created by markito on 7/8/14.
 */
public class JDBCBatchSampler extends AbstractJavaSamplerClient {

    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = LoggingManager.getLoggerForClass();
    //private static final ThreadLocal<BatchDataGenerator> batchDataGenerator = new ThreadLocal<BatchDataGenerator>();

    @Override
    public org.apache.jmeter.samplers.SampleResult runTest(JavaSamplerContext context) {

        // from JavaRequest properties
        int batchSize = context.getIntParameter("Batch size");
        // from JDBC Configuration element
        String dbPool = context.getParameter("Connection Pool Name");
        // SQL to be used for prepareStatement
        String sqlString = context.getParameter("SQL with Params");
        // fields TYPE,SIZE format
        String fieldPair = context.getParameter("Fields Type and Size");

        Connection conn = null;
        PreparedStatement pstm = null;

        SampleResult result = new SampleResult();

        // generate data before sampling
        //String[] randomData = batchDataGenerator.get().generateRandomData(fieldPair, batchSize);
        String[] randomData = BatchDataGenerator.generateRandomData(fieldPair, batchSize);

        try {

            result.sampleStart();
            conn = DataSourceElement.getConnection(dbPool);

            conn.setAutoCommit(false);

            pstm = conn.prepareStatement(sqlString);

            int paramCount = pstm.getParameterMetaData().getParameterCount();

            for (String rowData: randomData) {
                String[] fields = rowData.split(",");

                for (int i = 1; i <= paramCount; i++) {
                    pstm.setString(i, fields[i-1]);
                }

                pstm.addBatch();
            }

            // submit
            pstm.executeBatch();
            conn.commit();
            pstm.clearBatch();

            result.setSampleCount(batchSize);
            result.setSuccessful(true);
            result.setResponseMessage("Ok.");
            result.setResponseCodeOK();

        } catch (SQLException e) {

            LOGGER.error(e.getMessage());

            result.setResponseMessage(e.getMessage());
            result.setSuccessful(false);
            result.setResponseCode("-1");

            try {
                conn.rollback();
            } catch (SQLException e1) {
                e1.printStackTrace();
            }


        } finally {

            result.sampleEnd();

            try {
                if (pstm != null) pstm.close();
                if (conn != null) conn.close();

            } catch (Exception ex) {
                LOGGER.error(ex.getMessage());
            }
        }

        return result;
    }

    public Arguments getDefaultParameters()
    {
        Arguments defaultParameters = new Arguments();
        defaultParameters.addArgument("Batch size", "50");
        defaultParameters.addArgument("Connection Pool Name", "pool");
        defaultParameters.addArgument("SQL with Params", "INSERT INTO table VALUES (?,?)");
        defaultParameters.addArgument("Fields Type and Size", "VARCHAR,30;VARCHAR,30;");

        return defaultParameters;
    }

}
