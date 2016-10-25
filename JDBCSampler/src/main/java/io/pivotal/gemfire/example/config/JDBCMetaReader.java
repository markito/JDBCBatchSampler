package io.pivotal.gemfire.example.config;

import java.sql.*;
import java.util.logging.Logger;

/**
 * Created by wmarkito on 11/21/14.
 */
public class JDBCMetaReader {

    // options
    enum OPT {INSERT, TYPEPAIR}

    private static final Logger LOGGER = Logger.getLogger(JDBCMetaReader.class.getCanonicalName());

    // database properties
    final static String URL = System.getProperty("URL", "jdbc:gemfirexd://localhost:1527/");
    final static String USER = System.getProperty("USER", "APP");
    final static String PASS = System.getProperty("PASSWORD", "APP");
    final static String SCHEMA = System.getProperty("SCHEMA", "APP");
    final static String TABLES = System.getProperty("TABLES", "%");

    /**
     * table name constant position under metadata
     */
    final static int TABLE_NAME_POS = 3;

    /**
     * data type constant position under metadata
     */
    final static int DATA_TYPE_POS = 6;

    // run type
    final static String TYPE = System.getProperty("TYPE", OPT.INSERT.toString()).toUpperCase();

    public static void main(String[] args) {
        LOGGER.info("JDBC Metadata reader");

        Connection conn = null;
        ResultSet resultSet = null;

        try {

            conn = DriverManager.getConnection(URL, USER, PASS);
            DatabaseMetaData meta = conn.getMetaData();
            resultSet = meta.getTables(null, SCHEMA, TABLES, null); // catalog (null) , schema, table pattern

            if (TYPE.equals(OPT.INSERT.toString())) {
                generateInsertWithParams(meta, resultSet);

            } else if (TYPE.equals(OPT.TYPEPAIR.toString())) {
                generateFieldTypePair(meta, resultSet);

            } else {
                printHelp();
                throw new IllegalArgumentException("Error: Parameters not recognized or not well formatted.");

            }

            // generateInsertStmt(meta, resultSet);
            //generateFieldTypePair(meta, resultSet);
            //generateInsertWithParams(meta, resultSet);
//
//            List<String> columns = new ArrayList<String>();
//            while (rs.next()) {
//                // 1: none
//                // 2: schema
//                // 3: table name
//                // 4: column name
//                // 5: length
//                // 6: data type (CHAR, VARCHAR, TIMESTAMP, ...)
//                System.out.println(rs.getString(4) + "- TYPE:" + rs.getString(6) + "- SIZE:" + rs.getString("COLUMN_SIZE"));
//               //columns.add(rs.getString(4));
//            }

        } catch (SQLException ex) {

            LOGGER.severe("SQLException: " + ex.getMessage());
            LOGGER.severe("SQLState: " + ex.getSQLState());

        } catch (Exception ex) {

            LOGGER.severe("Error: " + ex.getMessage());

        } finally {
            try {
                if (resultSet != null) resultSet.close();
                if (conn != null) conn.close();

            } catch (Exception ex) {
                LOGGER.severe(ex.getMessage());
            }
        }
    }

    private static void printHelp() {
        System.out.println("System properties available: \n " +
                           "-DUSER=[APP] -DPASS=[APP] -DURL=[jdbc:gemfirexd://localhost:1527/] -DSCHEMA=[APP] -DTYPE=[INSERT, TYPEPAIR] \n");
    }


    /**
     * Print insert statements like INSERT INTO TABLE VALUES (?,?,?)
     * @param meta DatabaseMetaData
     * @param rsTable
     * @throws SQLException
     */
    private static void generateInsertWithParams(DatabaseMetaData meta, ResultSet rsTable) throws SQLException {
        int tableCounter=0;
        while (rsTable.next()) {
            String currentTable = rsTable.getString(TABLE_NAME_POS); // tableName

            ResultSet rsColumns = meta.getColumns("%", SCHEMA, currentTable, "%");
            StringBuilder insertStmtVars = new StringBuilder();

            insertStmtVars.append("INSERT INTO ").append(currentTable).append(" VALUES (");
            while (rsColumns.next()) {
                insertStmtVars.append("?").append(",");
            }
            insertStmtVars.deleteCharAt(insertStmtVars.lastIndexOf(","));

            insertStmtVars.append(")");
            LOGGER.info(String.format("Table: %s %n Statement: %s", currentTable, insertStmtVars.toString()));
            tableCounter++;
        }

        if (tableCounter == 0) LOGGER.warning("No tables found.");
    }

    /**
     * Print string pairs like TYPE,SIZE; for every column on a table
     * Example: VARCHAR,30;CHAR,15;TIMESTAMP,26;CHAR,10;
     * @param meta DatabaseMetaData
     * @param rsTable
     * @throws SQLException
     */
    private static void generateFieldTypePair(DatabaseMetaData meta, ResultSet rsTable) throws SQLException {
        int tableCounter=0;
        while (rsTable.next()) {
            String currentTable = rsTable.getString(TABLE_NAME_POS); // tableName

            ResultSet rsColumns = meta.getColumns("%", SCHEMA, currentTable, "%");
            StringBuilder csvTypes = new StringBuilder();

            while (rsColumns.next()) {
                String type = rsColumns.getString(DATA_TYPE_POS); // data type
                csvTypes.append(type).append(",").append(rsColumns.getString("COLUMN_SIZE")).append(";");
            }

            LOGGER.info(String.format("Table: %s %n Types: %s", currentTable, csvTypes.toString()));
            tableCounter++;
        }
        if (tableCounter == 0) LOGGER.warning("No tables found.");
    }


}