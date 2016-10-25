package io.pivotal.gemfire.example.generator;

/**
 * @author markito
 */
public enum DataTypes {

    SMALLINT("SMALLINT"),
    INTEGER("INTEGER"),
    INT("INT"),
    BIGINT("BIGINT"),
    NUMERIC("NUMERIC"),
    DECIMAL("DECIMAL"),
    DOUBLE("DOUBLE"),
    FLOAT("FLOAT"),
    REAL("REAL"),
    CHAR("CHAR"),
    VARCHAR("VARCHAR"),
    DATE("DATE"),
    TIMESTAMP("TIMESTAMP"),
    INTRANGE("INTRANGE"), // INTRANGE can be used to keep some ids between a range
    FIXED("FIXED"); // fixed data for relationships - no type check

    private final String type;

    DataTypes(String type) {
        this.type = type;
    }

    public String toString() {
        return this.type;
    }
}
