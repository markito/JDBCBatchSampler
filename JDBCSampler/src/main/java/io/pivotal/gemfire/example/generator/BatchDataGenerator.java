package io.pivotal.gemfire.example.generator;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.StringUtils;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author markito
 */
public class BatchDataGenerator {

    private final static ThreadLocal<SimpleDateFormat> dateFormat = new ThreadLocal<SimpleDateFormat>() {
        @Override protected SimpleDateFormat initialValue() {
            return new SimpleDateFormat("yyyy-MM-dd");
        }
    };
    private final static ThreadLocal<SimpleDateFormat> timestampFormat =  new ThreadLocal<SimpleDateFormat>() {
        @Override protected SimpleDateFormat initialValue() {
            return new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
        }
    };

    private final static Date randomDate = new Date();

    private final static long beginTime = Timestamp.valueOf("1999-01-01 00:00:00").getTime();
    private final static long endTime = Timestamp.valueOf("2014-12-31 00:58:00").getTime();

//    static {
//        dateFormat.set(new SimpleDateFormat("yyyy-MM-dd"));
//        timestampFormat.set(new SimpleDateFormat("yyyy-MM-dd hh:mm:ss"));
//    }

    /**
     * Generate random data using Apache commons lang library.
     *
     * @param input     String ; separated with common SQL types or 'FIXED' for hardcoded data (relationship)
     * @param batchSize int number of entries per batch
     * @return
     */
    public static String[] generateRandomData(final String input, final int batchSize) {
        String[] fields = StringUtils.split(input, ";"); //fieldPattern.split(input);  //input.split(";"); // 0 type, 1 size

        StringBuilder sb = new StringBuilder();
        String[] results = new String[batchSize];

        for (int i = 0; i < batchSize; i++) {
            //clear for next row
            sb.setLength(0);

            for (String item : fields) {
                String[] field = StringUtils.split(item, ","); //itemPattern.split(item); // item.split(",");

                try {

                    // custom fixed field for relationships
                    if (field[0].contains(DataTypes.FIXED.toString())) {
                        // example FIXED:Key // FIXED:1
                        String[] fixed = field[0].split(":");

                        if (fixed.length > 0) {
                            sb.append(fixed[1]).append(",");
                            continue;
                        } else {
                            throw new RuntimeException(String.format("Error in specified FIXED field: %s:%s", field[0], field[1]));
                        }
                        // range fixed fields
                    } else if (field[0].contains(DataTypes.INTRANGE.toString())) {
                        // example INTRANGE:START-END // INTRANGE:1-100
                        String[] intrange = field[0].split(":");
                        // 0 start - 1 - end
                        String[] limit = intrange[1].split("-");
                        int start = Integer.parseInt(limit[0]);
                        int end = Integer.parseInt(limit[1]);

                        if (end > start) {
                            sb.append(RandomUtils.nextInt(start, end + 1)).append(",");
                            continue;
                        } else {
                            throw new RuntimeException(String.format("Error in specified INTRANGE field: %s:%s", field[0], field[1]));
                        }
                    } else {
                        DataTypes type = DataTypes.valueOf(field[0].toUpperCase());

                        switch (type) {
                            case SMALLINT:
                                sb.append(RandomUtils.nextInt(0, Short.MAX_VALUE - 1)).append(",");
                                break;
                            case INTEGER:
                                sb.append(RandomUtils.nextInt(0, Integer.MAX_VALUE - 1)).append(",");
                                break;
                            case INT:
                                sb.append(RandomUtils.nextInt(0, Integer.MAX_VALUE - 1)).append(",");
                                break;
                            case BIGINT:
                                sb.append(RandomUtils.nextLong(0, Long.MAX_VALUE - 1)).append(",");
                                break;
                            case DECIMAL:
                                // float good enough probably ?
                                sb.append(RandomUtils.nextFloat(0, 89000F)).append(",");
                                break;
                            case DOUBLE:
                                sb.append(RandomUtils.nextDouble(0, 012345678901236789012D)).append(",");
                                break;
                            case FLOAT:
                                sb.append(RandomUtils.nextFloat(0, 89000F)).append(",");
                                break;
                            case CHAR:
                                sb.append(RandomStringUtils.random(Integer.parseInt(field[1]), true, true)).append(",");
                                break;
                            case VARCHAR:
                                sb.append(RandomStringUtils.random(Integer.parseInt(field[1]), true, true)).append(",");
                                break;
                            case DATE:
                                randomDate.setTime(rndDate());
                                sb.append(dateFormat.get().format(randomDate)).append(",");
                                break;
                            case TIMESTAMP:
                                randomDate.setTime(rndDate());
                                sb.append(timestampFormat.get().format(randomDate)).append(",");
                                break;
                            case REAL:
                                sb.append(RandomUtils.nextFloat(0, 80000F)).append(",");
                                break;
                            case NUMERIC:
                                sb.append(RandomUtils.nextFloat(0, 89000F)).append(",");
                                break;
                            default:
                                throw new IllegalArgumentException("Error parsing unrecognized type: " + field[0]);

                        }
                    }
                } catch (Exception ex) {
                    ex.printStackTrace();
                    throw new RuntimeException(String.format("Error processing %s - Message: %s",item, ex.getMessage()));
                }
            }
            sb.deleteCharAt(sb.lastIndexOf(","));
            // batch results
            results[i] = sb.toString();
        }

        return results;
    }

    /**
     * Return random date in milliseconds from <code>beginTime</code> and <code>endTime</code>
     *
     * @return
     */
    private static long rndDate() {
        long diff = endTime - beginTime + 1;
        return beginTime + (long) (Math.random() * diff);
    }

}