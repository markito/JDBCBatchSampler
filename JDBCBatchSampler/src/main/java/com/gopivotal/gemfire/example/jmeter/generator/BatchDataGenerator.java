package com.gopivotal.gemfire.example.jmeter.generator;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.StringUtils;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by markito on 7/9/14.
 */
public class BatchDataGenerator {
    final static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
    final static SimpleDateFormat timestampFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
    final static Date randomDate = new Date();

    public static String[] generateRandomData(final String input, final int batchSize) {
        String[] fields = StringUtils.split(input,";"); //fieldPattern.split(input);  //input.split(";"); // 0 type, 1 size

        StringBuilder sb = new StringBuilder();
        String[] results = new String[batchSize];

        for (int i = 0; i < batchSize; i++) {
            //clear for next row
            sb.setLength(0);

            for (String item : fields) {
                String[] field = StringUtils.split(item,","); //itemPattern.split(item); // item.split(",");

                if (field[0].equals("SMALLINT")) {
                    sb.append(RandomUtils.nextInt(0, Short.MAX_VALUE - 1)).append(",");

                } else if (field[0].equals("INTEGER") || field[0].equals("INT")) {
                    sb.append(RandomUtils.nextInt(0, Integer.MAX_VALUE - 1)).append(",");

                } else if (field[0].equals("BIGINT")) {
                    sb.append(RandomUtils.nextLong(0, Long.MAX_VALUE - 1)).append(",");
                    //sb.append(RandomUtils.nextLong(1727838964209424310L, 1727838964209424384L)).append(",");

                } else if (field[0].equals("DECIMAL") || field[0].equals("NUMERIC")) {
                    //sb.append(new BigDecimal(3).toString()).append(",");
                    //sb.append(new BigDecimal(3).toString()).append(",");
                    sb.append("0.0").append(",");

                } else if (field[0].equals("DOUBLE")) {
                    sb.append(RandomUtils.nextDouble(0, 012345678901236789012D)).append(",");

                } else if ((field[0].equals("FLOAT")) || (field[0].equals("REAL")) ) {
                    sb.append(RandomUtils.nextFloat(0, 80000F)).append(",");

                } else if (field[0].equals("CHAR")) {
                    sb.append(RandomStringUtils.random(Integer.parseInt(field[1]), true, true)).append(",");

                } else if (field[0].equals("VARCHAR")) {
                    sb.append(RandomStringUtils.random(Integer.parseInt(field[1]), true, true)).append(",");

                } else if (field[0].equals("DATE")) {

                    randomDate.setTime(rndDate());
                    sb.append(dateFormat.format(randomDate)).append(",");

                } else if (field[0].equals("TIMESTAMP")) {
                    randomDate.setTime(rndDate());
                    sb.append(timestampFormat.format(randomDate)).append(",");

                } else {
                    System.out.println("Error Parsing type " + field[0]);
                    break;
                    //System.exit(1);
                }
            }
            sb.deleteCharAt(sb.lastIndexOf(","));
            // batch results
            results[i] = sb.toString();
        }
        return results;
    }

    /**
     * Return random date in milliseconds from 1940 to 2014
     * @return
     */
    private static long rndDate() {
        return -946771200000L + (Math.abs(RandomUtils.nextLong(1L,1416551342L)) % (70L * 365 * 24 * 60 * 60 * 1000));
    }

    public static void main (String[] args) {
        // tests ??

        String[] input = {
                "INTEGER,10;INTEGER,10;INTEGER,10;VARCHAR,16;VARCHAR,2;VARCHAR,16;VARCHAR,20;VARCHAR,20;VARCHAR,20;VARCHAR,2;VARCHAR,9;VARCHAR,16;TIMESTAMP,26;VARCHAR,2;NUMERIC,12;NUMERIC,4;NUMERIC,12;NUMERIC,12;INTEGER,10;INTEGER,10;VARCHAR,500;"
        };

        for (int i=0;i < 10; i++)
            for (String s: input) {
                String[] tmp = new BatchDataGenerator().generateRandomData(s,50);

                System.out.println(tmp[0]);
            }


    }
}
