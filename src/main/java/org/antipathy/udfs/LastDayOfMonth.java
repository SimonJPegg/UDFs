package org.antipathy.udfs;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;

@UDFType(deterministic = false, stateful = false)
@Description(name = "last_day",
        value = "_FUNC_(inputString,dateFormat) - returns the last day of the month" +
                "from a date string based on the pattern specified by dateFormat"
)
public class LastDayOfMonth extends UDF {

    /** Returns the last day of the month from the date string
     * @param inputString the date string to parse
     * @param dateFormat the format of the date string
     * @return the last day of the month as an integer
     */
    public IntWritable evaluate(Text inputString, Text dateFormat) {
        try {
            DateTime date = DateTimeFormat.forPattern(dateFormat.toString()).parseDateTime(inputString.toString());
            return new IntWritable(date.dayOfMonth().withMaximumValue().getDayOfMonth());
        } catch (Exception e) {
            return null;
        }
    }
}
