package org.antipathy.udfs;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.io.Text;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;

@UDFType(deterministic = false, stateful = false)
@Description(name = "str_to_timestamp",
        value = "_FUNC_(input,inputFormatText,outputFormatText) - Converts the date (input) from the format " +
                "specified by inputFormatText to the format specified by outputFormatText"
)
public class StrToTimeStamp extends UDF {

    /**
     * Converts the date (input) from the format specified by inputFormatText to the format specified by outputFormatText
     *
     * @param input the input string to parse
     * @param inputFormatText the date format of the input
     * @param outputFormatText the date format of the output
     * @return a string in the dateformat specified by outputFormatText
     */
    public Text evaluate(Text input, Text inputFormatText, Text outputFormatText) {
        DateTime date = DateTimeFormat.forPattern(inputFormatText.toString()).parseDateTime(input.toString());
        return new Text(DateTimeFormat.forPattern(outputFormatText.toString()).print(date));
    }
}

