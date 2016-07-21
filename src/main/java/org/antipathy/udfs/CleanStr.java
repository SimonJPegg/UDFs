package org.antipathy.udfs;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.io.Text;

@UDFType(deterministic = false, stateful = false)
@Description(name = "clean_str",
        value = "_FUNC_(inputString,replaceWithnremoveChars) - Replaces chars in the inputString " +
                "specified by removeChars with the string specified by replaceWith"
)
public class CleanStr extends UDF {

    /** Replaces chars in the inputString specified by removeChars with the string specified by
     * replaceWith
     *
     * @param inputString the string to parse
     * @param replaceWith the replacement char
     * @param removeChars a list of chars to replace
     * @return a string stripped of all removeChars.
     */
    public Text evaluate(Text inputString, Text replaceWith, Text... removeChars) {
        if (null != inputString
                && null != replaceWith
                && null != removeChars) {
            String output = inputString.toString();
            for (int removeIter = 0; removeIter < removeChars.length; removeIter ++) {
                if (null != removeChars[removeIter]) {
                    output = output.replaceAll(removeChars[removeIter].toString(),
                            replaceWith.toString());
                }
            }
            return new Text(output);
        }
        return null;
    }
}
