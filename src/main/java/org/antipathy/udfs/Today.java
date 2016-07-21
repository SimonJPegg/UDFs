package org.antipathy.udfs;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.io.Text;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;

@UDFType(deterministic = false, stateful = false)
@Description(name = "today",
        value = "_FUNC_() - returns the current date in the format yyyy-MM-dd HH:mm:ss.SSS" +
                "_FUNC_(dateFormat) - returns the current date in the format specified by dateFormat"
)
public class Today extends UDF {

    public Text evaluate() {
        DateTime date = new DateTime();
        return new Text(DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS").print(date));
    }

    public Text evaluate(Text dateFormat) {
        try {
            DateTime date = new DateTime();
            return new Text(DateTimeFormat.forPattern(dateFormat.toString()).print(date));
        } catch (Exception e) {
            return null;
        }
    }
}
