package org.antipathy.udfs;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.io.Text;
import org.jsoup.Jsoup;
import org.jsoup.examples.HtmlToPlainText;

@UDFType(deterministic = false, stateful = false)
@Description(name = "strip_html",
        value = "_FUNC_(input) - strips html tags from the given input String"
)
public class StripHtml extends UDF {

    /** Strips html tags from the given input.
     * @param input the html input to parse
     * @return a string with HTML tags removed.
     */
    public Text evaluate(Text input) {
        if (null != input) {
            return new Text(new HtmlToPlainText().getPlainText(Jsoup.parse(input.toString())));
        }
        return null;
    }
}
