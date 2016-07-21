package org.antipathy.udfs.test;

import junit.framework.Assert;
import org.antipathy.udfs.StripHtml;
import org.apache.hadoop.io.Text;
import org.junit.Test;

public class StripHtmlTest {

    @Test
    public void testUDF() {
        String html = "<!DOCTYPE html>\n" +
                "<html lang=\"en\">\n" +
                "<head>\n" +
                "    <meta charset=\"UTF-8\">\n" +
                "    <title>Some Page</title>\n" +
                "</head>\n" +
                "<body>\n" +
                "Some text\n" +
                "</body>\n" +
                "</html>";
        StripHtml udf = new StripHtml();
        Assert.assertTrue(udf.evaluate(new Text(html)).toString().equals("Some Page  Some text "));
    }

    @Test
    public void testUDFWithNull() {
        StripHtml udf = new StripHtml();
        Assert.assertEquals(udf.evaluate(null), null);
    }

    @Test
    public void testUDFwithEmptyString() {
        StripHtml udf = new StripHtml();
        String testString = "";
        Assert.assertTrue(udf.evaluate(new Text(testString)).toString().equals(testString));
    }

    @Test
    public void testUDFWithNoHTML() {
        StripHtml udf = new StripHtml();
        String testString = "Non-HTML text";
        Assert.assertTrue(udf.evaluate(new Text(testString)).toString().equals(testString));
    }
}
