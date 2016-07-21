package org.antipathy.udfs.test;

import junit.framework.Assert;
import org.antipathy.udfs.StrToTimeStamp;
import org.apache.hadoop.io.Text;
import org.junit.Test;


public class StrToTimeStampTest {

    @Test
    public void testUDF() {
        StrToTimeStamp udf = new StrToTimeStamp();

        Assert.assertTrue(udf.evaluate(new Text("03/04/2016"), new Text("dd/MM/yyyy"),
                new Text("yyyy-MM-dd HH:mm:ss.SSS")).toString()
                .equals("2016-04-03 00:00:00.000"));
    }

    @Test
    public void testWithInvalidFormat() {
        StrToTimeStamp udf = new StrToTimeStamp();

        Assert.assertEquals(udf.evaluate(new Text("03/04/2016"), new Text("dd/MM/yyyy"),
                new Text("")),
                null);
    }
}
