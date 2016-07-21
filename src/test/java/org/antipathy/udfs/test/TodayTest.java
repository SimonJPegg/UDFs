package org.antipathy.udfs.test;

import org.joda.time.DateTime;
import org.joda.time.DateTimeUtils;
import org.joda.time.format.DateTimeFormat;
import org.junit.Test;
import org.apache.hadoop.io.Text;
import junit.framework.Assert;
import org.antipathy.udfs.Today;

public class TodayTest {

    @Test
    public void testUDF() {
        DateTimeUtils.setCurrentMillisFixed(new DateTime().getMillis());
        Today udf = new Today();
        Assert.assertEquals(udf.evaluate().toString(), DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS").print(new DateTime()));
    }

    @Test
    public void testWithFormat() {
        DateTimeUtils.setCurrentMillisFixed(new DateTime().getMillis());
        Today udf = new Today();
        Assert.assertEquals(udf.evaluate(new Text("dd/MM/yyyy")).toString(), DateTimeFormat.forPattern("dd/MM/yyyy").print(new DateTime()));
    }

    @Test
    public void testWithInvalidFormat() {
        Today udf = new Today();
        Assert.assertEquals(udf.evaluate(new Text("")),null);
    }
}
