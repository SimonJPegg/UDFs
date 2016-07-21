package org.antipathy.udfs.test;

import junit.framework.Assert;
import org.antipathy.udfs.LastDayOfMonth;
import org.apache.hadoop.io.Text;
import org.junit.Test;

public class LastDayOFMonthTest {

    @Test
    public void testUDF() {
        LastDayOfMonth udf = new LastDayOfMonth();
        Assert.assertEquals(udf.evaluate(new Text("2015-02-03 14:21:00.000"),
                new Text("yyyy-MM-dd HH:mm:ss.SSS")).get(),28);
    }

    @Test
    public void testUDFWithLeapYear() {
        LastDayOfMonth udf = new LastDayOfMonth();
        Assert.assertEquals(udf.evaluate(new Text("03/02/2016"),
                new Text("dd/MM/yyyy")).get(),29);
    }

    @Test
    public void testWithInvalidFormat() {
        LastDayOfMonth udf = new LastDayOfMonth();
        Assert.assertEquals(udf.evaluate(new Text("03/02/2016"),
                new Text("")),null);
    }
}
