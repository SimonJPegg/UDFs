package org.antipathy.udfs.test;

import junit.framework.Assert;
import org.antipathy.udfs.CleanStr;
import org.apache.hadoop.io.Text;
import org.junit.Test;

public class CleanStrTest {

    @Test
    public void testUDFWithSingleArg() {
        CleanStr cleanStr = new CleanStr();
        Assert.assertTrue(cleanStr.evaluate(new Text("replace all spaces "), new Text(""),
                new Text(" ")).toString().equals("replaceallspaces"));
    }

    @Test
    public void testUDFWithMultiArg() {
        CleanStr cleanStr = new CleanStr();
        Assert.assertTrue(cleanStr.evaluate(new Text("replace all spaces\nand\nnewlines"), new Text(""),
                new Text(" "), new Text("\n")).toString().equals("replaceallspacesandnewlines"));
    }

    @Test
    public void testUDFWithEmptyStrings() {
        CleanStr cleanStr = new CleanStr();
        Assert.assertTrue(cleanStr.evaluate(new Text("someString"), new Text(""),
                new Text("")).toString().equals("someString"));
    }
}
