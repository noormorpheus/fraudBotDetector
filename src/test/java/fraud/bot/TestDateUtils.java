package fraud.bot;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.junit.Assert;
import org.junit.Test;

public class TestDateUtils {
    DateTimeFormatter dateTimeFormatter = DateTimeFormat.forPattern("dd/MMM/yyyy:HH:mm:ss");

    @Test
    public void testDateFormat () {

        DateTime logDateTime = dateTimeFormatter.parseDateTime("25/May/2015:23:11:54");
        Assert.assertNotNull(logDateTime);
    }

    @Test
    public void splitDateString () {
        String dateString = "25/May/2015:23:11:54 +0000";
        String[] stringSplit = dateString.split("\\+");
        Assert.assertEquals(2,stringSplit.length);
        DateTime logDateTime = dateTimeFormatter.parseDateTime(stringSplit[0].trim());
        Assert.assertNotNull(logDateTime);
    }
}
