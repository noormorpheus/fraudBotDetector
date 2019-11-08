package fraud.bot;

import org.junit.Assert;
import org.junit.Test;

public class TestLogParsing {
    @Test
    public void testSplittingLogLine () {
        String inptTestStr = "79.133.142.132 - - [25/May/2015:23:11:54 +0000] \"GET / HTTP/1.0\" 200 3557 \"-\" \"Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; snprtz|S26320700000083|2600#Service Pack 1#2#5#154321|isdn)";
        String[] inptTestArry = inptTestStr.split(" ");
        Assert.assertTrue(inptTestArry.length>2);

        Assert.assertEquals(inptTestArry[3], "[25/May/2015:23:11:54");

        System.out.println(inptTestArry[3].substring(1,inptTestArry[3].length()));
    }
}
