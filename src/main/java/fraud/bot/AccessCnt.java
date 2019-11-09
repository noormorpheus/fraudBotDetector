package fraud.bot;

import org.joda.time.DateTime;

public class AccessCnt {
    DateTime firstAccessTime;
    //number of times a candidate bot violated the window count threshold
    Integer cnt;

    public DateTime getFirstAccessTime() {
        return firstAccessTime;
    }

    public void setFirstAccessTime(DateTime firstAccessTime) {
        this.firstAccessTime = firstAccessTime;
    }

    public Integer getCnt() {
        return cnt;
    }

    public void setCnt(Integer cnt) {
        this.cnt = cnt;
    }
}
