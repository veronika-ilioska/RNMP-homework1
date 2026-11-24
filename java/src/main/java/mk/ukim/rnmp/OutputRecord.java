package mk.ukim.rnmp;

public class OutputRecord {
    public String key;
    public long timestamp;
    public String json;

    public OutputRecord() {}

    public OutputRecord(String key, long timestamp, String json) {
        this.key = key;
        this.timestamp = timestamp;
        this.json = json;
    }
}
