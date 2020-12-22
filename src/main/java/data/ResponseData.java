package data;

public class ResponseData {
    private int intValue;

    public ResponseData() {
    }

    public ResponseData(int intValue) {
        this.intValue = intValue;
    }

    public int getIntValue() {
        return intValue;
    }

    public ResponseData setIntValue(int intValue) {
        this.intValue = intValue;
        return this;
    }
}
