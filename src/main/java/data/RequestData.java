package data;

public class RequestData {
    private int intValue;
    private String strValue;

    public RequestData() {
    }

    public RequestData(int intValue, String strValue) {
        this.intValue = intValue;
        this.strValue = strValue;
    }

    public int getIntValue() {
        return intValue;
    }

    public String getStrValue() {
        return strValue;
    }

    public RequestData setIntValue(int intValue) {
        this.intValue = intValue;
        return this;
    }

    public RequestData setStrValue(String strValue) {
        this.strValue = strValue;
        return this;
    }
}
