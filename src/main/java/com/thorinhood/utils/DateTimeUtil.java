package com.thorinhood.utils;

import java.io.File;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

public class DateTimeUtil {

    private final static SimpleDateFormat SDF = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss 'GMT'",
            Locale.ENGLISH);

    public static String parseDateTime(File file) {
        return SDF.format(new Date(file.lastModified()));
    }

    public static Date parseStrTime(String dateTime) throws ParseException {
        return SDF.parse(dateTime);
    }

    public static String currentDateTime() {
        return SDF.format(new Date());
    }

}
