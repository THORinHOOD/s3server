package com.thorinhood.utils;

import java.io.File;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.Date;
import java.util.Locale;

public class DateTimeUtil {

    private final static SimpleDateFormat SDF = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss 'GMT'",
            Locale.ENGLISH);
    private final static SimpleDateFormat SDF_SECOND = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'",
            Locale.ENGLISH);

    public static String parseDateTime(File file) {
        return SDF.format(new Date(file.lastModified()));
    }

    public static String parseDateTimeISO(File file) {
        return SDF_SECOND.format(new Date(file.lastModified()));
    }

    public static Date parseStrTime(String dateTime) throws ParseException {
        return SDF.parse(dateTime);
    }

    public static String currentDateTime() {
        return SDF.format(new Date());
    }

}
