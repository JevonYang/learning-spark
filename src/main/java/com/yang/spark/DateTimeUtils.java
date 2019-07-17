package com.yang.spark;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class DateTimeUtils {

    private static final long ONE_HOUR = 1000 * 60 * 60;
    private static final long ONE_DAY = 1000 * 60 * 60 * 23;

    public static LocalDateTime getDateTimeOfTimestamp(long timestamp) {
        Instant instant = Instant.ofEpochMilli(timestamp);
        ZoneId zone = ZoneId.systemDefault();
        return LocalDateTime.ofInstant(instant, zone);
    }

    private static long getTimestampOfDateTime(LocalDateTime localDateTime) {
        ZoneId zone = ZoneId.systemDefault();
        Instant instant = localDateTime.atZone(zone).toInstant();
        return instant.toEpochMilli();
    }

    public static long dropMinutesAndAfter(long timestamp) {
        LocalDateTime time = DateTimeUtils.getDateTimeOfTimestamp(timestamp);
        return DateTimeUtils.getTimestampOfDateTime(LocalDateTime.of(time.toLocalDate(), LocalTime.of(time.getHour(), 0)));
    }

    private static long dropMinutesAndAfter(LocalDateTime dateTime) {
        return DateTimeUtils.getTimestampOfDateTime(LocalDateTime.of(dateTime.toLocalDate(), LocalTime.of(dateTime.getHour(), 0)));
    }

    public static String getDateFromTimeStamp(long timestamp) {
        return DateTimeUtils.getDateTimeOfTimestamp(timestamp).format(DateTimeFormatter.ofPattern("HH:mm"));
    }

    public static String getDateWithoutMinutesFromTimeStamp(long timestamp) {
        LocalDateTime time = DateTimeUtils.getDateTimeOfTimestamp(timestamp/1000);
        return LocalDateTime.of(time.toLocalDate(), LocalTime.of(time.getHour(), 0)).format(DateTimeFormatter.ISO_DATE_TIME);
    }

    public static long oneDayBeforeInTimestamp() {
        return getTimestampOfDateTime(LocalDateTime.now()) - ONE_DAY;
    }

    public static List<Long> last24HoursInTimeStamp() {
        long nowTimestampWithoutMinute = DateTimeUtils.dropMinutesAndAfter(LocalDateTime.now());
        long oneDayBefore = nowTimestampWithoutMinute - ONE_DAY;
        List<Long> list = new ArrayList<>();
        for (int i = 0; i < 24; i++) {
            list.add(oneDayBefore + i * ONE_HOUR);
        }
        return list;
    }

}
