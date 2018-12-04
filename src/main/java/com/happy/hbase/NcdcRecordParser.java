package com.happy.hbase;


import org.apache.commons.net.ntp.TimeStamp;
import org.apache.hadoop.io.Text;

import java.text.DateFormat;
import java.util.Date;

/**
 *  http://www.ncdc.noaa.gov/
 */

public class NcdcRecordParser {
    private static final int MISSING_TEMPERATURE = 9999;
    private String year;
    private int airTempearture;
    private String quality;
    private String stationId;
    private String observationDate;


    public void parse(String record) {
        stationId = record.substring(4, 10);
        year = record.substring(15, 19);
        observationDate = record.substring(15, 23);
        String airTemperatureString;
        if (record.charAt(87) == '+') {
            airTemperatureString = record.substring(88, 92);
        } else {
            airTemperatureString = record.substring(87, 92);
        }
        airTempearture = Integer.parseInt(airTemperatureString);
        quality = record.substring(92, 93);
    }

    public void parse(Text record) {
        parse(record.toString());
    }

    public boolean isValidTemp() {
        return airTempearture != MISSING_TEMPERATURE && quality.matches("[01459]");
    }

    public String getStationId() {
        return stationId;
    }

    public String getYear() {
        return year;
    }

    public long getObservationDate() {
        return Long.parseLong(observationDate);
    }

    public int getAirTempearture() {
        return airTempearture;
    }
}
