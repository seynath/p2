package com.weather.analytics.model;

import java.util.Objects;

/**
 * Represents a location record from locationData.csv.
 */
public class LocationRecord {
    private final int locationId;
    private final double latitude;
    private final double longitude;
    private final int elevation;
    private final int utcOffsetSeconds;
    private final String timezone;
    private final String timezoneAbbreviation;
    private final String cityName;

    public LocationRecord(int locationId, double latitude, double longitude, int elevation,
                          int utcOffsetSeconds, String timezone, String timezoneAbbreviation, String cityName) {
        this.locationId = locationId;
        this.latitude = latitude;
        this.longitude = longitude;
        this.elevation = elevation;
        this.utcOffsetSeconds = utcOffsetSeconds;
        this.timezone = timezone;
        this.timezoneAbbreviation = timezoneAbbreviation;
        this.cityName = cityName;
    }

    public int getLocationId() { return locationId; }
    public double getLatitude() { return latitude; }
    public double getLongitude() { return longitude; }
    public int getElevation() { return elevation; }
    public int getUtcOffsetSeconds() { return utcOffsetSeconds; }
    public String getTimezone() { return timezone; }
    public String getTimezoneAbbreviation() { return timezoneAbbreviation; }
    public String getCityName() { return cityName; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LocationRecord that = (LocationRecord) o;
        return locationId == that.locationId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(locationId);
    }

    @Override
    public String toString() {
        return "LocationRecord{" +
                "locationId=" + locationId +
                ", cityName='" + cityName + '\'' +
                ", latitude=" + latitude +
                ", longitude=" + longitude +
                '}';
    }
}
