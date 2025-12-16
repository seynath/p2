package com.weather.analytics.model;

import java.time.LocalDate;
import java.util.Objects;

/**
 * Represents a single weather observation record from weatherData.csv.
 */
public class WeatherRecord {
    private final int locationId;
    private final LocalDate date;
    private final int weatherCode;
    private final Double temperature2mMax;
    private final Double temperature2mMin;
    private final Double temperature2mMean;
    private final Double apparentTemperatureMax;
    private final Double apparentTemperatureMin;
    private final Double apparentTemperatureMean;
    private final Double daylightDuration;
    private final Double sunshineDuration;
    private final Double precipitationSum;
    private final Double rainSum;
    private final Double precipitationHours;
    private final Double windSpeed10mMax;
    private final Double windGusts10mMax;
    private final Double windDirection10mDominant;
    private final Double shortwaveRadiationSum;
    private final Double et0FaoEvapotranspiration;
    private final String sunrise;
    private final String sunset;

    private WeatherRecord(Builder builder) {
        this.locationId = builder.locationId;
        this.date = builder.date;
        this.weatherCode = builder.weatherCode;
        this.temperature2mMax = builder.temperature2mMax;
        this.temperature2mMin = builder.temperature2mMin;
        this.temperature2mMean = builder.temperature2mMean;
        this.apparentTemperatureMax = builder.apparentTemperatureMax;
        this.apparentTemperatureMin = builder.apparentTemperatureMin;
        this.apparentTemperatureMean = builder.apparentTemperatureMean;
        this.daylightDuration = builder.daylightDuration;
        this.sunshineDuration = builder.sunshineDuration;
        this.precipitationSum = builder.precipitationSum;
        this.rainSum = builder.rainSum;
        this.precipitationHours = builder.precipitationHours;
        this.windSpeed10mMax = builder.windSpeed10mMax;
        this.windGusts10mMax = builder.windGusts10mMax;
        this.windDirection10mDominant = builder.windDirection10mDominant;
        this.shortwaveRadiationSum = builder.shortwaveRadiationSum;
        this.et0FaoEvapotranspiration = builder.et0FaoEvapotranspiration;
        this.sunrise = builder.sunrise;
        this.sunset = builder.sunset;
    }


    // Getters
    public int getLocationId() { return locationId; }
    public LocalDate getDate() { return date; }
    public int getWeatherCode() { return weatherCode; }
    public Double getTemperature2mMax() { return temperature2mMax; }
    public Double getTemperature2mMin() { return temperature2mMin; }
    public Double getTemperature2mMean() { return temperature2mMean; }
    public Double getApparentTemperatureMax() { return apparentTemperatureMax; }
    public Double getApparentTemperatureMin() { return apparentTemperatureMin; }
    public Double getApparentTemperatureMean() { return apparentTemperatureMean; }
    public Double getDaylightDuration() { return daylightDuration; }
    public Double getSunshineDuration() { return sunshineDuration; }
    public Double getPrecipitationSum() { return precipitationSum; }
    public Double getRainSum() { return rainSum; }
    public Double getPrecipitationHours() { return precipitationHours; }
    public Double getWindSpeed10mMax() { return windSpeed10mMax; }
    public Double getWindGusts10mMax() { return windGusts10mMax; }
    public Double getWindDirection10mDominant() { return windDirection10mDominant; }
    public Double getShortwaveRadiationSum() { return shortwaveRadiationSum; }
    public Double getEt0FaoEvapotranspiration() { return et0FaoEvapotranspiration; }
    public String getSunrise() { return sunrise; }
    public String getSunset() { return sunset; }

    public int getMonth() { return date.getMonthValue(); }
    public int getYear() { return date.getYear(); }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        WeatherRecord that = (WeatherRecord) o;
        return locationId == that.locationId && Objects.equals(date, that.date);
    }

    @Override
    public int hashCode() {
        return Objects.hash(locationId, date);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private int locationId;
        private LocalDate date;
        private int weatherCode;
        private Double temperature2mMax;
        private Double temperature2mMin;
        private Double temperature2mMean;
        private Double apparentTemperatureMax;
        private Double apparentTemperatureMin;
        private Double apparentTemperatureMean;
        private Double daylightDuration;
        private Double sunshineDuration;
        private Double precipitationSum;
        private Double rainSum;
        private Double precipitationHours;
        private Double windSpeed10mMax;
        private Double windGusts10mMax;
        private Double windDirection10mDominant;
        private Double shortwaveRadiationSum;
        private Double et0FaoEvapotranspiration;
        private String sunrise;
        private String sunset;

        public Builder locationId(int locationId) { this.locationId = locationId; return this; }
        public Builder date(LocalDate date) { this.date = date; return this; }
        public Builder weatherCode(int weatherCode) { this.weatherCode = weatherCode; return this; }
        public Builder temperature2mMax(Double val) { this.temperature2mMax = val; return this; }
        public Builder temperature2mMin(Double val) { this.temperature2mMin = val; return this; }
        public Builder temperature2mMean(Double val) { this.temperature2mMean = val; return this; }
        public Builder apparentTemperatureMax(Double val) { this.apparentTemperatureMax = val; return this; }
        public Builder apparentTemperatureMin(Double val) { this.apparentTemperatureMin = val; return this; }
        public Builder apparentTemperatureMean(Double val) { this.apparentTemperatureMean = val; return this; }
        public Builder daylightDuration(Double val) { this.daylightDuration = val; return this; }
        public Builder sunshineDuration(Double val) { this.sunshineDuration = val; return this; }
        public Builder precipitationSum(Double val) { this.precipitationSum = val; return this; }
        public Builder rainSum(Double val) { this.rainSum = val; return this; }
        public Builder precipitationHours(Double val) { this.precipitationHours = val; return this; }
        public Builder windSpeed10mMax(Double val) { this.windSpeed10mMax = val; return this; }
        public Builder windGusts10mMax(Double val) { this.windGusts10mMax = val; return this; }
        public Builder windDirection10mDominant(Double val) { this.windDirection10mDominant = val; return this; }
        public Builder shortwaveRadiationSum(Double val) { this.shortwaveRadiationSum = val; return this; }
        public Builder et0FaoEvapotranspiration(Double val) { this.et0FaoEvapotranspiration = val; return this; }
        public Builder sunrise(String sunrise) { this.sunrise = sunrise; return this; }
        public Builder sunset(String sunset) { this.sunset = sunset; return this; }

        public WeatherRecord build() {
            return new WeatherRecord(this);
        }
    }
}
