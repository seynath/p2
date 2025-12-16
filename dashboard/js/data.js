/**
 * Sri Lanka Weather Analytics Dashboard - Data Module
 * Contains hardcoded analysis results from MapReduce/Hive/Spark processing
 * 
 * Data derived from Sri Lanka weather dataset (2010-2024)
 * covering 27 districts with daily meteorological observations
 * 
 * Generated: 2025-12-16 05:03:09
 */

const WeatherData = {
    // Precipitation data by district - Most precipitous month for each district
    // Based on MapReduce analysis of total precipitation hours per district per month
    precipitationByDistrict: {
        labels: ["Ampara", "Anuradhapura", "Badulla", "Bandarawela", "Batticaloa", "Colombo", "Galle", "Gampaha", "Hambantota", "Jaffna", "Kalutara", "Kandy", "Kegalle", "Kilinochchi[1]", "Kurunegala", "Mannar", "Matale", "Matara", "Moneragala", "Mullaitivu", "Nuwara Eliya", "Polonnaruwa", "Puttalam", "Ratnapura", "Trincomalee", "Vavuniya", "Welimada"],
        mostPrecipitousMonth: ["December", "November", "December", "November", "December", "May", "May", "June", "November", "November", "May", "October", "June", "November", "June", "November", "October", "May", "November", "November", "June", "December", "October", "June", "December", "November", "December"],
        // Average precipitation hours for the most precipitous month (2010-2024)
        precipitationHours: [11.9, 9.5, 11.0, 10.8, 13.2, 15.5, 15.2, 17.3, 9.6, 12.2, 15.6, 10.2, 14.3, 12.2, 11.8, 10.6, 10.5, 13.6, 10.1, 13.2, 12.2, 10.1, 11.3, 18.5, 12.7, 9.7, 10.6],
        // Season classification for most precipitous period
        mostPrecipitousSeason: ["Northeast Monsoon", "Northeast Monsoon", "Northeast Monsoon", "Northeast Monsoon", "Northeast Monsoon", "Southwest Monsoon", "Southwest Monsoon", "Southwest Monsoon", "Northeast Monsoon", "Northeast Monsoon", "Southwest Monsoon", "Inter-monsoon", "Southwest Monsoon", "Northeast Monsoon", "Southwest Monsoon", "Northeast Monsoon", "Inter-monsoon", "Southwest Monsoon", "Northeast Monsoon", "Northeast Monsoon", "Southwest Monsoon", "Northeast Monsoon", "Inter-monsoon", "Southwest Monsoon", "Northeast Monsoon", "Northeast Monsoon", "Northeast Monsoon"]
    },

    // Top 5 districts by total precipitation hours (2010-2024)
    // Based on MapReduce aggregation of precipitation_hours across all years
    topDistrictsPrecipitation: {
        labels: ["Ratnapura", "Galle", "Kalutara", "Colombo", "Gampaha"],
        values: [62920, 59687, 58260, 57559, 56254]
    },

    // Summary statistics for precipitation
    // Derived from MapReduce analysis identifying highest precipitation month/year
    precipitationStats: {
        highestPrecipMonth: 'October',
        highestPrecipYear: '2018',
        highestPrecipValue: '11,007 hrs',
        avgMonthlyPrecipHours: '6,004 hrs'
    },

    // Temperature data - percentage of months with mean temp > 30°C by year
    // Based on analysis of temperature_2m_mean across all districts
    // Note: Sri Lanka's tropical climate rarely produces monthly mean temps > 30°C
    highTempMonthsByYear: {
        labels: ["2010", "2011", "2012", "2013", "2014", "2015", "2016", "2017", "2018", "2019", "2020", "2021", "2022", "2023", "2024"],
        percentages: [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]
    },

    // Temperature trends over time
    // Yearly average max and mean temperatures across all districts
    temperatureTrends: {
        labels: ["2010", "2011", "2012", "2013", "2014", "2015", "2016", "2017", "2018", "2019", "2020", "2021", "2022", "2023", "2024"],
        avgMaxTemp: [29.0, 28.7, 29.3, 28.8, 29.1, 29.0, 29.8, 29.5, 29.1, 29.7, 29.8, 29.1, 29.1, 29.5, 30.7],
        avgMeanTemp: [25.7, 25.5, 25.8, 25.6, 25.7, 25.7, 26.3, 25.8, 25.4, 25.9, 26.0, 25.5, 25.3, 25.7, 26.4]
    },

    // Summary statistics for temperature
    temperatureStats: {
        avgHighTemp: '29.3°C',
        hottestDistrict: 'Polonnaruwa',
        highTempMonthsPercentage: '0.0%'
    },

    // Extreme weather events by year
    // Extreme weather defined as: precipitation_sum > 10mm AND wind_gusts_10m_max > 50 km/h
    extremeWeatherByYear: {
        labels: ["2010", "2011", "2012", "2013", "2014", "2015", "2016", "2017", "2018", "2019", "2020", "2021", "2022", "2023", "2024"],
        days: [192, 100, 111, 189, 60, 52, 79, 229, 209, 151, 253, 218, 195, 78, 81]
    },

    // Extreme weather events by district (Top 10)
    extremeWeatherByDistrict: {
        labels: ["Nuwara Eliya", "Galle", "Kalutara", "Matale", "Colombo", "Kurunegala", "Kandy", "Matara", "Kegalle", "Puttalam"],
        days: [306, 273, 147, 131, 131, 120, 118, 106, 104, 94]
    },

    // Summary statistics for extreme weather
    extremeWeatherStats: {
        totalExtremeDays: '2,197',
        avgExtremePerYear: '146.5',
        mostAffectedDistrict: 'Nuwara Eliya'
    }
};

// Export for use in charts.js
if (typeof module !== 'undefined' && module.exports) {
    module.exports = WeatherData;
}
