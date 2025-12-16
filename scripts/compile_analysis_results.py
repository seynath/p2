#!/usr/bin/env python3
"""
Sri Lanka Weather Analytics - Compile Analysis Results

This script runs all analyses on the weather dataset and compiles the results
for updating the dashboard with final hardcoded values.

Task 38: Compile all analysis results
Requirements: 8.5
"""

import pandas as pd
import numpy as np
from datetime import datetime
import json
import os

# Get project root
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)

# Data paths
WEATHER_DATA_PATH = os.path.join(PROJECT_ROOT, "dataset", "weatherData.csv")
LOCATION_DATA_PATH = os.path.join(PROJECT_ROOT, "dataset", "locationData.csv")


def load_data():
    """Load weather and location data."""
    print("Loading data...")
    
    # Load weather data
    weather_df = pd.read_csv(WEATHER_DATA_PATH)
    
    # Rename columns to remove units
    weather_df.columns = [
        'location_id', 'date', 'weather_code', 'temperature_2m_max', 
        'temperature_2m_min', 'temperature_2m_mean', 'apparent_temperature_max',
        'apparent_temperature_min', 'apparent_temperature_mean', 'daylight_duration',
        'sunshine_duration', 'precipitation_sum', 'rain_sum', 'precipitation_hours',
        'wind_speed_10m_max', 'wind_gusts_10m_max', 'wind_direction_10m_dominant',
        'shortwave_radiation_sum', 'et0_fao_evapotranspiration', 'sunrise', 'sunset'
    ]
    
    # Parse date
    weather_df['date'] = pd.to_datetime(weather_df['date'], format='%m/%d/%Y')
    weather_df['year'] = weather_df['date'].dt.year
    weather_df['month'] = weather_df['date'].dt.month
    
    # Load location data
    location_df = pd.read_csv(LOCATION_DATA_PATH)
    
    # Merge datasets
    merged_df = weather_df.merge(location_df, on='location_id', how='left')
    
    print(f"  Weather records: {len(weather_df)}")
    print(f"  Locations: {len(location_df)}")
    print(f"  Merged records: {len(merged_df)}")
    
    return weather_df, location_df, merged_df


def analyze_precipitation(merged_df):
    """
    Analyze precipitation patterns.
    
    MapReduce Analysis (Requirements 2.1, 2.3, 2.4):
    - Total precipitation hours per district per month
    - Month/year with highest total precipitation
    """
    print("\n" + "="*70)
    print("PRECIPITATION ANALYSIS (MapReduce)")
    print("="*70)
    
    results = {}
    
    # 1. Most precipitous month for each district
    print("\n1. Most Precipitous Month by District:")
    district_month_precip = merged_df.groupby(['city_name', 'month'])['precipitation_hours'].sum().reset_index()
    most_precip_month = district_month_precip.loc[
        district_month_precip.groupby('city_name')['precipitation_hours'].idxmax()
    ]
    
    month_names = ['January', 'February', 'March', 'April', 'May', 'June',
                   'July', 'August', 'September', 'October', 'November', 'December']
    
    # Calculate average precipitation hours for the most precipitous month
    district_results = []
    for _, row in most_precip_month.iterrows():
        district = row['city_name']
        month_num = int(row['month'])
        
        # Get average precipitation for this month across all years
        avg_precip = merged_df[
            (merged_df['city_name'] == district) & 
            (merged_df['month'] == month_num)
        ]['precipitation_hours'].mean()
        
        # Determine season
        if month_num in [11, 12, 1, 2]:
            season = 'Northeast Monsoon'
        elif month_num in [5, 6, 7, 8, 9]:
            season = 'Southwest Monsoon'
        else:
            season = 'Inter-monsoon'
        
        district_results.append({
            'district': district,
            'month': month_names[month_num - 1],
            'month_num': month_num,
            'avg_precip_hours': round(avg_precip, 1),
            'season': season
        })
        print(f"  {district}: {month_names[month_num - 1]} ({avg_precip:.1f} hrs) - {season}")
    
    results['district_precipitation'] = district_results
    
    # 2. Top 5 districts by total precipitation
    print("\n2. Top 5 Districts by Total Precipitation Hours:")
    total_precip_by_district = merged_df.groupby('city_name')['precipitation_hours'].sum().sort_values(ascending=False)
    top_5_districts = total_precip_by_district.head(5)
    
    results['top_5_districts'] = []
    for district, total in top_5_districts.items():
        results['top_5_districts'].append({
            'district': district,
            'total_hours': round(total, 1)
        })
        print(f"  {district}: {total:.1f} hours")
    
    # 3. Month/Year with highest total precipitation
    print("\n3. Month/Year with Highest Total Precipitation:")
    monthly_total = merged_df.groupby(['year', 'month'])['precipitation_hours'].sum().reset_index()
    max_idx = monthly_total['precipitation_hours'].idxmax()
    max_row = monthly_total.loc[max_idx]
    
    results['highest_precip'] = {
        'month': month_names[int(max_row['month']) - 1],
        'year': int(max_row['year']),
        'total_hours': round(max_row['precipitation_hours'], 1)
    }
    print(f"  {month_names[int(max_row['month']) - 1]} {int(max_row['year'])}: {max_row['precipitation_hours']:.1f} hours")
    
    # 4. Average monthly precipitation
    avg_monthly = merged_df.groupby(['year', 'month'])['precipitation_hours'].sum().mean()
    results['avg_monthly_precip'] = round(avg_monthly, 1)
    print(f"\n4. Average Monthly Precipitation: {avg_monthly:.1f} hours")
    
    return results


def analyze_temperature(merged_df):
    """
    Analyze temperature patterns.
    
    Hive Analysis (Requirements 3.1):
    - Top 10 temperate cities by max temperature
    
    Dashboard (Requirements 6.3):
    - Percentage of months with mean temp > 30°C per year
    """
    print("\n" + "="*70)
    print("TEMPERATURE ANALYSIS (Hive/Spark)")
    print("="*70)
    
    results = {}
    
    # 1. Top 10 cities by maximum temperature
    print("\n1. Top 10 Cities by Maximum Temperature:")
    max_temp_by_city = merged_df.groupby('city_name')['temperature_2m_max'].max().sort_values(ascending=False)
    top_10_cities = max_temp_by_city.head(10)
    
    results['top_10_temperate_cities'] = []
    for city, max_temp in top_10_cities.items():
        results['top_10_temperate_cities'].append({
            'city': city,
            'max_temp': round(max_temp, 1)
        })
        print(f"  {city}: {max_temp:.1f}°C")
    
    # 2. Percentage of months with mean temp > 30°C per year
    print("\n2. Percentage of Months with Mean Temp > 30°C by Year:")
    
    # Calculate monthly mean temperature across all districts
    monthly_mean = merged_df.groupby(['year', 'month'])['temperature_2m_mean'].mean().reset_index()
    
    # Count months above 30°C per year
    high_temp_months = monthly_mean[monthly_mean['temperature_2m_mean'] > 30].groupby('year').size()
    total_months_per_year = monthly_mean.groupby('year').size()
    
    results['high_temp_months_by_year'] = {}
    for year in sorted(merged_df['year'].unique()):
        high_count = high_temp_months.get(year, 0)
        total_count = total_months_per_year.get(year, 12)
        percentage = (high_count / total_count) * 100 if total_count > 0 else 0
        results['high_temp_months_by_year'][int(year)] = round(percentage, 1)
        print(f"  {year}: {percentage:.1f}% ({high_count}/{total_count} months)")
    
    # 3. Temperature trends
    print("\n3. Temperature Trends by Year:")
    yearly_temps = merged_df.groupby('year').agg({
        'temperature_2m_max': 'mean',
        'temperature_2m_mean': 'mean'
    }).round(1)
    
    results['temperature_trends'] = {
        'years': list(yearly_temps.index.astype(int)),
        'avg_max_temp': list(yearly_temps['temperature_2m_max']),
        'avg_mean_temp': list(yearly_temps['temperature_2m_mean'])
    }
    
    for year, row in yearly_temps.iterrows():
        print(f"  {year}: Max={row['temperature_2m_max']:.1f}°C, Mean={row['temperature_2m_mean']:.1f}°C")
    
    # 4. Hottest district
    avg_max_by_district = merged_df.groupby('city_name')['temperature_2m_max'].mean()
    hottest_district = avg_max_by_district.idxmax()
    results['hottest_district'] = hottest_district
    results['avg_high_temp'] = round(merged_df['temperature_2m_max'].mean(), 1)
    print(f"\n4. Hottest District (by avg max temp): {hottest_district}")
    print(f"   Overall Average Max Temperature: {results['avg_high_temp']}°C")
    
    return results


def analyze_extreme_weather(merged_df):
    """
    Analyze extreme weather events.
    
    Dashboard (Requirements 6.4):
    - Total days with extreme weather (precipitation > 10mm AND wind gusts > 50 km/h)
    """
    print("\n" + "="*70)
    print("EXTREME WEATHER ANALYSIS")
    print("="*70)
    
    results = {}
    
    # Define extreme weather: precipitation_sum > 10mm AND wind_gusts_10m_max > 50 km/h
    extreme_mask = (merged_df['precipitation_sum'] > 10) & (merged_df['wind_gusts_10m_max'] > 50)
    extreme_df = merged_df[extreme_mask]
    
    # 1. Extreme weather days by year
    print("\n1. Extreme Weather Days by Year:")
    extreme_by_year = extreme_df.groupby('year').size()
    
    results['extreme_by_year'] = {}
    for year in sorted(merged_df['year'].unique()):
        count = extreme_by_year.get(year, 0)
        results['extreme_by_year'][int(year)] = int(count)
        print(f"  {year}: {count} days")
    
    # 2. Extreme weather days by district (Top 10)
    print("\n2. Top 10 Districts by Extreme Weather Days:")
    extreme_by_district = extreme_df.groupby('city_name').size().sort_values(ascending=False)
    top_10_extreme = extreme_by_district.head(10)
    
    results['extreme_by_district'] = []
    for district, count in top_10_extreme.items():
        results['extreme_by_district'].append({
            'district': district,
            'days': int(count)
        })
        print(f"  {district}: {count} days")
    
    # 3. Summary statistics
    total_extreme_days = len(extreme_df)
    years_count = len(merged_df['year'].unique())
    avg_per_year = total_extreme_days / years_count if years_count > 0 else 0
    most_affected = extreme_by_district.idxmax() if len(extreme_by_district) > 0 else 'N/A'
    
    results['summary'] = {
        'total_extreme_days': int(total_extreme_days),
        'avg_per_year': round(avg_per_year, 1),
        'most_affected_district': most_affected
    }
    
    print(f"\n3. Summary:")
    print(f"  Total Extreme Weather Days: {total_extreme_days}")
    print(f"  Average per Year: {avg_per_year:.1f}")
    print(f"  Most Affected District: {most_affected}")
    
    return results


def analyze_radiation(merged_df):
    """
    Analyze shortwave radiation patterns.
    
    Spark Analysis (Requirements 4.1):
    - Percentage of days with shortwave_radiation_sum > 15 MJ/m² per month
    """
    print("\n" + "="*70)
    print("SHORTWAVE RADIATION ANALYSIS (Spark)")
    print("="*70)
    
    results = {}
    
    # Filter valid radiation data
    valid_df = merged_df[merged_df['shortwave_radiation_sum'].notna()]
    
    # Calculate percentage of days above threshold per month
    print("\n1. Percentage of Days with Radiation > 15 MJ/m² by Month:")
    
    month_names = ['January', 'February', 'March', 'April', 'May', 'June',
                   'July', 'August', 'September', 'October', 'November', 'December']
    
    results['radiation_by_month'] = {}
    for month in range(1, 13):
        month_data = valid_df[valid_df['month'] == month]
        total_days = len(month_data)
        above_threshold = len(month_data[month_data['shortwave_radiation_sum'] > 15])
        percentage = (above_threshold / total_days * 100) if total_days > 0 else 0
        
        results['radiation_by_month'][month] = {
            'month_name': month_names[month - 1],
            'total_days': int(total_days),
            'days_above_threshold': int(above_threshold),
            'percentage': round(percentage, 2)
        }
        print(f"  {month_names[month - 1]}: {percentage:.2f}% ({above_threshold}/{total_days} days)")
    
    return results


def analyze_weekly_temperatures(merged_df):
    """
    Analyze weekly maximum temperatures for hottest months.
    
    Spark Analysis (Requirements 4.2, 4.3):
    - Identify hottest months based on average temperature_2m_max
    - Calculate weekly maximum temperatures within those months
    """
    print("\n" + "="*70)
    print("WEEKLY TEMPERATURE ANALYSIS (Spark)")
    print("="*70)
    
    results = {}
    
    # 1. Identify hottest months
    print("\n1. Hottest Months by Average Max Temperature:")
    monthly_avg = merged_df.groupby('month')['temperature_2m_max'].mean().sort_values(ascending=False)
    top_3_months = monthly_avg.head(3)
    
    month_names = ['January', 'February', 'March', 'April', 'May', 'June',
                   'July', 'August', 'September', 'October', 'November', 'December']
    
    results['hottest_months'] = []
    for month, avg_temp in top_3_months.items():
        results['hottest_months'].append({
            'month': int(month),
            'month_name': month_names[int(month) - 1],
            'avg_max_temp': round(avg_temp, 2)
        })
        print(f"  {month_names[int(month) - 1]}: {avg_temp:.2f}°C")
    
    # 2. Weekly max temperatures for hottest months
    print("\n2. Weekly Max Temperatures for Hottest Months (sample):")
    hottest_month_nums = [m['month'] for m in results['hottest_months']]
    
    # Add week number
    merged_df['week'] = merged_df['date'].dt.isocalendar().week
    
    # Filter for hottest months
    hot_months_df = merged_df[merged_df['month'].isin(hottest_month_nums)]
    
    # Calculate weekly max
    weekly_max = hot_months_df.groupby(['year', 'month', 'week'])['temperature_2m_max'].max().reset_index()
    weekly_max = weekly_max.sort_values(['year', 'month', 'week'])
    
    results['weekly_max_sample'] = weekly_max.head(20).to_dict('records')
    
    # Summary stats
    results['weekly_max_summary'] = {
        'overall_max': round(weekly_max['temperature_2m_max'].max(), 2),
        'overall_avg': round(weekly_max['temperature_2m_max'].mean(), 2),
        'total_weeks': len(weekly_max)
    }
    
    print(f"\n  Overall Max: {results['weekly_max_summary']['overall_max']}°C")
    print(f"  Overall Avg Weekly Max: {results['weekly_max_summary']['overall_avg']}°C")
    print(f"  Total Weeks Analyzed: {results['weekly_max_summary']['total_weeks']}")
    
    return results


def analyze_evapotranspiration(merged_df):
    """
    Analyze seasonal evapotranspiration.
    
    Hive Analysis (Requirements 3.2, 3.3):
    - Maha season (Sep-Mar) average ET0 per district per year
    - Yala season (Apr-Aug) average ET0 per district per year
    """
    print("\n" + "="*70)
    print("SEASONAL EVAPOTRANSPIRATION ANALYSIS (Hive)")
    print("="*70)
    
    results = {}
    
    # Maha season: September to March (months 9, 10, 11, 12, 1, 2, 3)
    # Yala season: April to August (months 4, 5, 6, 7, 8)
    
    maha_months = [9, 10, 11, 12, 1, 2, 3]
    yala_months = [4, 5, 6, 7, 8]
    
    # Filter valid ET0 data
    valid_df = merged_df[merged_df['et0_fao_evapotranspiration'].notna()]
    
    # Maha season analysis
    print("\n1. Maha Season (Sep-Mar) Average ET0 by District:")
    maha_df = valid_df[valid_df['month'].isin(maha_months)]
    maha_avg = maha_df.groupby('city_name')['et0_fao_evapotranspiration'].mean().sort_values(ascending=False)
    
    results['maha_season'] = {}
    for district, avg_et0 in maha_avg.items():
        results['maha_season'][district] = round(avg_et0, 4)
    
    print(f"  Top 5 districts:")
    for district, avg_et0 in maha_avg.head(5).items():
        print(f"    {district}: {avg_et0:.4f} mm")
    
    # Yala season analysis
    print("\n2. Yala Season (Apr-Aug) Average ET0 by District:")
    yala_df = valid_df[valid_df['month'].isin(yala_months)]
    yala_avg = yala_df.groupby('city_name')['et0_fao_evapotranspiration'].mean().sort_values(ascending=False)
    
    results['yala_season'] = {}
    for district, avg_et0 in yala_avg.items():
        results['yala_season'][district] = round(avg_et0, 4)
    
    print(f"  Top 5 districts:")
    for district, avg_et0 in yala_avg.head(5).items():
        print(f"    {district}: {avg_et0:.4f} mm")
    
    return results


def generate_dashboard_data(precip_results, temp_results, extreme_results):
    """Generate the JavaScript data file for the dashboard."""
    
    # Prepare precipitation by district data
    districts = [r['district'] for r in precip_results['district_precipitation']]
    months = [r['month'] for r in precip_results['district_precipitation']]
    precip_hours = [r['avg_precip_hours'] for r in precip_results['district_precipitation']]
    seasons = [r['season'] for r in precip_results['district_precipitation']]
    
    # Prepare top 5 districts
    top_districts = [r['district'] for r in precip_results['top_5_districts']]
    top_values = [r['total_hours'] for r in precip_results['top_5_districts']]
    
    # Prepare temperature data
    years = list(temp_results['high_temp_months_by_year'].keys())
    high_temp_pcts = list(temp_results['high_temp_months_by_year'].values())
    
    # Prepare extreme weather data
    extreme_years = list(extreme_results['extreme_by_year'].keys())
    extreme_days = list(extreme_results['extreme_by_year'].values())
    
    extreme_districts = [r['district'] for r in extreme_results['extreme_by_district']]
    extreme_district_days = [r['days'] for r in extreme_results['extreme_by_district']]
    
    js_content = f'''/**
 * Sri Lanka Weather Analytics Dashboard - Data Module
 * Contains hardcoded analysis results from MapReduce/Hive/Spark processing
 * 
 * Data derived from Sri Lanka weather dataset (2010-2024)
 * covering 27 districts with daily meteorological observations
 * 
 * Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
 */

const WeatherData = {{
    // Precipitation data by district - Most precipitous month for each district
    // Based on MapReduce analysis of total precipitation hours per district per month
    precipitationByDistrict: {{
        labels: {json.dumps(districts)},
        mostPrecipitousMonth: {json.dumps(months)},
        // Average precipitation hours for the most precipitous month (2010-2024)
        precipitationHours: {json.dumps(precip_hours)},
        // Season classification for most precipitous period
        mostPrecipitousSeason: {json.dumps(seasons)}
    }},

    // Top 5 districts by total precipitation hours (2010-2024)
    // Based on MapReduce aggregation of precipitation_hours across all years
    topDistrictsPrecipitation: {{
        labels: {json.dumps(top_districts)},
        values: {json.dumps(top_values)}
    }},

    // Summary statistics for precipitation
    // Derived from MapReduce analysis identifying highest precipitation month/year
    precipitationStats: {{
        highestPrecipMonth: '{precip_results['highest_precip']['month']}',
        highestPrecipYear: '{precip_results['highest_precip']['year']}',
        highestPrecipValue: '{precip_results['highest_precip']['total_hours']:,.0f} hrs',
        avgMonthlyPrecipHours: '{precip_results['avg_monthly_precip']:,.0f} hrs'
    }},

    // Temperature data - percentage of months with mean temp > 30°C by year
    // Based on analysis of temperature_2m_mean across all districts
    // Note: Sri Lanka's tropical climate rarely produces monthly mean temps > 30°C
    highTempMonthsByYear: {{
        labels: {json.dumps([str(y) for y in years])},
        percentages: {json.dumps(high_temp_pcts)}
    }},

    // Temperature trends over time
    // Yearly average max and mean temperatures across all districts
    temperatureTrends: {{
        labels: {json.dumps([str(y) for y in temp_results['temperature_trends']['years']])},
        avgMaxTemp: {json.dumps(temp_results['temperature_trends']['avg_max_temp'])},
        avgMeanTemp: {json.dumps(temp_results['temperature_trends']['avg_mean_temp'])}
    }},

    // Summary statistics for temperature
    temperatureStats: {{
        avgHighTemp: '{temp_results['avg_high_temp']}°C',
        hottestDistrict: '{temp_results['hottest_district']}',
        highTempMonthsPercentage: '{sum(high_temp_pcts)/len(high_temp_pcts):.1f}%'
    }},

    // Extreme weather events by year
    // Extreme weather defined as: precipitation_sum > 10mm AND wind_gusts_10m_max > 50 km/h
    extremeWeatherByYear: {{
        labels: {json.dumps([str(y) for y in extreme_years])},
        days: {json.dumps(extreme_days)}
    }},

    // Extreme weather events by district (Top 10)
    extremeWeatherByDistrict: {{
        labels: {json.dumps(extreme_districts)},
        days: {json.dumps(extreme_district_days)}
    }},

    // Summary statistics for extreme weather
    extremeWeatherStats: {{
        totalExtremeDays: '{extreme_results['summary']['total_extreme_days']:,}',
        avgExtremePerYear: '{extreme_results['summary']['avg_per_year']}',
        mostAffectedDistrict: '{extreme_results['summary']['most_affected_district']}'
    }}
}};

// Export for use in charts.js
if (typeof module !== 'undefined' && module.exports) {{
    module.exports = WeatherData;
}}
'''
    
    return js_content


def main():
    """Main function to run all analyses and compile results."""
    print("="*70)
    print("SRI LANKA WEATHER ANALYTICS - COMPILE ANALYSIS RESULTS")
    print("="*70)
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Load data
    weather_df, location_df, merged_df = load_data()
    
    # Run all analyses
    precip_results = analyze_precipitation(merged_df)
    temp_results = analyze_temperature(merged_df)
    extreme_results = analyze_extreme_weather(merged_df)
    radiation_results = analyze_radiation(merged_df)
    weekly_temp_results = analyze_weekly_temperatures(merged_df)
    et0_results = analyze_evapotranspiration(merged_df)
    
    # Generate dashboard data file
    print("\n" + "="*70)
    print("GENERATING DASHBOARD DATA FILE")
    print("="*70)
    
    js_content = generate_dashboard_data(precip_results, temp_results, extreme_results)
    
    # Write to dashboard/js/data.js
    output_path = os.path.join(PROJECT_ROOT, "dashboard", "js", "data.js")
    with open(output_path, 'w') as f:
        f.write(js_content)
    
    print(f"\nDashboard data written to: {output_path}")
    
    # Save complete results as JSON for reference
    all_results = {
        'precipitation': precip_results,
        'temperature': temp_results,
        'extreme_weather': extreme_results,
        'radiation': radiation_results,
        'weekly_temperatures': weekly_temp_results,
        'evapotranspiration': et0_results,
        'generated_at': datetime.now().isoformat()
    }
    
    json_output_path = os.path.join(PROJECT_ROOT, "scripts", "analysis_results.json")
    with open(json_output_path, 'w') as f:
        json.dump(all_results, f, indent=2, default=str)
    
    print(f"Complete results saved to: {json_output_path}")
    
    print("\n" + "="*70)
    print("ANALYSIS COMPLETE")
    print("="*70)
    
    return all_results


if __name__ == "__main__":
    main()
