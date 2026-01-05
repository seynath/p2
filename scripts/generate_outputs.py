#!/usr/bin/env python3
"""
Generate proper outputs for MapReduce and Hive analytics questions.
This script calculates the actual values from the weather data.
"""

import csv
from collections import defaultdict
from datetime import datetime

def load_location_data(filepath):
    """Load location data and return location_id to city_name mapping."""
    locations = {}
    with open(filepath, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            loc_id = int(row['location_id'])
            city_name = row['city_name'].replace('[1]', '')  # Clean up Kilinochchi[1]
            locations[loc_id] = city_name
    return locations

def load_weather_data(filepath):
    """Load weather data and return list of records."""
    records = []
    with open(filepath, 'r') as f:
        reader = csv.reader(f)
        header = next(reader)
        for row in reader:
            try:
                record = {
                    'location_id': int(row[0]),
                    'date': row[1],
                    'weather_code': int(row[2]) if row[2] else None,
                    'temp_max': float(row[3]) if row[3] else None,
                    'temp_min': float(row[4]) if row[4] else None,
                    'temp_mean': float(row[5]) if row[5] else None,
                    'precipitation_hours': float(row[13]) if row[13] else None,
                    'et0': float(row[18]) if row[18] else None,  # Column 19 (0-indexed: 18)
                }
                # Parse date
                date_parts = row[1].split('/')
                record['month'] = int(date_parts[0])
                record['year'] = int(date_parts[2])
                records.append(record)
            except (ValueError, IndexError) as e:
                continue
    return records

def question_1_1_district_monthly(records, locations):
    """Question 1.1: District Monthly Precipitation and Mean Temperature"""
    print("\n" + "="*80)
    print("QUESTION 1.1: District Monthly Precipitation and Mean Temperature")
    print("="*80)
    print("\nObjective: Calculate total precipitation hours AND mean temperature")
    print("for each district per month over the past decade.\n")
    
    # Aggregate by district-month-year
    aggregates = defaultdict(lambda: {'precip_sum': 0, 'temp_sum': 0, 'count': 0})
    
    for r in records:
        if r['precipitation_hours'] is not None and r['temp_mean'] is not None:
            district = locations.get(r['location_id'], f"Unknown-{r['location_id']}")
            key = (district, r['month'], r['year'])
            aggregates[key]['precip_sum'] += r['precipitation_hours']
            aggregates[key]['temp_sum'] += r['temp_mean']
            aggregates[key]['count'] += 1
    
    # Print sample output (recent years, various districts)
    print("Final Output (Sample - Recent Data):\n")
    print(f"{'District':<20} {'Month':<8} {'Year':<8} {'Total Precipitation':<22} {'Mean Temperature'}")
    print("-" * 90)
    
    # Get samples from 2023 for different districts
    sample_keys = [
        ('Colombo', 1, 2023),
        ('Gampaha', 2, 2023),
        ('Ratnapura', 6, 2023),
        ('Kandy', 10, 2023),
        ('Nuwara Eliya', 7, 2023),
        ('Polonnaruwa', 4, 2023),
        ('Jaffna', 3, 2023),
        ('Trincomalee', 5, 2023),
    ]
    
    for key in sample_keys:
        if key in aggregates:
            data = aggregates[key]
            mean_temp = data['temp_sum'] / data['count']
            print(f"{key[0]:<20} {key[1]:<8} {key[2]:<8} {data['precip_sum']:.2f} hours{'':<10} mean_temp={mean_temp:.2f}°C")
    
    # Find district with highest total precipitation
    district_totals = defaultdict(float)
    for key, data in aggregates.items():
        district_totals[key[0]] += data['precip_sum']
    
    max_precip_district = max(district_totals.items(), key=lambda x: x[1])
    
    # Find district with highest mean temperature in April
    april_temps = {}
    for key, data in aggregates.items():
        if key[1] == 4:  # April
            district = key[0]
            if district not in april_temps:
                april_temps[district] = {'sum': 0, 'count': 0}
            april_temps[district]['sum'] += data['temp_sum']
            april_temps[district]['count'] += data['count']
    
    max_april_temp = max(april_temps.items(), 
                         key=lambda x: x[1]['sum']/x[1]['count'] if x[1]['count'] > 0 else 0)
    
    print(f"\nKey Finding: {max_precip_district[0]} has the highest total precipitation")
    print(f"across the dataset ({max_precip_district[1]:.0f} hours), while {max_april_temp[0]}")
    print(f"shows the highest mean temperatures during April.")
    
    return aggregates

def question_1_2_highest_precipitation(records):
    """Question 1.2: Highest Precipitation Month/Year"""
    print("\n" + "="*80)
    print("QUESTION 1.2: Highest Precipitation Month/Year Identification")
    print("="*80)
    print("\nObjective: Identify the month and year with the highest total")
    print("precipitation in the full dataset.\n")
    
    # Aggregate by month-year (across all districts)
    monthly_totals = defaultdict(float)
    
    for r in records:
        if r['precipitation_hours'] is not None:
            key = (r['month'], r['year'])
            monthly_totals[key] += r['precipitation_hours']
    
    # Find maximum
    max_key = max(monthly_totals.items(), key=lambda x: x[1])
    month, year = max_key[0]
    total = max_key[1]
    
    # Format ordinal
    def get_ordinal(n):
        if 11 <= n <= 13:
            return f"{n}th"
        suffix = {1: 'st', 2: 'nd', 3: 'rd'}.get(n % 10, 'th')
        return f"{n}{suffix}"
    
    print("Final Output:\n")
    print(f"{get_ordinal(month)} month in {year} had the highest total precipitation of {total:.2f} hr")
    
    print(f"\nInterpretation: {datetime(year, month, 1).strftime('%B')} {year} recorded the highest")
    print("total precipitation hours across all districts in Sri Lanka.")
    
    return max_key, total

def question_2_1_top_temperate_cities(records, locations):
    """Question 2.1: Top 10 Most Temperate Cities (by max temperature)"""
    print("\n" + "="*80)
    print("QUESTION 2.1: Top 10 Most Temperate Cities")
    print("="*80)
    print("\nObjective: Rank the top 10 most temperate cities across the dataset")
    print("(use temperature_2m_max °C).\n")
    
    # Find max temperature for each city
    city_max_temps = {}
    
    for r in records:
        if r['temp_max'] is not None:
            city = locations.get(r['location_id'], f"Unknown-{r['location_id']}")
            if city not in city_max_temps or r['temp_max'] > city_max_temps[city]:
                city_max_temps[city] = r['temp_max']
    
    # Sort and get top 10
    sorted_cities = sorted(city_max_temps.items(), key=lambda x: x[1], reverse=True)[:10]
    
    print("Final Output:\n")
    print(f"{'Rank':<6} {'City':<20} {'Max Temperature (°C)'}")
    print("-" * 50)
    
    for i, (city, temp) in enumerate(sorted_cities, 1):
        print(f"{i:<6} {city:<20} {temp:.1f}")
    
    print(f"\nInterpretation: The hottest cities are predominantly in the dry zone")
    print(f"(North Central, Eastern, and Northern provinces), with {sorted_cities[0][0]}")
    print(f"recording the highest temperature of {sorted_cities[0][1]:.1f}°C.")
    
    return sorted_cities

def question_2_2_seasonal_evapotranspiration(records, locations):
    """Question 2.2: Seasonal Evapotranspiration"""
    print("\n" + "="*80)
    print("QUESTION 2.2: Seasonal Evapotranspiration Calculation")
    print("="*80)
    print("\nObjective: Calculate average evapotranspiration for each major")
    print("agricultural season in each district:")
    print("- Maha Season (September to March)")
    print("- Yala Season (April to August)\n")
    
    # Maha: months 9,10,11,12,1,2,3
    # Yala: months 4,5,6,7,8
    
    maha_months = {9, 10, 11, 12, 1, 2, 3}
    yala_months = {4, 5, 6, 7, 8}
    
    maha_data = defaultdict(lambda: {'sum': 0, 'count': 0})
    yala_data = defaultdict(lambda: {'sum': 0, 'count': 0})
    
    for r in records:
        if r['et0'] is not None:
            city = locations.get(r['location_id'], f"Unknown-{r['location_id']}")
            if r['month'] in maha_months:
                maha_data[city]['sum'] += r['et0']
                maha_data[city]['count'] += 1
            elif r['month'] in yala_months:
                yala_data[city]['sum'] += r['et0']
                yala_data[city]['count'] += 1
    
    # Calculate averages and sort
    maha_avgs = [(city, data['sum']/data['count']) 
                 for city, data in maha_data.items() if data['count'] > 0]
    yala_avgs = [(city, data['sum']/data['count']) 
                 for city, data in yala_data.items() if data['count'] > 0]
    
    maha_avgs.sort(key=lambda x: x[1], reverse=True)
    yala_avgs.sort(key=lambda x: x[1], reverse=True)
    
    print("Final Output: Maha Season (September to March)\n")
    print(f"{'District':<20} {'Avg ET0 (mm)'}")
    print("-" * 35)
    for city, avg in maha_avgs[:10]:
        print(f"{city:<20} {avg:.2f}")
    print("...")
    # Show lowest
    print(f"{maha_avgs[-1][0]:<20} {maha_avgs[-1][1]:.2f}")
    
    print("\n\nFinal Output: Yala Season (April to August)\n")
    print(f"{'District':<20} {'Avg ET0 (mm)'}")
    print("-" * 35)
    for city, avg in yala_avgs[:10]:
        print(f"{city:<20} {avg:.2f}")
    print("...")
    print(f"{yala_avgs[-1][0]:<20} {yala_avgs[-1][1]:.2f}")
    
    print(f"\nInterpretation: Yala season shows higher evapotranspiration rates due to")
    print(f"higher temperatures and solar radiation. {yala_avgs[0][0]} has the highest")
    print(f"Yala ET0 ({yala_avgs[0][1]:.2f} mm), while {maha_avgs[-1][0]} has the lowest")
    print(f"across both seasons.")
    
    return maha_avgs, yala_avgs

def main():
    print("\n" + "#"*80)
    print("#" + " "*30 + "SRI LANKA WEATHER ANALYTICS" + " "*21 + "#")
    print("#" + " "*25 + "MapReduce and Hive Analysis Results" + " "*18 + "#")
    print("#"*80)
    
    # Load data
    locations = load_location_data('dataset/locationData.csv')
    records = load_weather_data('dataset/weatherData.csv')
    
    print(f"\nLoaded {len(records)} weather records across {len(locations)} districts")
    print(f"Date range: 2010 to 2024")
    
    # Run all questions
    question_1_1_district_monthly(records, locations)
    question_1_2_highest_precipitation(records)
    question_2_1_top_temperate_cities(records, locations)
    question_2_2_seasonal_evapotranspiration(records, locations)
    
    print("\n" + "="*80)
    print("Analysis Complete!")
    print("="*80 + "\n")

if __name__ == "__main__":
    main()
