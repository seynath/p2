# Sri Lanka Weather Analytics - Analysis Summary

**Generated:** December 16, 2025

This document summarizes the key findings from the comprehensive weather data analysis covering Sri Lanka's 27 districts from 2010 to June 2024.

## Dataset Overview
- **Weather Records:** 142,371 daily observations
- **Locations:** 27 districts
- **Time Period:** January 2010 - June 2024

---

## 1. Precipitation Analysis (MapReduce)

### Most Precipitous Month by District
| District | Peak Month | Avg Hours | Season |
|----------|------------|-----------|--------|
| Ratnapura | June | 18.5 hrs | Southwest Monsoon |
| Gampaha | June | 17.3 hrs | Southwest Monsoon |
| Kalutara | May | 15.6 hrs | Southwest Monsoon |
| Colombo | May | 15.5 hrs | Southwest Monsoon |
| Galle | May | 15.2 hrs | Southwest Monsoon |

### Top 5 Districts by Total Precipitation (2010-2024)
1. **Ratnapura:** 62,920 hours
2. **Galle:** 59,687 hours
3. **Kalutara:** 58,260 hours
4. **Colombo:** 57,559 hours
5. **Gampaha:** 56,254 hours

### Key Statistics
- **Highest Precipitation Month/Year:** October 2018 (11,007 hours)
- **Average Monthly Precipitation:** 6,004 hours

---

## 2. Temperature Analysis (Hive/Spark)

### Top 10 Temperate Cities by Maximum Temperature
| Rank | City | Max Temp (°C) |
|------|------|---------------|
| 1 | Polonnaruwa | 40.3 |
| 2 | Ampara | 39.8 |
| 3 | Moneragala | 39.4 |
| 4 | Vavuniya | 39.4 |
| 5 | Kilinochchi | 39.4 |
| 6 | Batticaloa | 38.9 |
| 7 | Trincomalee | 38.8 |
| 8 | Mullaitivu | 38.6 |
| 9 | Kegalle | 38.4 |
| 10 | Kurunegala | 38.3 |

### Temperature Trends
- **Average Max Temperature:** 29.3°C
- **Hottest District:** Polonnaruwa
- **Months with Mean Temp > 30°C:** 0% (Sri Lanka's tropical climate rarely produces monthly mean temps > 30°C)

### Hottest Months (by Average Max Temperature)
1. April: 30.77°C
2. March: 30.36°C
3. May: 30.20°C

---

## 3. Extreme Weather Events

**Definition:** Days with precipitation > 10mm AND wind gusts > 50 km/h

### Summary Statistics
- **Total Extreme Weather Days:** 2,197
- **Average per Year:** 146.5 days
- **Most Affected District:** Nuwara Eliya

### Top 10 Districts by Extreme Weather Days
| Rank | District | Days |
|------|----------|------|
| 1 | Nuwara Eliya | 306 |
| 2 | Galle | 273 |
| 3 | Kalutara | 147 |
| 4 | Matale | 131 |
| 5 | Colombo | 131 |
| 6 | Kurunegala | 120 |
| 7 | Kandy | 118 |
| 8 | Matara | 106 |
| 9 | Kegalle | 104 |
| 10 | Puttalam | 94 |

### Extreme Weather by Year
| Year | Days | Year | Days |
|------|------|------|------|
| 2010 | 192 | 2018 | 209 |
| 2011 | 100 | 2019 | 151 |
| 2012 | 111 | 2020 | 253 |
| 2013 | 189 | 2021 | 218 |
| 2014 | 60 | 2022 | 195 |
| 2015 | 52 | 2023 | 78 |
| 2016 | 79 | 2024 | 81 |
| 2017 | 229 | | |

---

## 4. Shortwave Radiation Analysis (Spark)

### Percentage of Days with Radiation > 15 MJ/m² by Month
| Month | Percentage | Days Above/Total |
|-------|------------|------------------|
| January | 78.85% | 9,900/12,555 |
| February | 89.76% | 10,276/11,448 |
| March | 96.25% | 12,084/12,555 |
| April | 95.56% | 11,611/12,150 |
| May | 85.00% | 10,672/12,555 |
| June | 88.41% | 10,217/11,556 |
| July | 89.35% | 10,470/11,718 |
| August | 88.49% | 10,369/11,718 |
| September | 85.92% | 9,743/11,340 |
| October | 76.11% | 8,918/11,718 |
| November | 61.32% | 6,954/11,340 |
| December | 59.28% | 6,946/11,718 |

---

## 5. Seasonal Evapotranspiration Analysis (Hive)

### Maha Season (September - March)
Top 5 Districts by Average ET0:
1. Mannar: 4.3846 mm
2. Hambantota: 4.3710 mm
3. Jaffna: 4.3129 mm
4. Mullaitivu: 4.2674 mm
5. Kilinochchi: 4.2616 mm

### Yala Season (April - August)
Top 5 Districts by Average ET0:
1. Polonnaruwa: 5.7132 mm
2. Trincomalee: 5.6871 mm
3. Kilinochchi: 5.6592 mm
4. Mullaitivu: 5.6241 mm
5. Mannar: 5.2854 mm

---

## 6. Weekly Maximum Temperature Analysis (Spark)

### Hottest Months Identified
1. April: 30.77°C average max
2. March: 30.36°C average max
3. May: 30.20°C average max

### Weekly Max Temperature Summary
- **Overall Maximum:** 40.3°C
- **Average Weekly Max:** 35.43°C
- **Total Weeks Analyzed:** 235

---

## Files Generated
- `dashboard/js/data.js` - Dashboard data with hardcoded values
- `scripts/analysis_results.json` - Complete analysis results in JSON format
- `scripts/ANALYSIS_SUMMARY.md` - This summary document
