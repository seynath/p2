/**
 * Sri Lanka Weather Analytics Dashboard - Charts Module
 * Renders Chart.js visualizations using data from data.js
 * Task 37: Enhanced with accessibility and color coding for severity levels
 */

// Chart color palette with WCAG AA compliant contrast ratios
const ChartColors = {
    primary: 'rgba(37, 99, 235, 1)',
    primaryLight: 'rgba(37, 99, 235, 0.2)',
    success: 'rgba(22, 163, 74, 1)',
    successLight: 'rgba(22, 163, 74, 0.2)',
    warning: 'rgba(217, 119, 6, 1)',
    warningLight: 'rgba(217, 119, 6, 0.2)',
    danger: 'rgba(220, 38, 38, 1)',
    dangerLight: 'rgba(220, 38, 38, 0.2)',
    info: 'rgba(8, 145, 178, 1)',
    infoLight: 'rgba(8, 145, 178, 0.2)',
    palette: [
        'rgba(37, 99, 235, 0.85)',
        'rgba(22, 163, 74, 0.85)',
        'rgba(217, 119, 6, 0.85)',
        'rgba(220, 38, 38, 0.85)',
        'rgba(139, 92, 246, 0.85)',
        'rgba(236, 72, 153, 0.85)',
        'rgba(8, 145, 178, 0.85)',
        'rgba(249, 115, 22, 0.85)',
        'rgba(99, 102, 241, 0.85)',
        'rgba(168, 85, 247, 0.85)'
    ]
};

// Common chart options with accessibility enhancements
const commonOptions = {
    responsive: true,
    maintainAspectRatio: false,
    plugins: {
        legend: {
            position: 'bottom',
            labels: {
                padding: 20,
                usePointStyle: true,
                font: { size: 12, weight: '500' },
                color: '#1e293b'
            }
        },
        tooltip: {
            backgroundColor: 'rgba(30, 41, 59, 0.95)',
            titleFont: { size: 14, weight: '600' },
            bodyFont: { size: 13 },
            padding: 12,
            cornerRadius: 8
        }
    },
    animation: { duration: 750, easing: 'easeOutQuart' }
};

// Check for reduced motion preference
if (window.matchMedia('(prefers-reduced-motion: reduce)').matches) {
    commonOptions.animation = { duration: 0 };
}

function initializeCharts() {
    initPrecipitationByDistrictChart();
    initTopDistrictsPrecipitationChart();
    initHighTempMonthsChart();
    initTemperatureTrendsChart();
    initExtremeWeatherByYearChart();
    initExtremeWeatherByDistrictChart();
    updateStatistics();
}


function initPrecipitationByDistrictChart() {
    const ctx = document.getElementById('precipitationByDistrictChart');
    if (!ctx) return;

    const backgroundColors = WeatherData.precipitationByDistrict.precipitationHours.map(value => {
        if (value >= 180) return ChartColors.danger;
        if (value >= 140) return ChartColors.warning;
        if (value >= 100) return ChartColors.info;
        return ChartColors.success;
    });

    new Chart(ctx, {
        type: 'bar',
        data: {
            labels: WeatherData.precipitationByDistrict.labels,
            datasets: [{
                label: 'Avg Precipitation Hours',
                data: WeatherData.precipitationByDistrict.precipitationHours,
                backgroundColor: backgroundColors,
                borderColor: backgroundColors,
                borderWidth: 2,
                borderRadius: 4
            }]
        },
        options: {
            ...commonOptions,
            plugins: {
                ...commonOptions.plugins,
                legend: { display: false },
                tooltip: {
                    ...commonOptions.plugins.tooltip,
                    callbacks: {
                        title: (ctx) => ctx[0].label,
                        label: (ctx) => `Precipitation: ${ctx.parsed.y.toFixed(1)} hours`,
                        afterLabel: (ctx) => {
                            const i = ctx.dataIndex;
                            return [
                                `Peak Month: ${WeatherData.precipitationByDistrict.mostPrecipitousMonth[i]}`,
                                `Season: ${WeatherData.precipitationByDistrict.mostPrecipitousSeason[i]}`
                            ];
                        }
                    }
                }
            },
            scales: {
                y: {
                    beginAtZero: true,
                    title: { display: true, text: 'Precipitation Hours', font: { weight: '600' }, color: '#1e293b' },
                    grid: { color: 'rgba(0, 0, 0, 0.08)' },
                    ticks: { color: '#64748b' }
                },
                x: {
                    title: { display: true, text: 'District', font: { weight: '600' }, color: '#1e293b' },
                    ticks: { maxRotation: 45, minRotation: 45, color: '#64748b', font: { size: 11 } },
                    grid: { display: false }
                }
            }
        }
    });
}

function initTopDistrictsPrecipitationChart() {
    const ctx = document.getElementById('topDistrictsPrecipitationChart');
    if (!ctx) return;

    const gradientColors = [ChartColors.danger, ChartColors.warning, ChartColors.warning, ChartColors.info, ChartColors.info];

    new Chart(ctx, {
        type: 'bar',
        data: {
            labels: WeatherData.topDistrictsPrecipitation.labels,
            datasets: [{
                label: 'Total Precipitation (hours)',
                data: WeatherData.topDistrictsPrecipitation.values,
                backgroundColor: gradientColors,
                borderColor: gradientColors,
                borderWidth: 2,
                borderRadius: 4
            }]
        },
        options: {
            ...commonOptions,
            indexAxis: 'y',
            plugins: {
                ...commonOptions.plugins,
                legend: { display: false },
                tooltip: {
                    ...commonOptions.plugins.tooltip,
                    callbacks: { label: (ctx) => `Total: ${ctx.parsed.x.toLocaleString()} hours` }
                }
            },
            scales: {
                x: {
                    beginAtZero: true,
                    title: { display: true, text: 'Total Precipitation Hours', font: { weight: '600' }, color: '#1e293b' },
                    grid: { color: 'rgba(0, 0, 0, 0.08)' },
                    ticks: { color: '#64748b', callback: (v) => v.toLocaleString() }
                },
                y: { grid: { display: false }, ticks: { color: '#1e293b', font: { weight: '500' } } }
            }
        }
    });
}

function initHighTempMonthsChart() {
    const ctx = document.getElementById('highTempMonthsChart');
    if (!ctx) return;

    const backgroundColors = WeatherData.highTempMonthsByYear.percentages.map(value => {
        if (value >= 50) return ChartColors.danger;
        if (value >= 25) return ChartColors.warning;
        if (value > 0) return ChartColors.info;
        return ChartColors.success;
    });

    new Chart(ctx, {
        type: 'bar',
        data: {
            labels: WeatherData.highTempMonthsByYear.labels,
            datasets: [{
                label: '% Months with Mean Temp > 30°C',
                data: WeatherData.highTempMonthsByYear.percentages,
                backgroundColor: backgroundColors,
                borderColor: backgroundColors,
                borderWidth: 2,
                borderRadius: 4
            }]
        },
        options: {
            ...commonOptions,
            plugins: {
                ...commonOptions.plugins,
                legend: { display: false },
                subtitle: {
                    display: true,
                    text: 'Note: Monthly mean temperatures in Sri Lanka rarely exceed 30°C',
                    font: { size: 11, style: 'italic' },
                    color: '#64748b',
                    padding: { bottom: 10 }
                }
            },
            scales: {
                y: {
                    beginAtZero: true,
                    max: 100,
                    title: { display: true, text: 'Percentage (%)', font: { weight: '600' }, color: '#1e293b' },
                    ticks: { stepSize: 20, color: '#64748b' },
                    grid: { color: 'rgba(0, 0, 0, 0.08)' }
                },
                x: {
                    title: { display: true, text: 'Year', font: { weight: '600' }, color: '#1e293b' },
                    ticks: { color: '#64748b' },
                    grid: { display: false }
                }
            }
        }
    });
}


function initTemperatureTrendsChart() {
    const ctx = document.getElementById('temperatureTrendsChart');
    if (!ctx) return;

    new Chart(ctx, {
        type: 'line',
        data: {
            labels: WeatherData.temperatureTrends.labels,
            datasets: [
                {
                    label: 'Avg Max Temperature (°C)',
                    data: WeatherData.temperatureTrends.avgMaxTemp,
                    borderColor: ChartColors.danger,
                    backgroundColor: ChartColors.dangerLight,
                    fill: true,
                    tension: 0.3,
                    pointRadius: 5,
                    pointHoverRadius: 7,
                    pointBackgroundColor: ChartColors.danger,
                    pointBorderColor: '#ffffff',
                    pointBorderWidth: 2,
                    borderWidth: 3
                },
                {
                    label: 'Avg Mean Temperature (°C)',
                    data: WeatherData.temperatureTrends.avgMeanTemp,
                    borderColor: ChartColors.warning,
                    backgroundColor: ChartColors.warningLight,
                    fill: true,
                    tension: 0.3,
                    pointRadius: 5,
                    pointHoverRadius: 7,
                    pointBackgroundColor: ChartColors.warning,
                    pointBorderColor: '#ffffff',
                    pointBorderWidth: 2,
                    borderWidth: 3
                }
            ]
        },
        options: {
            ...commonOptions,
            interaction: { intersect: false, mode: 'index' },
            scales: {
                y: {
                    min: 24,
                    max: 32,
                    title: { display: true, text: 'Temperature (°C)', font: { weight: '600' }, color: '#1e293b' },
                    ticks: { stepSize: 1, color: '#64748b' },
                    grid: { color: 'rgba(0, 0, 0, 0.08)' }
                },
                x: {
                    title: { display: true, text: 'Year', font: { weight: '600' }, color: '#1e293b' },
                    ticks: { color: '#64748b' },
                    grid: { display: false }
                }
            },
            plugins: {
                ...commonOptions.plugins,
                tooltip: {
                    ...commonOptions.plugins.tooltip,
                    callbacks: { label: (ctx) => `${ctx.dataset.label}: ${ctx.parsed.y.toFixed(1)}°C` }
                }
            }
        }
    });
}

function initExtremeWeatherByYearChart() {
    const ctx = document.getElementById('extremeWeatherByYearChart');
    if (!ctx) return;

    const backgroundColors = WeatherData.extremeWeatherByYear.days.map(value => {
        if (value >= 200) return ChartColors.danger;
        if (value >= 100) return ChartColors.warning;
        return ChartColors.success;
    });

    new Chart(ctx, {
        type: 'bar',
        data: {
            labels: WeatherData.extremeWeatherByYear.labels,
            datasets: [{
                label: 'Extreme Weather Days',
                data: WeatherData.extremeWeatherByYear.days,
                backgroundColor: backgroundColors,
                borderColor: backgroundColors,
                borderWidth: 2,
                borderRadius: 4
            }]
        },
        options: {
            ...commonOptions,
            plugins: {
                ...commonOptions.plugins,
                legend: { display: false },
                tooltip: {
                    ...commonOptions.plugins.tooltip,
                    callbacks: {
                        label: (ctx) => {
                            const value = ctx.parsed.y;
                            let severity = value >= 200 ? 'High' : value >= 100 ? 'Medium' : 'Low';
                            return [`Days: ${value}`, `Severity: ${severity}`];
                        }
                    }
                }
            },
            scales: {
                y: {
                    beginAtZero: true,
                    title: { display: true, text: 'Number of Days', font: { weight: '600' }, color: '#1e293b' },
                    grid: { color: 'rgba(0, 0, 0, 0.08)' },
                    ticks: { color: '#64748b' }
                },
                x: {
                    title: { display: true, text: 'Year', font: { weight: '600' }, color: '#1e293b' },
                    ticks: { color: '#64748b' },
                    grid: { display: false }
                }
            }
        }
    });
}

function initExtremeWeatherByDistrictChart() {
    const ctx = document.getElementById('extremeWeatherByDistrictChart');
    if (!ctx) return;

    new Chart(ctx, {
        type: 'doughnut',
        data: {
            labels: WeatherData.extremeWeatherByDistrict.labels,
            datasets: [{
                data: WeatherData.extremeWeatherByDistrict.days,
                backgroundColor: ChartColors.palette,
                borderColor: '#ffffff',
                borderWidth: 3,
                hoverOffset: 8
            }]
        },
        options: {
            ...commonOptions,
            cutout: '55%',
            plugins: {
                legend: {
                    position: 'right',
                    labels: { padding: 12, usePointStyle: true, font: { size: 11 }, color: '#1e293b' }
                },
                tooltip: {
                    ...commonOptions.plugins.tooltip,
                    callbacks: {
                        label: (ctx) => {
                            const total = ctx.dataset.data.reduce((a, b) => a + b, 0);
                            const pct = ((ctx.parsed / total) * 100).toFixed(1);
                            return `${ctx.label}: ${ctx.parsed} days (${pct}%)`;
                        }
                    }
                }
            }
        }
    });
}


function populatePrecipitationTable() {
    const tableBody = document.getElementById('precipitationTableBody');
    if (!tableBody) return;

    const { labels, mostPrecipitousMonth, mostPrecipitousSeason, precipitationHours } = 
        WeatherData.precipitationByDistrict;

    let html = '';
    for (let i = 0; i < labels.length; i++) {
        const seasonClass = mostPrecipitousSeason[i].toLowerCase().includes('northeast') ? 'northeast' :
                           mostPrecipitousSeason[i].toLowerCase().includes('southwest') ? 'southwest' : 
                           'inter-monsoon';
        
        const precipValue = precipitationHours[i];
        let precipClass = 'low';
        if (precipValue >= 180) precipClass = 'very-high';
        else if (precipValue >= 140) precipClass = 'high';
        else if (precipValue >= 100) precipClass = 'moderate';
        
        html += `
            <tr>
                <td><strong>${labels[i]}</strong></td>
                <td>${mostPrecipitousMonth[i]}</td>
                <td><span class="season-tag ${seasonClass}" role="status">${mostPrecipitousSeason[i]}</span></td>
                <td><span class="precip-level ${precipClass}">${precipitationHours[i].toFixed(1)} hrs</span></td>
            </tr>
        `;
    }
    tableBody.innerHTML = html;
}

function populateExtremeWeatherTable() {
    const tableBody = document.getElementById('extremeWeatherTableBody');
    if (!tableBody) return;

    const { labels, days } = WeatherData.extremeWeatherByDistrict;
    const totalDays = 2197;

    let html = '';
    for (let i = 0; i < labels.length; i++) {
        const percentage = ((days[i] / totalDays) * 100).toFixed(1);
        let severityClass = 'severity-low';
        let severityLabel = 'Low severity';
        if (days[i] > 200) {
            severityClass = 'severity-high';
            severityLabel = 'High severity';
        } else if (days[i] > 100) {
            severityClass = 'severity-medium';
            severityLabel = 'Medium severity';
        }
        
        html += `
            <tr>
                <td><strong>#${i + 1}</strong></td>
                <td><strong>${labels[i]}</strong></td>
                <td><span class="extreme-count ${severityClass}" role="status" aria-label="${days[i]} days, ${severityLabel}">${days[i]} days</span></td>
                <td>${percentage}%</td>
            </tr>
        `;
    }
    tableBody.innerHTML = html;
}

function updateStatistics() {
    updateElement('highestPrecipMonth', 
        `${WeatherData.precipitationStats.highestPrecipMonth} ${WeatherData.precipitationStats.highestPrecipYear}`);
    updateElement('highestPrecipValue', WeatherData.precipitationStats.highestPrecipValue);
    updateElement('avgPrecipHours', WeatherData.precipitationStats.avgMonthlyPrecipHours);
    updateElement('avgHighTemp', WeatherData.temperatureStats.avgHighTemp);
    updateElement('hottestDistrict', WeatherData.temperatureStats.hottestDistrict);
    updateElement('highTempMonthsPct', WeatherData.temperatureStats.highTempMonthsPercentage);
    updateElement('totalExtremeDays', WeatherData.extremeWeatherStats.totalExtremeDays);
    updateElement('avgExtremePerYear', WeatherData.extremeWeatherStats.avgExtremePerYear);
    updateElement('mostAffectedDistrict', WeatherData.extremeWeatherStats.mostAffectedDistrict);
}

function updateElement(id, value) {
    const element = document.getElementById(id);
    if (element) element.textContent = value;
}

function initNavigation() {
    const navLinks = document.querySelectorAll('.nav-link');
    const sections = document.querySelectorAll('.dashboard-section');

    function updateActiveLink(activeId) {
        navLinks.forEach(link => {
            const isActive = link.getAttribute('href') === `#${activeId}`;
            link.classList.toggle('active', isActive);
            link.setAttribute('aria-current', isActive ? 'true' : 'false');
        });
    }

    const observer = new IntersectionObserver((entries) => {
        entries.forEach(entry => {
            if (entry.isIntersecting) {
                updateActiveLink(entry.target.getAttribute('id'));
            }
        });
    }, { threshold: 0.3, rootMargin: '-80px 0px 0px 0px' });

    sections.forEach(section => observer.observe(section));

    navLinks.forEach(link => {
        link.addEventListener('click', (e) => {
            e.preventDefault();
            const targetId = link.getAttribute('href').substring(1);
            const targetSection = document.getElementById(targetId);
            if (targetSection) {
                targetSection.scrollIntoView({ behavior: 'smooth' });
                targetSection.setAttribute('tabindex', '-1');
                targetSection.focus();
            }
        });

        link.addEventListener('keydown', (e) => {
            if (e.key === 'Enter' || e.key === ' ') {
                e.preventDefault();
                link.click();
            }
        });
    });
}

document.addEventListener('DOMContentLoaded', () => {
    initializeCharts();
    initNavigation();
    populatePrecipitationTable();
    populateExtremeWeatherTable();
});
