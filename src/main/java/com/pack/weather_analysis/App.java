package com.pack.weather_analysis;

import tech.tablesaw.api.Table;

public class App
{
    public static void main( String[] args )
    {

        Analysis analysis = new Analysis();

        //add the four years of data to the dataframe
        analysis.setUpDataFrames();
        analysis.loadWeatherData();

        //lets look at the avg wind speed and air pressure for the four years
        Table avgAirPressureByMonthYear = analysis.avgAirPressureByMonthYear();
        Table avgWindSpeedByMonthYear = analysis.avgWindSpeedByMonthYear();
        Table avgSkyCvgByMonthYear = analysis.avgSkyCvgByMonthYear();

        //display plots
        analysis.plotLineGraph("Avg State Air Pressure by Month", avgAirPressureByMonthYear, "MONTH", "[SEA_LVL_PRESSURE]", "YEAR");
        analysis.plotLineGraph("Avg State Wind Speed by Month", avgWindSpeedByMonthYear, "MONTH", "[WIND_SPEED_RATE]", "YEAR");
        analysis.plotLineGraph("Avg State Sky Coverage by Month", avgSkyCvgByMonthYear, "MONTH", "[SKY_CVRG_CODE]", "YEAR");
        //air pressure and wind speed have not changed significantly in the past four decades

        //lets join the tables and print them out
        Table joinedPressureCvg = analysis.innerJoinTable(avgAirPressureByMonthYear, avgSkyCvgByMonthYear, "YEAR", "MONTH");
        Table weatherConditions = analysis.innerJoinTable(joinedPressureCvg, avgWindSpeedByMonthYear, "YEAR", "MONTH").setName("Weather Conditions");
        System.out.println(weatherConditions);


        //lets take a look at the avg monthly temp of texas for the four years
        Table avgStateTempByMonthYear = analysis.avgStateTempByMonthYear();
        analysis.plotLineGraph("Avg State Temperature by Month", avgStateTempByMonthYear, "MONTH", "[AIR_TEMP]", "YEAR");
        //1940, 1960, and 1970 dont show any serious changes; however the temp
        //for 1950 dips significantly after month 7

        //lets load data for 1951 to see if the low temps continue
        analysis.appendTableToMainDataFrame(analysis.load1951Data());

        //lets take a look at the avg temp of texas for the five years
        avgStateTempByMonthYear = analysis.avgStateTempByMonthYear();
        analysis.plotLineGraph("Avg State Temperature by Month", avgStateTempByMonthYear, "MONTH", "[AIR_TEMP]", "YEAR");
        //the avg monthly temp for the first six months of 1951 is significantly lower than the other four
        //the avg monthly temp for the last six months of 1951 correspond with the avg temp of the last
        //six months of 1950.

        //lets take another look at the weather conditions, but with 1951 added this time
        avgAirPressureByMonthYear = analysis.avgAirPressureByMonthYear();
        avgWindSpeedByMonthYear = analysis.avgWindSpeedByMonthYear();
        avgSkyCvgByMonthYear = analysis.avgSkyCvgByMonthYear();

        analysis.plotLineGraph("Avg State Air Pressure by Month", avgAirPressureByMonthYear, "MONTH", "[SEA_LVL_PRESSURE]", "YEAR");
        analysis.plotLineGraph("Avg State Wind Speed by Month", avgWindSpeedByMonthYear, "MONTH", "[WIND_SPEED_RATE]", "YEAR");
        analysis.plotLineGraph("Avg State Sky Coverage by Month", avgSkyCvgByMonthYear, "MONTH", "[SKY_CVRG_CODE]", "YEAR");

        //so the air pressure and wind speed did not change significantly between 1951 and
        //the other years.
        //month 10 of 1950 had the lowest average of the sky code coverage, meaning that month
        //had the most days with minimal cloud coverage


        //lets join the tables with the updated years and print it
        joinedPressureCvg = analysis.innerJoinTable(avgAirPressureByMonthYear, avgSkyCvgByMonthYear, "YEAR", "MONTH");
        weatherConditions = analysis.innerJoinTable(joinedPressureCvg, avgWindSpeedByMonthYear, "YEAR", "MONTH").setName("Weather Conditions");
        System.out.println(weatherConditions);


    }
}
