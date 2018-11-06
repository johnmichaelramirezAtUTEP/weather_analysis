package com.pack.weather_analysis;

public class App 
{
    public static void main( String[] args )
    {

        Analysis analysis = new Analysis();

        //add the four years of data to the dataframe
        analysis.setUpDataFrames();
        //lets look at the avg wind speed and air pressure for the four years
        analysis.weatherConditions();
        //air pressure and wind speed have not changed significantly in the past four decades
        //

        //lets take a look at the avg monthly temp of texas for the four years
        analysis.avgStateTemp();
        //1940, 1960, and 1970 dont show any serious changes; however the temp
        //for 1950 dips significantly after month 7

        //lets load data for 1951 to see if the low temps continue
        analysis.add1951Data();

        //lets take a look at the avg temp of texas for the five years
        analysis.avgStateTemp();
        //the avg monthly temp for the first six months of 1951 is significantly lower than the other four
        //the avg monthly temp for the last six months of 1951 correspond with the avg temp of the last
        //six months of 1950.

        //lets take another look at the weather conditions, but with 1951 added this time
        analysis.weatherConditions();
        //so the air pressure and wind speed did not change significantly between 1951 and
        //the other years.
        //month 10 of 1950 had the lowest average of the sky code coverage, meaning that month
        //had the most days with minimal cloud coverage

        //in conclusion, the last 6 months of 1950 and the entirety of 1951 experienced a signficant drop
        //in temperature.

        //1000 HectoPascals = 100 KiloPascals
        //Atmosphere at sea level ~ 100 KiloPascals
    }
}
