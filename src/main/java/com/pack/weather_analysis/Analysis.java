package com.pack.weather_analysis;

import tech.tablesaw.aggregate.NumericAggregateFunction;
import tech.tablesaw.api.*;
import tech.tablesaw.columns.Column;
import tech.tablesaw.joining.DataFrameJoiner;
import tech.tablesaw.plotly.*;
import tech.tablesaw.plotly.api.LinePlot;

import tech.tablesaw.selection.Selection;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;


public class Analysis {

    //data files were pre filtered in a separate python script to only show data for Texas
    //the reason I used python is because I wanted to take advantage of multiprocessing due to the
    //density of the data in each year. Running multiple interpreters was far less resource intensive
    //than running multiple JVMs.


    InputStream isd_history, us_data_1940_file, us_data_1950_file, us_data_1951_file, us_data_1960_file, us_data_1970_file;
    Table stationsDF, weatherDataDF;

    //Initialize the dataframe with all the year data we will be using.
    //The year data is stored in the resources folder of this project.
    public void setUpDataFrames() {
        //get our data as input streams from the resources folder
        us_data_1940_file = getClass().getResourceAsStream("1940_data.csv");
        us_data_1950_file = getClass().getResourceAsStream("1950_data.csv");
        us_data_1960_file = getClass().getResourceAsStream("1960_data.csv");
        us_data_1970_file = getClass().getResourceAsStream("1970_data.csv");

        try
        {
            //convert the input streams to buffered readers
            BufferedReader bufferedReader1940 = new BufferedReader( new InputStreamReader( us_data_1940_file ) );
            BufferedReader bufferedReader1950 = new BufferedReader( new InputStreamReader( us_data_1950_file ) );
            BufferedReader bufferedReader1960 = new BufferedReader( new InputStreamReader( us_data_1960_file ) );
            BufferedReader bufferedReader1970 = new BufferedReader( new InputStreamReader( us_data_1970_file ) );

            //read the data into our weatherData dataframe
            weatherDataDF = Table.read().csv(bufferedReader1940, "1940 - 1970 Weather Data");
            weatherDataDF.append(Table.read().csv(bufferedReader1950, ""));
            weatherDataDF.append(Table.read().csv(bufferedReader1960, ""));
            weatherDataDF.append(Table.read().csv(bufferedReader1970, ""));
        }
        catch( IOException e )
        {
            System.err.println( "Error: " + e );
        }

    }

    //Adds the data for 1951 to our dataframe
    public void add1951Data(){
        us_data_1951_file = getClass().getResourceAsStream("1951_data.csv");
        BufferedReader bufferedReader1951 = new BufferedReader( new InputStreamReader( us_data_1951_file ) );
        try {
            weatherDataDF.append(Table.read().csv(bufferedReader1951, ""));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //Calculates the average temperature for each month of each year in our dataframe
    public Table avgStateTempByMonthYear() {
        //Get the average air temperature for all US stations for all four years (1940, 1950, 1960, 1970)
        Table avgTemp = weatherDataDF.summarize("AIR_TEMP", new NumericAggregateFunction("") {
            @Override
            public Double summarize(NumericColumn<?> column) {
                return column.mean() / 10;
            }
        }).by("YEAR", "MONTH");

        return avgTemp;
    }

    //Calculates the average air pressure for each month of each year in our dataframe
    public Table avgAirPressureByMonthYear(){
        //Filter out the -9999 values in the sea_lvl_pressure column
        Selection airPressureFilter = weatherDataDF.numberColumn("SEA_LVL_PRESSURE").isNotEqualTo(-9999);
        Table filteredAirPressure = weatherDataDF.where(airPressureFilter);

        Table airPressureMean = filteredAirPressure.summarize("SEA_LVL_PRESSURE", new NumericAggregateFunction("") {
            @Override
            public Double summarize(NumericColumn<?> column) {
                return column.mean() / 10;
            }
        }).by("YEAR", "MONTH").setName("AVG Air Pressure");

        return airPressureMean;
    }

    //Calculates the average wind speed for each month of each year in our dataframe
    public Table avgWindSpeedByMonthYear(){
        //Filter out the -9999 values in the wind_speed_rate column
        Selection windSpeedFilter = weatherDataDF.numberColumn("WIND_SPEED_RATE").isNotEqualTo(-9999);
        Table filteredWindSpeed = weatherDataDF.where(windSpeedFilter);

        Table windSpeedMean = filteredWindSpeed.summarize("WIND_SPEED_RATE", new NumericAggregateFunction("") {
            @Override
            public Double summarize(NumericColumn<?> column) {
                return column.mean() / 10;
            }
        }).by("YEAR", "MONTH").setName("AVG Wind Speed");

        return windSpeedMean;
    }

    //Calculates the average sky coverage for each month of each year in our dataframe
    public Table avgSkyCvgByMonthYear(){
        //Filter out the -9999 values in the sky_cvrg_code column
        Selection skyCoverageFilter = weatherDataDF.numberColumn("SKY_CVRG_CODE").isNotEqualTo(-9999);
        Table skyCoverageInfo = weatherDataDF.where(skyCoverageFilter);
        Table skyCoverageMean = skyCoverageInfo.summarize("SKY_CVRG_CODE", new NumericAggregateFunction("") {
            @Override
            public Double summarize(NumericColumn<?> column) {
                return column.mean();
            }
        }).by("YEAR", "MONTH").setName("Avg Sky Coverage per Month");
        return skyCoverageMean;
    }

    public void plotLineGraph(String title, Table table, String xColName, String yColName, String groupColName){
        for (Column<?> column: table.columns()) {
            if(column.isEmpty()){
                table.removeColumns(column.name());
            }
        }
        if(table.columnCount() == 0){
            return;
        }
        Plot.show(LinePlot.create(title, table, xColName, yColName, groupColName));
    }

    public Table innerJoinTable(Table table1, Table table2, String... columnNames){
        DataFrameJoiner joiner = table1.join(columnNames);
        return joiner.inner(table2);
    }
}
