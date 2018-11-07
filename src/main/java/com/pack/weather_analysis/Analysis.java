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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;


public class Analysis {

    //data files were pre filtered in a separate python script to only show data for Texas
    //the reason I used python is because I wanted to take advantage of multiprocessing due to the
    //density of the data in each year. Running multiple interpreters was far less resource intensive
    //than running multiple JVMs.

    InputStream us_data_1940_file, us_data_1950_file, us_data_1951_file, us_data_1960_file, us_data_1970_file;
    Table weatherDataDF;

    //Initialize the dataframe with the 1940 data to establish columns.
    //The year data is stored in the resources folder of this project.
    public void setUpDataFrames() {
        weatherDataDF = load1940Data();
    }

    public void loadWeatherData(){
        weatherDataDF.append(load1950Data());
        weatherDataDF.append(load1960Data());
        weatherDataDF.append(load1970Data());
    }

    public Table load1940Data(){
        //get our data as input streams from the resources folder
        us_data_1940_file = getClass().getResourceAsStream("1940_data.csv");

        //convert the input streams to buffered readers
        BufferedReader bufferedReader = new BufferedReader( new InputStreamReader( us_data_1940_file ) );
        try {
            return Table.read().csv(bufferedReader, "");
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public Table load1950Data(){
        //get our data as input streams from the resources folder
        us_data_1950_file = getClass().getResourceAsStream("1950_data.csv");

        //convert the input streams to buffered readers
        BufferedReader bufferedReader = new BufferedReader( new InputStreamReader( us_data_1950_file ) );
        try {
            return Table.read().csv(bufferedReader, "");
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public Table load1951Data(){
        //get our data as input streams from the resources folder
        us_data_1951_file = getClass().getResourceAsStream("1951_data.csv");

        //convert the input streams to buffered readers
        BufferedReader bufferedReader = new BufferedReader( new InputStreamReader( us_data_1951_file ) );
        try {
            return Table.read().csv(bufferedReader, "");
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public Table load1960Data(){
        //get our data as input streams from the resources folder
        us_data_1960_file = getClass().getResourceAsStream("1960_data.csv");

        //convert the input streams to buffered readers
        BufferedReader bufferedReader = new BufferedReader( new InputStreamReader( us_data_1960_file ) );
        try {
            return Table.read().csv(bufferedReader, "");
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public Table load1970Data(){
        //get our data as input streams from the resources folder
        us_data_1970_file = getClass().getResourceAsStream("1970_data.csv");

        //convert the input streams to buffered readers
        BufferedReader bufferedReader = new BufferedReader( new InputStreamReader( us_data_1970_file ) );
        try {
            return Table.read().csv(bufferedReader, "");
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
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

    public void appendTableToMainDataFrame(Table table){
        weatherDataDF.append(table);
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

    public InputStream getUs_data_1940_file() {
        return us_data_1940_file;
    }

    public InputStream getUs_data_1950_file() {
        return us_data_1950_file;
    }

    public InputStream getUs_data_1951_file() {
        return us_data_1951_file;
    }

    public InputStream getUs_data_1960_file() {
        return us_data_1960_file;
    }

    public InputStream getUs_data_1970_file() {
        return us_data_1970_file;
    }

    public Table getWeatherDataDF() {
        return weatherDataDF;
    }
}
