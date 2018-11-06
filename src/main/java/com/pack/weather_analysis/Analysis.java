package com.pack.weather_analysis;

import tech.tablesaw.aggregate.NumericAggregateFunction;
import tech.tablesaw.api.*;
import tech.tablesaw.columns.Column;
import tech.tablesaw.io.csv.CsvReadOptions;
import tech.tablesaw.plotly.*;
import tech.tablesaw.plotly.api.LinePlot;
import tech.tablesaw.plotly.api.ScatterPlot;
import tech.tablesaw.selection.Selection;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

public class Analysis {

    InputStream isd_history, us_data_1940_file, us_data_1950_file, us_data_1951_file, us_data_1960_file, us_data_1970_file;
    Table stationsDF, weatherDataDF;

    //Initialize the dataframe with all the year data we will be using.
    //The year data is stored in the resources folder of this project.
    public void setUpDataFrames() {
        //get our data as input streams from the resources folder
        isd_history = getClass().getResourceAsStream("isd_history.csv");
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
//            BufferedReader bufferedReaderStations = new BufferedReader( new InputStreamReader( isd_history ) );

//            stationsDF = Table.read().csv(CsvReadOptions
//                            .builder(bufferedReaderStations, "Weather Stations")
//                            .sample(false));

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
    public void avgStateTemp() {
        //Get the average air temperature for all US stations for all four years (1940, 1950, 1960, 1970)
        Table avgTemp = weatherDataDF.summarize("AIR_TEMP", new NumericAggregateFunction("") {
            @Override
            public Double summarize(NumericColumn<?> column) {
                return column.mean() / 10;
            }
        }).by("YEAR", "MONTH");

        //display the dataframes as lineplots
        Plot.show(LinePlot.create("Avg Temp in Texas 1940 - 1970", avgTemp, "MONTH", "[AIR_TEMP]", "YEAR"));
    }

    //Calculates the average air pressure, average wind speed,
    //and average sky code for each year in our dataframe
    public void weatherConditions(){
        //Filter out the -9999 values in the sea_lvl_pressure column
        Selection airPressureFilter = weatherDataDF.numberColumn("SEA_LVL_PRESSURE").isNotEqualTo(-9999);
        Table filteredAirPressure = weatherDataDF.where(airPressureFilter);

        //Filter out the -9999 values in the wind_speed_rate column
        Selection windSpeedFilter = filteredAirPressure.numberColumn("WIND_SPEED_RATE").isNotEqualTo(-9999);
        Table filteredWeatherConditions = filteredAirPressure.where(windSpeedFilter);

        //calculate the mean sea_lvl_pressure and wind speed rate in our filtered dataframe,
        //groups the summary by year and month
        Table weatherInfo = filteredWeatherConditions.summarize("SEA_LVL_PRESSURE", "WIND_SPEED_RATE", new NumericAggregateFunction("") {
            @Override
            public Double summarize(NumericColumn<?> column) {
                return column.mean() / 10;
            }
        }).by("YEAR", "MONTH").setName("AVG Air Pressure & Wind Speed");

        //Filter out the -9999 values in the sky_cvrg_code column
        Selection skyCoverageFilter = weatherDataDF.numberColumn("SKY_CVRG_CODE").isNotEqualTo(-9999);
        Table skyCoverageInfo = weatherDataDF.where(skyCoverageFilter);
        skyCoverageInfo = skyCoverageInfo.summarize("SKY_CVRG_CODE", new NumericAggregateFunction("") {
            @Override
            public Double summarize(NumericColumn<?> column) {
                return column.mean();
            }
        }).by("YEAR", "MONTH").setName("Avg Sky Coverage per Month");

        //display the dataframes as lineplots
        Plot.show(LinePlot.create("Avg Air Pressure in Texas 1940 - 1970", weatherInfo, "MONTH", "[SEA_LVL_PRESSURE]", "YEAR"));
        Plot.show(LinePlot.create("Avg Wind Speed in Texas 1940 - 1970", weatherInfo, "MONTH", "[WIND_SPEED_RATE]", "YEAR"));
        Plot.show(LinePlot.create("Avg Sky Coverage in Texas 1950 - 1970", skyCoverageInfo, "MONTH", "[SKY_CVRG_CODE]", "YEAR"));
    }
}
