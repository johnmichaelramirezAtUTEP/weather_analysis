package com.pack.weather_analysis;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import tech.tablesaw.api.Table;

import static org.junit.Assert.*;

public class AnalysisTest {

    Analysis analysis;

    @Before
    public void setUp() throws Exception {
        analysis = new Analysis();
        analysis.setUpDataFrames();
        analysis.loadWeatherData();
    }

    @After
    public void tearDown() throws Exception {
    }


    //Test that the tables were properly initialized with the file data

    @Test
    public void weatherDataFor1940Loaded(){
        Table table = analysis.load1940Data();
        assertNotNull(table);
    }

    @Test
    public void weatherDataFor1950Loaded(){
        Table table = analysis.load1950Data();
        assertNotNull(table);
    }

    @Test
    public void weatherDataFor1951Loaded(){
        Table table = analysis.load1951Data();
        assertNotNull(table);
    }

    @Test
    public void weatherDataFor1960Loaded(){
        Table table = analysis.load1960Data();
        assertNotNull(table);
    }

    @Test
    public void weatherDataFor1970Loaded(){
        Table table = analysis.load1970Data();
        assertNotNull(table);
    }
    //

    //Test the number of columns contained in each Table

    @Test
    public void avgStateTempHasThreeColumns() {
        Table avgStateTempByMonthYear = analysis.avgStateTempByMonthYear();
        //the returned table should only have 3 columns
        assertTrue(avgStateTempByMonthYear.columnCount() == 3);
        //
    }

    @Test
    public void avgAirPressureHasThreeColumns() {
        Table avgAirPressurebyMonthYear = analysis.avgAirPressureByMonthYear();
        //the returned table should only have 3 columns
        assertTrue(avgAirPressurebyMonthYear.columnCount() == 3);
        //
    }

    @Test
    public void avgWindSpeedHasThreeColumns() {
        Table avgWindSpeedByMonthYear = analysis.avgWindSpeedByMonthYear();
        //the returned table should only have 3 columns
        assertTrue(avgWindSpeedByMonthYear.columnCount() == 3);
        //
    }

    @Test
    public void avgSkyCvgHasThreeColumns() {
        Table avgSkyCvgByMonthYear = analysis.avgSkyCvgByMonthYear();
        //the returned table should only have 3 columns
        assertTrue(avgSkyCvgByMonthYear.columnCount() == 3);
        //
    }

    //


    //Test tables to see if they contain the appropriate column name

    @Test
    public void avgStateTempHasAirTempColumn() {
        Table avgStateTempByMonthYear = analysis.avgStateTempByMonthYear();
        //the returned table should only have a "[AIR_TEMP]" column
        assertTrue(avgStateTempByMonthYear.columnNames().contains("[AIR_TEMP]"));
    }

    @Test
    public void avgAirPressureHasPressureColumn() {
        Table avgAirPressureByMonthYear = analysis.avgAirPressureByMonthYear();
        //the returned table should only have a "[SEA_LVL_PRESSURE]" column
        assertTrue(avgAirPressureByMonthYear.columnNames().contains("[SEA_LVL_PRESSURE]"));
    }

    @Test
    public void avgWindSpeedHasWindSpeedColumn() {
        Table avgWindSpeedByMonthYear = analysis.avgWindSpeedByMonthYear();
        //the returned table should only have a "[WIND_SPEED_RATE]" column
        assertTrue(avgWindSpeedByMonthYear.columnNames().contains("[WIND_SPEED_RATE]"));
    }

    @Test
    public void avgSkyCvgHasSkyCvgColumn() {
        Table avgSkyCvgByMonthYear = analysis.avgSkyCvgByMonthYear();
        //the returned table should only have a "[SKY_CVRG_CODE]" column
        assertTrue(avgSkyCvgByMonthYear.columnNames().contains("[SKY_CVRG_CODE]"));
    }

    //
}