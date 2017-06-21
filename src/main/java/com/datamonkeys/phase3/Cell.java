package com.datamonkeys.phase3;

/**
 * Created by schachte on 11/25/16.
 */
public class Cell implements Comparable<Cell> {
    private int lat;
    private int lon;
    private int day;
    private double gValue;

    public Cell(int lat, int lon, int day, double gValue)
    {
        this.lat = lat;
        this.lon = lon;
        this.day = day;
        this.gValue = gValue;
    }

    public Cell() {}

    public int getLat() { return lat; }
    public int getLon() { return lon; }
    public int getDay() { return day; }
    public double getGValue() { return gValue; }

    public void setLat(int lat) { this.lat = lat; }
    public void setLon(int lon) { this.lon = lon; }
    public void setDay(int day) { this.day = day; }
    public void setGValue(double gValue) { this.gValue = gValue; }



    //Will return whether the gValue of this is greater than the gValue of the cell passed in
    @Override
    public int compareTo(Cell other) {
        return Double.valueOf(gValue).compareTo(other.gValue);
    }

}
