package com.datamonkeys.phase3.MapReduce;

import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by SMKSYosemite on 12/2/16.
 */
public class MapInputString implements PairFunction<String, String, Integer> {

    private static final int dateColumnIndex = 1;
    private static final int lonColumnIndex = 5;
    private static final int latColumnIndex = 6;



    @Override
    public Tuple2<String, Integer> call(String line) throws Exception {

        Calendar cal = Calendar.getInstance();
        SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        //Split by comma
        List<String> lineSplit = Arrays.asList(line.split(","));

        //Determine the day
        String dateString = lineSplit.get(dateColumnIndex);

        Date date = f.parse(dateString);
        cal.setTime(date);

        Integer dayOfMonth = cal.get(Calendar.DAY_OF_MONTH ) - 1;

        //Get the lat and lon as integer
        String latString = lineSplit.get(latColumnIndex);
        String lonString = lineSplit.get(lonColumnIndex);

        Double latDouble = Double.parseDouble(latString) * 100;
        Double lonDouble = Double.parseDouble(lonString) * 100;

        Integer intLat = (int) Math.round(latDouble - .5);
        Integer intLon = (int) Math.round(lonDouble - .5);

        String keyValue = intLat + "," + intLon + "," + dayOfMonth;

        return new Tuple2<>(keyValue, 1);
    }

}
