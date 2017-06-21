package com.datamonkeys.phase3.MapReduce;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by SMKSYosemite on 12/3/16.
 *
 * The getPickupCountMap method will take in a JavaRDD<String> and will return the HashMap that will be used for
 * calculating the zscores of the cells.
 */

public class MapReduceSolution {

    private double STEP_SIZE = .01;
    private double MIN_LAT = 40.5;
    private double MAX_LAT = 40.9;
    private double MIN_LON = -74.25;
    private double MAX_LON = -73.7;

    public HashMap<String, Integer> getPickupCountMap(JavaRDD<String> rawInputStrings)
    {
        HashMap<String, Integer> pickupCountMap = new HashMap<>();

        System.out.println("Initializing the hashMap");
        initializeHashMap(pickupCountMap);

        System.out.println("Loading the JavaRDD<Point>");

        System.out.println("Starting mapper");
        JavaPairRDD<String, Integer> mappedValues = rawInputStrings.mapToPair(new MapInputString());

        System.out.println("Starting reducer");
        JavaPairRDD<String, Integer> reducedValues = mappedValues.reduceByKey(new ReduceToCount());

        System.out.println("Collecting as map");
        Map<String, Integer> resultMap = reducedValues.collectAsMap();

        int numberOfPointsSeen = 0;
        for( Map.Entry<String, Integer> entry : resultMap.entrySet() )
        {
            String key = entry.getKey();
            Integer numberOfPoints = entry.getValue();

            //If the key already has a value in the hashmap (it will be 0) then put the value
            //This is to make sure that outlyer points don't get put in the map
            if( pickupCountMap.get(key) != null )
                pickupCountMap.put(key, numberOfPoints);

            numberOfPointsSeen += numberOfPoints;
        }

        System.out.println("number of points seen after MapReduce: " + numberOfPointsSeen);
        //Get the top 50 cells based on GValue

        return pickupCountMap;
    }

    // Create the RectangleRDD that contains all of the possible cells for the range that we are looking at
    private void initializeHashMap( HashMap<String, Integer> pickupCountMap)
    {
        int count = 0;
        double lon_boundary = Math.round(((MAX_LON - MIN_LON) / STEP_SIZE));
        double lat_boundary = Math.round(((MAX_LAT - MIN_LAT) / STEP_SIZE));

        for( int longitude = 0; longitude < lon_boundary; longitude++ )
        {
            for( int latitude = 0; latitude < lat_boundary; latitude++ )
            {
                Double min_lat		= (MIN_LAT + STEP_SIZE * latitude) * 100;
                Double min_long 	= (MIN_LON + STEP_SIZE * longitude) * 100;

                Integer intLat = (int) Math.round(min_lat);
                Integer intLon = (int) Math.round(min_long);

                String envKey = intLat + "," + intLon;

                for( int day = 0; day < 31; day++ )
                {
                    count++;
                    String key = envKey + "," + day;
                    pickupCountMap.put(key, 0);
                }
            }
        }
        System.out.println("Initialized " + count + " values in the hashmap");
    }
}
