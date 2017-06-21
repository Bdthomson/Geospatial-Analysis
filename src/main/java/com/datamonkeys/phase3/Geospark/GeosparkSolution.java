package com.datamonkeys.phase3.Geospark;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Point;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.datasyslab.geospark.spatialOperator.JoinQuery;
import org.datasyslab.geospark.spatialRDD.PointRDD;
import org.datasyslab.geospark.spatialRDD.RectangleRDD;

import java.util.*;

/**
 * Created by SMKSYosemite on 12/4/16.
 *
 * This class is our solution to the taxi cab hot spot problem using GeoSpark.  It should be noted
 * that while our answers are close, they are not perfect.  Quickly seeing that the MapReduce implementation
 * was much faster and more optimized, we quickly put more resources into perfecting that implementation.
 */
public class GeosparkSolution {

    private double STEP_SIZE = .01;
    private double MIN_LAT = 40.5;
    private double MAX_LAT = 40.9;
    private double MIN_LON = -74.25;
    private double MAX_LON = -73.7;
    private int NUMBER_OF_PARTITIONS = 100;
    private JavaSparkContext sc;
    private HashMap<String, Integer> pickupCountMap;

    //The Geospark solution needs the spark context because we create another RectangleRDD
    public GeosparkSolution(JavaSparkContext sc) { this.sc = sc; }


    public HashMap<String, Integer> getPickupCountMap(JavaRDD<String> rawInputStrings)
    {
        pickupCountMap = new HashMap<>();

        System.out.println("Loading the rectangleRDD");
        RectangleRDD rectangleRDD = createRectangleRDD();

        System.out.println("Creating the Java<Point> using a custom mapper to reduce the userData size.");
        JavaRDD<Point> rawPointRDD = rawInputStrings.map(new PointFormatMapperReduced(1, 5, "csv"));

        System.out.println("Performing the join using the rawPointRDD and rectangles");
        joinThenPartitionByDay(rawPointRDD, rectangleRDD);

        return pickupCountMap;
    }

    // Create the RectangleRDD that contains all of the possible cells for the range that we are looking at
    private RectangleRDD createRectangleRDD()
    {
        List<Envelope> envs = new ArrayList<>();

        for( int longitude = 0; longitude < (MAX_LON - MIN_LON) / STEP_SIZE; longitude++ )
        {
            for( int latitude = 0; latitude < (MAX_LAT - MIN_LAT) / STEP_SIZE; latitude++ )
            {
                double min_long 	= MIN_LON+ STEP_SIZE * longitude;
                double min_lat		= MIN_LAT + STEP_SIZE * latitude;
                double max_long	    = MIN_LON + STEP_SIZE * (longitude + 1);
                double max_lat		= MIN_LAT + STEP_SIZE * (latitude + 1);
                envs.add( new Envelope( min_long, max_long, min_lat, max_lat ) );
            }
        }
        JavaRDD<Envelope> rddEnvelope = this.sc.parallelize(envs, NUMBER_OF_PARTITIONS);
        return new RectangleRDD(rddEnvelope, "rtree");
    }

    //This method will take all of the points given and join them with the rectangleRDD given.  After the join
    //      is performed the joinResults will be passed through a mapper that will return the number of points
    //      in that envelope for each day.  Once we have that we will increment all of the necesarry counters
    //      in the hash map that holds the counts for each cell
    private void joinThenPartitionByDay(JavaRDD<Point> rawPointRDD, RectangleRDD rectangleRDD )
    {
        PointRDD pointRDD = new PointRDD(rawPointRDD, "rtree", NUMBER_OF_PARTITIONS);
        JoinQuery joinQuery = new JoinQuery(sc, pointRDD, rectangleRDD);
        pointRDD.buildIndex("rtree");

        System.out.println("Performing join query");
        JavaPairRDD<Envelope, HashSet<Point>> joinResults = joinQuery.SpatialJoinQueryUsingIndex(pointRDD, rectangleRDD);

        System.out.println("Mapping the joinresults to return int array");
        JavaPairRDD<String, int[]> envelopeToArrayMap = joinResults.mapToPair(new PartitionByDayMapper());

        System.out.println("Collecting as map");
        Map<String, int[]> mapOfEnvelopesToInt = envelopeToArrayMap.collectAsMap();
        System.out.println("Collected as map");

        //Get an Envelope, int[].  Loop through each value in the int[] and increment the counter
        //      for the cell corresponding to the envelope for the given day.  The day is determined
        //      by the location in the int[]
        System.out.println("Iterating through the map");
        int i = 0;
        for( Map.Entry<String, int[]> entry : mapOfEnvelopesToInt.entrySet() )
        {
            i++;
            if( i % 1000 == 0)
                System.out.println("Iterated through " + i + " values in the map");


            String envKey = entry.getKey();
            int[] countArray = entry.getValue();

            for(int day = 0; day < 31; day++) {
                incrementCellValueInHashMap( envKey, day, countArray[day]);

            }
        }
        System.out.println("Finished iterating through the map");
    }


    //This method is passed the cell information and the number of new points that we've found in
    //  the cell.  It will get the current count of the cell and increment that value
    private void incrementCellValueInHashMap( String envKey, int day, int count)
    {
        //String key = getKey(lat, lon, day);
        String key = envKey + "," + day;

        Integer currentValue = pickupCountMap.get(key);
        if(currentValue == null )
            currentValue = 0;
        else
            System.out.println("Found a Duplicate");

        currentValue += count;

        pickupCountMap.put(key, currentValue);
        //System.out.println("New value of " + key + " is " + currentValue);
    }

}
