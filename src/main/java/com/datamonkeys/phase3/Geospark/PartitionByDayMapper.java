package com.datamonkeys.phase3.Geospark;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Point;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.Serializable;
import java.util.*;


/**
 * Created by SMKSYosemite on 11/30/16.
 *
 * This mapper is used to map the return values from a spatial join query from holding the Envelope and a set of points
 * to just map the Envelope to an array of integers.  This array corresponds to the number of points found on that day.
 * if the outputArray[0] = 5 that means that on January 1st there were 5 points found in the envelope for that day.
 */
public class PartitionByDayMapper implements Serializable, PairFunction<Tuple2<Envelope, HashSet<Point>>, String, int[]>
{
    @Override
    public Tuple2<String, int[]> call(Tuple2<Envelope, HashSet<Point>> envelopeHashSetTuple2) throws Exception
    {


        int[] outputArray = new int[31];

        //Iterate all of the Points in the hashset and increment the counter of the day that this point corresponds to
        for (Point point : envelopeHashSetTuple2._2)
        {
            Integer dayOfMonthToIncrement = (Integer) point.getUserData();
            outputArray[dayOfMonthToIncrement]++;
        }

        Envelope env = envelopeHashSetTuple2._1;

        return new Tuple2<>(getKey(env), outputArray);
    }

    private String getKey(Envelope env)
    {
        Double lat = env.getMinY() * 100;
        Double lon = env.getMinX() * 100;

        Integer intLat = (int) Math.round(lat);
        Integer intLon = (int) Math.round(lon);


        return intLat + "," + intLon;
    }
}
