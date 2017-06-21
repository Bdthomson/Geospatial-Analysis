package com.datamonkeys.phase3;

/**
 *  Modified GISCup Spark TaxiCab Implementation
 *

/* Import Statements */

import com.datamonkeys.phase3.Geospark.GeosparkSolution;
import com.datamonkeys.phase3.MapReduce.MapReduceSolution;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;

import org.apache.spark.api.java.JavaSparkContext;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;


public class Driver {

    private JavaSparkContext sc;
    private String pointLoc;
    private HashMap<String, Integer> pickupCountMap;
    private int NUMBER_OF_PARTITIONS = 100;
    private int TAKE_SIZE = 40000;
    private int QUEUE_SIZE = 50;
    private double numberOfRectangles = 68200;
    private double numberOfPointsUsed = 0; //Note this will only take into account those that fall within a valid envelope
    public double SValue = 0;
    private double STEP_SIZE = .01;
    private double XBar;

    long startTime;

    public static void main(String[] args)
    {
        if( args.length < 2) {
            System.out.println("Not enough parameters given, please give the input and output locations.");
            return;
        }


        SparkConf conf = new SparkConf();
        JavaSparkContext sc = new JavaSparkContext(conf);

        String master = conf.get("spark.master");

        if(master.equalsIgnoreCase("local")){
            debug(sc);
        }

        sc.setLogLevel("WARN");
        try {
            Driver driver = new Driver(sc, args[0], args[1], "MapReduce"); //Run the program using the MapReduce method.
        } catch (java.text.ParseException e) {
            e.printStackTrace();
        } finally {
            System.out.println("Stopping the job");
            sc.stop();
        }
    }

    private static void debug(JavaSparkContext sc)
    {
        sc.setCheckpointDir(("/tmp/spark/"));
        System.out.println("------Attach Debugger----------");
        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     *
     * @param sc The JavaSparkContext that is used to utilize the spark cluster
     * @param inputLoc The HDFS location of the input points
     * @param outputLoc The location in the local file system where the file should be written
     * @param implementationType Either 'MapReduce' or 'GeoSpark'
     * @throws java.text.ParseException
     *
     * NOTE: There are two implementation types but the GeoSpark implementation was not perfected due to
     *       lack of time. Also, the MapReduce method was determined to be a much more efficient solution
     *       both in runtime, memory usage, and CPU usage and therefore more time was spent perfecting it.
     *       The GeoSpark implementation remains to show where many, many fine hours of our work was spent.
     */
    public Driver(JavaSparkContext sc, String inputLoc, String outputLoc, String implementationType) throws java.text.ParseException
    {
        this.sc = sc;
        this.pointLoc = inputLoc;
        this.pickupCountMap = new HashMap<>();

        startTime = System.currentTimeMillis();


        System.out.println("Loading the JavaRDD<String>");
        JavaRDD<String> rawStrings = sc.textFile(pointLoc, NUMBER_OF_PARTITIONS);
        numberOfPointsUsed = rawStrings.count();


        if( implementationType.equals("MapReduce"))
        {
            //Get pickupCountMap using the MapReduceSolution
            MapReduceSolution MRSol = new MapReduceSolution();
            pickupCountMap = MRSol.getPickupCountMap(rawStrings);
        }
        else
        {
            //Get the pickupCountMap using the GeosparkSolution
            GeosparkSolution GSSol = new GeosparkSolution(sc);
            pickupCountMap = GSSol.getPickupCountMap(rawStrings);
        }


        System.out.println("Getting Top 50 G Values");
        ArrayList<String> top50Cells = findTopGValues();

        System.out.println("Writing the top 50 cells to file.");
        writeContentsToOutputFile(top50Cells, outputLoc);

        System.out.println("wholeTimeToExecute:" + ( System.currentTimeMillis() - startTime ) );

        //Printing the contents of the pickupCountMapper
        int i = 0;
        for( Map.Entry<String, Integer> entry : pickupCountMap.entrySet() )
        {
            //System.out.println(entry.getKey() + "," + entry.getValue());
            i += entry.getValue();
        }

        System.out.println("Total number of points: " + i);
        System.out.println("Total execution time:" + (System.currentTimeMillis() - startTime));
    }


    //Returns the S value that is used to calculate the g value
    private void calculateSValue()
    {
        double totalPoints = 0;
        double SValueInter = 0;

        for( Map.Entry<String, Integer> entry : pickupCountMap.entrySet() )
        {
            double count = entry.getValue().doubleValue();
            totalPoints += count;
            SValueInter += ( count * count );
        }

        this.XBar = totalPoints / numberOfRectangles;

        //Set the global values that we'll use the calculate the G value
        this.SValue = ( SValueInter / numberOfRectangles ) - (XBar * XBar);
        this.SValue = Math.sqrt(SValue);
        this.numberOfPointsUsed = totalPoints; //Set the total number of points
    }


    //This method will be called after we determine the number of points in each cell and will calculate
    //      and return the GValue of a given cell.
    private double getGValue(int lat, int lon, int day)
    {
        int numberOfNeighbors = 0;
        double numberOfNeighborPoints = 0;
        int tempDay = day-1;

        //Loop through each plane 3 times, on each plane we get the 9 cells around the middle
        //  and determine if each is a neighbor
        for(int i = 1; i <= 3; i++)
        {
            Integer[] tempNeighbors = new Integer[10];
            //Top row
            Integer stepSize = (int)(STEP_SIZE * 100);

            tempNeighbors[1] = pickupCountMap.get(getKey(lat + stepSize, lon - stepSize, tempDay));
            tempNeighbors[2] = pickupCountMap.get(getKey(lat + stepSize, lon, tempDay));
            tempNeighbors[3] = pickupCountMap.get(getKey(lat + stepSize, lon + stepSize, tempDay));

            //Middle row
            tempNeighbors[4] = pickupCountMap.get(getKey(lat, lon - stepSize, tempDay));
            tempNeighbors[5] = pickupCountMap.get(getKey(lat, lon, tempDay));
            tempNeighbors[6] = pickupCountMap.get(getKey(lat, lon + stepSize, tempDay));

            //Bottom row
            tempNeighbors[7] = pickupCountMap.get(getKey(lat - stepSize, lon - stepSize, tempDay));
            tempNeighbors[8] = pickupCountMap.get(getKey(lat - stepSize, lon, tempDay));
            tempNeighbors[9] = pickupCountMap.get(getKey(lat - stepSize, lon + stepSize, tempDay));

            for(int j = 1; j <= 9; j++)
            {
                if( tempNeighbors[j] != null)
                {
                    numberOfNeighbors++;
                    numberOfNeighborPoints += tempNeighbors[j];
                }
            }
            tempDay++;
        }

        //Calculate G Value using the given equation
        double avgPointsPerCell = numberOfPointsUsed / (numberOfRectangles);
        double GStarValueTop = numberOfNeighborPoints - avgPointsPerCell * numberOfNeighbors;
        double GStarValueBottom = numberOfPointsUsed * numberOfNeighbors - ( numberOfNeighbors * numberOfNeighbors);
        GStarValueBottom = GStarValueBottom / (numberOfPointsUsed - 1);
        double GStarValue = GStarValueTop / (SValue * Math.sqrt(GStarValueBottom));

        return GStarValue;
    }

    //Returns the key in the HashMap
    private String getKey(Integer lat, Integer lon, int day)
    {
        return lat.toString() + "," + lon.toString() + "," + day;
    }


    //This method will return a list of the top 50 cells based on the G value
    private ArrayList<String> findTopGValues()
    {
        PriorityQueue<Cell> queue = new PriorityQueue<>(50);
        calculateSValue(); //Calculates the SValue and sets the member variable for it

        //Loop through each envelope and calculate the Gvalue and add it to the priorityQueue
        for( Map.Entry<String, Integer> entry : pickupCountMap.entrySet() )
        {
            //if( entry.getValue() > 0 )
            //System.out.println("Found non zero");

            Cell newCell = new Cell();

            String[] values = entry.getKey().split(",");
            newCell.setLat( Integer.parseInt( values[0] ) );
            newCell.setLon( Integer.parseInt( values[1] ) );
            newCell.setDay( Integer.parseInt( values[2] ) );
            double thisGValue = getGValue( newCell.getLat(), newCell.getLon(), newCell.getDay() );
//            System.out.println("GValue: " + thisGValue);
            newCell.setGValue(thisGValue);

            if (!Double.isNaN(newCell.getGValue())) {
                if (queue.size() >= QUEUE_SIZE) {
                    Cell lowestCell = queue.peek();
                    if (lowestCell.compareTo(newCell) < 1) {
                        queue.poll();
                        queue.add(newCell);
                    }
                } else {
                    queue.add(newCell);
                }
            }
        }

        ArrayList<String> top50Cells = new ArrayList<>(QUEUE_SIZE);

        while( !queue.isEmpty() )
        {
            Cell poppedCell = queue.poll();
            int displayLon = poppedCell.getLon();
            int displayLat = poppedCell.getLat();
            String csvString = displayLon + "," + displayLat + "," + poppedCell.getDay() + "," + poppedCell.getGValue();

            top50Cells.add(csvString);
        }

        return top50Cells;
    }


    private void writeContentsToOutputFile( ArrayList<String> top50Cells, String outputLoc)
    {
        try {
            File outputFile = new File(outputLoc);
            if( !outputFile.exists() )
                outputFile.createNewFile();

            FileWriter writer = new FileWriter(outputLoc);
            BufferedWriter buffWriter = new BufferedWriter(writer);

            for( int i = top50Cells.size() - 1; i >= 0; i-- )
            {
                System.out.println(top50Cells.get(i));
                buffWriter.write(top50Cells.get(i) + '\n');
            }

            buffWriter.close();

        } catch (IOException e) { e.printStackTrace(); }
    }
}
