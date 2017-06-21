package com.datamonkeys.phase3.MapReduce;

import org.apache.spark.api.java.function.Function2;

/**
 * Created by SMKSYosemite on 12/2/16.
 *
 * Simple reduce method that will return the number of items that correspond to each
 * key value.  In this instance the key value is a string that corresponds to a cell
 */
public class ReduceToCount implements Function2<Integer, Integer, Integer> {


    @Override
    public Integer call(Integer a, Integer b) throws Exception {
        return a + b;
    }
}
