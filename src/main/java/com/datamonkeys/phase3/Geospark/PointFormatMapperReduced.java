package com.datamonkeys.phase3.Geospark;

import com.vividsolutions.jts.geom.Point;
import org.datasyslab.geospark.formatMapper.PointFormatMapper;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

/**
 * Created by schachte on 11/30/16.
 */
public class PointFormatMapperReduced extends PointFormatMapper {

    String splitter;
    Integer columnToKeep;
    public PointFormatMapperReduced(Integer columnToKeep, Integer Offset, String Splitter) {
        super(Offset, Splitter);
        this.splitter = Splitter;
        this.columnToKeep = columnToKeep;
    }

    @Override
    public Point call(String line) throws Exception {

        Calendar cal = Calendar.getInstance();
        SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        //If the splitter is a csv we will get the user data and split by a comma
        if( this.splitter.equals("csv")) {
            Point pointToReturn = super.call(line);
            List<String> lineSplit = Arrays.asList(line.split(","));
            String dateFieldValue = lineSplit.get(columnToKeep);

            Date date = f.parse(dateFieldValue);
            cal.setTime(date);
            Integer dayOfMonth = cal.get(Calendar.DAY_OF_MONTH);

            pointToReturn.setUserData(dayOfMonth - 1);//Set the day of the week to be 0 index

            return pointToReturn;
        }
        else
            return super.call(line);

    }
}
