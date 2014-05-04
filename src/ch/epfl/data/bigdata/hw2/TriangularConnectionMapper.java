package ch.epfl.data.bigdata.hw2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.*;

/**
 * @author Ivan Gavrilović
 */
public class TriangularConnectionMapper extends Mapper<LongWritable, Text, IntWritable, IntArrayWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        Map<Integer, List<IntWritable>> fileUserIds = new HashMap<Integer, List<IntWritable>>();
        int lineId = 0;
        StringTokenizer textTokenizer = new StringTokenizer(value.toString());
        // this will read line by line
        while (textTokenizer.hasMoreTokens()) {
            String line = textTokenizer.nextToken();

            StringTokenizer lineTokenizer = new StringTokenizer(line, ", ");
            List<IntWritable> usersInLine = new ArrayList<IntWritable>();

            while (lineTokenizer.hasMoreTokens()) {
                usersInLine.add(new IntWritable(Integer.parseInt(lineTokenizer.nextToken())));
            }

            fileUserIds.put(lineId++, usersInLine);
        }

        // iterate read map
        List<IntWritable> singleLine;
        for (Integer mapEntry : fileUserIds.keySet()) {
            singleLine = fileUserIds.get(mapEntry);
            if (singleLine.size() > 1) {
                for (int i = 1; i < singleLine.size(); i++) {
                    IntWritable baseUser = singleLine.get(0);
                    IntWritable iFriend = singleLine.get(i);

                    IntWritable[] friendArray = new IntWritable[2];
                    friendArray[0] = baseUser;
                    friendArray[1] = baseUser;
                    IntArrayWritable outputVal = new IntArrayWritable();
                    outputVal.set(friendArray);

                    // output x, (f_i, f_i) just to mark friend relationship, if base is smaller
                    if (iFriend.get() < baseUser.get()) {
                        context.write(iFriend, outputVal);
                    }

                    for (int j = i + 1; j < singleLine.size(); j++) {
                        IntWritable jFriend = singleLine.get(j);
                        friendArray[0] = baseUser;
                        if (iFriend.get() < jFriend.get() && iFriend.get() < baseUser.get()) {
                            // output f_i, (x, f_j), if base is the smallest
                            friendArray[1] = jFriend;
                            outputVal.set(friendArray);
                            context.write(iFriend, outputVal);
                        }
                        if (jFriend.get() < iFriend.get() && jFriend.get() < baseUser.get()) {
                            friendArray[1] = iFriend;
                            outputVal.set(friendArray);
                            context.write(jFriend, outputVal);
                        }
                    }
                }
            }
        }
    }
}
