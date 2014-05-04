package ch.epfl.data.bigdata.hw2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * @author Ivan Gavrilović
 */
public class TriangularConnectionsReducer extends Reducer<IntWritable, IntArrayWritable, IntWritable, IntWritable> {
    @Override
    protected void reduce(IntWritable key, Iterable<IntArrayWritable> values, Context context) throws IOException, InterruptedException {
        Map<Integer, TriangleEntry> entries = new HashMap<Integer, TriangleEntry>();
        while (values.iterator().hasNext()) {
            IntArrayWritable singleVal = values.iterator().next();
            IntWritable[] userIds = new IntWritable[2];
            int ind = 0;
            for (Writable writable : singleVal.get()) {
                userIds[ind++] = (IntWritable) writable;
            }

            int fst = userIds[0].get();
            int snd = userIds[1].get();
            // direct friends
            if (fst == snd) {
                if (entries.containsKey(fst)) {
                    TriangleEntry triangleEntry = entries.get(fst);
                    triangleEntry.valid = Boolean.TRUE;
                    entries.put(fst, triangleEntry);
                } else {
                    entries.put(fst, new TriangleEntry(null, Boolean.TRUE));
                }
            }
            // possible triangle
            else {
                if (entries.containsKey(snd)) {
                    TriangleEntry triangleEntry = entries.get(snd);
                    triangleEntry.userIds.add(fst);
                    entries.put(snd, triangleEntry);
                } else {
                    List<Integer> fstToAdd = new LinkedList<Integer>();
                    fstToAdd.add(fst);
                    entries.put(snd, new TriangleEntry(fstToAdd, Boolean.FALSE));
                }
            }
        }
        int numTriangular = 0;
        for (Integer mapKey : entries.keySet()) {
            TriangleEntry mapValue = entries.get(mapKey);
            if (mapValue.valid) {
                for (Integer triangleVertex : mapValue.userIds) {
                    // count only if this user has the min id among the three, so no duplicates
                    if (key.get() < mapKey && key.get() < triangleVertex && mapKey < triangleVertex) {
                        numTriangular++;
                    }
                }
            }
        }
        context.write(key, new IntWritable(numTriangular));
    }
}
