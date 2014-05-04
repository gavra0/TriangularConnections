package ch.epfl.data.bigdata.hw2;

import java.util.LinkedList;
import java.util.List;

/**
 * @author Ivan Gavrilović
 */
public class TriangleEntry {
    List<Integer> userIds;
    Boolean valid;

    public TriangleEntry(List<Integer> userIds, Boolean valid) {
        if (userIds == null) {
            this.userIds = new LinkedList<Integer>();
        } else {
            this.userIds = userIds;
        }
        this.valid = valid;
    }
}
