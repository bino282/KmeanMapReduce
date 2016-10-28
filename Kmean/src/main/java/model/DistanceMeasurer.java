package model;

/**
 * Created by bineau on 28/10/2016.
 */
public class DistanceMeasurer {
    public final double measureDistance(ClusterCenter center, DoubleVector v) {
        double sum = 0;
        int length = v.getVector().length;
        for (int i = 0; i < length; i++) {
            sum += Math.abs(center.getCenter().getVector()[i]
                    - v.getVector()[i]);
        }

        return sum;
    }
}
