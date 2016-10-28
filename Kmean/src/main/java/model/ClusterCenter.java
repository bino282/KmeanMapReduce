package model;

import org.apache.hadoop.io.WritableComparable;

/**
 * Created by bineau on 28/10/2016.
 */
public class ClusterCenter implements WritableComparable<ClusterCenter> {
    private  DoubleVector center;
    private int kTimeIncremented=1;
    private int clusterIndex;

    public ClusterCenter(){
        super();
    }

    public ClusterCenter(DoubleVector center){
        super();
        this.center=center;
    }

    public DoubleVector getCenter() {
        return center;
    }

    public void setCenter(DoubleVector center) {
        this.center = center;
    }

    public int getClusterIndex() {
        return clusterIndex;
    }

    public void setClusterIndex(int clusterIndex) {
        this.clusterIndex = clusterIndex;
    }
}
