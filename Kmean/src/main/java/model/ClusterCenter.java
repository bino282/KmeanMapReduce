package model;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

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

    public int compareTo(ClusterCenter clusterCenter) {
        return 0;
    }

    public void write(DataOutput dataOutput) throws IOException {

    }

    public void readFields(DataInput dataInput) throws IOException {

    }
}
