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
    private int clusterIndex;

    public ClusterCenter(){
        super();
    }

    public ClusterCenter(DoubleVector center){
        super();
        this.center=center;
    }
    public ClusterCenter(double x,double y,int clusterIndex){
        this.center=new DoubleVector(x,y);
        this.clusterIndex=clusterIndex;
    }
    public final boolean converged(ClusterCenter c) {
        return calculateError(c.getCenter()) > 0;
    }
    public final double calculateError(DoubleVector v) {
        return Math.sqrt(Math.abs(Math.pow(center.getVector()[0]-v.getVector()[0],2)+Math.abs(Math.pow(center.getVector()[1]-v.getVector()[1],2))));
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
