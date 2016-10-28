package model;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by bineau on 28/10/2016.
 */
public class DoubleVector implements WritableComparable<DoubleVector>{
    private double[] vector;
    public DoubleVector(){
        super();
    }
    public DoubleVector(double x,double y){
        super();
        this.vector=new double[]{x,y};
    }
    public void write(DataOutput out) throws IOException {
        out.writeInt(vector.length);
        for(int i=0;i<vector.length;i++){
            out.writeDouble(vector[i]);
        }
    }
    public void readFields(DataInput intput) throws IOException {
        int size=intput.readInt();
        vector=new double[size];
        for (int i=0;i<vector.length;i++){
            vector[i]=intput.readDouble();
        }
    }
    public int compareTo(DoubleVector v){
        boolean equals=true;
        for (int i=0;i<vector.length;i++){
            double d=vector[i]-v.vector[i];
            if(d!=0.0){
                return 1;
            }
        }
        return 0;
    }

    public double[] getVector() {
        return vector;
    }

    public void setVector(double[] vector) {
        this.vector = vector;
    }
}
