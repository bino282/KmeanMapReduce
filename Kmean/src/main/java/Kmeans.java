import model.ClusterCenter;
import model.DistanceMeasurer;
import model.DoubleVector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by bineau on 28/10/2016.
 */
public class Kmeans {
    public static class map extends Mapper<LongWritable,Text,ClusterCenter,DoubleVector>{
        private final List<ClusterCenter> centers=new ArrayList<ClusterCenter>();
        private DistanceMeasurer distanceMeasurer;

        protected void setup(Context context) throws IOException {
            Configuration conf=context.getConfiguration();
            Path pathcentroid=new Path(conf.get("Centroid.path"));
            FileSystem fs=FileSystem.get(conf);
            BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pathcentroid)));
            String line;
            int index=0;
            line=br.readLine();
            while (line!=null){
                String[] token=line.split("\\s+");
                if(token[0]!=null && token[1]!=null) {
                    double x = Double.parseDouble(token[0]);
                    double y = Double.parseDouble(token[1]);
                    ClusterCenter clusterCenter = new ClusterCenter(new DoubleVector(x, y));
                    clusterCenter.setClusterIndex(index++);
                    centers.add(clusterCenter);
                }
                line=br.readLine();
            }
            distanceMeasurer=new DistanceMeasurer();

        }

        public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException {
            ClusterCenter nearest=null;
            String[] token=value.toString().split("\\s+");
            DoubleVector vector=new DoubleVector(Double.parseDouble(token[0]),Double.parseDouble(token[1]));
            double nearestDistance=Double.MAX_VALUE;
            for (ClusterCenter c:centers){
                double dis=distanceMeasurer.measureDistance(c,vector);
                if(nearest==null){
                    nearest=c;
                    nearestDistance=dis;
                }
                else{
                    if(nearestDistance>dis){
                        nearest=c;
                        nearestDistance=dis;
                    }
                }
            }
            context.write(nearest,vector);


        }
    }

    public static class Reduce extends Reducer<ClusterCenter,DoubleVector,Text,IntWritable> {
        public static enum Counter {
            CONVERGED
        }
        private final List<ClusterCenter> centers = new ArrayList<ClusterCenter>();
        public void reduce(ClusterCenter key,Iterable<DoubleVector> values,Context context) throws IOException, InterruptedException {
            List<DoubleVector> vectorList = new ArrayList<DoubleVector>();
            double sumx=0;
            double sumy=0;
            for(DoubleVector value:values){
                vectorList.add(value);
                sumx+=value.getVector()[0];
                sumy+=value.getVector()[1];
            }
            double x=sumx*1.0/vectorList.size();
            double y=sumx*1.0/vectorList.size();
            ClusterCenter newCenter=new ClusterCenter(x,y,key.getClusterIndex());
            centers.add(newCenter);
            for (DoubleVector vector:vectorList){
                context.write(new Text(vector.getVector()[0]+" "+vector.getVector()[1]),new IntWritable(newCenter.getClusterIndex()));
            }
            if (newCenter.converged(key))
                context.getCounter(Counter.CONVERGED).increment(1);


        }
        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);
            Configuration conf = context.getConfiguration();
            Path outPath = new Path(conf.get("Centroid.path"));
            FileSystem fs = FileSystem.get(conf);
            fs.delete(outPath, true);
            FSDataOutputStream fin = fs.create(outPath);
            for(ClusterCenter center:centers) {
                fin.writeUTF(String.valueOf(center.getCenter().getVector()[0]+" "+center.getCenter().getVector()[1])+" "+center.getClusterIndex());
            }
            fin.close();
        }

    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        int iteration=1;
        Configuration conf = new Configuration();
        conf.set("num.iteration", iteration + "");
        Path in = new Path(args[0]);
        Path center = new Path(args[1]);
        conf.set("Centroid.path", center.toString());
        Path out = new Path(args[2]+"/data_out_1");
        Job job=new Job(conf);
        job.setJobName("KMeans Clustering");

        job.setMapperClass(map.class);
        job.setReducerClass(Reduce.class);
        job.setJarByClass(map.class);

        FileInputFormat.addInputPath(job, in);
//        FileSystem fs = FileSystem.get(conf);
//        if (fs.exists(out)) {
//            fs.delete(out, true);
//        }
//
//        if (fs.exists(center)) {
//            fs.delete(out, true);
//        }
//
//        if (fs.exists(in)) {
//            fs.delete(in, true);
//        }
        FileOutputFormat.setOutputPath(job, out);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.waitForCompletion(true);

//        long counter = job.getCounters().findCounter(Reduce.Counter.CONVERGED).getValue();
//        iteration++;
//        while (counter>0){
//            conf = new Configuration();
//            conf.set("centroid.path", center.toString());
//            conf.set("num.iteration", iteration + "");
//            job = new Job(conf);
//            job.setJobName("KMeans Clustering " + iteration);
//
//            job.setMapperClass(map.class);
//            job.setReducerClass(Reduce.class);
//            job.setJarByClass(map.class);
//
//            in = new Path(args[2]+"/data_out_" + (iteration - 1) + "/");
//            out = new Path(args[2]+"/output/data_out_" + iteration);
//
//            FileInputFormat.addInputPath(job, in);
////            if (fs.exists(out))
////                fs.delete(out, true);
//
//            FileOutputFormat.setOutputPath(job, out);
//            job.setInputFormatClass(TextInputFormat.class);
//            job.setOutputFormatClass(TextOutputFormat.class);
//            job.setOutputKeyClass(TextOutputFormat.class);
//            job.setOutputValueClass(IntWritable.class);
//
//            job.waitForCompletion(true);
//            iteration++;
//            counter = job.getCounters().findCounter(Reduce.Counter.CONVERGED).getValue();
//        }
    }
}
