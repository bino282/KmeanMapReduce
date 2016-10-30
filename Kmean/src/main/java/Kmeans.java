import model.ClusterCenter;
import model.DistanceMeasurer;
import model.DoubleVector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
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
import java.util.ArrayList;
import java.util.List;

/**
 * Created by bineau on 28/10/2016.
 */
public class Kmeans {
    private static final Logger LOG = LogManager.getLogger(Kmeans.class);
    public static class map extends Mapper<ClusterCenter, DoubleVector,ClusterCenter,DoubleVector>{
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
                double x=Double.parseDouble(token[0]);
                double y=Double.parseDouble(token[1]);
                ClusterCenter clusterCenter=new ClusterCenter(new DoubleVector(x,y));
                clusterCenter.setClusterIndex(index++);
                centers.add(clusterCenter);
                line=br.readLine();
            }
            distanceMeasurer=new DistanceMeasurer();

        }

        public void map(ClusterCenter key,DoubleVector value,Context context) throws IOException, InterruptedException {
            ClusterCenter nearest=null;
            double nearestDistance=Double.MAX_VALUE;
            for (ClusterCenter c:centers){
                double dis=distanceMeasurer.measureDistance(c,value);
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
            context.write(nearest,value);


        }
    }

    public static class Reduce extends Reducer<ClusterCenter,DoubleVector,ClusterCenter,DoubleVector> {
        public static enum Counter {
            CONVERGED
        }
        private final List<ClusterCenter> centers = new ArrayList<ClusterCenter>();
        public void reduce(ClusterCenter key,Iterable<DoubleVector> values,Context context){
            List<DoubleVector> vectorList = new ArrayList<DoubleVector>();
            DoubleVector newcenter=null;
            for(DoubleVector value:values){
                vectorList.add(value);
                if(newcenter==null){
                    newcenter=value;
                }
            }
        }

    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        int iteration=1;
        Configuration conf = new Configuration();
        conf.set("num.iteration", iteration + "");
        Path in = new Path("path in");
        Path center = new Path("path center");
        conf.set("centroid.path", center.toString());
        Path out = new Path("path out");
        Job job=new Job(conf);
        job.setJobName("KMeans Clustering");

        job.setMapperClass(map.class);
        job.setReducerClass(Reduce.class);
        job.setJarByClass(map.class);

        FileInputFormat.addInputPath(job, in);
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(out)) {
            fs.delete(out, true);
        }

        if (fs.exists(center)) {
            fs.delete(out, true);
        }

        if (fs.exists(in)) {
            fs.delete(in, true);
        }
        FileOutputFormat.setOutputPath(job, out);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setOutputKeyClass(ClusterCenter.class);
        job.setOutputValueClass(DoubleVector.class);

        job.waitForCompletion(true);

        long counter = job.getCounters().findCounter(Reduce.Counter.CONVERGED).getValue();
        iteration++;
        while (counter>0){
            conf = new Configuration();
            conf.set("centroid.path", center.toString());
            conf.set("num.iteration", iteration + "");
            job = new Job(conf);
            job.setJobName("KMeans Clustering " + iteration);

            job.setMapperClass(map.class);
            job.setReducerClass(Reduce.class);
            job.setJarByClass(map.class);

            in = new Path("files/clustering/depth_" + (iteration - 1) + "/");
            out = new Path("files/clustering/depth_" + iteration);

            FileInputFormat.addInputPath(job, in);
            if (fs.exists(out))
                fs.delete(out, true);

            FileOutputFormat.setOutputPath(job, out);
            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);
            job.setOutputKeyClass(ClusterCenter.class);
            job.setOutputValueClass(DoubleVector.class);

            job.waitForCompletion(true);
            iteration++;
            counter = job.getCounters().findCounter(Reduce.Counter.CONVERGED).getValue();
        }
    }
}
