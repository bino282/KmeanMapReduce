import model.ClusterCenter;
import model.DistanceMeasurer;
import model.DoubleVector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by bineau on 28/10/2016.
 */
public class Kmeans {

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
        private final List<ClusterCenter> centers = new ArrayList<ClusterCenter>();


    }
}
