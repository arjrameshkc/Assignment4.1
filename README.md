# Assignment4.1

package mapreduce;

    import org.apache.hadoop.fs.Path; 
        import org.apache.hadoop.conf.*;
        import org.apache.hadoop.mapreduce.Job;
        import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; 
        import org.apache.hadoop.mapreduce.lib.input.TextInputFormat; 
        import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat; 
        import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
    //    import org.apache.hadoop.io.IntWritable;
        import org.apache.hadoop.io.Text;
// Driver class
        public class InvaildDriver{
            @SuppressWarnings("deprecation")
            public static void main(String[] args) throws Exception {
                Configuration conf = new Configuration();
                Job job = new Job(conf, "InvaildDriver");
                job.setJarByClass(InvaildDriver.class);

                job.setMapOutputKeyClass(Text.class);
                job.setMapOutputValueClass(Text.class);

                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(Text.class);
                job.setMapperClass(InvalidMapper.class);
    //            job.setReducerClass(0);
                 
                job.setInputFormatClass(TextInputFormat.class);
                job.setOutputFormatClass(TextOutputFormat.class);

                FileInputFormat.addInputPath(job, new Path(args[0])); 
                FileOutputFormat.setOutputPath(job,new Path(args[1]));
                
                /*
                Path out=new Path(args[1]);
                out.getFileSystem(conf).delete(out);
                */
                
                job.waitForCompletion(true);
            }
        }
    


package mapreduce;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*; 

    public class InvalidMapper extends Mapper<Text, Text, Text, Text> {
        private static final Text NA = null;
        private static final Text Val = NA;
        
        public void map( Text key, Text value, Context context) 
                throws IOException, InterruptedException {
             String[] line = value.toString().split("|");
              Text CompName = new Text(line[0]);
    
        if(CompName != Val ){
            context.write(key,CompName);
          }
        }
    }
