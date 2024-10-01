package ma.enset.Exercice2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class LogAnalysisDriver {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: LogAnalysisDriver <input path> <output path>");
            System.exit(-1);
        }

        Job job = Job.getInstance(new Configuration(), "log analysis");
        job.setJar("/opt/hadoop/apps3");
        job.setJarByClass(LogAnalysisDriver.class);
        job.setMapperClass(LogMapper.class);
        job.setReducerClass(LogReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0])); // Chemin des fichiers de logs
        FileOutputFormat.setOutputPath(job, new Path(args[1])); // Chemin de sortie

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
