package ma.enset.Exercice1.Job1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ventesReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int somme = 0;
        for (IntWritable val : values) {
            somme += val.get();
        }
        context.write(key, new IntWritable(somme));
    }
}