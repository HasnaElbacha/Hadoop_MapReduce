package ma.enset.Exercice1.Job2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class VentesAnneeReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int totalVentes = 0;
        for (IntWritable val : values) {
            totalVentes += val.get();
        }
        context.write(key, new IntWritable(totalVentes)); // Emit (ville, totalVentes)
    }
}
