package ma.enset.Exercice2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class LogMapper extends Mapper<Object, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text ip = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // Diviser la ligne en parties en utilisant un espace comme séparateur
        String[] fields = value.toString().split(" ");

        if (fields.length > 6) {
            String ipAddress = fields[0]; // Adresse IP est le premier champ
            String httpCode = fields[fields.length - 2]; // Code HTTP est l'avant-dernier champ

            ip.set(ipAddress);

            // Emettre la paire (IP, 1) pour compter toutes les requêtes
            context.write(new Text(ipAddress + "_TOTAL"), one);

            // Emettre la paire (IP, 1) pour compter les requêtes avec code 200
            if (httpCode.equals("200")) {
                context.write(new Text(ipAddress + "_SUCCESS"), one);
            }
        }
    }
}
