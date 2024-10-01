package ma.enset.Exercice1.Job2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class VentesAnneeDriver {
    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: VentesAnneeDriver <annee> <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        conf.set("annee", args[0]); // Récupérer l'année à partir des arguments

        Job job = Job.getInstance(conf, "ventes par ville pour une année donnée");
        job.setJar("/opt/hadoop/apps2");
        job.setJarByClass(VentesAnneeDriver.class);
        job.setMapperClass(VentesAnneeMapper.class);
        job.setReducerClass(VentesAnneeReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[1])); // Chemin du fichier ventes.txt
        FileOutputFormat.setOutputPath(job, new Path(args[2])); // Chemin du dossier de sortie

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
