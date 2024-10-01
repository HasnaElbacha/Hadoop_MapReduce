package ma.enset.Exercice1.Job2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class VentesAnneeMapper extends Mapper<Object, Text, Text, IntWritable> {

    private final static IntWritable prix = new IntWritable();
    private Text ville = new Text();
    private String anneeFiltre;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // Récupérer l'année à partir de la configuration
        anneeFiltre = context.getConfiguration().get("annee");
    }

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(" ");
        if (fields.length == 4) {
            String date = fields[0]; // Format de la date : YYYY-MM-DD
            String annee = date.split("-")[0]; // Extraire l'année

            // Vérifier si l'année correspond à l'année recherchée
            if (annee.equals(anneeFiltre)) {
                ville.set(fields[1]); // Ville
                prix.set(Integer.parseInt(fields[3])); // Prix
                context.write(ville, prix); // Emit (ville, prix)
            }
        }
    }
}
