import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

public class WordCount {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{

        private final static IntWritable one = new IntWritable(1);
        private Text documentLine = new Text();
        private Text documentWord = new Text();

        public void map(Object key, Text wholeDocument, Context context) throws IOException, InterruptedException {

            StringTokenizer documentLines = new StringTokenizer(wholeDocument.toString(), "\n", false); //rozdelim file na riadky

            while (documentLines.hasMoreTokens()) { //prechadzam vsetky riadky

                documentLine.set(documentLines.nextToken()); //priradim riadok do documentLine
                StringTokenizer wordsFromLine = new StringTokenizer(documentLine.toString(), "\t", false); //rozdelim riadok na slova

                while(wordsFromLine.hasMoreTokens()){ // prechadzam kazde slovo v riadku
                    documentWord.set(wordsFromLine.nextToken()); //priradim slovo do documentWord
                    String tmp = documentWord.toString(); //z Text object menime documentWord na string aby bolo mozne porovnavat
                    if(tmp.equals("<http://rdf.freebase.com/ns/m.084x28q>")) // porovnanie...neskor by bolo potrebe priradit REGEX
                    context.write(documentLine, one); // ak dane slovo je najdene zapiseme CELY RIADOK kde sa slovo nachadza nie len slovo
                }
            }
        }
    }

    public static class IntSumReducer
            extends Reducer<Text,IntWritable,Text,IntWritable> {

    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}