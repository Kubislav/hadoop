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
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class WordCount {



    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{

        private final static IntWritable one = new IntWritable(1);
        private Text documentLine = new Text();
        private Text documentWord = new Text();

        public Hashtable<String, Integer> ID_table = new Hashtable<String, Integer>();
        public List <StringTokenizer> listName = new ArrayList<>(20);

        public String tmp = null;
        public String[] longTmp = null;
        public String tmpID = null;
        public String longTmpID = null;

        public int flagMatchGetInfoBelow = 0;

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {


            StringTokenizer wordsFromLine = new StringTokenizer(value.toString(), "\t", false);

            if (flagMatchGetInfoBelow == 1){
                tmp = wordsFromLine.nextToken();
                longTmp = tmp.split("/");
                tmpID = longTmp[4].substring(0, longTmp[4].length() - 1);

                if(!tmpID.equals(longTmpID)){
                    flagMatchGetInfoBelow = 0;
                }else{
                    documentWord = new Text(wordsFromLine.toString());
                    context.write(documentWord, one);
                }

            }else
            {
                if(listName.size() == 20){
                    listName.remove(0);
                }
                listName.add(wordsFromLine);


                Pattern pattern = Pattern.compile("book.book_edition.(isbn)");
                String riadok = value.toString();

                Matcher matcher = pattern.matcher(riadok);

                if(matcher.find()){
                    String[] first = riadok.split("\t");
                    String[] second = first[0].split("/");
                    String ID = second[4].substring(0, second[4].length() - 1);
                    //pridat vsetko nadtym

                    for(int i = 0; i < listName.size(); i++){
                        tmp = listName.get(i).nextToken();
                        longTmp = tmp.split("/");
                        tmpID = longTmp[4].substring(0, longTmp[4].length() - 1);

                        if (tmpID.equals(ID)){
                            documentWord = new Text(wordsFromLine.toString());
                            context.write(documentWord, one);

                        }

                    }
                    longTmpID = tmpID;
                    flagMatchGetInfoBelow = 1;
                }
            }
        }


    }

    public static class printer{
        public void printerMethod(){
            System.out.println("GET FUCKED !");
        }
    }

    public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {

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