import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
public class WordCount {



    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text documentLine = new Text();
        private Text documentWord = new Text();
        public List<StringTokenizer> listName = new ArrayList<>(25);


        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            Pattern pattern = Pattern.compile("book.book_edition.(isbn)");
            String riadok = value.toString();

            Matcher matcher = pattern.matcher(riadok);

            if (matcher.find()) {
                try {
                    documentWord = new Text(getID(value, false));
                    context.write(documentWord, one);

                } catch (Exception e) {
                    System.out.println(e);
                }

            }

        }


    }

    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    }

    public static class FindBooks extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);

        public List<StringTokenizer> listName = new ArrayList<>(4);

        String foundID = null;
        boolean flagBookHere = false;
        public String badID = null;
        String x = null;
        int attributeValue = 0;
        int byteValue = 0;

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            StringTokenizer wordsFromLine = new StringTokenizer(value.toString(), "\t", false);

            Configuration conf = context.getConfiguration();
            File idFile = new File(conf.get("idFile"));

            String ID = getID(value, true);

            if (!ID.equals(badID)) {
                if (flagBookHere && ID.equals(foundID)) { //viem ze je to kniha chcem vybrane atributy
                    attributeValue = importantAttribute(value);
                    if(attributeValue != 0){
                        byteValue += attributeValue;
                        String parsed = value.toString();
                        String[] splittedAtr = parsed.split("\t");

                        if(attributeValue == 1){ //meno
                            Pattern pattern = Pattern.compile("(?<=\\\")(.*?)(?=\\\")");
                            Matcher matcher = pattern.matcher(splittedAtr[2]);
                            if (matcher.find())
                                splittedAtr[2] = matcher.group(0);
                        }else if(attributeValue == 2){ //publication date
                            Pattern pattern = Pattern.compile("(?<=\\\")(.*?)(?=\\\")");
                            Matcher matcher = pattern.matcher(splittedAtr[2]);
                            if (matcher.find())
                                splittedAtr[2] = matcher.group(0);
                        }else if(attributeValue == 4){ //ISBN
                            Pattern pattern = Pattern.compile("(?<=\\\")(.*?)(?=\\\")");
                            Matcher matcher = pattern.matcher(splittedAtr[2]);
                            if (matcher.find())
                                splittedAtr[2] = matcher.group(0);
                        }else{ //number_of_pages

                        }
                        x = x + "\t" + splittedAtr[2];
                    }

                } else {
                    if(x != null){
                        x = x.replaceAll("null", "");
                        context.write(new Text(x), one);
                    }
                    byteValue = 0;
                    attributeValue = 0;
                    x = null;
                    flagBookHere = false;
                    badID = ID;
                    try (BufferedReader br = new BufferedReader(new FileReader(idFile))) {
                        String line;
                        while ((line = br.readLine()) != null) {
                            if (line.equals(ID)) {
                                flagBookHere = true;
                                foundID = ID;
                                badID = null;
                                //context.write(new Text(wordsFromLine.nextToken()), one);
                                break;
                            }
                        }
                    } catch (Exception e) {
                        System.out.println(e);
                    }
                }
            }
        }
    }

    public static class BooksReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static String getID(Text value, boolean extraTab){
        String riadok = value.toString();
        String[] first = riadok.split("\t");
        String[] second = first[0].split("/");
        String ID = second[4].substring(0, second[4].length() - 1);

        if(extraTab)
            ID = ID + "\t1";

        return ID;
    }

    public static int importantAttribute(Text value){
        String stringValue = value.toString();
        //MENO KNIHY
        Pattern menoKnihy = Pattern.compile("type\\.*object\\.*name>");
        Matcher matcherMeno = menoKnihy.matcher(stringValue);
        if(matcherMeno.find())
            return 1;

        Pattern rokVydania = Pattern.compile("book\\.*book_edition\\.*publication_date>");
        Matcher matcherRokVydania = rokVydania.matcher(stringValue);
        if(matcherRokVydania.find())
            return 2;

        Pattern ISBN = Pattern.compile("book\\.*book_edition\\.*(ISBN)>");
        Matcher matcherISBN = ISBN.matcher(stringValue);
        if(matcherISBN.find())
            return 4;

        Pattern numOfPages = Pattern.compile("book\\.*book_edition\\.*number_of_pages>");
        Matcher matcherPages = numOfPages.matcher(stringValue);
        if(matcherPages.find())
            return 8;

        return 0;
    }

    public static class getLinks extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text documentLine = new Text();
        private Text documentWord = new Text();



        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            Configuration conf = context.getConfiguration();
            File beforeParseFile = new File(conf.get("beforeParseFile"));
            context.write(value, one);


        }


    }


    public static class LinksReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        //prvy job
        Configuration conf = new Configuration();
        //conf.set("dfs.block.size", "41943040");
        Job job = Job.getInstance(conf, "FindIDs");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        //job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);

        System.out.println("Prvy job hotovy");
        //druhy job
        Configuration conf2 = new Configuration();
        conf2.set("idFile", args[4]);

        Job job2 = Job.getInstance(conf2, "FindBooks");
        job2.setJarByClass(WordCount.class);
        job2.setMapperClass(FindBooks.class);
        //job2.setCombinerClass(BooksReducer.class); // zakomentovat mozno
        job2.setReducerClass(BooksReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job2, new Path(args[0]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));
        job2.waitForCompletion(true);

        System.out.println("Druhy job hotovy");
        //druhy job
        Configuration conf3 = new Configuration();
        conf3.set("beforeParseFile", args[5]);

        Job job3 = Job.getInstance(conf3, "getLinks");
        job3.setJarByClass(WordCount.class);
        job3.setMapperClass(getLinks.class);
        //job2.setCombinerClass(BooksReducer.class); // zakomentovat mozno
        job3.setReducerClass(LinksReducer.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job3, new Path(args[0]));
        FileOutputFormat.setOutputPath(job3, new Path(args[3]));
        System.exit(job3.waitForCompletion(true) ? 0 : 1);

        System.out.println("Treti job hotovy");
    }

}


//StringTokenizer wordsFromLine = new StringTokenizer(value.toString(), "\t", false);
