import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.*;
import java.util.HashSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class WordCount {


    static HashSet<String> hashIDset = new HashSet<String>();
    static public boolean writeID = true;

    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text documentWord = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String riadok = value.toString();

            Pattern ISBNpattern = Pattern.compile("book.book_edition.(isbn)");
            Matcher matcher = ISBNpattern.matcher(riadok);

            Pattern BOOKpattern = Pattern.compile("book.book_edition.book");
            Matcher BOOKmatcher = BOOKpattern.matcher(riadok);


            if (matcher.find()) {
                try {
                    documentWord = new Text(getID(value, false));
                    context.write(documentWord, one);

                } catch (Exception e) {
                    System.out.println(e);
                }
            }
            else if(BOOKmatcher.find()){
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
        static final int pocetAtributov = 4;

        final String[] listAttributes = new String[pocetAtributov];
        String foundID = null;
        boolean flagBookHere = false;
        public String badID = null;
        String x = null;
        int attributeValue = 0;
        boolean notEmpty = false;
        boolean hasName = true;

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            if (writeID){
                Configuration conf = context.getConfiguration();
                File idFile = new File(conf.get("idFile"));
                FileSystem fileSystem = FileSystem.get(conf);
                Path id_file = new Path(String.valueOf(idFile));
                String currLine = "";

                try(BufferedReader br = new BufferedReader(new InputStreamReader(fileSystem.open(id_file)))){
                    while((currLine = br.readLine()) != null){
                        String[] tmpID = currLine.split("\t");
                        hashIDset.add(tmpID[0]);
                    }
                }

                writeID = false;
            }



            String ID = getID(value, false);

            if (!ID.equals(badID)) {
                if (flagBookHere && ID.equals(foundID)) {
                    attributeValue = importantAttribute(value);
                    if(attributeValue != 0){
                        String parsed = value.toString();
                        String[] splittedAtr = parsed.split("\t");

                        if(attributeValue == 1 || attributeValue == 4 || attributeValue == 8){
                            Pattern pattern = Pattern.compile("(?<=\\\")(.*?)(?=\\\")");
                            Matcher matcher = pattern.matcher(splittedAtr[2]);

                            if (matcher.find())
                                splittedAtr[2] = matcher.group(0);

                            if (attributeValue == 1){
                                Pattern httpPattern = Pattern.compile("https://");
                                Matcher httpmatcher = httpPattern.matcher(splittedAtr[2]);

                                if(splittedAtr[2].equals("\\")){ 
                                    listAttributes[0] = null;
                                }
                                else{
                                    if(httpmatcher.find())
                                        listAttributes[0] = null;
                                    else
                                        listAttributes[0] = "\"Meno\": " + "\""+splittedAtr[2]+"\""+ " <?!?>";
                                }

                            }
                            else if (attributeValue == 4)
                                listAttributes[3] = "\"ISBN\": " +"\""+splittedAtr[2]+"\""+ " <?!?>";
                            else if (attributeValue == 8)
                                listAttributes[1] = "\"Autor\": " +"\""+splittedAtr[2]+"\""+ " <?!?>";
                            notEmpty = true;

                        }else{
                            Pattern pattern = Pattern.compile("(?<=\\\")(.*?)(?=\\\")");
                            Matcher matcher = pattern.matcher(splittedAtr[2]);

                            if (matcher.find())
                                splittedAtr[2] = matcher.group(0);

                            Pattern patternLongDate = Pattern.compile("[0-9]{4}-[0-9]{2}-[0-9]{2}");
                            Matcher matcherLongDate = patternLongDate.matcher(splittedAtr[2]);

                            Pattern pattern2 = Pattern.compile("[0-9]{4}-[012]{1}");
                            Matcher matcher2 = pattern2.matcher(splittedAtr[2]);

                            Pattern patternOther = Pattern.compile("[0-9]{4}-[3|4|5|6|7|8|9]{1}");
                            Matcher matcherOther = patternOther.matcher(splittedAtr[2]);

                            if(matcherLongDate.find()){
                                splittedAtr[2] = splittedAtr[2].substring(0,4);
                            }

                            else if(matcher2.find()){
                                splittedAtr[2] = splittedAtr[2].substring(0,4) + "-20"+ splittedAtr[2].substring(5,7);
                            }

                            else if(matcherOther.find()){
                                splittedAtr[2] = splittedAtr[2].substring(0,4) + "-19"+ splittedAtr[2].substring(5,7);
                            }

                            listAttributes[2] = "\"Rok vydania\": " +"\""+splittedAtr[2]+"\""+ " <?!?>";
                            notEmpty = true;
                        }
                    }

                } else {
                    if(notEmpty){

                        if(listAttributes[0] == null){
                            hasName = false;
                        }
                        else{
                            if(listAttributes[1] == null)
                                listAttributes[1] = "\"Autor\""+":"+"\" NOT_FOUND\" <?!?>";

                            if(listAttributes[2] == null)
                                listAttributes[2] = "\"Rok vydania\""+":"+"\" NOT_FOUND\" <?!?>";

                            if(listAttributes[3] == null)
                                listAttributes[3] = "\"ISBN\""+":"+"\" NOT_FOUND\" <?!?>";

                            for(int i = 0; i < pocetAtributov; i++)
                                x = x + listAttributes[i];

                            for(int i = 0; i < pocetAtributov; i++) // vycistit pole
                                listAttributes[i] = null;

                            x = x.replaceAll("null", "");
                            x = x.replaceAll("\\\\", "");
                            x = x.replaceAll("\\\\\\\\", "");
                            context.write(new Text(x), one);
                        }
                        hasName = true;
                    }

                    attributeValue = 0;
                    x = null;
                    flagBookHere = notEmpty =false;
                    badID = ID;
                    if(hashIDset.contains(ID)){
                        flagBookHere = true;
                        foundID = ID;
                        badID = null;
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

        Pattern author = Pattern.compile("media_common\\.*creative_work\\.*credit>");
        Matcher matcherAuthor = author.matcher(stringValue);
        if(matcherAuthor.find())
            return 8;

        return 0;
    }

    public static class getLinks extends Mapper<Object, Text, Text, NullWritable> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String sender;
            String line = value.toString();
            String[] first = line.split("<\\?!\\?>");
            //first[0] = first[0].replaceAll("Meno: ","");
            sender = "{"+ first[0] + ","+first[1]+", "+first[2]+", "+first[3]+"},";
            context.write(new Text(sender), NullWritable.get());
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
        Job job = Job.getInstance(conf, "FindIDs");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);

        System.out.println("First job done");


        Configuration conf2 = new Configuration();
        conf2.set("idFile", args[4]);

        Job job2 = Job.getInstance(conf2, "FindBooks");
        job2.setJarByClass(WordCount.class);
        job2.setMapperClass(FindBooks.class);
        job2.setReducerClass(BooksReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job2, new Path(args[0]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));
        job2.waitForCompletion(true);

        System.out.println("Second job done");

        Configuration conf3 = new Configuration();
        conf3.set("parsedFile", args[5]);

        Job job3 = Job.getInstance(conf3, "getLinks");
        job3.setJarByClass(WordCount.class);
        job3.setMapperClass(getLinks.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(job3, new Path(args[5]));
        FileOutputFormat.setOutputPath(job3, new Path(args[3]));
        System.exit(job3.waitForCompletion(true) ? 0 : 1);

        System.out.println("Third job done");
    }

}


//StringTokenizer wordsFromLine = new StringTokenizer(value.toString(), "\t", false);