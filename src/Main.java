import com.google.gson.Gson;
import org.apache.avro.generic.GenericData;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import java.io.*;
import java.util.ArrayList;
import java.util.List;


import static java.lang.System.exit;

public class Main {

    public static void main(String[] args) throws IOException {
        String SRC_PATH = "D:\\Roli\\freebase-head-1000000\\freebase-head-1000000";

        File dir = new File("D:\\RES2");
        PrintWriter pw = null;
        try {
             pw = new PrintWriter("Filtered");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        if(pw == null) {
            System.out.println("Log: output file could not be opened");
            exit(0);
        }

        SparkConf conf = new SparkConf().setAppName("VINF").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> fb = sc.textFile(SRC_PATH);

        //keep only the lines that have an attribute we are interested in
        JavaRDD<String> filtered_fb = fb.filter(x -> x.contains("http://rdf.freebase.com/ns/book.book.genre") | x.contains("http://rdf.freebase.com/ns/common.topic.alias") |
                                                     x.contains("http://rdf.freebase.com/ns/book.book.characters") | x.contains("http://rdf.freebase.com/ns/type.object.type") |
                                                     x.contains("http://rdf.freebase.com/ns/type.object.name"))
                                        .map(s -> s.replaceAll("(<http:\\/\\/rdf\\.freebase\\.com\\/ns\\/)|(>)|(\\.$)",""));
        //filter out machine ids for books
        JavaRDD<String> ids = filtered_fb.filter(x -> x.contains("http://rdf.freebase.com/ns/type.object.type"))
                                         .filter(x -> x.contains("http://rdf.freebase.com/ns/book.book"))
                                         .map(s -> s = s.replaceAll("(.*)(g.1[0-9a-np-z][0-9a-np-z_]{6,8})(.*)","$2"));



        List<String> BooksList  = new ArrayList<String>();

        Gson g = new Gson();
        BooksList.add(g.toJson(new Book("The great Gatsby")));
        JavaRDD<String> books = sc.parallelize(BooksList);
        books.saveAsTextFile("res");

        filtered_fb.saveAsTextFile("D:\\RES2");

        String[] partNames = dir.list();
        for (String fileName : partNames) {
            System.out.println("Reading from " + fileName);

            //if its not a part file, skip
            if(!fileName.matches("^part?(.*)")) {
                System.out.println("Skipping");
                continue;
            }

            File f = new File(dir, fileName);

            BufferedReader br = new BufferedReader(new FileReader(f));

            // Read from current file
            String line = br.readLine();
            while (line != null) {

                // write to the output file
                pw.println(line);
                line = br.readLine();
            }
            pw.flush();
        }
    }
}
