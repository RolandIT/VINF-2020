import com.google.gson.Gson;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.*;
import java.util.ArrayList;
import java.util.List;


import static java.lang.System.exit;

public class Main {

    public static void main(String[] args) throws IOException {
        //Path to the input data
        String SRC_PATH = "D:\\Roli\\freebase-head-10000000\\freebase-head-10000000";
        Gson g = new Gson();

        //local spark configuration
        SparkConf conf = new SparkConf().setAppName("VINF").setMaster("local").set("spark.executor.memory", "8g")
                .set("spark.driver.memory", "8g");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> fb = sc.textFile(SRC_PATH);

        //keep only the lines that have an attribute we are interested in, get rid of junk from each line
        JavaRDD<String> filtered_fb = fb.filter(x -> x.contains("http://rdf.freebase.com/ns/book.book.genre") | x.contains("http://rdf.freebase.com/ns/common.topic.alias") |
                                                     x.contains("http://rdf.freebase.com/ns/book.book.characters") | x.contains("http://rdf.freebase.com/ns/type.object.type") |
                                                     x.contains("http://rdf.freebase.com/ns/type.object.name"))
                                        .map(s -> s.replaceAll("(<http:\\/\\/rdf\\.freebase\\.com\\/ns\\/)|(>)|(\\.$)",""));



        //filter out machine ids for books
        JavaRDD<String> ids = filtered_fb.filter(x -> x.contains("type.object.type"))
                                         .filter(x -> x.contains("book.book\t"))
                                         .map(s -> s = s.replaceAll("(.*)(g.1[0-9a-np-z][0-9a-np-z_]{6,8})(.*)","$2"))
                                         .map(s -> s = s.replaceAll("(.*)(m.0[0-9a-z_]{2,7}|m.01[0123][0-9a-z_]{5})(.*)","$2"));

        JavaRDD<Object> fbd = filtered_fb.map(line -> {
                                                String parts [] = line.split("\t");
                                                Object o = new Object();
                                                o.setObject(parts[0]);
                                                o.setRelationship(parts[1]);
                                                o.setSubject(parts[2]);
                                                return o;
                                            });
        //SPARK SQL
        SparkSession spark = SparkSession
                .builder()
                .appName("VINF")
                .config("spark.some.config.option", "some-value")
                .getOrCreate();

        Dataset<Row> DF = spark.createDataFrame(fbd, Object.class);


        ids.saveAsTextFile("D:\\RES");
        IOutils.combinePartFiles("D:\\RES","Filtered");

        DF.persist();
        //JavaRDD<String> parsedBooks = sc.emptyRDD();
        BufferedReader reader;
        try {
            reader = new BufferedReader(new FileReader(
                    "filtered"));
            String line = reader.readLine();
            while (line != null) {
                //System.out.println(line);
                Book b = new Book();
                List<Row> list = DF.filter("Object ='"+line+"'").collectAsList();
                for(Row l : list){
                    if(l.get(1).equals("type.object.name"))
                        b.setName(l.get(2).toString());
                    else if(l.get(1).equals("book.book.genre"))
                        b.setGenre(l.get(2).toString());
                    else if(l.get(1).equals("common.topic.alias"))
                        b.setAlias(l.get(2).toString());
                    else if(l.get(1).equals("book.book.characters"))
                        b.setCharacters(l.get(2).toString());
                }
                IOutils.writeLineToFile("Parsed",g.toJson(b).toString());
                line = reader.readLine();
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
