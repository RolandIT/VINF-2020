import com.google.gson.Gson;
import org.json.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.*;

public class Main {

    public static void main(String[] args) throws IOException {
        //Path to the input data
        String SRC_PATH = args[1];//"D:\\Roli\\freebase-head-1000000\\freebase-head-1000000";


        System.out.println(args[0]);
        if(args[0].equals("-p")) {
            Gson g = new Gson();
            IOutils.combinePartFiles(args[1], args[2]);

            BufferedReader reader;
            try {
                reader = new BufferedReader(new FileReader(
                        args[2]));
                String line = reader.readLine();
                JSONObject obj = new JSONObject(line);
                Book b = new Book();
                String bookid = obj.getString("object");
                while (line != null) {
                    obj = new JSONObject(line);
                    if(!bookid.equals(obj.getString("object"))){
                        IOutils.writeLineToFile(args[3],g.toJson(b));
                        bookid = obj.getString("object");
                        b = new Book();
                    }

                    if(obj.getString("relationship").equals("type.object.name"))
                        b.setName(obj.getString("subject"));
                    else if(obj.getString("relationship").equals("book.book.genre"))
                        if(obj.has("subjectB")){
                            b.setGenre(obj.getString("subjectB"));
                        }
                        else{
                            b.setGenre(obj.getString("subject"));
                        }
                    else if(obj.getString("relationship").equals("common.topic.alias"))
                        b.setAlias(obj.getString("subject"));
                    else if(obj.getString("relationship").equals("book.book.characters"))
                        if(obj.has("subjectB")) {
                            b.setCharacters(obj.getString("subjectB"));
                        }
                        else{
                            b.setCharacters(obj.getString("subject"));
                        }

                    line = reader.readLine();
                }
                reader.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            return;
        }



        //local spark configuration
        /*SparkConf conf = new SparkConf().setAppName("VINF")
                                        .setMaster("local")
                                        .set("spark.executor.memory", "8g")
                                        .set("spark.driver.memory", "8g");*/

        SparkConf conf = new SparkConf().setAppName("VINF");
        conf.setJars(new String[]{args[0]});
        JavaSparkContext sc = new JavaSparkContext(conf);

        //JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> fb = sc.textFile(SRC_PATH);

        //keep only the lines that have an attribute we are interested in, get rid of junk from each line
        JavaRDD<String> filtered_fb = fb.filter(x -> x.contains("http://rdf.freebase.com/ns/book.book.genre") | x.contains("http://rdf.freebase.com/ns/common.topic.alias") |
                                                     x.contains("http://rdf.freebase.com/ns/book.book.characters") | x.contains("http://rdf.freebase.com/ns/type.object.type") |
                                                     x.contains("http://rdf.freebase.com/ns/type.object.name"))
                                        .map(s -> s.replaceAll("(<http:\\/\\/rdf\\.freebase\\.com\\/ns\\/)|(>)|(\\.$)",""));

        //dataset that contains only id - type.object.name - name tripples
        JavaRDD<Object> objectIds = filtered_fb.filter(x -> x.contains("type.object.name"))
                                                .map(s -> {
                                                    String parts [] = s.split("\t");
                                                    Object o = new Object();
                                                    o.setObject(parts[0]);
                                                    o.setRelationship(parts[1]);
                                                    o.setSubject(parts[2]);
                                                    return o;
                                                });

        //filter out machine ids for books
        JavaRDD<Id> ids = filtered_fb.filter(x -> x.contains("type.object.type"))
                                         .filter(x -> x.contains("book.book\t"))
                                         .map(s -> s = s.replaceAll("(.*)(g.1[0-9a-np-z][0-9a-np-z_]{6,8})(.*)","$2"))
                                         .map(s -> s = s.replaceAll("(.*)(m.0[0-9a-z_]{2,7}|m.01[0123][0-9a-z_]{5})(.*)","$2"))
                                         .map(s -> {
                                             String parts [] = s.split("\t");
                                             Id i = new Id();
                                             i.setId(parts[0]);
                                             return i;
                                         });
        //SPARK SQL
        SparkSession spark = SparkSession
                .builder()
                .appName("VINF")
                .config("spark.some.config.option", "some-value")
                .getOrCreate();

        //convert JavaRDDs to Datasets
        JavaRDD<Object> fbd = filtered_fb.map(line -> {
                                                String parts [] = line.split("\t");
                                                Object o = new Object();
                                                o.setObject(parts[0]);
                                                o.setRelationship(parts[1]);
                                                o.setSubject(parts[2]);
                                                return o;
                                            });


        Dataset<Row> DF = spark.createDataFrame(fbd, Object.class);
        Dataset<Row> ID = spark.createDataFrame(ids,Id.class);
        Dataset<Row> OID = spark.createDataFrame(objectIds,Object.class);



        DF = DF.join(ID,ID.col("id").equalTo(DF.col("object")), "leftsemi");
        OID = OID.withColumnRenamed("object","objectB")
                .withColumnRenamed("subject","subjectB")
                .withColumnRenamed("relationship","relationshipB");

        DF = DF.join(OID,OID.col("objectB").equalTo(DF.col("subject")),"left").drop("objectB").drop("relationshipB").orderBy("object");
        DF.write().format("json").save(args[2]);

        //DF.persist();
        //JavaRDD<String> parsedBooks = sc.emptyRDD();
        /*BufferedReader reader;
        try {
            reader = new BufferedReader(new FileReader(
                    args[2]));
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
                System.out.println("writing to file "+args[3]+ " also this is read from "+args[2]);
                IOutils.writeLineToFile(args[3],g.toJson(b).toString());
                line = reader.readLine();
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }*/
    }
}
