import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

import static java.lang.System.exit;

public class IOutils {

    public static void combinePartFiles(String srcFolderPath,String dstFilePath) throws IOException {
        PrintWriter pw = null;
        try {
            pw = new PrintWriter(dstFilePath);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        if(pw == null) {
            System.out.println("Log: output file could not be opened");
            exit(0);
        }

        //combine the saved RDD part files
        File tdir = new File(srcFolderPath);
        String[] tpartNames = tdir.list();
        for (String fileName : tpartNames) {
            System.out.println("Reading from " + fileName);

            //if its not a part file, skip
            if(!fileName.matches("^part?(.*)")) {
                System.out.println("Skipping");
                continue;
            }

            File f = new File(tdir, fileName);

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

    public static void writeLineToFile(String filePath,String line) throws IOException {
        PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(filePath, true)));
        out.println(line);
        out.close();
    }
}
