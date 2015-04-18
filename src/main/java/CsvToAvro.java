import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Set;
import java.util.Map;
import java.util.TreeMap;
import java.io.File;

import org.apache.avro.io.DatumReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.Schema;


public class CsvToAvro {
  private static String schemafile = "/home/lgaza/sandbox/gabor/csvtoavro/products.avsc";



  public static void main(String [] args) {
    try {
      AvroSchema as = new AvroSchema(schemafile);
      System.out.println(as.getClassName());

      BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
      String line = in.readLine();
      String [] csvHeaders;
      if (line != null) {
        csvHeaders = line.split(",");
      } else {
        System.out.println("Input is empty");
        return;
      }

      // to keep the order
      Map<String, Boolean> headers = new TreeMap<String, Boolean>();
      Set<String> avroFields = as.getFields();

      if (csvHeaders != null) {
        for (String s: csvHeaders) {
          if (avroFields.contains(s)) {
            headers.put(s, true);
          } else {
            headers.put(s, false);
          }
        }
      }

      Schema schema = new Schema.Parser().parse(new File(schemafile));
      DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(schema);
      //String s;
      //while ((s = in.readLine()) != null && s.length() != 0) {
      //  System.out.println(s);
      //}
    } catch(Exception ex) {
      ex.printStackTrace();
    }
  }
}
