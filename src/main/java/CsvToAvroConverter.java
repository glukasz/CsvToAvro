import java.io.InputStream;
import java.io.OutputStream;
import java.io.IOException;
import java.io.File;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.file.DataFileWriter.AppendWriteException;

public class CsvToAvroConverter {
  private final static String CSV_DELIMITER = ",";

  private CsvReader csvReader;
  private AvroSchema as;
  private Schema schema;
  private File outFile;
  private int errorCount = 0;


  public CsvToAvroConverter(String inputPath, String schemaPath, String outputPath) throws IOException {
    csvReader = new CsvReader(inputPath);
    as = new AvroSchema(schemaPath);
    schema = new Schema.Parser().parse(new File(schemaPath));
    outFile = new File(outputPath);
  }

  public CsvToAvroConverter(InputStream inStream, String schemaPath, OutputStream outputStream) throws IOException {
    csvReader = new CsvReader(inStream);
    as = new AvroSchema(schemaPath);
    schema = new Schema.Parser().parse(new File(schemaPath));
    //outFile = new File(outputPath);
  }

  public void convert() throws IOException {

    String [] csvHeaders = csvReader.getHeaderFields();
    int errorCount = 0;

    DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
    DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);
    dataFileWriter.create(schema, outFile);  

    Map<String, String> schemaFields = as.getFields();

    String line;
    while ((line = csvReader.readLine()) != null) {
      String [] data = line.split(CSV_DELIMITER);

      // if given line in csv file has different number of fields than
      // the number of fields in headers it mean the line is corrupted
      // - skip it then
      if (data.length != csvHeaders.length) {
        ++errorCount;
        continue;
      }

      GenericRecord product = new GenericData.Record(schema);
      for (int i=0; i<data.length; ++i) {
        // store csv entry in avro file if given csv field exists in avro schema
        // schema it as base name (it case alias is used in csv file)
        if (csvHeaders[i].equals("additional_image_link") || csvHeaders[i].equals("price") || csvHeaders[i].equals("sale_price") || csvHeaders[i].equals("return_days")) {
          continue;
        }
       
        if (schemaFields.containsKey(csvHeaders[i])) {
          if (data[i].trim().equals("")) {
            product.put(schemaFields.get(csvHeaders[i]), null);
          } else {
            product.put(schemaFields.get(csvHeaders[i]), data[i].trim());
          }
        }
      }
      try {
        dataFileWriter.append(product);
      } catch (AppendWriteException ex) {
        // this exception is thrown when given csv file line has schema different than the 
        // one expected by avro schema (e.g. enum value isn;t in the set of possible values
        // or non-null value has value of null)
        // in that case whole serialization shouldn;t be interrupted
        // but it should continue and appropriate log entry should be added
        // TODO add logging here
        System.out.println(ex.getMessage());
      }
    }
    dataFileWriter.close();
  }

  public void close() throws IOException {
    if (csvReader != null) {
      csvReader.close();
    }
  }

  //private String [] 
}



 /*     String line = in.readLine();
      String [] csvHeaders;
      if (line != null) {
        csvHeaders = line.split(",");
      } else {
        System.out.println("Input is empty");
        return;
      }

      // to keep the order
      Map<String, Boolean> headers = new LinkedHashMap<String, Boolean>();
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


      DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(schema);*/
