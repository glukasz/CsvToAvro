import java.util.Set;
import java.util.HashSet;




public class CsvToAvro {
  private final static String PROGRAM_NAME = "CsvToAvro";
  private final static String SCHEMA_ARG = "--schema";
  private final static String INPUT_ARG = "--input";
  private final static String OUTPUT_ARG = "--output";
  private final static String STREAM_ARG = "-c";
  private final static int FILE_INPUT_ARGS = 3;
  private final static int STREAM_INPUT_ARGS = 1;

  private static boolean streamInput = false;
  private static boolean fileInput = false;
  private static String inputPath;
  private static String outputPath;
  private static String schemaPath;

  private static String schemafile = "/home/lgaza/sandbox/gabor/csvtoavro/products.avsc";
  


  public static void main(String [] args) {
    if (processArgs(args) == false) {
      printUsage();
      return; 
    }
    try {
      CsvToAvroConverter converter;

      if (fileInput) {
        converter = new CsvToAvroConverter(inputPath, schemaPath, outputPath);
        converter.convert();
      } else if (streamInput) {

      }

    } catch(Exception ex) {
      ex.printStackTrace();
    }
  }

  private static void printUsage() {
    System.out.println("usage: CsvToAvro --input <csv_file> --schema <avro_schema> --output <avro_file>");
    System.out.println("\tCsvToAvro --schema <avro_schema> -c < <intput_csv> > <avro_file>");
  }

  private static boolean processArgs(String [] args) {
    Set<String> allowedArgs = new HashSet<String>();
    allowedArgs.add(SCHEMA_ARG);
    allowedArgs.add(INPUT_ARG);
    allowedArgs.add(OUTPUT_ARG);
    allowedArgs.add(STREAM_ARG);
    for (int i=0; i<args.length; ++i) {
      if (args[i].trim().equals(SCHEMA_ARG)) {
        fileInput = true;
        ++i;
        if ((args[i] != null) && !allowedArgs.contains(args[i].trim())) {
          schemaPath = args[i];
        } else {
System.out.println("1 ");
          return false;
        }
      } else if (args[i].trim().equals(INPUT_ARG)) {
        fileInput = true;
        ++i;
        if ((args[i] != null) && !allowedArgs.contains(args[i].trim())) {
          inputPath = args[i];
        } else {
System.out.println("2 ");
          return false;
        }
      } else if (args[i].trim().equals(OUTPUT_ARG)) {
        fileInput = true;
        ++i;
        if ((args[i] != null) && !allowedArgs.contains(args[i].trim())) {
          outputPath = args[i];
        } else {
System.out.println("3 ");
          return false;
        }
      } else if (args[i].trim().equals(STREAM_ARG)) {
        streamInput = true;
      } else if (args[i].trim().equals(PROGRAM_NAME)) {
        System.out.println("I'm here " + args[i]);
        // pass
      } else {
        System.out.println("4 >>>" + args[i] + "<<<");
        return false;
      }
    }
    if (fileInput && streamInput) {
      return false;
    }
    return true;
  }
}
