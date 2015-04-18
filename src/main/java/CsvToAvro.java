


public class CsvToAvro {
  private static String schema = "/home/lgaza/sandbox/gabor/csvtoavro/products.avsc";

  public static void main(String [] args) {
    try {
      AvroSchema as = new AvroSchema(schema);
      System.out.println(as.getClassName());
    } catch(Exception ex) {
      ex.printStackTrace();
    }
  }
}
