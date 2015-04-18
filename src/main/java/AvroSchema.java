import java.io.IOException;
import java.nio.file.Paths;
import java.nio.file.Files;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;

public class AvroSchema {

  private String className;

  public AvroSchema(String schemaFile) throws IOException {
    // read JSON file
    // is assume schema file has reasonable size and can be safely kept in memory
    byte[] jsonData = Files.readAllBytes(Paths.get(schemaFile));

    ObjectMapper objectMapper = new ObjectMapper();

    // read JSON like DOM parser
    JsonNode rootNode = objectMapper.readTree(jsonData);
    JsonNode nameNode = rootNode.path("name");

    className = nameNode.asText();
  }

  public String getClassName() {
    return className;
  }
}
