import java.io.IOException;
import java.nio.file.Paths;
import java.nio.file.Files;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import java.util.Set;
import java.util.HashSet;

// TODO 
// handle aliases
// handle null values
// handle enums and symbols
// think about optional elements
// how to understand that:
//      {"name": "additional_image_link", "type": [{ "type": "array", "items": "string" }, "null"]},
// how to understand that
//        {"name": "condition", "type": {
//      "type":"enum", "name":"condition_values", "symbols":["new","used"], "default": "new"}

// handle different order of items
public class AvroSchema {

  private String className;
  // keep fields in hash collection for performance reason
  // so looking for fields (while checking if they exist in csv file)
  // is in constant time, not in e.g. linear time
  private Set<String> fields = new HashSet<String>();

  public AvroSchema(String schemaFile) throws IOException {
    // read JSON file
    // is assume schema file has reasonable size and can be safely kept in memory
    byte[] jsonData = Files.readAllBytes(Paths.get(schemaFile));

    ObjectMapper objectMapper = new ObjectMapper();

    // read JSON like DOM parser
    JsonNode rootNode = objectMapper.readTree(jsonData);
    JsonNode nameNode = rootNode.path("name");
    className = nameNode.asText();

    JsonNode fieldsNode = rootNode.path("fields");
    if (fieldsNode.isArray()) {
      for (JsonNode field: fieldsNode) {
        fields.add(field.path("name").asText());
      }
    }
  }

  public String getClassName() {
    return className;
  }

  public Set<String> getFields() {
    return fields;
  }
}
