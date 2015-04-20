import java.io.IOException;
import java.nio.file.Paths;
import java.nio.file.Files;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import org.apache.avro.AvroRuntimeException;

public class AvroSchema {

  private final static String NAME_PATH = "name";
  private final static String FIELDS_PATH = "fields";
  private final static String ALIASES_PATH = "aliases";

  private String className;
  // keep fields in hash collection for performance reason
  // so looking for fields (while checking if they exist in csv file)
  // is in constant time, not in e.g. linear time
  // the key is name of field, the value is the field the key is alias for
  // if given key has no aliases key and value are the same
  private Map<String,String> fields = new HashMap<String,String>();

  public AvroSchema(String schemaFile) throws IOException {
    // read JSON file
    // is assume schema file has reasonable size and can be safely kept in memory
    byte[] jsonData = Files.readAllBytes(Paths.get(schemaFile));

    ObjectMapper objectMapper = new ObjectMapper();

    // read JSON like DOM parser
    JsonNode rootNode = objectMapper.readTree(jsonData);

    if (rootNode == null) {
      throw new AvroRuntimeException("Corrupted avro schema file - file is empty");
    }

    JsonNode nameNode = rootNode.path(NAME_PATH);
    if (nameNode == null) {
      throw new AvroRuntimeException("Corrupted avro schema file - no 'name' field");
    }
      
    className = nameNode.asText();

    JsonNode fieldsNode = rootNode.path(FIELDS_PATH);
    if (fieldsNode == null) {
      throw new AvroRuntimeException("Corrupted avro schema file - no 'fields' field");
    }

    if (fieldsNode.isArray()) {
      for (JsonNode field: fieldsNode) {
        String fieldName = field.path(NAME_PATH).asText();
        fields.put(fieldName, fieldName);

        // add to the map also the aliases with information what given field is alias for
        JsonNode aliasesNode = field.path(ALIASES_PATH);
        if (aliasesNode.isArray()) {
           for (JsonNode alias: aliasesNode) {
             fields.put(alias.asText(), fieldName);
           }
        }
      }
    } else {
      throw new AvroRuntimeException("Corrupted avro schema file - 'fields' field not an array");
    }

      
  }

  public String getClassName() {
    return className;
  }

  public Set<String> getFieldsSet() {
    return fields.keySet();
  }

  public Map<String, String> getFields() {
    return fields;
  }
}
