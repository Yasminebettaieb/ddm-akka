package de.ddm.actors.profiling;

// Necessary imports for serialization and data representation.
import de.ddm.serialization.AkkaSerializable;
import lombok.Data;

import java.util.HashSet;

// Annotates class with getters, setters, equals, hashCode, toString methods.
@Data
public class Content implements AkkaSerializable {
    private static final long serialVersionUID = -8025238529984914107L;
    // Unique identifier for serialization
    private int id; // ID of the content, likely used to uniquely identify it.
    private String columnName; // Name of the column this content belongs to.
    private String name; // General name for the content, possibly a label or descriptor.
    private HashSet<String> content; // A set containing the actual data values.

    // Constructor to initialize a Content object with id, columnName, and name.
    // HashSet for content is initialized to ensure no duplicate values.
    public Content(int id, String columnName, String name) {
        this.content = new HashSet<>(); // Initialize content as a HashSet to store unique values.
        this.columnName = columnName; // Assigns the name of the column.
        this.name = name; // Assigns the general name to the content.
    }

    // Method to add a single value to the content set.
    // Ensures that each value is unique due to the properties of HashSet.
    public void add(String value){
        this.content.add(value); // Adds the value to the content HashSet.
    }
}
