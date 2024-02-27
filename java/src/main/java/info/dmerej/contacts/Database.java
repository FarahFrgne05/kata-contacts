package info.dmerej.contacts;

import java.io.File;
import java.sql.*;
import java.util.function.Consumer;
import java.util.stream.Stream;

public class Database {
    private Connection connection;
    public static final int BATCH_SIZE = 10000;

    public Database(File databaseFile) {
        String databasePath = databaseFile.getPath();
        try {
            connection = DriverManager.getConnection("jdbc:sqlite:" + databasePath);
        } catch (SQLException e) {
            throw new RuntimeException("Could not create connection: " + e.toString());
        }
    }

    public void migrate() {
        System.out.println("Migrating database ...");
        try {
            Statement statement = connection.createStatement();
            statement.execute("""
                    CREATE TABLE contacts(
                    id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    email TEXT NOT NULL
                    )
                    """
            );
        } catch (SQLException e) {
            throw new RuntimeException("Could not migrate db: " + e.toString());
        }
        System.out.println("Done migrating database");
    }

    public void insertContacts(Stream<Contact> contacts) {

        System.out.println("Inserting contacts ...");
        String query = "INSERT INTO contacts (name, email) VALUES (?,?)";

        int batchCount = 0;
        int insertedCount = 0;

        try{
            PreparedStatement ps = connection.prepareStatement(query);
            connection.setAutoCommit(false);
        
            for (Contact contact: (Iterable<Contact>) contacts::iterator) {
                ps.setString(1, contact.name());
                ps.setString(2, contact.email());
                ps.addBatch();
                insertedCount++;
                if (insertedCount % BATCH_SIZE == 0) {
                    batchCount++;
                    ps.executeBatch();
                    connection.commit();
                    ps.clearBatch();
                    System.out.println("Inserted " + insertedCount + " contacts in batch " + batchCount);
                }
            }

            // Ensure that the last batch is executed
            ps.executeBatch();
            connection.commit();
            ps.clearBatch();
        } catch (SQLException e) {
            throw new RuntimeException("Not insert into db: " + e.toString());
        }
        System.out.println("Done inserting contacts");
    }

    public String getContactNameFromEmail(String email) {
        String query = "SELECT name FROM contacts WHERE email = ?";
        try {
            PreparedStatement statement = connection.prepareStatement(query);
            statement.setString(1, email);
            ResultSet result = statement.executeQuery();
            if (result.next()) {
                return result.getString(1);
            } else {
                throw new RuntimeException("No match in the db for email: " + email);
            }
        } catch (SQLException e) {
            throw new RuntimeException("Error when looking up contacts from db: " + e.toString());
        }
    }

    public void close() {
        if (connection == null) {
            return;
        }

        try {
            connection.close();
        } catch (SQLException e) {
            throw new RuntimeException("Could not close db: " + e.toString());
        }
    }

}
