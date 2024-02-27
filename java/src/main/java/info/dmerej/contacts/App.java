package info.dmerej.contacts;


import java.io.File;
import java.util.stream.Stream;

public class App {

    private Database database;
    private ContactsGenerator contactsGenerator;

    public App() {
        File file = new File("contacts.sqlite3");
        if (!file.exists()) {
            database = new Database(file);
            database.migrate();
        } else {
            database = new Database(file);
        }
        contactsGenerator = new ContactsGenerator();
    }

    public static void main(String[] args) {
        if (args.length != 1) {
            System.err.println("Not enough args");
            System.exit(2);
        }
        int count = Integer.parseInt(args[0]);

        App app = null;
        try {
            app = new App();
            app.insertContacts(count);
            app.lookupContact(count);
        } finally {
            if (app != null) {
                app.close();
            }
        }
    }

    private void insertContacts(int count) {
        Stream<Contact> contacts = contactsGenerator.generateContacts(count);
        database.insertContacts(contacts);

    }

    private void lookupContact(int count) {
        String email = String.format("email-%d@tld", count);
        long start = System.currentTimeMillis();
        database.getContactNameFromEmail(email);
        long end = System.currentTimeMillis();
        long elapsed = end - start;
        System.out.format("Query took %f seconds\n", elapsed / 1000.0);
    }

    public void close() {
        database.close();
    }

}

