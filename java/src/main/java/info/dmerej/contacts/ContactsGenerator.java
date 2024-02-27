package info.dmerej.contacts;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class ContactsGenerator {
  public Stream<Contact> generateContacts(int count) {

    Contact[] contacts = new Contact[count];

    for(int i = 0; i<=count; i++){
      String name = "Alice"+i;
      String email = "email-"+i+"@tld";
      Contact contact = new Contact(name, email);
      contacts[i] = contact;
    }

    System.out.println("Generated " + count + " contacts");

      //new Contact("Alice", "alice@aol.com"),
      //new Contact("Bob", "bob@gmail.com"),
      //new Contact("Eve", "eve@fastmail.com"),

    return Arrays.stream(contacts);
  }
}
