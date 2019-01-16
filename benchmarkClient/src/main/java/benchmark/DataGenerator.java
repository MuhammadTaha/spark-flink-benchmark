package benchmark;

import java.util.Date;
import java.util.Locale;

import com.github.javafaker.Book;
import com.github.javafaker.Faker;

import java.io.*;

public class DataGenerator {

    public void generateData(int data_size) {

        PrintWriter pw = null;
        try {
            pw = new PrintWriter(new File("data.csv"));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        String data = "";
        Faker faker = new Faker(new Locale("de"));
        int users = 10 * data_size;
        int id = 0;
        for (int userId = 0; userId < users; userId++) {
            int amountOfBooksBought = (int) (Math.random() * 10);
            for (int j = 0; j < amountOfBooksBought; j++) {
                String idString = Integer.toString(id++);
                String userIdString = Integer.toString(userId);
                Book book = faker.book();
                String title = book.title();
                String genre = book.genre();
                String author = book.author();
                String pages = Integer.toString(10 + ((int) (Math.random() * 1000)));
                String publisher = book.publisher();
                String date = faker.date().between(new Date(2000, 0, 1), new Date(2018, 0, 1)).toString();
                String price = Double.toString(((int) (Math.random() * 100) / 100));
                data = String.join(", ", idString, userIdString, title, genre, author, pages, publisher, date, price);
                data += "\n";
                pw.write(data);


                System.out.println(
                        String.join(", ", idString, userIdString, title, genre, author, pages, publisher, date, price));
            }

        }

        pw.close();
    }
}
