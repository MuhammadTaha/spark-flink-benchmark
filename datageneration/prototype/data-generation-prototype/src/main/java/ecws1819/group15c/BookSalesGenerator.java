package ecws1819.group15c;

import java.util.Date;
import java.util.Locale;

import com.github.javafaker.Book;
import com.github.javafaker.Faker;

public class BookSalesGenerator {
	public static void main(String[] args) {
		Faker faker = new Faker(new Locale("de"));
		int users = 10;
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
				String date = faker.date().between(new Date(), new Date(2018, 0, 1)).toString();
				String price = Double.toString(((int) (Math.random() * 100) / 100));
				System.out.println(
						String.join(", ", idString, userIdString, title, genre, author, pages, publisher, date, price));
			}

		}
	}
}
