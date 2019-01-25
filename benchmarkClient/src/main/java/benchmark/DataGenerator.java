package benchmark;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.Calendar;
import java.util.Locale;

import com.github.javafaker.Book;
import com.github.javafaker.Faker;

public class DataGenerator {
	enum Unit {
		ENTITIES, SPACE
	}

	private static final String delimiter = ",";

	public File generateData(String data_size) {
		if (data_size.isEmpty())
			return null;

		long size;
		Unit unit = Unit.ENTITIES;
		try {
			size = Integer.parseInt(data_size);
		} catch (Exception e) {
			try {
				size = Integer.parseInt(data_size.substring(0, data_size.length() - 2));
				switch (data_size.toLowerCase().substring(data_size.length() - 2)) {
				case "kb":
					unit = Unit.SPACE;
					size = size * 1024;
					break;
				case "mb":
					unit = Unit.SPACE;
					size = size * 1024 * 1024;
					break;
				case "gb":
					unit = Unit.SPACE;
					size = size * 1024 * 1024 * 1024;
					break;
				}
			} catch (Exception f) {
				size = 10;
			}
		}

		System.out.println("gen: " + size + (unit == Unit.ENTITIES ? " entities" : " bytes"));

		PrintWriter pw = null;
		File dataFile = new File("data.csv");
		try {
			pw = new PrintWriter(dataFile);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		Faker faker = new Faker(new Locale("en"));
		int id = 0;
		Calendar from = Calendar.getInstance();
		from.set(2000, 0, 1);
		Calendar to = Calendar.getInstance();
		to.set(2018, 11, 31);

		String data = String.join(", ", "id", "userId", "title", "genre", "author", "pages", "publisher", "date",
				"price");
		data += "\n";
		pw.write(data);

		for (int userId = 0; unit == Unit.ENTITIES ? id < size : dataFile.length() < size; userId++) {
			int amountOfBooksBought = (int) (Math.random() * 10);
			for (int j = 0; (j < amountOfBooksBought)
					&& (unit == Unit.ENTITIES ? id < size : dataFile.length() < size); j++) {
				String idString = Integer.toString(id++);
				String userIdString = Integer.toString(userId);
				Book book = faker.book();

				String title = clean(book.title());
				String genre = clean(book.genre());
				String author = clean(book.author());
				String pages = Integer.toString(10 + ((int) (Math.random() * 1000)));
				String publisher = clean(book.publisher());

				String date = clean(faker.date().between(from.getTime(), to.getTime()).toString());
				String price = Double.toString(((int) (Math.random() * 100 * 100) / 100));
				data = String.join(delimiter, idString, userIdString, title, genre, author, pages, publisher, date,
						price);
				data += "\n";
				pw.write(data);

				if (id % 10000 == 0) {
					System.out.println(
							"gen: " + (unit == Unit.ENTITIES ? percent(id, size) : percent(dataFile.length(), size)));
				}
			}
		}
		System.out.println("gen: 100.0%");
		pw.close();
		return dataFile;
	}

	private String percent(long count, long total) {
		return (Math.floor((double) count / total * 1000) / 10) + "%";
	}

	private String clean(String s) {
		return s.replaceAll(delimiter, "");
	}
}
