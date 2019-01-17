package ecws1819.group15c;

import java.util.Locale;

import com.github.javafaker.Address;
import com.github.javafaker.Faker;
import com.github.javafaker.Name;

public class UserDataGenerator {
	public static void main(String[] args) {
		Faker faker = new Faker(new Locale("de"));
		for (int i = 0; i < 10; i++) {
			String firstName, lastName, city, street, zipCode, dateOfBirth, randomRating;
			Name name = faker.name();
			firstName = name.firstName();
			lastName = name.lastName();
			Address address = faker.address();
			city = address.city();
			street = address.streetAddress();
			zipCode = address.zipCode().toString();
			dateOfBirth = faker.date().birthday(18, 64).toLocaleString();
			randomRating = Double.toString(Math.random() * 40);
			System.out.println(String.join(", ", Integer.toString(i), firstName, lastName, city, street, zipCode,
					dateOfBirth, randomRating));
		}
	}
}
