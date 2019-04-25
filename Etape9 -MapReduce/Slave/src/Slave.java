
import java.util.HashSet;
import java.util.Scanner;
import java.util.*;
import java.io.*;

public class Slave {

	public static void main(String[] args) {
		int N = args.length;
		if (N < 2) {
			System.err.println("No arguments passed");
		} else {

			if (args[0].equals("0")) {
				Slave s = new Slave();
				s.map(args[1]);
			}

			else if (args[0].equals("1")) {
				Slave s = new Slave();
				ArrayList<String> files = new ArrayList<String>();
				for (int i = 3; i < N; i++) {
					files.add(args[i]);
				}
				s.shuffle(args[1], args[2], files);
			}

			else if (args[0].equals("2")) {
				Slave s = new Slave();
				s.reduce(args[1], args[2], args[3]);
			}

		}
	}

	// java -jar Slave.jar 0 Sx.txt
	void map(String filename) {
		int number = Integer.parseInt(filename.charAt(1) + "");
		String outputFilename = "UM" + number + ".txt";

		PrintWriter output = null;
		Scanner input = null;
		Set<String> keys = new HashSet<String>();

		try {
			ProcessBuilder pb = new ProcessBuilder("bash", "-c", "mkdir -p /tmp/atoon/maps");
			pb.inheritIO();
			Process p = pb.start();
			p.waitFor();

			output = new PrintWriter(new FileWriter("/tmp/atoon/maps/" + outputFilename));

			File inputFile = new File("/tmp/atoon/splits/" + filename);
			input = new Scanner(inputFile);

			while (input.hasNext()) {

				String word = input.next();
				output.println(word + " 1");
				keys.add(word);
			}

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			output.close();
			input.close();
			for (String i : keys) {
				System.out.println(i);
			}

		}

	}

	// java -jar Slave.jar 1 Car SM0.txt UM1.txt UM2.txt
	void shuffle(String key, String SMxFilename, ArrayList<String> files) {

		PrintWriter output = null;
		Scanner input = null;

		try {

			output = new PrintWriter(new FileWriter("/tmp/atoon/maps/" + SMxFilename));

			for (String filename : files) {

				File inputFile = new File("/tmp/atoon/maps/" + filename);
				input = new Scanner(inputFile);

				while (input.hasNext()) {

					String word = input.next();
					String value = input.next();

					if (word.equals(key)) {
						output.println(word + " " + value);
					}

				}

			}

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			output.close();
			input.close();
		}

	}

	// java -jar Slave.jar 2 Car SMz.txt RM1.txt
	void reduce(String key, String SMxFilename, String RMxFilename) {

		PrintWriter output = null;
		Scanner input = null;
		Set<String> keys = new HashSet<String>();

		try {
			ProcessBuilder pb = new ProcessBuilder("bash", "-c", "mkdir -p /tmp/atoon/reduces");
			pb.inheritIO();
			Process p = pb.start();
			p.waitFor();

			output = new PrintWriter(new FileWriter("/tmp/atoon/reduces/" + RMxFilename));

			File inputFile = new File("/tmp/atoon/maps/" + SMxFilename);
			input = new Scanner(inputFile);

			int sum = 0;
			while (input.hasNext()) {
				input.next();
				sum += Integer.parseInt(input.next());
			}
			output.println(key + " " + sum);

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			output.close();
			input.close();
			for (String i : keys) {
				System.out.println(i);
			}

		}
	}

}
