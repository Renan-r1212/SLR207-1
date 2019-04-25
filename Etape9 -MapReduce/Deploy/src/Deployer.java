
import java.io.*;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Deployer {

	ArrayList<Thread> answerThreads;
	ArrayList<Thread> errorThreads;
	ArrayList<Process> processes;
	ArrayList<String> machines;
	ArrayList<LinkedBlockingQueue<String>> answerQueues;
	ArrayList<LinkedBlockingQueue<String>> errorQueues;
	String hostname;

	public static void main(String[] args) {
		Deployer d = new Deployer();
		d.readFile();
	}

	public Deployer() {
		this.answerThreads = new ArrayList<Thread>();
		this.errorThreads = new ArrayList<Thread>();
		this.processes = new ArrayList<Process>();
		this.machines = new ArrayList<String>();
		this.answerQueues = new ArrayList<LinkedBlockingQueue<String>>();
		this.errorQueues = new ArrayList<LinkedBlockingQueue<String>>();
		InetAddress inetAddress;
		try {
			inetAddress = InetAddress.getLocalHost();
			hostname = inetAddress.getHostName();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}

	}


	public void readFile() {

		BufferedReader reader = null;

		try {
			reader = new BufferedReader(new FileReader("machines.txt"));
			String s = "";

			while ((s = reader.readLine()) != null) {

				machines.add(s);
				ProcessBuilder pb = new ProcessBuilder("bash", "-c",
						"ssh atoon@" + s + " 'mkdir -p /tmp/atoon' && " + "echo Hello" + "&& scp " + hostname
								+ ":/tmp/atoon/Slave.jar " + s + ":/tmp/atoon/Slave.jar" + " && hostname");
				Process p = pb.start();
				processes.add(p);

				LinkedBlockingQueue<String> answerQueue = new LinkedBlockingQueue<String>();
				answerQueues.add(answerQueue);
				LinkedBlockingQueue<String> errorQueue = new LinkedBlockingQueue<String>();
				errorQueues.add(errorQueue);

				Thread answer = new OutputThread(answerQueue, p);
				answerThreads.add(answer);
				Thread error = new ErrorThread(errorQueue, p);
				errorThreads.add(error);

				answer.start();
				error.start();

			}

		} catch (Exception ex) {
			System.out.println(ex);
		} finally {
			try {
				reader.close();
			} catch (Exception ex) {
				Logger.getLogger(Deployer.class.getName()).log(Level.SEVERE, null, ex);
			}

			for (int i = 0; i < answerThreads.size(); i++) {

				try {
					String output = answerQueues.get(i).poll(6, TimeUnit.SECONDS);
					String err = errorQueues.get(i).poll(6, TimeUnit.SECONDS);

					if (output == null && err == null) {
						System.out.println("Timeout--Machine " + machines.get(i));
						answerThreads.get(i).interrupt();
						errorThreads.get(i).interrupt();
						processes.get(i).destroy();
					} else if (!output.equals("")) {
						System.out.println("Ouput--Machine " + machines.get(i) + ":\n" + output);
					} else {
						System.out.println("Errors--Machine " + machines.get(i) + ":\n" + err);
					}

				} catch (InterruptedException ex) {
					System.out.println(ex);
				}
			}

		}
	}

	public void readFile_q38() {

		BufferedReader reader = null;

		try {
			reader = new BufferedReader(new FileReader("machines.txt"));
			String s = "";

			while ((s = reader.readLine()) != null) {

				ProcessBuilder pb = new ProcessBuilder("ssh", "atoon@" + s, "'hostname'");
				Process p = pb.start();
				processes.add(p);

				LinkedBlockingQueue<String> answerQueue = new LinkedBlockingQueue<String>();
				answerQueues.add(answerQueue);
				LinkedBlockingQueue<String> errorQueue = new LinkedBlockingQueue<String>();
				errorQueues.add(errorQueue);

				Thread answer = new OutputThread(answerQueue, p);
				answerThreads.add(answer);
				Thread error = new ErrorThread(errorQueue, p);
				errorThreads.add(error);

				answer.start();
				error.start();

			}

		} catch (Exception ex) {
			System.out.println(ex);
		} finally {
			try {
				reader.close();
			} catch (Exception ex) {
				Logger.getLogger(Deployer.class.getName()).log(Level.SEVERE, null, ex);
			}

			for (int i = 0; i < answerThreads.size(); i++) {

				try {
					String output = answerQueues.get(i).poll(6, TimeUnit.SECONDS);
					String err = errorQueues.get(i).poll(6, TimeUnit.SECONDS);

					if (output == null && err == null) {
						System.out.println("Timeout--Thread " + i);
						answerThreads.get(i).interrupt();
						errorThreads.get(i).interrupt();
						processes.get(i).destroy();
					} else if (!output.equals("")) {
						System.out.println("Ouput--Thread " + i + ":\n" + output);
					} else {
						System.out.println("Errors--Thread " + i + ":\n" + err);
					}

				} catch (InterruptedException ex) {
					System.out.println(ex);
				}
			}

		}
	}

}
