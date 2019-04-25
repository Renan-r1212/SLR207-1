
import java.io.*;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Master {

	ArrayList<Thread> answerThreads;
	ArrayList<Thread> errorThreads;
	ArrayList<Process> processes;
	ArrayList<String> machines;
	ArrayList<LinkedBlockingQueue<String>> answerQueues;
	ArrayList<LinkedBlockingQueue<String>> errorQueues;
	String hostname;

	Map<String, String> UMxMachine;
	Map<String, ArrayList<String>> keyUMx;

	public static void main(String[] args) {
		Master d = new Master();
		d.deploySplits();
		d.runMap();
		d.collectMap();
		d.prepareShuffle();
		d.runShuffle();
		d.runReduce();
		d.collectReduce();
	}

	public Master() {
		this.answerThreads = new ArrayList<Thread>();
		this.errorThreads = new ArrayList<Thread>();
		this.processes = new ArrayList<Process>();
		this.machines = new ArrayList<String>();
		this.answerQueues = new ArrayList<LinkedBlockingQueue<String>>();
		this.errorQueues = new ArrayList<LinkedBlockingQueue<String>>();
		this.UMxMachine = new HashMap<String, String>();
		this.keyUMx = new HashMap<String, ArrayList<String>>();
		InetAddress inetAddress;
		try {
			inetAddress = InetAddress.getLocalHost();
			hostname = inetAddress.getHostName();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}

	}

//    'mkdir -p /tmp/atoon && cd /tmp && ls

	/**
	 * Method that deploys text splits on machines mentioned in the "machines.txt"
	 * file
	 */
	public void deploySplits() {

		System.out.println("\tDeploying splits...");
		BufferedReader reader = null;

		try {
			reader = new BufferedReader(new FileReader("machines.txt"));
			String s = "";
			int i = 0;
			while ((s = reader.readLine()) != null) {

				machines.add(s);
				ProcessBuilder pb = new ProcessBuilder("bash", "-c",
						"ssh atoon@" + s + " 'mkdir -p /tmp/atoon/splits' && " + "echo Hello" + "&& scp " + hostname
								+ ":/tmp/atoon/splits/S" + i + ".txt " + s + ":/tmp/atoon/splits/S" + i + ".txt"
								+ " && hostname");

//				Process p = pb.start();
//				p.waitFor();
				pb.start();
				i++;

			}
			
			System.out.println("\tDone: Deploying splits.");

		} catch (Exception ex) {
			System.out.println(ex);
		} finally {
			try {
				reader.close();
			} catch (Exception ex) {
				Logger.getLogger(Master.class.getName()).log(Level.SEVERE, null, ex);
			}
		}
	}

	/**
	 * Method that runs the map mode of Slave.jar on distant machines and creates queues and threads
	 * to listen to the errors and outputs of each distant process running on a
	 * machine
	 */
	public void runMap() {

		System.out.println("\tMapping...");
		try {
			for (int i = 0; i < machines.size(); i++) {

				ProcessBuilder pb = new ProcessBuilder("bash", "-c",
						"ssh atoon@" + machines.get(i) + " 'java -jar /tmp/atoon/Slave.jar 0 S" + i + ".txt'");

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
			
			System.out.println("\tDone: Mapping.");
		} catch (Exception e) {
			System.out.println(e);
		}

	}

	/**
	 * Method that runs threads to read the output of Map jobs and fill the key -
	 * Umx and the Umx - Machine maps
	 */
	public void collectMap() {

		System.out.println("\tCollecting mapping results...");
		Thread[] mapOutputThreads = new Thread[answerThreads.size()];

		// read outputs
		for (int i = 0; i < answerThreads.size(); i++) {
			mapOutputThreads[i] = new MapReaderThread(i);
			mapOutputThreads[i].start();
		}

		// wait for all outputs to be read
		try {
			for (int i = 0; i < mapOutputThreads.length; i++) {
				mapOutputThreads[i].join();
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		// print Map results
		mapResults();
	}

	/**
	 * print the Map job results: -UMx -- Machine map and Key -- Um List map
	 */
	private void mapResults() {

		System.out.println("\tMapping Results\nUMx -- Machine");
		for (String i : UMxMachine.keySet()) {
			System.out.println(i + " - " + UMxMachine.get(i));
		}

		System.out.println("\nKey -- Um List");
		for (String i : keyUMx.keySet()) {
			System.out.println(i + " - " + keyUMx.get(i));
		}
		System.out.println("\n");
	}

	/**
	 * Map that keeps track of the machine assigned to perform the reduce of a given
	 * key in the prepareShuffle phase.
	 */
	private HashMap<String, String> key_AssignedMachine;

	/**
	 * Method that picks for each key the machine that will have to receive the
	 * key's UMx files and perform the Shuffle phase. The method first give the work
	 * to free machines (by trying to pick the ones already containing some files of
	 * a given key). If all the machines are used, the method loops over busy
	 * machines and picks the one that has the higher score, the score being the
	 * number of the key's UMx files already present on the machine. When the method
	 * picks the candidate machine, it prepares the set of UMx files left to be
	 * copied to it and calls the moveFilesOnMachine method to copy them. Each copy
	 * command is performed in a different process (in parallel) on the candidate
	 * machine (files don't get copied on the Master).
	 */
	public void prepareShuffle() {
		System.out.println("\tPreparing Shuffle...");
		key_AssignedMachine = new HashMap<String, String>();
		HashMap<String, HashSet<String>> machine_AssignedUMs = new HashMap<String, HashSet<String>>();// Map that keeps
																										// track of the
																										// UMs assigned
																										// to each
																										// machine in
																										// the shuffle
																										// phase. It
																										// aims to avoid
																										// copying the
																										// same UMx
																										// twice on a
																										// machine.
		HashSet<String> unusedMachines = new HashSet<String>(this.machines);// set to keep track of unused machines

		HashMap<String, Process> key_process = new HashMap<String, Process>();// keep track of the word and the process
																				// copying its files

		for (String word : keyUMx.keySet()) {// decide for each key the destination machine and the necessary UMx files
												// to be copied on it.
			ArrayList<String> wordUMx = keyUMx.get(word);// get the list of UMx files needed for the considered key

			String candidateMachine = null;
			HashSet<String> filesToCopy = null;
			Process p = null;
			if (unusedMachines.size() != 0) {// if there are some unused machines

				for (String UMx : wordUMx) {// loop over the key's UMx files hoping to find a free machine containing
											// them
					candidateMachine = UMxMachine.get(UMx);// consider the machine containing the UMx

					if (unusedMachines.contains(candidateMachine)) {// if the machine is unused copy the rest of the UMx
																	// files to it
						filesToCopy = new HashSet<String>(wordUMx);
						filesToCopy.remove(UMx);
						unusedMachines.remove(candidateMachine);// the machine is now used
						// call the copying method and break loop
						p = moveFilesOnMachine(candidateMachine, filesToCopy);
						System.out.println("Candidate picked because free & contains " + UMx);
						// add the files to the machine - Assigned UMs map
						machine_AssignedUMs.put(candidateMachine, filesToCopy);
						break;
					}

				}

				if (candidateMachine == null || filesToCopy == null) {// no machine containing the key's files is free,
																		// we therefore pick a random free machine
																		// (priority is using all free machines first)
					candidateMachine = unusedMachines.iterator().next();// pick the first free machine
					filesToCopy = new HashSet<String>(wordUMx);
					unusedMachines.remove(candidateMachine);// the machine is now used
					// call the copying method
					System.out.println("Candidate picked because free ");
					p = moveFilesOnMachine(candidateMachine, filesToCopy);
					// add the files to the machine - Assigned UMs map
					machine_AssignedUMs.put(candidateMachine, filesToCopy);
				}

			}

			else {// all machines are already used -> we need to pick the one that contains the
					// most files containing the key

				int maxScore = 0;
				candidateMachine = machine_AssignedUMs.keySet().iterator().next();

				for (String machine : machine_AssignedUMs.keySet()) {
					int score = 0;
					HashSet<String> umxOnMachine = machine_AssignedUMs.get(machine);
					for (String UMx : wordUMx) {
						if (umxOnMachine.contains(UMx))
							score += 1;// increment machine's score if it contains a file we need
					}
					if (score > maxScore) {// pick machine with highest score
						candidateMachine = machine;
						maxScore = score;
					}
				} // we have our best candidate machine

				filesToCopy = new HashSet<String>(wordUMx);
				filesToCopy.removeAll(machine_AssignedUMs.get(candidateMachine));// remove all files that are already on
																					// the picked machine
				// call the copying method and break loop
				p = moveFilesOnMachine(candidateMachine, filesToCopy);
				System.out.println("Candidate picked because used & contains " + maxScore + " UMx");
				// add the files to the machine - Assigned UMs map
				machine_AssignedUMs.get(candidateMachine).addAll(filesToCopy);

			}
			System.out.println("Candidate picked= " + candidateMachine + " word= " + word + "\n\n");

			key_AssignedMachine.put(word, candidateMachine);
			key_process.put(word, p);

		}

		System.out.println("Waiting for copy commands, Time: " + System.currentTimeMillis());
		for (String i : key_process.keySet()) {
			try {
				key_process.get(i).waitFor();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		System.out.println("Copy finished, Time: " + System.currentTimeMillis());

		System.out.println("\n\tShuffle Prepare Results\nKey -- Assigned Machine");
		for (String i : key_AssignedMachine.keySet()) {
			System.out.println(i + " - " + key_AssignedMachine.get(i));
		}
		
		System.out.println("\nMachine -- Copied UMx files");
		for (String i : machine_AssignedUMs.keySet()) {
			System.out.println(i + " - " + machine_AssignedUMs.get(i));
		}
		
		

	}

	/**
	 * 
	 * @param DestinationMachine
	 * @param files              Move all the files in the list to the Destination
	 *                           Machine. Action done in a distributed way: the
	 *                           master does not have a copy of the UMx, it's a
	 *                           distant copy command run on the machines containing
	 *                           the files wanted.
	 * 
	 */
	private Process moveFilesOnMachine(String destinationMachine, HashSet<String> files) {

		try {
			if (files.size() == 0) {
				ProcessBuilder pb = new ProcessBuilder("bash", "-c", "echo 'no files to copy'");
				return pb.start();
			}
			for (String filename : files) {
				String originMachine = UMxMachine.get(filename);
				ProcessBuilder pb = new ProcessBuilder("bash", "-c",
						"ssh atoon@" + originMachine + " 'scp " + originMachine + ":/tmp/atoon/maps/" + filename
								+ ".txt " + destinationMachine + ":/tmp/atoon/maps/" + filename + ".txt'");

				pb.inheritIO();
				return pb.start();
			}

		} catch (Exception e) {
			System.out.println(e);
		}
		return null;

	}

	/**
	 * Thread Class that reads the output of a Mapping of a given process and fills
	 * the UM-machine and the key-UM maps
	 */
	class MapReaderThread extends Thread {
		int i;
		String machineName;

		public MapReaderThread(int number) {
			i = number;
			machineName = machines.get(i);
		}

		@Override
		public void run() {

			try {
				String output = answerQueues.get(i).poll(6, TimeUnit.SECONDS);
				String err = errorQueues.get(i).poll(6, TimeUnit.SECONDS);

				if (output == null && err == null) {
					System.out.println("Timeout--Machine " + machineName);
					answerThreads.get(i).interrupt();
					errorThreads.get(i).interrupt();
					processes.get(i).destroy();

				} else if (output != null && !output.equals("")) {
					System.out.println("Ouput--Machine " + machineName + ":\n" + output);

					// add UMx-Machine to map
					UMxMachine.put("UM" + i, machineName);

					// add the UMx to key map
					String[] outputKeys = output.split("[\\r\\n]+");
					for (int i1 = 0; i1 < outputKeys.length; i1++) {
						addKeyUM(outputKeys[i1], "UM" + i);
					}

				} else {
					System.out.println("Errors--Machine " + machineName + ":\n" + err);
				}

			} catch (InterruptedException ex) {
				System.out.println(ex);
			}

		}

	}

	/**
	 * @param key
	 * @param UM  add the UMx that contains the associated key into the Key - UMx
	 *            map
	 */
	private void addKeyUM(String key, String UM) {
		if (this.keyUMx.containsKey(key)) {
			ArrayList<String> l = keyUMx.get(key);
			l.add(UM);
			keyUMx.put(key, l);
		} else {
			ArrayList<String> l = new ArrayList<String>();
			l.add(UM);
			keyUMx.put(key, l);
		}
	}
	
	/**
	 * Map that keeps track of the SMx file that contains the shuffle 
	 * phase of a given key (useful for the reduce phase)
	 */
	HashMap<String,String> keySMx;
	
	/**
	 * Method that runs the shuffle method on the distant slave machines assigned according to the prepareShuffle method.
	 * Runs a process for each key and then waits for them all to finish
	 */
	public void runShuffle() {

		System.out.println("\n\tShuffling...");
		System.out.println("\nKey -- Assigned Machine -- Output File");
		keySMx=new HashMap<String,String>();
		HashSet<Process> shuffleProcesses=new HashSet<Process>();
		
		try {
			int i=0;
			for (String key : key_AssignedMachine.keySet()) {

				String umxFilesArgument="";
				for (String UMx:keyUMx.get(key)) {
					umxFilesArgument+=" "+UMx+".txt";
				}
				
				ProcessBuilder pb = new ProcessBuilder("bash", "-c",
						"ssh atoon@" + key_AssignedMachine.get(key) + " 'java -jar /tmp/atoon/Slave.jar 1 "+key+" SM" + i + ".txt"+umxFilesArgument+"'");

				System.out.println(key+" - "+key_AssignedMachine.get(key)+
						" - SM" + i +".txt");
				keySMx.put(key, "SM"+ i +".txt");
				i++;
				shuffleProcesses.add(pb.start());
			}
			
			//wait for shuffle processes
			for (Process p : shuffleProcesses) {
				try {
					p.waitFor();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			
			System.out.println("\tDone: Shuffling.");
		} catch (Exception e) {
			System.out.println(e);
		}

	}
	
	/**
	 * Map that keeps track of the RMx file that contains the reduce 
	 * phase of a given key (useful for testing the reduce phase)
	 */
	HashMap<String,String> keyRMx;
	
	/**
	 * Method that runs the reduce method on the distant slave machines assigned according to the prepareShuffle method,
	 * and to the SMx files produces during the shuffle method.
	 * Runs, in parallel, a process for each key and then waits for them all to finish
	 */
	public void runReduce() {

		System.out.println("\n\tReducing...");
		System.out.println("\nKey -- Assigned Machine -- Reduce File");
		keyRMx=new HashMap<String,String>();
		HashSet<Process> reduceProcesses=new HashSet<Process>();
		
		try {
			int i=0;
			for (String key : key_AssignedMachine.keySet()) {

				ProcessBuilder pb = new ProcessBuilder("bash", "-c",
						"ssh atoon@" + key_AssignedMachine.get(key) + " 'java -jar /tmp/atoon/Slave.jar 2 "+key+" " +  keySMx.get(key)+ " RM"+i+".txt'");

				System.out.println(key+" - "+key_AssignedMachine.get(key)+
						" - RM" + i +".txt");
				keyRMx.put(key, "RM"+ i +".txt");
				i++;
				reduceProcesses.add(pb.start());
			}
			
			//wait for reduce processes
			for (Process p : reduceProcesses) {
				try {
					p.waitFor();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			
			System.out.println("\tDone: Reducing.");
		} catch (Exception e) {
			System.out.println(e);
		}

	}
	
	/**
	 * Method that reads the output of the reduce jobs done by the distant machines.
	 */
	public void collectReduce() {

		System.out.println("\n\tReduce Results...");
		System.out.println("\nKey -- Count");
		
		HashSet<Process> readerProcesses=new HashSet<Process>();
		
		try {
			
			for (String machine : machines) {

				ProcessBuilder pb = new ProcessBuilder("bash", "-c",
						"ssh atoon@" + machine + " 'cat /tmp/atoon/reduces/RM*.txt'");

				pb.inheritIO();
				readerProcesses.add(pb.start());
			}
			
			//wait for reader processes
			for (Process p : readerProcesses) {
				try {
					p.waitFor(2, TimeUnit.SECONDS);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			
			System.out.println("\tDone: Map-Reduce!!!");
		} catch (Exception e) {
			System.out.println(e);
		}

	}

}
