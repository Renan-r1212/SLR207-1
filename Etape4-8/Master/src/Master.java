
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
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

    public static void main(String[] args) {
    	Master d=new Master();
        d.readFile();
    }

    public Master() {
        this.answerThreads = new ArrayList<Thread>();
        this.errorThreads = new ArrayList<Thread>();
        this.processes = new ArrayList<Process>();
        this.machines=new ArrayList<String>();
        this.answerQueues = new ArrayList<LinkedBlockingQueue<String>>() ;
        this.errorQueues = new ArrayList<LinkedBlockingQueue<String>>() ;
        
        
    }
    
    public void readFile() {

    	BufferedReader reader = null;

        try {
            reader = new BufferedReader(new FileReader("machines.txt"));
            String s = "";

            while ((s = reader.readLine()) != null) {

              machines.add(s);
                ProcessBuilder pb = new ProcessBuilder("bash","-c","ssh atoon@c125-21 'java -jar /tmp/atoon/Slave1.jar'");
//                ProcessBuilder pb = new ProcessBuilder("bash","-c","ssh atoon@"+s+" 'java -jar /tmp/atoon/Slave.jar'");
//                pb.inheritIO();
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
                Logger.getLogger(Master.class.getName()).log(Level.SEVERE, null, ex);
            }

            for (int i = 0; i < answerThreads.size(); i++) {
                
                try {
                    String output = answerQueues.get(i).poll(6,TimeUnit.SECONDS);
                    String err = errorQueues.get(i).poll(6, TimeUnit.SECONDS);

                    if (output == null && err == null) {
                        System.out.println("Timeout--Thread "+i);
                        answerThreads.get(i).interrupt();
                        errorThreads.get(i).interrupt();
                        processes.get(i).destroy();
                    } else if (output!=null && ! output.equals("")) {
                        System.out.println("Ouput--Machine "+machines.get(i)+":\n" + output);
                    } else {
                        System.out.println("Errors--Machine "+machines.get(i)+":\n" + err);
                    }

                } catch (InterruptedException ex) {
                    System.out.println(ex);
                }
            }

        }
    }
}
