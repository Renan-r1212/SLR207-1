
import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class Master {

    public static void main(String[] args) {

        ProcessBuilder pb = new ProcessBuilder("java", "-jar", "/Users/anna-monica/Desktop/TP-SLR207/Etape4/Slave.jar");
//        ProcessBuilder pb = new ProcessBuilder("java", "-jar","/tmp/atoon/Slave.jar");
        try {

            Process p = pb.start();

            LinkedBlockingQueue<String> answerQueue = new LinkedBlockingQueue();
            LinkedBlockingQueue<String> errorQueue = new LinkedBlockingQueue();

            Thread answer = new OutputThread(answerQueue, p);
            Thread error = new ErrorThread(errorQueue, p);

            answer.start();
            error.start();

            try {

                String output = answerQueue.poll(6, TimeUnit.SECONDS);
                String err = errorQueue.poll(6, TimeUnit.SECONDS);

                if (output == null && err == null) {
                    System.out.println("Timeout!");
                    answer.interrupt();
                    error.interrupt();
                    p.destroy();
                } else if (!output.equals("")) {
                    System.out.println("Ouput:\n" + output);
                } else {
                    System.out.println("Errors:\n" + err);
                }

            } catch (InterruptedException ex) {
                System.out.println(ex);
            }

//            pb.redirectErrorStream(true);
//            pb.redirectOutput(Redirect.INHERIT);
        } catch (IOException ex) {
            System.out.println(ex);
        }

    }

}
