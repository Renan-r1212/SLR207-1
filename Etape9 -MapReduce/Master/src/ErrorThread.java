
import java.io.*;
import java.util.concurrent.LinkedBlockingQueue;


/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
/**
 *
 * @author anna-monica
 */
public class ErrorThread extends Thread {

    private LinkedBlockingQueue<String> queue;
    private Process process;

    public ErrorThread(LinkedBlockingQueue<String> queue, Process process) {
        this.queue = queue;
        this.process = process;
    }

    @Override
    public void run() {
        try {

            BufferedReader reader
                    = new BufferedReader(new InputStreamReader(process.getErrorStream()));
            StringBuilder builder = new StringBuilder();
            String line = null;
            while ((line = reader.readLine()) != null) {
                builder.append(line);
                builder.append(System.getProperty("line.separator"));
            }
            String result = builder.toString();
            queue.put(result);

        } catch (IOException ex) {
            System.out.println(ex);
        } catch (InterruptedException ex) {

        }

    }

}
