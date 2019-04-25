
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
/**
 *
 * @author anna-monica
 */
public class OutputThread extends Thread {

    private LinkedBlockingQueue<String> queue;
    private Process process;

    public OutputThread(LinkedBlockingQueue queue, Process process) {
        this.queue = queue;
        this.process = process;
    }

    @Override
    public void run() {
        try {
            BufferedReader reader
                    = new BufferedReader(new InputStreamReader(process.getInputStream()));
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
