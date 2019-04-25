
import java.io.*;
import java.util.*;

public class Etape0 {

    HashMap<String, Integer> words;
    FileInputStream file;

    public Etape0(FileInputStream f) {
        words = new HashMap<String, Integer>();
        file = f;
    }

    public static void main(String[] args) {
        try {
//            etape0();
//            etape4();
//            etape567("deontologie_police_nationale.txt");//5 premiers mots: de=86, la=40, police=29, et=27, à=25
//            etape567("domaine_public_fluvial.txt");//5 premiers mots: de=621, le=373, du=347, la=330, et=266
//            etape567("sante_publique.txt");//5 premiers mots: de=189699, la=74433, des=66705, à=65462, et=60940
//            etape8_time("sante_publique.txt");
            /**
             * Counting Time: 1469
             * Sorting Time: 53
             */
            etape8_time("CC-MAIN-20170322212949-00140-ip-10-233-31-227.ec2.internal.warc.wet");
            /**
             * Counting Time: 29403
             * Sorting Time: 6196
             */
        } catch (FileNotFoundException ex) {
            System.out.println(ex);
        }

    }

    void wordCount() {
        Scanner input = new Scanner(file);
        input.useDelimiter("[ \n]+");

        while (input.hasNext()) {
            String s = input.next();
            if (words.containsKey(s)) {
                words.put(s, words.get(s) + 1);
            } else {
                words.put(s, 1);
            }
        }

    }

    public static void etape0() throws FileNotFoundException {
        FileInputStream f0 = new FileInputStream("input.txt");
        Etape0 count0 = new Etape0(f0);
        count0.wordCount();
        count0.sort();
        System.out.println(count0);
    }

    public static void etape4() throws FileNotFoundException {
        FileInputStream f4 = new FileInputStream("forestier_mayotte.txt");
        Etape0 count4 = new Etape0(f4);
        count4.wordCount();
        count4.sort();
        System.out.println(count4);
    }

    public static void etape567(String filename) throws FileNotFoundException {
        FileInputStream f5 = new FileInputStream(filename);
        Etape0 count5 = new Etape0(f5);
        count5.wordCount();
        count5.sort();
//        System.out.println(count5);

        Map<String, Integer> first50 = count5.first50words();
        System.out.println(first50);

    }

    public static void etape8_time(String filename) throws FileNotFoundException {
        FileInputStream f5 = new FileInputStream(filename);
        Etape0 count5 = new Etape0(f5);
        long startTime = System.currentTimeMillis();
        count5.wordCount();
        long endTime = System.currentTimeMillis();
        long totalCountTime = endTime - startTime;

         startTime = System.currentTimeMillis();
        count5.sort();
         endTime = System.currentTimeMillis();
        long totalSortTime = endTime - startTime;
//        System.out.println(count5);
        System.out.println("Counting Time: "+totalCountTime);
        System.out.println("Sorting Time: "+totalSortTime);

        

    }

    public void sort() {
        // Create a list from elements of HashMap 
        List<Map.Entry<String, Integer>> list
                = new LinkedList<Map.Entry<String, Integer>>(words.entrySet());

        // Sort the list 
        Collections.sort(list, new Comparator<Map.Entry<String, Integer>>() {
            public int compare(Map.Entry<String, Integer> o1,
                    Map.Entry<String, Integer> o2) {
                if (((o1.getValue()).compareTo(o2.getValue())) == 0) {

                    return (o1.getKey()).compareTo(o2.getKey());
                } else {

                    return -1 * ((o1.getValue()).compareTo(o2.getValue()));
                }

            }
        });

        HashMap<String, Integer> temp = new LinkedHashMap<String, Integer>();
        for (Map.Entry<String, Integer> aa : list) {
            temp.put(aa.getKey(), aa.getValue());
        }

        words = temp;

    }

    public Map<String, Integer> first50words() {
        List<Map.Entry<String, Integer>> list
                = new LinkedList<Map.Entry<String, Integer>>(words.entrySet());

        Map<String, Integer> temp = new LinkedHashMap<String, Integer>();

        for (int i = 0; i < 50; i++) {
            Map.Entry<String, Integer> entry = list.get(i);
            temp.put(entry.getKey(), entry.getValue());
        }
        return temp;
    }

    @Override
    public String toString() {
        String res = "";
        for (String i : words.keySet()) {
            res += i + " : " + words.get(i) + '\n';
        }
        return res;
    }

}
