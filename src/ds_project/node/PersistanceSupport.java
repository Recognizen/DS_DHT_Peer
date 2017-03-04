/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ds_project.node;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 *
 * @author recognition
 */
public final class PersistanceSupport {

    public static void persistStore(Map<Integer, LocalItem> items, String fileName) throws IOException {
        FileWriter fw = new FileWriter(fileName);

        Iterator it = items.keySet().iterator();

        while (it.hasNext()) {
            LocalItem item = items.get((Integer) it.next());
            fw.write(item.toString() + "\n");
        }

        fw.close();
    }

    public static Map<Integer, LocalItem> retrieveStore(String fileName) throws FileNotFoundException, IOException {
        Map<Integer, LocalItem> items = new HashMap<>();

        String line;

        BufferedReader reader = new BufferedReader(new FileReader(fileName));

        while ((line = reader.readLine()) != null) {
            String[] l = line.split(" ");

            Integer key = Integer.valueOf(l[0]);
            items.put(key, new LocalItem(key, l[1], Integer.valueOf(l[2])));
            System.out.println(items.get(key).toString());
        }

        reader.close();
        
        return items;
    }
}
