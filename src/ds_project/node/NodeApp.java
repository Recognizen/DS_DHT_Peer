package ds_project.node;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Collections;
import akka.actor.ActorRef;
import akka.actor.UntypedActor;
import akka.actor.ActorSystem;
import akka.actor.Props;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.Config;
import ds_project.node.Messages.DataItem;
import ds_project.node.Messages.GetKey;
import ds_project.node.Messages.Update;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeMap;

public class NodeApp {
    static private String remotePath = null; // Akka path of the bootstrapping peer
    static private int myId; // ID of the local node    
    
    //Replication Parameters
    static final private int N = 1;
    static final private int R = 1;
    static final private int W = 1;
    
    public static class Node extends UntypedActor {
		
        // The table of all nodes in the system id->ref
        private final Map<Integer, ActorRef> nodes = new TreeMap<>();
        private final Map<Integer, Item> dataItems = new HashMap<>();
        
        private ArrayList<Item> bufferedDataItems = null;
        private ActorRef client = null;
        
        private boolean writeQuorum = false;
        private Item latestItem = null;
        
        @Override
        public void preStart() {
            if (remotePath != null) {
                getContext().actorSelection(remotePath).tell(new RequestNodelist(), getSelf());
               // getContext().actorSelection(remotePath).tell(new GetKey(1), getSelf());
            }
            nodes.put(myId, getSelf());
            nodes.put(70, getSelf());
            nodes.put(60, getSelf());
            dataItems.put(myId, new Item(61,"test"+myId ,1));     
        }

        @Override
        public void onReceive(Object message) {
            
            if (message instanceof RequestNodelist) {
                getSender().tell(new Nodelist(nodes), getSelf());
            }
            else if (message instanceof Nodelist) {
                nodes.putAll(((Nodelist)message).nodes);
                for (ActorRef n: nodes.values()) {
                    n.tell(new Join(myId), getSelf());
                }
            }
            else if (message instanceof Join) {
                    int id = ((Join)message).id;
                    System.out.println("Node " + id + " joined");
                    nodes.put(id, getSender());
            }  
            //When receiving a GetKey request message
            else if (message instanceof GetKey){
                //extract the keyId from the message
                Integer itemKey = ((GetKey)message).keyId;
                
                //TODO: Check to see if I am coordinator (node1 for testing)
                if(getSelf().path().name().equals("node1")){
                    //keep track of client to later respond
                    client = getSender();        
                    //initialise bufferedDataItems, anticipating incoming dataItems from peers
                    bufferedDataItems = new ArrayList<>();
                    
                    //Calculate interested Nodes and iterate over them
                    for(Integer node : calculateInterestedNodes(itemKey)){
                        //If I am interested
                        if(node.equals(myId)){
                            //retrieve the item locally and buffer it
                            bufferedDataItems.add(dataItems.get(itemKey));
                        }
                        //If different node
                        else{    
                            //send same GetKey request but with coordinator as sender
                            nodes.get(node).tell(message, getSelf());
                        }
                    }
                    //<start timer somewhere here>
                } 
                //I am simply a peer, I just need to return my local copy
                else{
                    //respond to the sender with the local dataItem having that key or a "not present" dataItem
                    DataItem item = new DataItem(new Item(itemKey, "", 0));
                    if(dataItems.containsKey(itemKey))
                        item = new DataItem(dataItems.get(itemKey));
                    getSender().tell(item, getSelf());
                }
            }
            
            //When receiving an Update request message
            else if (message instanceof Update){
                //extract the keyId from the message
                Integer itemKey = ((Update)message).keyId;
                String itemValue = ((Update)message).value;
                int itemVersion = ((Update)message).version;
                
                 //TODO: Check to see if I am coordinator (node1 for testing)
                if(getSelf().path().name().equals("node1")){
                    
                    //keep track of client to later respond
                    client = getSender();
                    
                    //initialise bufferedDataItems, anticipating incoming dataItems from peers
                    bufferedDataItems = new ArrayList<>();
                    
                    //I am trying to establish an Update Quorum
                    writeQuorum = true;
                    //as far as I know the dataItem I received is the latest
                    latestItem = new Item(itemKey, itemValue, itemVersion);
                    
                    //Calculate interested Nodes and iterate over them
                    for(Integer node : calculateInterestedNodes(itemKey)){
                        //If I am interested
                        if(node.equals(myId)){
                            //retrieve the item locally and buffer it
                            bufferedDataItems.add(dataItems.get(itemKey));
                        }
                        //If different node
                        else{    
                            //send same GetKey request but with coordinator as sender
                            nodes.get(node).tell(message, getSelf());
                        }
                    }
                    //<start timer somewhere here>
                } 
                //I am peer so I should just write
                else{
                    //simply write the dataItem I received
                    Item newItem = new Item(itemKey, itemValue, itemVersion);
                    //save or replace the dataItem
                    dataItems.put(itemKey, newItem);
                }
            }
             //When receiving a DataItem as response
            else if(message instanceof DataItem){
                //if not null then I previously initiated a read quorum request 
                if(bufferedDataItems != null){
                    /**if not enough replies received 
                    * two cases: 
                    *    ReadQuorum - if not enough replies yet and not a writeQuorum expected
                    *    WriteQuorum - if not enough replies yet and writeQuorum expected
                    */
                    if( (bufferedDataItems.size() < R && !writeQuorum) || 
                            (bufferedDataItems.size() < Integer.max(R,W) && writeQuorum)){
                        //buffer the freshly received dataItem
                        Item item = ((DataItem)message).item;
                        bufferedDataItems.add(item);
                    }
                    //Read/Write Quorum reached: I have received enough replies 
                    else if((bufferedDataItems.size() == R && !writeQuorum) || 
                            (bufferedDataItems.size() == Integer.max(R,W) && writeQuorum)) {
                        
                        //<stop timeout timer>
                        
                        //Find latest item based on version
                        for(Item i : bufferedDataItems){
                            //replace latestItem with the item having highest version
                            if(i.getVersion() > latestItem.getVersion())
                                latestItem = i;
                        }
                        
                        //If client ActorRef exists
                        if(client != null){
                            //if I am trying to establish a writeQuorum
                            if(writeQuorum){
                                //TODO: Answer client - successful write
                                client.tell(new DataItem(latestItem), getSelf());
                                //Increment item version before writing
                                latestItem.setVersion(latestItem.getVersion()+1);
                                //for every interestedNode
                                for(Integer node : calculateInterestedNodes(latestItem.getKey())){
                                    //If I am interested
                                    if(node.equals(myId)){
                                        //perform local update
                                        dataItems.put(latestItem.getKey(), latestItem);
                                    }
                                    //If different node
                                    else{    
                                        //send Update request to other interestedNodes with latest dataItem
                                        nodes.get(node).tell(
                                                new Update(latestItem.getKey(),
                                                           latestItem.getValue(),
                                                           latestItem.getVersion())
                                                , getSelf());
                                    }
                                }
                            }
                            //readQuorum = !writeQuorum
                            else{
                                client.tell(new DataItem(latestItem), getSelf());
                            }
                        }
                        //quorum reached - reset values
                        client = null;
                        bufferedDataItems = null;
                        writeQuorum = false;
                        latestItem = null;
                    }
                    else
                        unhandled(message);
                }
                else{
                    //no quorum initiated because bufferedDataItems == null
                    unhandled(message);
                    System.out.println("Not expecting any messages... No quorum initated");
                }    
            }
            //Do not handle messages you don't know
            else
            	unhandled(message);
        }
        
        //check on number of nodes should be done earlier
        public ArrayList<Integer> calculateInterestedNodes(Integer itemKey){
            //N replication parameter
            ArrayList<Integer> interestedNodes = new ArrayList<>();
            Set<Integer> keys = nodes.keySet();
            
            System.out.println("Printing key set "+ keys.toString());
            
            Iterator it = keys.iterator();
            
            Integer key;
            
            //find the N clockwise nodes
            while(interestedNodes.size() < N && it.hasNext()){
                key = (Integer) it.next();
                if(key != null && key >= itemKey){
                    interestedNodes.add(key);
                    itemKey = key+1;
                }
            }
            
            it = keys.iterator();
            while(interestedNodes.size() < N && it.hasNext()){
                key = (Integer) it.next();
                interestedNodes.add(key);
            }
            
            //find the N clockwise nodes
            /*do{
                key = keys.ceiling(itemKey);
                if(key != null){
                    interestedNodes.add(key);
                    itemKey = key+1;
                }
            } while(interestedNodes.size() == N || key == null);
            
            //need to wrap around the set until N nodes are found
            itemKey = 0;
            
            while(interestedNodes.size()<N){
                key = keys.floor(itemKey);
                interestedNodes.add(key);
                itemKey = key + 1;
            }
            */
            System.out.println("Selected Nodes "+interestedNodes.toString());
            return interestedNodes;
        }
       
    }    
    
    // ****** Messages needed to establish DHT network *****
    public static class RequestNodelist implements Serializable {}
    
    public static class Nodelist implements Serializable {
        Map<Integer, ActorRef> nodes;
        public Nodelist(Map<Integer, ActorRef> nodes) {
            this.nodes = Collections.unmodifiableMap(new HashMap<>(nodes)); 
        }
    }
    
    public static class Join implements Serializable {
        final int id;
        public Join(int id) {
            this.id = id;
        }
    }

    public static void main(String[] args) {
		
        if (args.length != 1 && args.length !=3 ) {
            System.out.println("Wrong number of arguments: [conf] (+ [remote_ip remote_port] )");
            return;
        }

        // Load the "application.conf"
        Config config = ConfigFactory.load(args[0]);
        myId = config.getInt("nodeapp.id");
        if (args.length == 3) {
            // Starting with a bootstrapping node
            String ip = args[1];
            String port = args[2];
            // The Akka path to the bootstrapping peer
            remotePath = "akka.tcp://mysystem@"+ip+":"+port+"/user/node";
            System.out.println("Starting node " + myId + "; bootstrapping node: " + ip + ":"+ port);
        }
        else 
            System.out.println("Starting disconnected node " + myId);

        // Create the actor system
        final ActorSystem system = ActorSystem.create("mysystem", config);

        // Create a single node actor
        final ActorRef receiver = system.actorOf(
                        Props.create(Node.class),	// actor class 
                        args[0]						// actor name
                        );
        
        if(args[0].equals("node3")){
            System.out.println("Trying to send message");
            system.actorSelection(remotePath).tell(new GetKey(61), receiver); 
            system.actorSelection(remotePath).tell(new Update(61, "yoyo"), receiver);
            system.actorSelection(remotePath).tell(new GetKey(61), receiver);
        }
    }
}
