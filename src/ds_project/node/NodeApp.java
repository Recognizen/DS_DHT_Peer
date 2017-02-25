package ds_project.node;

import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import akka.actor.ActorRef;
import akka.actor.UntypedActor;
import akka.actor.ActorSystem;
import akka.actor.Props;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.Config;
import ds_project.node.Messages.*;
import static java.lang.Thread.sleep;
import java.util.Iterator;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeMap;
import java.util.logging.Level;
import java.util.logging.Logger;

public class NodeApp {
    static private String remotePath = null; // Akka path of the bootstrapping peer
    static private int myId; // ID of the local node    
    
    //Replication Parameters
    static final private int N = 2;
    static final private int R = 2;
    static final private int W = 2;
    
    //Timeout Interval in ms
    static final private int T = 2000;
    
    
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
            remotePath = "akka.tcp://mysystem@"+ip+":"+port+"/user/node1";
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
            
            try {
                sleep(1000);
            } catch (InterruptedException ex) {
                Logger.getLogger(NodeApp.class.getName()).log(Level.SEVERE, null, ex);
            }
            
            System.out.println("-------------> Trying to send message");
            //system.actorSelection(remotePath).tell(new GetKey(1), receiver); 
            system.actorSelection("akka.tcp://mysystem@127.0.0.1:10002/user/node2").tell(new GetKey(2), receiver); 
            
             try {
                sleep(5000);
            } catch (InterruptedException ex) {
                Logger.getLogger(NodeApp.class.getName()).log(Level.SEVERE, null, ex);
            }
            //system.actorSelection(remotePath).tell(new Update(2, "truffles"), receiver);
           // system.actorSelection("akka.tcp://mysystem@127.0.0.1:10002/user/node2").tell(new Update(2, "truffles"), receiver); 
            
             try {
                sleep(5000);
            } catch (InterruptedException ex) {
                Logger.getLogger(NodeApp.class.getName()).log(Level.SEVERE, null, ex);
            }
            //system.actorSelection(remotePath).tell(new GetKey(2), receiver);
           //  system.actorSelection("akka.tcp://mysystem@127.0.0.1:10002/user/node2").tell(new GetKey(2), receiver); 
            
            //system.actorSelection(remotePath).tell(new GetKey(2), receiver);
        }
    }
    
    public static class Node extends UntypedActor {
		
        // The table of all nodes in the system id->ref
        private final Map<Integer, ActorRef> nodes = new TreeMap<>();
        //In-Memory copy of the stored Items
        private final Map<Integer, Item> dataItems = new HashMap<>();
        
        //Variables used when peer is coordinator
        
        //to buffer messages until quorum is reached
        private ArrayList<Item> bufferedDataItems = null;
        //keep track of the client that initiated the request
        private ActorRef client = null;
        
        //remember if writeQuorum; readQuorum otherwise
        private boolean writeQuorum = false;
        //keep track of the item the user sent
        private Item latestItem = null;
        
        //Timeout variables
        
        //timer used to schedule and fire tasks
        private final Timer timer = new java.util.Timer();
        //task containing the code to run in case of timeout
        private TimerTask timerTask;
        //keep track if timeout has fired
        private boolean TO = false;
        
        @Override
        public void preStart() {
            if (remotePath != null) {
                getContext().actorSelection(remotePath).tell(new RequestNodelist(), getSelf());
            }
            nodes.put(myId, getSelf());
            dataItems.put(1, new Item(1,"test"+1 ,1));   
            dataItems.put(2, new Item(2,"test"+2 ,1));    
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
                if(getSelf().path().name().equals("node2")){
                    
                    System.out.println("[GET] I AM COORDINATOR NODE "+ getSelf().path().name() );
                    //keep track of client to later respond
                    client = getSender();        
                    //initialise bufferedDataItems, anticipating incoming dataItems from peers
                    bufferedDataItems = new ArrayList<>();
      
                    //Retrieve Nodes of interest
                    ArrayList<Integer> interestedNodes = calculateInterestedNodes(itemKey);
                    
                    //If R = 1 and I am part of the interested nodes then no need to pass through the network
                    if(interestedNodes.contains(myId) && R == 1){
                        //simply reply to client
                        client.tell(new DataItem(dataItems.get(itemKey)), getSelf());
                        //and cleanup variables state
                        cleanup();
                    }
                    //for when contacting other peers is needed
                    else{
                        for(Integer node : interestedNodes){
                            //If I am interested
                            if(node.equals(myId)){
                                //retrieve the item locally and buffer it
                                bufferedDataItems.add(dataItems.get(itemKey));
                            }
                            //If different node
                            else{    
                                //send same GetKey request but with coordinator as sender
                                (nodes.get(node)).tell(message, getSelf());
                            }
                        }
                       // System.out.println("Starting timer");
                        //timer = new java.util.Timer();   
                        //<start timer somewhere here>
                        setTimoutTask();
                        timer.schedule(timerTask,T);
                    }
                } 
                //I am simply a peer, I just need to return my local copy
                else{
                    System.out.println("Sleeping 5 sec before replying");
                    try {
                        sleep(5000);
                    } catch (InterruptedException ex) {
                        Logger.getLogger(NodeApp.class.getName()).log(Level.SEVERE, null, ex);
                    }
                    System.out.println("[GET] I AM PEER NODE "+ getSelf().path().name() );
                    //respond to the sender with the local dataItem having that key or a "not present" dataItem
                    DataItem item = new DataItem(new Item(itemKey, "", 0));
                    if(dataItems.containsKey(itemKey))
                        item = new DataItem(dataItems.get(itemKey));
                    System.out.println("From: "+getSender().path().name());
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
                if(getSelf().path().name().equals("node2")){
                    
                    System.out.println("[Update] I AM COORDINATOR NODE "+ getSelf().path().name() );
                    
                    //keep track of client to later respond
                    client = getSender();
                    
                    //initialise bufferedDataItems, anticipating incoming dataItems from peers
                    bufferedDataItems = new ArrayList<>();
                    
                    //I am trying to establish an Update Quorum
                    writeQuorum = true;
                    //as far as I know the dataItem I received is the latest
                    latestItem = new Item(itemKey, itemValue, itemVersion);
                    
                    Item item = new Item(itemKey, "", 0);
                    //Retrieve Nodes of interest
                    ArrayList<Integer> interestedNodes = calculateInterestedNodes(itemKey);
                    
                    //If R = W = 1 and I am part of the interested nodes then no need to pass through the network
                    if(interestedNodes.contains(myId) && R == 1 && W == 1){
                        if(dataItems.containsKey(itemKey))
                            item = dataItems.get(itemKey);
                        latestItem.setVersion(item.getVersion()+1);
                        //simply reply to client
                        client.tell(new DataItem(latestItem), getSelf());
                        dataItems.put(itemKey, latestItem);
                        //and cleanup variables state
                        cleanup();
                    }
                    else{
                        for(Integer node : interestedNodes){
                            //If I am interested
                            if(node.equals(myId)){
                                if(dataItems.containsKey(itemKey))
                                    item = dataItems.get(itemKey);
                                //retrieve the item locally and buffer it
                                bufferedDataItems.add(item);
                            }
                            //If different node
                            else{    
                                //send same GetKey request but with coordinator as sender
                                (nodes.get(node)).tell(new GetKey(itemKey), getSelf());
                            }
                        }

                        //<start timer somewhere here>
                        setTimoutTask();
                        timer.schedule(timerTask,T);
                    }
                } 
                //I am peer so I should just write
                else{
                    System.out.println("[Update] I AM PEER NODE "+ getSelf().path().name() );
                    //simply write the dataItem I received
                    Item newItem = new Item(itemKey, itemValue, itemVersion);
                    //save or replace the dataItem
                    dataItems.put(itemKey, newItem);
                }
            }
             //When receiving a DataItem as response
            else if(message instanceof DataItem){
                System.out.println("TO status: "+TO);
                //if not null then I previously initiated a read quorum request 
                if(bufferedDataItems != null && !TO){
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
                    System.out.println("THIS IS SIZE "+ bufferedDataItems.size());
                    System.out.println(bufferedDataItems.toString());
                    //Read/Write Quorum reached: I have received enough replies 
                    if((bufferedDataItems.size() == R && !writeQuorum) || 
                            (bufferedDataItems.size() == Integer.max(R,W) && writeQuorum)) {
                        
                        //<stop timeout timer>
                        System.out.println("Stopping timer");
                        timerTask.cancel();
                        //timer = null;
                        
                        //Find latest item based on version
                        for(Item i : bufferedDataItems){
                            if(latestItem == null)
                                latestItem = i;
                            //replace latestItem with the item having highest version
                            if(i.getVersion() > latestItem.getVersion())
                                latestItem = i;
                        }
                        
                        //If client ActorRef exists
                        if(client != null){
                            System.out.println("Requester is: "+client.path().name());
                            //if I am trying to establish a writeQuorum
                            if(writeQuorum){
                                //Increment item version before writing
                                latestItem.setVersion(latestItem.getVersion()+1);
                                
                                
                                System.out.println("Responding to: "+client.path().name() + " with " + latestItem.getKey() + " " + latestItem.getVersion());
                                //TODO: Answer client - successful write
                                client.tell(new DataItem(latestItem), getSelf());
                                
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
                                System.out.println("Sending answer to " + client.path().name());
                                client.tell(new DataItem(latestItem), getSelf());
                            }
                        }
                        //quorum reached - cleanup
                        cleanup();
                    }
                    else
                        //messages after quorum reached are not needed
                        unhandled(message);
                }
                else{
                    if(TO == true)
                        System.out.println("Timedout");
                    System.out.println(((DataItem)message).item.getKey() + " "+ 
                            ((DataItem)message).item.getValue() +" " +
                            ((DataItem)message).item.getVersion());
                    //no quorum initiated because bufferedDataItems == null
                    //unhandled(message);
                    //System.out.println("Not expecting any messages... No quorum initated");
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
            
            //for circularity: start over and get the remaining nodes
            it = keys.iterator();
            while(interestedNodes.size() < N && it.hasNext()){
                key = (Integer) it.next();
                interestedNodes.add(key);
            }

            System.out.println("Selected Nodes "+interestedNodes.toString());
            return interestedNodes;
        }  
            
        //resets values used when establishing read/write quorums
        public void cleanup(){
            client = null;
            bufferedDataItems = null;
            writeQuorum = false;
            latestItem = null;
            timerTask = null;
            TO = false;
        }
        
        public void setTimoutTask(){
            timerTask = new java.util.TimerTask() {
                               @Override
                               public void run() {
                                   TO = true;
                                   System.out.println("Timeout here");
                                   //timer = null;
                                   client.tell("Timeout", getSelf());
                                   cleanup();
                               }
                           };
        }
    }
}
