import java.io.Serializable;
import java.util.concurrent.TimeUnit;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Collections;
import akka.actor.ActorRef;
import akka.actor.UntypedActor;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.actor.Cancellable;
import scala.concurrent.duration.Duration;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.Config;
import ds_project.node.Item;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

public class NodeApp {
    static private String remotePath = null; // Akka path of the bootstrapping peer
    static private int myId; // ID of the local node    
    
    //Replication Parameters
    static final private int N = 2;
    static final private int R = 2;
    static final private int W = 2;
    
    public static class Node extends UntypedActor {
		
        // The table of all nodes in the system id->ref
        private Map<Integer, ActorRef> nodes = new TreeMap<>();
        private Map<Integer, Item> dataItems = new HashMap<>();
        
        private ArrayList<Item> bufferedDataItems = null;
        private ActorRef client = null;
        
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

        @SuppressWarnings("empty-statement")
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
                
                if("Coordintor".equals(getSelf())){
                    
                    //keep track of client to later respond
                    client = getSender();
                    
                    //initialise bufferedDataItems, anticipating incoming dataItems from peers
                    bufferedDataItems = new ArrayList<>();
                    
                    //Calculate interested Nodes and iterate over them
                    for(Integer node : calculateNodes(itemKey)){
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
                    //respond to the sender with the local dataItem having that key
                    DataItem item = new DataItem(dataItems.get(itemKey));
                    getSender().tell(item, getSelf());
                }
            }
            //When receiving a DataItem as response
            else if(message instanceof DataItem){
                //if not null then I previously initiated a read quorum request 
                if(bufferedDataItems != null){
                    //if not enough replies received
                    if(bufferedDataItems.size() < R){
                        //buffer the freshly received dataItem
                        Item item = ((DataItem)message).item;
                        bufferedDataItems.add(item);
                    }
                    //Read Quorum reached: I have received R items 
                    else{
                        //Find latest item based on version
                        Item latestItem = null;
                        for(Item i : bufferedDataItems){
                            if(latestItem == null)
                                latestItem = i;
                            //Store the item with highest version
                            else if(i.getVersion() > latestItem.getVersion())
                                latestItem = i;
                        }
                        
                        //Return latest item to the client
                        if(client != null){
                            client.tell(new DataItem(latestItem), getSelf());
                            client = null;
                            bufferedDataItems = null;
                        }
                    }
                    //System.out.println(item.getKey() + " , "+item.getValue() + " , "+ item.getVersion());
                }
            }

            //When receiving a GetKey request message
            else if (message instanceof Update){
                //extract the keyId from the message
                Integer itemKey = ((Update)message).keyId;
                String itemValue = ((Update)message).value;
                
                //make a fresh item
                Item newItem = new Item(itemKey, itemValue, 1);
                //retrieve previous item if exists
                Item current = dataItems.get(itemKey);                
                if(current != null){
                    //update the version
                    newItem.setVersion(current.getVersion()+1);
                }
                //save or replace the dataItem
                dataItems.put(itemKey, newItem);
            }
            
            else
            	unhandled(message);		// this actor does not handle any incoming messages
        }
        
        //check on number of nodes should be done earlier
        public ArrayList<Integer> calculateNodes(Integer itemKey){
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
    
    public static class Update implements Serializable{
        final Integer keyId; 
        final String value;
        public Update(Integer keyId, String value){
            this.keyId = keyId;
            this.value = value;
        }
    } 

    public static class GetKey implements Serializable{
        final Integer keyId; 
        public GetKey(Integer keyId){
            this.keyId = keyId;
        }
    } 
    
    public static class DataItem implements Serializable{
        final Item item; 
        public DataItem(Item item){
            this.item = item;
        }
    }
    
    public static class Join implements Serializable {
        final int id;
        public Join(int id) {
            this.id = id;
        }
    }
    
    public static class RequestNodelist implements Serializable {}
    
    public static class Nodelist implements Serializable {
        Map<Integer, ActorRef> nodes;
        public Nodelist(Map<Integer, ActorRef> nodes) {
            this.nodes = Collections.unmodifiableMap(new HashMap<Integer, ActorRef>(nodes)); 
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
                        "node"						// actor name
                        );
        
        if(args[0].equals("node3")){
            System.out.println("Trying to send message");
            system.actorSelection(remotePath).tell(new GetKey(61), receiver); 
            system.actorSelection(remotePath).tell(new Update(61, "yoyo"), receiver);
            system.actorSelection(remotePath).tell(new GetKey(61), receiver);
        }
    }
}
