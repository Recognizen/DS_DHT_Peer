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

public class NodeApp {
    static private String remotePath = null; // Akka path of the bootstrapping peer
    static private int myId; // ID of the local node    
    	
    public static class Node extends UntypedActor {
		
        // The table of all nodes in the system id->ref
        private Map<Integer, ActorRef> nodes = new HashMap<>();
        private Map<Integer, Item> dataItems = new HashMap<>();
        
        public void preStart() {
            if (remotePath != null) {
                getContext().actorSelection(remotePath).tell(new RequestNodelist(), getSelf());
               // getContext().actorSelection(remotePath).tell(new GetKey(1), getSelf());
            }
            nodes.put(myId, getSelf());
            dataItems.put(myId, new Item(myId,"test"+myId ,1));
            
        }

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
            
            //When receiving a GetKey request message
            else if (message instanceof GetKey){
                //extract the keyId from the message
                Integer itemKey = ((GetKey)message).keyId;
                //respond to the sender with the local dataItem having that key
                getSender().tell(new DataItem(dataItems.get(itemKey)), getSelf());
            }
            //When receiving a DataItem as response
            else if(message instanceof DataItem){
                Item item = ((DataItem)message).item;
                System.out.println(item.getKey() + " , "+item.getValue() + " , "+ item.getVersion());
            }
            else
            	unhandled(message);		// this actor does not handle any incoming messages
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
            system.actorSelection(remotePath).tell(new GetKey(1), receiver); 
            system.actorSelection(remotePath).tell(new Update(1, "yoyo"), receiver);
            system.actorSelection(remotePath).tell(new GetKey(1), receiver);
        }
    }
}
