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
    static final private int N = 1;
    static final private int R = 1;
    static final private int W = 1;

    //Timeout Interval in ms
    static final private int T = 2000;

    public static void main(String[] args) {

        if (args.length != 1 && args.length != 3) {
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
            remotePath = "akka.tcp://mysystem@" + ip + ":" + port + "/user/node1";
            System.out.println("Starting node " + myId + "; bootstrapping node: " + ip + ":" + port);
        } else {
            System.out.println("Starting disconnected node " + myId);
        }

        // Create the actor system
        final ActorSystem system = ActorSystem.create("mysystem", config);

        // Create a single node actor
        final ActorRef receiver = system.actorOf(
                Props.create(Node.class), // actor class 
                args[0] // actor name
        );

        if (args[0].equals("node3")) {

            try {
                sleep(1000);
            } catch (InterruptedException ex) {
                Logger.getLogger(NodeApp.class.getName()).log(Level.SEVERE, null, ex);
            }

            System.out.println("-------------> Trying to send message");
            //system.actorSelection(remotePath).tell(new GetKey(2), receiver); 
            //system.actorSelection("akka.tcp://mysystem@127.0.0.1:10002/user/node2").tell(new GetKey(1), receiver);

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
           // system.actorSelection("akka.tcp://mysystem@127.0.0.1:10002/user/node2").tell(new GetKey(2), receiver);

            //system.actorSelection(remotePath).tell(new GetKey(2), receiver);
        }
    }

    public static class Node extends UntypedActor {

        // The table of all nodes in the system id->ref
        private final Map<Integer, ActorRef> nodes = new TreeMap<>();
        //In-Memory copy of the stored Items
        private final Map<Integer, LocalItem> localItems = new HashMap<>();

        //Variables used when peer is coordinator
        //to buffer messages until quorum is reached
        private ArrayList<LocalItem> bufferedItems = null;
        //keep track of the client that initiated the request
        private ActorRef client = null;

        //remember if writeQuorum; readQuorum otherwise
        private boolean writeQuorum = false;
        //keep track of the item the user sent
        private LocalItem latestItem = null;

        //Timeout variables
        //timer used to schedule and fire tasks
        private final Timer timer = new java.util.Timer();
        //task containing the code to run in case of timeout
        private TimerTask timerTask;
        //keep track if timeout has fired
        private boolean TO = false;

        @Override
        public void preStart() {
            //retrieve persistedStore if exists here
            if (remotePath != null) {
                getContext().actorSelection(remotePath).tell(new RequestNodelist(), getSelf());
            }
            nodes.put(myId, getSelf());
           /* localItems.put(1, new LocalItem(1,"test"+1 ,1));   
            localItems.put(2, new LocalItem(2,"test"+2 ,1)); 
            //localItems.put(3, new LocalItem(3,"test"+3 ,1));
            localItems.put(100, new LocalItem(100,"test"+100 ,1)); 
           // localItems.put(50, new LocalItem(50,"test"+50 ,1));/**/
        }

        @Override
        public void onReceive(Object message) {

            if (message instanceof RequestNodelist) {
                getSender().tell(new Nodelist(nodes), getSelf());
            } 
            else if (message instanceof Nodelist) {
                nodes.putAll(((Nodelist) message).nodes);
                
                for (Integer node : nClockwiseNodes(myId+1, 1)) {
                    (nodes.get(node)).tell(new RequestItemlist(myId), getSelf());
                }
            } 
            //Return the nodes the requester is responsible for
            if (message instanceof RequestItemlist) {
                final int id = ((RequestItemlist)message).id;
                final Map<Integer, ImmutableItem> repartitionItems = new HashMap<>();
                
                for(Integer key : localItems.keySet()){
                    if( (myId > id && key < myId) 
                            || key == id
                          //  || (myId < id && key <= id && key > id) 
                            || (nodes.size() < N)){
                        LocalItem item = localItems.get(key);
                        repartitionItems.put(key, new ImmutableItem(item.getKey(),
                                                                    item.getValue(),
                                                                    item.getVersion()));
                    }
                    else{
                        //System.out.println("Calculating responsible nodes for key :" +key);
                        nodes.put(id, null);
                        ArrayList<Integer> responsibleNodes = nClockwiseNodes(key, N);
                        nodes.remove(id);
                        if(responsibleNodes.contains(id)){
                            LocalItem item = localItems.get(key);
                            repartitionItems.put(key, new ImmutableItem(item.getKey(),
                                                                    item.getValue(),
                                                                    item.getVersion()));
                         }
                    }
                }
                getSender().tell(new ItemList(repartitionItems), getSelf());
            } 
            //Receiving items I am responsible for
            else if (message instanceof ItemList){
                final Map<Integer, ImmutableItem> receivedItems = ((ItemList)message).items;
                //Add them to my localItems
                for(Integer key : receivedItems.keySet()){
                    ImmutableItem item = receivedItems.get(key);
                    localItems.put(key, new LocalItem(item.getKey(),
                                                      item.getValue(),
                                                      item.getVersion()));
                }
                //Then persist
                System.out.println("Received "+ receivedItems.keySet());
                //Announce my joining                
                for (ActorRef n : nodes.values()) {
                        n.tell(new Join(myId), getSelf());
                }
            }
            else if (message instanceof Join) {
                int id = ((Join) message).id;
                System.out.println("Node " + id + " joined");
                nodes.put(id, getSender());
                
                if(myId != id && !localItems.isEmpty()){
                    Iterator it = localItems.keySet().iterator();
                    while(it.hasNext()){
                        Integer key = (Integer) it.next();
                        ArrayList<Integer> responsibleNodes = nClockwiseNodes(key, N);
                        if(!responsibleNodes.contains(myId))
                            it.remove();
                    }
                }                   
                //Printout
                System.out.println("After "+id+" has joined ");
                System.out.println("["+getSelf().path().name()+"] : "+localItems.keySet());
            } 
            //When receiving a GetKey request message
            else if (message instanceof GetKey) {
                //extract the keyId from the message
                Integer itemKey = ((GetKey) message).keyId;

                //TODO: Check to see if I am coordinator (node1 for testing)
                if (getSelf().path().name().equals("node2")) {

                    System.out.println("[GET] I AM COORDINATOR NODE " + getSelf().path().name());
                    //keep track of client to later respond
                    client = getSender();
                    //initialise bufferedDataItems, anticipating incoming dataItems from peers
                    bufferedItems = new ArrayList<>();

                    //Retrieve Nodes of interest
                    ArrayList<Integer> interestedNodes = nClockwiseNodes(itemKey, N);

                    //If R = 1 and I am part of the interested nodes then no need to pass through the network
                    if (interestedNodes.contains(myId) && R == 1) {
                        //simply reply to client
                        LocalItem item = localItems.get(itemKey);
                        client.tell(new DataItem(new ImmutableItem(
                                                    item.getKey(),
                                                    item.getValue(),
                                                    item.getVersion()))
                                    , getSelf());
                        //and cleanup variables state
                        cleanup();
                    } //for when contacting other peers is needed
                    else {
                        for (Integer node : interestedNodes) {
                            //If I am interested
                            if (node.equals(myId)) {
                                //retrieve the item locally and buffer it
                                if (localItems.containsKey(itemKey)) {
                                    bufferedItems.add(localItems.get(itemKey));
                                } else {
                                    bufferedItems.add(new LocalItem(itemKey, "", 0));
                                }
                            } //If different node
                            else {
                                //send same GetKey request but with coordinator as sender
                                (nodes.get(node)).tell(message, getSelf());
                            }
                        }
                        // System.out.println("Starting timer");
                        //timer = new java.util.Timer();   
                        //<start timer somewhere here>
                        setTimoutTask();
                        timer.schedule(timerTask, T);
                    }
                } 
                //I am simply a peer, I just need to return my local copy
                else {
                    System.out.println("Sleeping 5 sec before replying");
                    try {
                        sleep(1000);
                    } catch (InterruptedException ex) {
                        Logger.getLogger(NodeApp.class.getName()).log(Level.SEVERE, null, ex);
                    }
                    System.out.println("[GET] I AM PEER NODE " + getSelf().path().name());
                    //respond to the sender with the local dataItem having that key or a "not present" dataItem
                    DataItem dataItem = new DataItem(new ImmutableItem(itemKey, "", 0));
                    if (localItems.containsKey(itemKey)) {
                        LocalItem item = localItems.get(itemKey);
                        dataItem = new DataItem(new ImmutableItem(
                                                    item.getKey(),
                                                    item.getValue(),
                                                    item.getVersion()));
                    }
                    getSender().tell(dataItem, getSelf());
                }
            } 
            //When receiving an Update request message
            else if (message instanceof Update) {
                //extract the keyId from the message
                Integer itemKey = ((Update) message).keyId;
                String itemValue = ((Update) message).value;
                int itemVersion = ((Update) message).version;

                //TODO: Check to see if I am coordinator (pre-set for testing)
                if (getSelf().path().name().equals("node2")) {

                    System.out.println("[Update] I AM COORDINATOR NODE " + getSelf().path().name());

                    //keep track of client to later respond
                    client = getSender();

                    //initialise bufferedItems, anticipating incoming Items from peers
                    bufferedItems = new ArrayList<>();

                    //I am trying to establish an Update Quorum
                    writeQuorum = true;
                    //as far as I know the dataItem I received is the latest
                    latestItem = new LocalItem(itemKey, itemValue, itemVersion);

                    LocalItem item = new LocalItem(itemKey, "", 0);
                    //Retrieve Nodes of interest
                    ArrayList<Integer> interestedNodes = nClockwiseNodes(itemKey, N);

                    //If R = W = 1 and I am part of the interested nodes then no need to pass through the network
                    if (interestedNodes.contains(myId) && R == 1 && W == 1) {
                        if (localItems.containsKey(itemKey)) {
                            item = localItems.get(itemKey);
                        }
                        latestItem.setVersion(item.getVersion() + 1);
                        //simply reply to client
                        client.tell(new DataItem(new ImmutableItem(
                                                       latestItem.getKey(),
                                                       latestItem.getValue(),
                                                       latestItem.getVersion()))
                                   , getSelf());
                        
                        localItems.put(itemKey, latestItem);
                        //and cleanup variables state
                        cleanup();
                    } else {
                        for (Integer node : interestedNodes) {
                            //If I am interested
                            if (node.equals(myId)) {
                                if (localItems.containsKey(itemKey)) {
                                    item = localItems.get(itemKey);
                                }
                                //retrieve the item locally and buffer it
                                bufferedItems.add(item);
                            } //If different node
                            else {
                                //send same GetKey request but with coordinator as sender
                                (nodes.get(node)).tell(new GetKey(itemKey), getSelf());
                            }
                        }

                        //<start timer somewhere here>
                        setTimoutTask();
                        timer.schedule(timerTask, T);
                    }
                } 
                //I am peer so I should just write
                else {
                    System.out.println("[Update] I AM PEER NODE " + getSelf().path().name());
                    //simply write the Item I received
                    LocalItem newItem = new LocalItem(itemKey, itemValue, itemVersion);
                    //save or replace the Item
                    localItems.put(itemKey, newItem);
                }
            } 
            //When receiving a DataItem as response
            else if (message instanceof DataItem) {
                System.out.println("TO status: " + TO);
                //if not null then I previously initiated a read quorum request 
                if (bufferedItems != null && !TO) {
                    /**
                     * if not enough replies received two cases: ReadQuorum - if
                     * not enough replies yet and not a writeQuorum expected
                     * WriteQuorum - if not enough replies yet and writeQuorum
                     * expected
                     */
                    if ((bufferedItems.size() < R && !writeQuorum)
                            || (bufferedItems.size() < Integer.max(R, W) && writeQuorum)) {
                        //buffer the freshly received Item
                        ImmutableItem item = ((DataItem) message).item;
                        bufferedItems.add(new LocalItem(item.getKey(),item.getValue(),item.getVersion()));
                    }
                    System.out.println("THIS IS SIZE " + bufferedItems.size());
                    System.out.println(bufferedItems.toString());
                    //Read/Write Quorum reached: I have received enough replies 
                    if ((bufferedItems.size() == R && !writeQuorum)
                            || (bufferedItems.size() == Integer.max(R, W) && writeQuorum)) {

                        //Stop timeout timer: Quorum reached
                        System.out.println("Stopping timer");
                        timerTask.cancel();

                        //Find latest item based on version
                        for (LocalItem i : bufferedItems) {
                            if (latestItem == null) {
                                latestItem = i;
                            }
                            //replace latestItem with the item having highest version
                            if (i.getVersion() > latestItem.getVersion()) {
                                latestItem = i;
                            }
                        }

                        //If client ActorRef exists
                        if (client != null) {
                            System.out.println("Requester is: " + client.path().name());
                            //if I am trying to establish a writeQuorum
                            if (writeQuorum) {
                                //Increment item version before writing
                                latestItem.setVersion(latestItem.getVersion() + 1);

                                System.out.println("Responding to: " + client.path().name() + " with " + latestItem.getKey() + " " + latestItem.getVersion());
                                //TODO: Answer client - successful write
                                client.tell(new DataItem(new ImmutableItem(
                                                            latestItem.getKey(),
                                                            latestItem.getValue(),
                                                            latestItem.getVersion()))
                                            , getSelf());

                                //for every interestedNode
                                for (Integer node : nClockwiseNodes(latestItem.getKey(), N)) {
                                    //If I am interested
                                    if (node.equals(myId)) {
                                        //perform local update
                                        localItems.put(latestItem.getKey(), latestItem);
                                    } //If different node
                                    else {
                                        //send Update request to other interestedNodes with latest Item
                                        nodes.get(node).tell(
                                                new Update(latestItem.getKey(),
                                                        latestItem.getValue(),
                                                        latestItem.getVersion()), getSelf());
                                    }
                                }
                            } 
                            //readQuorum = !writeQuorum
                            else {
                                System.out.println("Sending answer to " + client.path().name());
                                client.tell(new DataItem(new ImmutableItem(
                                                       latestItem.getKey(),
                                                       latestItem.getValue(),
                                                       latestItem.getVersion()))
                                            , getSelf());
                            }
                        }
                        //quorum reached - cleanup
                        cleanup();
                    } 
                    //messages after quorum reached are not needed
                    else 
                    {
                        unhandled(message);
                    }
                } 
                else {
                    if (TO == true)
                        System.out.println("Timedout");
                    System.out.println(((DataItem) message).item.getKey() + " "
                            + ((DataItem) message).item.getValue() + " "
                            + ((DataItem) message).item.getVersion());
                    //no quorum initiated because bufferedDataItems == null
                    //unhandled(message);
                    //System.out.println("Not expecting any messages... No quorum initated");
                }
            } 
            //Do not handle messages you don't know
            else {
                unhandled(message);
            }
        }

        //check on number of nodes should be done earlier
        public ArrayList<Integer> nClockwiseNodes(Integer itemKey, int n) {
            //N replication parameter
            ArrayList<Integer> interestedNodes = new ArrayList<>();
            Set<Integer> keys = nodes.keySet();

            System.out.println("[ClockwiseNodes] Printing key set " + keys.toString());

            Iterator it = keys.iterator();
            Integer key;

            //find the N - k clockwise nodes (where k is the number of nodes with id < itemKey)
            while (interestedNodes.size() < n && it.hasNext()) {
                key = (Integer) it.next();
                if (key != null && key >= itemKey) {
                    interestedNodes.add(key);
                    itemKey = key + 1;
                }
            }

            //for circularity: start over and get the k remaining nodes
            it = keys.iterator();
            while (interestedNodes.size() < n && it.hasNext()) {
                key = (Integer) it.next();
                interestedNodes.add(key);
            }

            System.out.println("[ClockwiseNodes] Selected Nodes " + interestedNodes.toString());
            return interestedNodes;
        }

        //resets values used when establishing read/write quorums
        public void cleanup() {
            client = null;
            bufferedItems = null;
            writeQuorum = false;
            latestItem = null;
            timerTask = null;
            TO = false;
        }

        public void setTimoutTask() {
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