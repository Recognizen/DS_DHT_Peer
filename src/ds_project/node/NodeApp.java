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
import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeMap;
import ds_project.resources.LocalItem;
import ds_project.resources.Messages.*;
import ds_project.resources.PersistanceSupport;
import ds_project.ReplicationParameters;

public class NodeApp {

    static private String remotePath = null; // Akka path of the bootstrapping peer
    static private int myId; // ID of the local node
    static private String fileName = null;
    static private boolean recover = false;

    //Replication Parameters
    static final private int N = ReplicationParameters.N;
    static final private int R = ReplicationParameters.R;
    static final private int W = ReplicationParameters.W;

    //Timeout Interval in ms
    static final private int T = ReplicationParameters.T;

    public static void main(String[] args) throws InterruptedException {

        if (args.length != 1 && args.length != 4) {
            System.out.println("Wrong number of arguments: [conf] (+ operation + [remote_ip remote_port] )");
            return;
        }

        if (N >= R + W) {
            System.out.println("Replication parameters are incorrect! N >= R + W");
            System.out.println("N:" + N + " >= R:" + R + "+ W:" + W);
            return;
        }
        // Load the "application.conf"
        Config config = ConfigFactory.load(args[0]);
        myId = config.getInt("nodeapp.id");
        fileName = "node" + myId + ".txt";

        if (args.length == 4) {
            if (args[1].equals("recover")) {
                recover = true;
            }
            // Starting with a bootstrapping node
            String ip = args[2];
            String port = args[3];
            // The Akka path to the bootstrapping peer
            remotePath = "akka://DHTsystem@" + ip + ":" + port + "/user/node";
            System.out.println("Starting node " + myId + "; bootstrapping node: " + ip + ":" + port);
        } else {
            System.out.println("Starting disconnected node " + myId);
        }

        // Create the actor system
        final ActorSystem system = ActorSystem.create("DHTsystem", config);

        // Create a single node actor
        final ActorRef receiver = system.actorOf(
                Props.create(Node.class), // actor class 
                "node" // actor name
        );
    }

    public static class Node extends UntypedActor {

        // The table of all nodes in the system id->ref
        private final Map<Integer, ActorRef> nodes = new TreeMap<>();
        //In-Memory copy of the stored Items
        private Map<Integer, LocalItem> localItems = new HashMap<>();

        //Variables used when peer is coordinator
        //to buffer messages until quorum is reached
        private ArrayList<LocalItem> bufferedItems = null;
        //keep track of the client that initiated the request
        private ActorRef client = null;

        //remember if writeQuorum; readQuorum otherwise
        private boolean writeQuorum = false;
        //keep track of the item the user sent
        private LocalItem clientItem = null;

        //Timeout variables
        //timer used to schedule and fire tasks
        private final Timer timer = new java.util.Timer();
        //task containing the code to run in case of timeout
        private TimerTask timerTask = null;
        //keep track if timeout has fired
        private boolean TIMEOUT = false;

        @Override
        public void preStart() throws IOException {
            //retrieve persistedStore if exists here
            if (recover) {
                System.out.println("\n -- [Recover] Attempting recovery -- \n");
                File file = new File(fileName);
                if (file.exists()) {
                    localItems = PersistanceSupport.retrieveStore(fileName);
                }
            }
            if (remotePath != null) {
                getContext().actorSelection(remotePath).tell(new RequestNodelist(myId), getSelf());
            }
            nodes.put(myId, getSelf());
        }

        @Override
        public void onReceive(Object message) throws IOException {

            if (message instanceof RequestNodelist) {
                if (!getSender().path().name().equals("client")) {
                    final int id = ((RequestNodelist) message).id;
                    if (nodes.containsKey(id)) {
                        System.out.println("\n -- [Recover] Requester is a recovering node -- \n");
                        nodes.remove(id);

                        System.out.println("[Recover] After node" + id + " has recovered ");
                        System.out.println("[Recover] node" + myId + " : " + localItems.keySet());

                        for (ActorRef node : nodes.values()) {
                            if (!node.equals(getSelf())) {
                                node.tell(new UpdateRef(id, getSender()), getSelf());
                            }
                        }

                        nodes.put(id, getSender());
                    }

                    System.out.println("[NodesList] Sending list of nodes to node" + id);
                    getSender().tell(new Nodelist(nodes), getSelf());
                }
            } else if (message instanceof Nodelist) {
                if (!getSender().path().name().equals("client")) {
                    System.out.println("[NodesList] Receiving list of nodes");
                    nodes.putAll(((Nodelist) message).nodes);

                    //I am joining -> request node list
                    if (!recover) {
                        //For each clockwise neighbour
                        for (Integer node : this.nClockwiseNodes(myId + 1, 1)) {
                            System.out.println("\n -- [Repartition] Requesting keys from node" + node + " -- \n");
                            (nodes.get(node)).tell(new RequestItemlist(myId), getSelf());
                        }
                    } //I am recovering - Check if keys still my responsibility
                    else {
                        Iterator it = localItems.keySet().iterator();
                        while (it.hasNext()) {
                            Integer key = (Integer) it.next();
                            ArrayList<Integer> responsibleNodes = this.nClockwiseNodes(key, N);
                            if (!responsibleNodes.contains(myId)) {
                                System.out.println("[Recover] key : " + key + " no longer my responsibility");
                                it.remove();
                            }
                        }

                        System.out.println("[Recover] After I recovered ");
                        System.out.println("[Recover] node" + myId + " : " + localItems.keySet());
                    }
                }
            } //Receive a new reference for a certain node
            else if (message instanceof UpdateRef) {
                if (!getSender().path().name().equals("client")) {
                    final int id = ((UpdateRef) message).id;
                    System.out.println("\n-- [Recover] Updating ref for node" + id + "--\n");
                    final ActorRef newRef = ((UpdateRef) message).actor;
                    nodes.put(id, newRef);

                    System.out.println("[Recover] After node" + id + " has recovered ");
                    System.out.println("[Recover] node" + myId + " : " + localItems.keySet());
                }
            } //Return the nodes the requester is responsible for
            else if (message instanceof RequestItemlist) {
                if (!getSender().path().name().equals("client")) {
                    final int id = ((RequestItemlist) message).id;
                    final Map<Integer, ImmutableItem> repartitionItems = new HashMap<>();

                    System.out.println("\n -- [Repartition] node" + id + " requesting items -- \n");

                    for (Integer key : localItems.keySet()) {
                        if ((nodes.size() < N) || /*(myId > id && key < myId) ||*/ key == id) {//optimization logic problem
                            repartitionItems.put(key, this.getImmutableItem(key));
                        } else {
                            nodes.put(id, null);
                            ArrayList<Integer> responsibleNodes = this.nClockwiseNodes(key, N);
                            nodes.remove(id);

                            if (responsibleNodes.contains(id)) {
                                repartitionItems.put(key, this.getImmutableItem(key));
                            }
                        }
                    }
                    System.out.println("[Repartition] Sending " + repartitionItems);
                    getSender().tell(new ItemList(repartitionItems, myId), getSelf());
                }
            } else if (message instanceof Leave) {
                if (getSender().path().name().equals("client")) {
                    client = getSender();
                }

                System.out.println("\n -- [Leave] I have been asked to leave. --\n");
                nodes.remove(myId);

                final Map<Integer, ImmutableItem> repartitionItems = new HashMap<>();
                for (Integer key : localItems.keySet()) {
                    repartitionItems.put(key, this.getImmutableItem(key));
                }

                final ArrayList<Integer> neighbours = this.nClockwiseNodes(myId, N);
                for (Integer node : neighbours) {
                    System.out.println("[Leave] Sending " + repartitionItems + " to node" + node);
                    (nodes.get(node)).tell(new ItemList(repartitionItems, myId, true), getSelf());
                }

                for (Integer node : nodes.keySet()) {
                    System.out.println("[Leave] Sending terminated to node" + node);
                    nodes.get(node).tell(new Terminated(), getSelf());
                }

                if (client != null) {
                    System.out.println("[Leave] Sending terminated to requester");
                    client.tell(new Terminated(), getSelf());
                }

                System.out.println("[Leave] Attempting stop!");
                getContext().stop(getSelf());
            } //Receiving items I am responsible for
            else if (message instanceof ItemList) {
                if (!getSender().path().name().equals("client")) {
                    final boolean leave = ((ItemList) message).leave;
                    final int senderId = ((ItemList) message).senderId;
                    final Map<Integer, ImmutableItem> receivedItems = ((ItemList) message).items;

                    System.out.println("[Repartition] Received " + receivedItems.keySet());

                    //Add them to my localItems
                    for (Integer key : receivedItems.keySet()) {
                        ImmutableItem item = receivedItems.get(key);

                        //double check responsibility for ambiguous keys (only needed on leave)
                        if (senderId > key && leave) {
                            ActorRef sender = nodes.get(senderId);
                            nodes.remove(senderId);
                            ArrayList<Integer> responsibleNodes = this.nClockwiseNodes(key, N);
                            nodes.put(senderId, sender);

                            if (responsibleNodes.contains(myId)) {
                                localItems.put(key, new LocalItem(item.getKey(),
                                        item.getValue(),
                                        item.getVersion()));
                            }
                        } else {
                            localItems.put(key, new LocalItem(item.getKey(),
                                    item.getValue(),
                                    item.getVersion()));
                        }
                    }

                    PersistanceSupport.persistStore(localItems, fileName);
                    System.out.println("[Repartition] node" + myId + " : " + localItems.keySet());

                    //if it is not from a Leaver then it is for when I join
                    if (!leave) {
                        System.out.println("[Join] Sending join messages");
                        //Announce my joining                
                        for (ActorRef n : nodes.values()) {
                            n.tell(new Join(myId), getSelf());
                        }
                    }
                }
            } else if (message instanceof Join) {
                if (!getSender().path().name().equals("client")) {
                    final int id = ((Join) message).id;
                    System.out.println(" \n -- [Join] Node " + id + " joined -- \n");
                    nodes.put(id, getSender());

                    if (myId != id && !localItems.isEmpty()) {
                        System.out.println("[Join] Removing items I am no longer responsible for");
                        Iterator it = localItems.keySet().iterator();
                        while (it.hasNext()) {
                            Integer key = (Integer) it.next();
                            ArrayList<Integer> responsibleNodes = this.nClockwiseNodes(key, N);
                            if (!responsibleNodes.contains(myId)) {
                                it.remove();
                            }
                        }
                    }

                    PersistanceSupport.persistStore(localItems, fileName);

                    //Printout
                    System.out.println("[Repartition] After " + id + " has joined ");
                    System.out.println("[Repartition] node" + myId + " : " + localItems.keySet());
                }
            } //When receiving a GetKey request message
            else if (message instanceof GetKey) {
                //extract the keyId from the message
                final int itemKey = ((GetKey) message).keyId;

                //Check to see if I am coordinator (request coming from a client)
                if (getSender().path().name().equals("client")) {
                    if (bufferedItems == null) {
                        System.out.println("\n[Read] I am coordinator [node" + myId + "]");
                        //keep track of client to later respond
                        client = getSender();
                        //initialise bufferedDataItems, anticipating incoming dataItems from peers
                        bufferedItems = new ArrayList<>();

                        //Retrieve Nodes of interest
                        ArrayList<Integer> interestedNodes = this.nClockwiseNodes(itemKey, N);

                        //If R = 1 and I am part of the interested nodes then no need to pass through the network
                        if (interestedNodes.contains(myId) && R == 1) {
                            System.out.println("[Read] R = 1 and I am responsible");
                            System.out.println("[Read] Sending Item to client : ");// + localItems.get(itemKey).toString());
                            //simply reply to client
                            client.tell(new DataItem(this.getImmutableItem(itemKey)), getSelf());
                            //and cleanup variables state
                            this.cleanup();
                        } //for when contacting other peers is needed
                        else {
                            System.out.println("\n ----- [Read] Initiating Quorum! ----- \n");
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

                            //Start TIMEOUT timer
                            this.setTimoutTask();
                            timer.schedule(timerTask, T);
                        }

                    } else {
                        System.out.println("[Read] Quorum already being attempted, request ignored");
                    }
                } //I am simply a peer, I just need to return my local copy
                else {
                    System.out.println("\n[Read] I am a peer: [node" + myId + "]");
                    //respond to the sender with the local dataItem having that key or a "not present" dataItem
                    DataItem dataItem = new DataItem(new ImmutableItem(itemKey, "", 0));
                    if (localItems.containsKey(itemKey)) {
                        dataItem = new DataItem(this.getImmutableItem(itemKey));
                    }
                    System.out.println("[Read] Replying with " + localItems.get(itemKey));
                    getSender().tell(dataItem, getSelf());
                }
            } //When receiving an Update request message
            else if (message instanceof Update) {
                if (nodes.size() < N) {
                    System.out.println("[Write] Ignoring request - Not enough peers");
                    getSender().tell("Not enough nodes present to perform Write operation", getSelf());
                } else {
                    //extract the keyId from the message
                    final int itemKey = ((Update) message).keyId;
                    final String itemValue = ((Update) message).value;
                    final int itemVersion = ((Update) message).version;

                    //Check to see if I am coordinator (request coming from client)
                    if (getSender().path().name().equals("client")) {
                        if (bufferedItems == null) {
                            System.out.println("\n[Write] I am coordinator [node" + myId + "]");

                            //keep track of client to later respond
                            client = getSender();

                            //initialise bufferedItems, anticipating incoming Items from peers
                            bufferedItems = new ArrayList<>();

                            //I am trying to establish an Update Quorum
                            writeQuorum = true;
                            //as far as I know the dataItem I received is the latest
                            clientItem = new LocalItem(itemKey, itemValue, itemVersion);

                            LocalItem item = new LocalItem(itemKey, "", 0);
                            //Retrieve Nodes of interest
                            ArrayList<Integer> interestedNodes = this.nClockwiseNodes(itemKey, N);

                            //If R = W = 1 and I am part of the interested nodes then no need to pass through the network
                            if (interestedNodes.contains(myId) && N == 1 && W == 1 && R == 1) {

                                System.out.println("[Write] N = W = R = 1 and I am responsible");

                                if (localItems.containsKey(itemKey)) {
                                    item = localItems.get(itemKey);
                                }
                                clientItem.setVersion(item.getVersion() + 1);

                                System.out.println("[Write] Reply with successful write to client");
                                //simply reply to client
                                client.tell("Successful write for key " + clientItem.getKey(), getSelf());

                                System.out.println("[Write] Saving item locally: " + clientItem.toString());

                                localItems.put(itemKey, clientItem);
                                PersistanceSupport.persistStore(localItems, fileName);
                                //and cleanup variables state
                                this.cleanup();
                            } else {
                                System.out.println("\n ----- [Write] Initiating Quorum! ----- \n");
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

                                //Start TIMEOUT timer
                                setTimoutTask();
                                timer.schedule(timerTask, T);
                            }
                        } else {
                            System.out.println("[Write] Quorum already being attempted, request ignored");
                        }
                    } //I am peer so I should just write
                    else {
                        System.out.println("\n[Write] I am a peer: [node" + myId + "]");
                        //simply write the Item I received
                        LocalItem newItem = new LocalItem(itemKey, itemValue, itemVersion);
                        //save or replace the Item

                        System.out.println("[Write] Saving item locally: " + newItem.toString());

                        localItems.put(itemKey, newItem);
                        PersistanceSupport.persistStore(localItems, fileName);
                    }
                }
            } //When receiving a DataItem as response
            else if (message instanceof DataItem) {
                if (!getSender().path().name().equals("client")) {
                    //if not null then I previously initiated a quorum request 
                    if (bufferedItems != null && !TIMEOUT) {
                        /**
                         * if not enough replies received two cases: ReadQuorum
                         * - if not enough replies yet and not a writeQuorum
                         * expected WriteQuorum - if not enough replies yet and
                         * writeQuorum expected
                         */
                        if ((bufferedItems.size() < R && !writeQuorum)
                                || (bufferedItems.size() < Integer.max(R, W) && writeQuorum)) {
                            //buffer the freshly received Item
                            ImmutableItem item = ((DataItem) message).item;
                            System.out.println("[Quorum] Buffering data item " + item.toString());
                            bufferedItems.add(new LocalItem(item.getKey(), item.getValue(), item.getVersion()));
                        }

                        //Read/Write Quorum reached: I have received enough replies 
                        if ((bufferedItems.size() == R && !writeQuorum)
                                || (bufferedItems.size() == Integer.max(R, W) && writeQuorum)) {

                            //Stop timeout timer: Quorum reached
                            System.out.println("[Quoru] Quorum Reached - Stopping timer");
                            timerTask.cancel();

                            LocalItem latestItem = null;
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
                                //if I am trying to establish a writeQuorum
                                if (writeQuorum) {
                                    //Increment item version before writing
                                    clientItem.setVersion(latestItem.getVersion() + 1);

                                    System.out.println("[Write] Quorum achieved! Responding successful write to client");
                                    //Answer client - successful write
                                    client.tell("Successful write for key " + clientItem.getKey(), getSelf());

                                    System.out.println("[Write] Propagating update of item: " + clientItem.toString());

                                    //for every interestedNode
                                    for (Integer node : this.nClockwiseNodes(clientItem.getKey(), N)) {
                                        //If I am interested
                                        if (node.equals(myId)) {
                                            System.out.println("[Write] Saving item locally: " + clientItem.toString());
                                            //perform local update
                                            localItems.put(clientItem.getKey(), clientItem);
                                            PersistanceSupport.persistStore(localItems, fileName);
                                        } //If different node
                                        else {
                                            //send Update request to other interestedNodes with latest Item
                                            (nodes.get(node)).tell(new Update(clientItem.getKey(),
                                                    clientItem.getValue(),
                                                    clientItem.getVersion()), getSelf());
                                        }
                                    }
                                } //readQuorum = !writeQuorum
                                else {
                                    clientItem = latestItem;

                                    System.out.println("[Read] Quorum achieved! Sending Item to client: " + client.path().toString());
                                    ImmutableItem responseItem = null;
                                    if (clientItem != null && clientItem.getVersion() > 0) {
                                        responseItem = new ImmutableItem(
                                                clientItem.getKey(),
                                                clientItem.getValue(),
                                                clientItem.getVersion());
                                    }
                                    client.tell(new DataItem(responseItem), getSelf());
                                    System.out.println("[Read] Response sent " + clientItem.toString());
                                }
                            }
                            //request has been handled - cleanup
                            this.cleanup();
                        }
                    } else if (TIMEOUT == true) {
                        System.out.println("\n --[TIMEOUT] Timed out --\n");
                    } else {
                        //no quorum initiated because bufferedDataItems == null
                        System.out.println("[Quorum] No quorum active! Dropping message: ");

                        //For debug only.. peer should not receive DataItems except if he is coordinator
                        ImmutableItem dropped = ((DataItem) message).item;
                        System.out.println(dropped.getKey() + " " + dropped.getValue() + " " + dropped.getVersion());
                    }
                }
            } else if (message instanceof Terminated) {
                if (!getSender().path().name().equals("client")) {
                    System.out.println("\n --[Leave] Received terminated message from " + getSender().path() + "--\n");
                    nodes.values().remove(getSender());

                    //Printout
                    System.out.println("[Leave] After " + getSender().path().toString() + " leaves");
                    System.out.println("[Leave] node" + myId + " :" + localItems.keySet());
                }
            } //Do not handle messages you don't know
            else {
                unhandled(message);
            }
        }

        @Override
        public void postStop() {
            System.out.print("[Leave] I have stopped!");
            System.exit(0);
        }

        //check on number of nodes should be done earlier
        public ArrayList<Integer> nClockwiseNodes(Integer itemKey, int n) {
            //N replication parameter
            ArrayList<Integer> interestedNodes = new ArrayList<>();
            Set<Integer> keys = nodes.keySet();

            //System.out.println("[ClockwiseNodes] Printing key set " + keys.toString());
            Integer key;
            //find the N - k clockwise nodes (where k is the number of nodes with id < itemKey)
            Iterator it = keys.iterator();
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
            System.out.println("[Cleanup] For next quorum");
            client = null;
            bufferedItems = null;
            writeQuorum = false;
            clientItem = null;
            timerTask = null;
            TIMEOUT = false;
        }

        public ImmutableItem getImmutableItem(int itemKey) {
            LocalItem item = localItems.get(itemKey);

            if (item != null) {
                return new ImmutableItem(item.getKey(),
                        item.getValue(),
                        item.getVersion());
            }
            return null;
        }

        public void setTimoutTask() {
            timerTask = new java.util.TimerTask() {
                @Override
                public void run() {
                    TIMEOUT = true;
                    System.out.println("\n -- [TIMEOUT] Timeout exceeded! --\n");
                    client.tell("TIMEDOUT before quorum is reached!", nodes.get(myId));
                    cleanup();
                }
            };
        }
    }
}
