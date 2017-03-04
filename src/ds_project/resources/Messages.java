/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ds_project.resources;

import akka.actor.ActorRef;
import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author recognition
 */
public class Messages {

    public static class Update implements Serializable {

        public final int keyId;
        public final String value;
        public final int version;

        public Update(int keyId, String value) {
            this.keyId = keyId;
            this.value = value;
            this.version = 0;
        }

        public Update(int keyId, String value, int version) {
            this.keyId = keyId;
            this.value = value;
            this.version = version;
        }
    }

    public static class GetKey implements Serializable {

        public final int keyId;

        public GetKey(int keyId) {
            this.keyId = keyId;
        }
    }

    public static class DataItem implements Serializable {

        public final ImmutableItem item;

        public DataItem(ImmutableItem item) {
            this.item = item;
        }
    }

    // ****** Messages needed to establish DHT network *****
    public static class RequestNodelist implements Serializable {
        public final int id;

        public RequestNodelist(int id) {
            this.id = id;
        }
    }    


    public static class Nodelist implements Serializable {

        public Map<Integer, ActorRef> nodes;

        public Nodelist(Map<Integer, ActorRef> nodes) {
            this.nodes = Collections.unmodifiableMap(new HashMap<>(nodes));
        }
    }

    public static class Join implements Serializable {
        public final int id;

        public Join(int id) {
            this.id = id;
        }
    }
    
    public static class Leave implements Serializable {
    } 
    
    public static class Terminated implements Serializable {
    }
    
    public static class RequestItemlist implements Serializable {
        public final int id;

        public RequestItemlist(int id) {
            this.id = id;
        }
    }    
    
    public static class ItemList implements Serializable {

        public final Map<Integer, ImmutableItem> items;
        public final boolean leave;
        public final int senderId;

        public ItemList(Map<Integer, ImmutableItem> items, int senderId) {
            this.items = Collections.unmodifiableMap(new HashMap<>(items));
            this.leave = false;
            this.senderId = senderId;
        }
        
        public ItemList(Map<Integer, ImmutableItem> items, int senderId , boolean leave) {
            this.items = Collections.unmodifiableMap(new HashMap<>(items));
            this.leave = leave;
            this.senderId = senderId;
        }
    }

    public static final class ImmutableItem implements Serializable {

        public final int key;
        public final String value;
        public final int version;

        public ImmutableItem(int key, String value, int version) {
            this.key = key;
            this.value = value;
            this.version = version;
        }

        public int getKey() {
            return key;
        }

        public String getValue() {
            return value;
        }

        public int getVersion() {
            return version;
        }
    }
    
    public static class UpdateRef implements Serializable {
        public final int id;
        public final ActorRef actor;

        public UpdateRef(int id, ActorRef actor) {
            this.id = id;
            this.actor = actor;
        }
    }
}
