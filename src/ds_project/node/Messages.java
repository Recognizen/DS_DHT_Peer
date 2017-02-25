/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ds_project.node;

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
            this.version = 1;
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
    }

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

    public final class ImmutableItem implements Serializable {

        private final int key;
        private final String value;
        private final int version;

        public ImmutableItem(int key, String value, int version) {
            this.key = key;
            this.value = value;
            this.version = version;
        }

        public Integer getKey() {
            return key;
        }

        public String getValue() {
            return value;
        }

        public int getVersion() {
            return version;
        }

    }

}
