/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ds_project.node;

import java.io.Serializable;

/**
 *
 * @author recognition
 */
public class Messages {
        
    public static class Update implements Serializable{
        public final Integer keyId; 
        public final String value;
        public final int version;
        public Update(Integer keyId, String value){
            this.keyId = keyId;
            this.value = value;
            this.version = 1;
        }
        public Update(Integer keyId, String value, int version){
            this.keyId = keyId;
            this.value = value;
            this.version = version;
        }
    } 

    public static class GetKey implements Serializable{
        public final Integer keyId; 
        public GetKey(Integer keyId){
            this.keyId = keyId;
        }
    } 
    
    public static class DataItem implements Serializable{
        public final Item item; 
        public DataItem(Item item){
            this.item = item;
        }
    }
    
}
