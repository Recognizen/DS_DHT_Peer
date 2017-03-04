/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ds_project.resources;

import java.io.Serializable;

/**
 *
 * @author recognition
 */
public class LocalItem implements Serializable{
    
    private Integer key;
    private String value;
    private int version;
    
    public LocalItem(Integer key, String value, int version){
        this.key = key;
        this.value = value;
        this.version = version;
    }

    public Integer getKey() {
        return key;
    }

    public void setKey(Integer key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }
    
    @Override
    public String toString(){
        return this.getKey() + " " + this.getValue() +" " + this.getVersion();
    }
}
