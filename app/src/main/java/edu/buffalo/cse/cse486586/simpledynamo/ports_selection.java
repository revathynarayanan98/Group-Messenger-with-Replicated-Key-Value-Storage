package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.ContentValues;
import android.support.annotation.NonNull;
import android.util.Log;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;


public class ports_selection implements Comparable<Node_Store>{

    ContentValues values;
    String key="";
    String circuit_controller="undergoing_contruction";
    Map<String,String> store;
    LinkedList<String> mini_circuit = new LinkedList<>();
    Map<String,String> hash_store = new HashMap<>();
    Map<String,Integer> conditions=new HashMap<>();


    public ports_selection(String key) {
        this.key=key;
    }

    public ports_selection(ContentValues values) {
        this.values=values;

    }

    public String locate_insertion_op(LinkedList ports, Map<String, String> store, String code) throws NoSuchAlgorithmException {
        this.store=store;
        int j=1;



        try {
            for (int i = 0; i < ports.size(); i++) {
                construct_circuit(ports,i);

                if (i == 3) {
                    store.put("next1",(String) ports.get(i+1));

                    store.put("next2",(String) ports.get(0));

                } else if (i == 4) {

                    store.put("next1",(String) ports.get(0));

                    store.put("next2",(String) ports.get(1));
                } else {

                    store.put("next1",(String) ports.get(i+1));
                    store.put("next2",(String) ports.get(i+2));
                }



                if (i == 0) {

                    store.put("previous",(String) ports.get(4));
                } else {
                    store.put("previous",(String) ports.get(i-1));
                }



                if(key.equals("")) {

                    hash_store.put("key",genHash(values.getAsString("key")));
                }else{
                    hash_store.put("key",genHash(key));
                }
                hash_store.put("current",genHash(store.get("id_of_current")));
                hash_store.put("previous",genHash(store.get("previous")));





                if (hash_store.get("previous").compareTo(hash_store.get("current")) > 0) {


                    if (hash_store.get("key").compareTo(hash_store.get("previous")) < 0 && hash_store.get("key").compareTo(hash_store.get("current")) < 0) {
                        circuit_controller="constructed";
                        construct_circuit(ports,i);

                        break;

                    } else if (hash_store.get("key").compareTo(hash_store.get("previous")) > 0 && hash_store.get("key").compareTo(hash_store.get("current")) > 0) {
                        circuit_controller="constructed";
                        construct_circuit(ports,i);


                        break;

                    } else {
                        continue;
                    }

                } else if (hash_store.get("key").compareTo(hash_store.get("previous")) > 0 && hash_store.get("key").compareTo(hash_store.get("current")) < 0) {
                    circuit_controller="constructed";
                    construct_circuit(ports,i);

                    break;

                } else {

                    continue;

                }
            }
        } catch (Exception e){
            e.printStackTrace();
        }


        if(code.equals("i")) {
            Node_Store node1 = new Node_Store(mini_circuit.get(1), genHash(mini_circuit.get(1)), mini_circuit.get(0), mini_circuit.get(2), mini_circuit.get(3), "i");
            node1.key = values.getAsString("key");
            node1.value = values.getAsString("value");
            node1.HASHED_CURRENT_ID = genHash(values.getAsString("key"));
            node1.VERSION_CONTROL = Long.toString(System.currentTimeMillis());
            return StringConstructor(node1);
        }
        else if(code.equals("u")){
            Node_Store node = new Node_Store(mini_circuit.get(0), null, store.get("prev"), mini_circuit.get(1), mini_circuit.get(2), "u");
            node.key = this.key;
            node.STARTER_ID = store.get("current");
            return StringConstructor(node);
        }
        else{
            return "None Formed";
        }

    }

    @Override
    public int compareTo(Node_Store node) {

        return node.HASHED_CURRENT_ID.compareTo(node.HASHED_CURRENT_ID);
    }

    private void construct_circuit(LinkedList intermediary,Integer construction_num){
        if(circuit_controller.equals("undergoing_contruction")){
            store.put("id_of_current", (String) intermediary.get(construction_num));
            store.put("previous","");
            store.put("next1","");
            store.put("next2","");
            return;
        }
        else if(circuit_controller.equals("constructed")){
            mini_circuit.add(store.get("previous"));
            mini_circuit.add(store.get("id_of_current"));
            mini_circuit.add(store.get("next1"));
            mini_circuit.add(store.get("next2"));
            circuit_controller="undergoing_construction";

            return;

        }


    }

    private String StringConstructor(Node_Store node){
        String formation = node.CURRENT_ID+"@@"+node.HASHED_CURRENT_ID+"@@"+node.PREVIOUS_ID+"@@"+node.NEXT_1_ID+"@@"+node.NEXT_2_ID+"@@"+node.DECISION_MAKER+"@@"+node.key+"@@"+node.value+"@@"+node.STARTER_ID+"@@"+node.VERSION_CONTROL;

        return formation;


    }

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }


}
