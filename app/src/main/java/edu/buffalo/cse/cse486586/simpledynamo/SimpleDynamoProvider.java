package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;
import static android.content.ContentValues.TAG;
import static edu.buffalo.cse.cse486586.simpledynamo.DBUtils.TABLE_NAME;


class Node_Store {

    String key;
    String value;
    String CURRENT_ID;
    String HASHED_CURRENT_ID;

    String DECISION_MAKER;

    String PREVIOUS_ID;
    String NEXT_1_ID;
    String NEXT_2_ID;

    String STARTER_ID;

    String VERSION_CONTROL;

    public Node_Store() {
        key = "";
        value = "";
        CURRENT_ID = null;
        HASHED_CURRENT_ID = null;

        DECISION_MAKER = "";

        PREVIOUS_ID = null;
        NEXT_1_ID = null;
        NEXT_2_ID = null;
        STARTER_ID = null;
        VERSION_CONTROL = null;
    }

    public Node_Store(String ID, String HASHED_CURRENT_ID, String PREVIOUS_ID, String NEXT_1_ID, String NEXT_2_ID, String code) {
        this.key = "";
        this.value = "";
        this.CURRENT_ID = ID;
        this.HASHED_CURRENT_ID = HASHED_CURRENT_ID;

        this.DECISION_MAKER = code;

        this.PREVIOUS_ID = PREVIOUS_ID;
        this.NEXT_1_ID = NEXT_1_ID;
        this.NEXT_2_ID = NEXT_2_ID;

        this.STARTER_ID = null;
        this.VERSION_CONTROL = null;
    }


}

public class SimpleDynamoProvider extends ContentProvider {

    Map<String, String> transitions = new HashMap<String, String>();
    LinkedList<String> ports = new LinkedList<String>();
    Map<String, String> store = new HashMap<String, String>();
    Map<String, Boolean> checkers = new HashMap<String, Boolean>();
    Map<String, Integer> store_int = new HashMap<>();
    public static final Map<String, Integer> caseswitcher = new HashMap<String, Integer>();

    String db_controller = "";
    Socket socket_conn;

    String[] get_keys;
    String[] get_vals;
    String[] get_starter;
    String[] get_versions;


    DBUtils dbUtils;
    SQLiteDatabase db;


    @Override
    public synchronized int delete(Uri uri, String selection, String[] selectionArgs) {

        if (selection.equals("@")) {
            db_controller = "delete";

            return perform_db_op(selection, db_controller);

        } else if (selection.equals("*")) {

            db_controller = "delete";

            return perform_db_op(selection, db_controller);

        } else {

            db_controller = "delete";
            return perform_db_op(selection, db_controller);
        }
    }


    @Override
    public synchronized Uri insert(Uri uri, ContentValues values) {

        try {

            try {
                ports_selection locator = new ports_selection(values);


                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, locator.locate_insertion_op(ports, store, "i"));


            } catch (Exception e) {
                e.printStackTrace();
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        return uri;
    }


    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub
        TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String get_port = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);


        checkers.put("global_get_check", false);
        checkers.put("delete_checker", false);


        store.put("unified_port", "10000");


        store.put("result", "");
        store.put("gkeys", "");
        store.put("gvals", "");


        transitions.put("5554", "5558");
        transitions.put("5556", "5554");
        transitions.put("5558", "5560");
        transitions.put("5560", "5562");
        transitions.put("5562", "5556");


        store.put("current", get_port);
        store.put("next1", get_port);
        store.put("prev", get_port);


        dbUtils = new DBUtils(getContext());
        db = dbUtils.getWritableDatabase();
        db.execSQL("CREATE TABLE IF NOT EXISTS  " + TABLE_NAME + "(belongs_to TEXT, version_control TEXT," + dbUtils.COL_1 + " TEXT, value TEXT PRIMARY KEY, UNIQUE (key) ON CONFLICT REPLACE)");
        db.execSQL("CREATE TABLE IF NOT EXISTS " + dbUtils.INTERMEDIARY + "( " + dbUtils.COL_1 + " TEXT, value TEXT PRIMARY KEY, UNIQUE (key) ON CONFLICT REPLACE)");

        try {

            ServerSocket serverSocket = new ServerSocket(Integer.valueOf(store.get("unified_port")));
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
            ports.add(get_port);
            ports.add(transitions.get(get_port));
            ports.add(transitions.get(transitions.get(get_port)));
            ports.add(transitions.get(transitions.get(transitions.get(get_port))));
            ports.add(transitions.get(transitions.get(transitions.get(transitions.get(get_port)))));

            store.put("next1", ports.get(1));
            store.put("next2", ports.get(2));
            store.put("prev", ports.get(4));


        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }

        String[] placement = {"key", "value"};


        Cursor c1 = db.query(dbUtils.INTERMEDIARY, placement, query_condition(dbUtils), new String[]{"INITIAL_START"}, null, null, null);
        if (c1.getCount() > 0) {
            Node_Store node = new Node_Store(store.get("current"), null, store.get("prev"), store.get("next1"), store.get("next2"), "GET_REPLICA");
            node.STARTER_ID = store.get("current");

            new RecoveryTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, StringConstructor(node));

        } else {
            ContentValues store_values = new ContentValues();
            store_values.put("key", "INITIAL_START");
            store_values.put("value", "INITIAL_START");
            db.insert(dbUtils.INTERMEDIARY, null, store_values);

            try {

                timeout_controller();


            } catch (Exception e) {

                Log.d(TAG, "i");
            }
        }
        return false;
    }

    private void timeout_controller() throws InterruptedException {
        if (!store.get("current").equals("5562")) {
            TimeUnit.SECONDS.sleep(5); //for 5000 Milli Seconds
        }

    }


    @Override
    public synchronized Cursor query(Uri uri, String[] projection, String selection,
                                     String[] selectionArgs, String sortOrder) {


        SQLiteDatabase db = dbUtils.getReadableDatabase();

        try {

            if (selection.equals("@")) {

                Cursor c1 = db.rawQuery("SELECT  * FROM " + TABLE_NAME, null);

                return c1;

            } else if (selection.equals("*")) {


                Cursor c1 = db.rawQuery("SELECT  * FROM " + TABLE_NAME, null);


                store.put("keys_alt", "");

                store.put("vals_alt", "");

                if (c1 != null) {
                    c1.moveToFirst();
                    cursor_set(c1);


                    while (!c1.isAfterLast()) {

                        store.put("keys_alt", store.get("keys_alt") + c1.getString(store_int.get("key")) + "POINTBREAK");

                        store.put("vals_alt", store.get("vals_alt") + c1.getString(store_int.get("value")) + "POINTBREAK");
                        c1.moveToNext();

                    }
                }

                Node_Store node_alter = new Node_Store(store.get("current"), null, store.get("prev"), store.get("next1"), store.get("next2"), "g");
                node_alter.STARTER_ID = store.get("current");
                node_alter.key = store.get("keys_alt");
                node_alter.value = store.get("vals_alt");


                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, StringConstructor(node_alter));


                while (!checkers.get("global_get_check")) {

                }

                get_keys = store.get("gkeys").split("POINTBREAK");
                get_vals = store.get("gvals").split("POINTBREAK");

                MatrixCursor all_cur = new MatrixCursor(new String[]{"key", "value"});

                for (int i = 0; i < get_keys.length; i++) {
                    all_cur.addRow(new String[]{get_keys[i], get_vals[i]});
                }

                store.put("gkeys", "");
                store.put("gvals", "");

                return all_cur;

            } else {
                String[] placement = {"key", "value"};


                Cursor c1 = db.query(TABLE_NAME, placement, query_condition(dbUtils), new String[]{selection}, null, null, null);


                if (c1 != null && c1.getCount() > 0) {
                    if (c1.moveToFirst())

                        return c1;

                } else {
                    ports_selection locator = new ports_selection(selection);

                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, locator.locate_insertion_op(ports, store, "u"));

                    while (store.get("result").equals("")) {

                    }
                    MatrixCursor res_cur = new MatrixCursor(new String[]{"key", "value"});
                    res_cur.addRow(new String[]{selection, store.get("result")});

                    store.put("result", "");

                    return res_cur;
                }

            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }

    private void cursor_set(Cursor myCursor) {

        store_int.put("key", myCursor.getColumnIndex("key"));
        store_int.put("value", myCursor.getColumnIndex("value"));
        store_int.put("belongs_to", myCursor.getColumnIndex("belongs_to"));
        store_int.put("version_control", myCursor.getColumnIndex("version_control"));
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection,
                      String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
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


    private int genHash_caseswitcher(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.hashCode();
    }

    public String what_code(String decision_maker) {
        String[] pieces = decision_maker.split("@@");
        return pieces[5];

    }


    public int perform_db_op(String criteria, String database_controller) {

        if (criteria.equals("@") && database_controller.equals("delete")) {
            store_int.put("number_of_deletes_performed", db.delete(TABLE_NAME, null, null));
            return store_int.get("number_of_deletes_performed");


        } else if (criteria.equals("*") && database_controller.equals("delete")) {
            store_int.put("number_of_deletes_performed", db.delete(TABLE_NAME, null, null));

            Node_Store node = new Node_Store(store.get("current"), null, store.get("prev"), store.get("next1"), store.get("next2"), "d_all");
            node.key = criteria;
            node.STARTER_ID = store.get("current");

            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, StringConstructor(node));

            return store_int.get("number_of_deletes_performed");

        } else if (!criteria.equals("@") && !criteria.equals("*") && database_controller.equals("delete")) {
            String get_col = dbUtils.COL_1 + " ='" + criteria + "'";
            store_int.put("number_of_deletes_performed", db.delete(TABLE_NAME, get_col, null));

            if (store_int.get("number_of_deletes_performed") > 0) {
                return store_int.get("number_of_deletes_performed");

            } else {
                Node_Store node = new Node_Store(store.get("current"), null, store.get("prev"), store.get("next1"), store.get("next2"), "d");
                node.key = criteria;
                node.STARTER_ID = store.get("current");


                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, StringConstructor(node));

                while (!checkers.get("delete_checker")) {

                }

                return 0;
            }
        } else {


            return 0;
        }

    }


    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];

            try {
                while (true) {
                    Socket clientSocket = serverSocket.accept();

                    DataInputStream dataInputStream = new DataInputStream(clientSocket.getInputStream());
                    DataOutputStream dataOutputStream = new DataOutputStream(clientSocket.getOutputStream());


                    try {
                        store.put("get_readable_message", dataInputStream.readUTF());
                        String decision_maker = what_code(store.get("get_readable_message"));
                        dataOutputStream.writeUTF("MESSAGE_READ");


                        switch (decision_maker) {
                            case "GET_REPLICA":
                                server_rep(store.get("get_readable_message"), dataOutputStream);
                                break;
                            case "i":
                                server_i(store.get("get_readable_message"));
                                break;
                            case "u":
                                server_u(store.get("get_readable_message"), dataOutputStream);
                                break;
                            case "v":
                                server_v(store.get("get_readable_message"));
                                break;
                            case "g":
                                server_g(store.get("get_readable_message"), dataOutputStream);
                                break;
                            case "gf":
                                server_gf(store.get("get_readable_message"));
                                break;
                            case "d":
                                server_d(store.get("get_readable_message"), dataOutputStream);
                                break;
                            case "d_all":
                                server_d_all(store.get("get_readable_message"));
                                break;

                        }

                    } catch (Exception e) {
                        e.printStackTrace();
                    }


                    dataInputStream.close();
                    ;
                    dataOutputStream.close();
                }

            } catch (Exception e) {
                e.printStackTrace();

            }
            return null;
        }

    }

    public int hasher(String code) throws NoSuchAlgorithmException {
        if (code.equals("i")) {
            return genHash_caseswitcher("i");
        }
        return 0;

    }

    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... data) {

            try {


                store.put("get_sendable_message", data[0]);

                Node_Store for_decision = nodeCreator(store.get("get_sendable_message"));

                final int case_switch = hasher(for_decision.DECISION_MAKER);

                switch (for_decision.DECISION_MAKER) {
                    case "i":
                        client_i(store.get("get_sendable_message"));
                        break;
                    case "u":
                        client_u(store.get("get_sendable_message"));
                        break;
                    case "v":
                        client_v(store.get("get_sendable_message"));
                        break;
                    case "g":
                        client_g(store.get("get_sendable_message"));
                        break;
                    case "gf":
                        client_gf(store.get("get_sendable_message"));
                        break;
                    case "d":
                        client_d(store.get("get_sendable_message"));
                        break;
                    case "df":
                        client_df(store.get("get_sendable_message"));
                        break;
                    case "d_all":
                        client_d_all(store.get("get_sendable_message"));
                        break;

                }


            } catch (Exception e) {
                e.printStackTrace();
            }

            return null;
        }
    }

    private class RecoveryTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... data) {

            try {
                store.put("get_sendable_message", data[0]);


                dbUtils = new DBUtils(getContext());
                db = dbUtils.getWritableDatabase();

                for (int j = 1; j < ports.size(); j++) {

                    socket_conn = socket_connector(ports.get(j));

                    DataOutputStream dataOutputStream = new DataOutputStream(socket_conn.getOutputStream());
                    try {
                        dataOutputStream.writeUTF(store.get("get_sendable_message"));

                        DataInputStream dataInputStream = new DataInputStream(socket_conn.getInputStream());
                        dataInputStream.readUTF();

                        Node_Store node_store = nodeCreator(dataInputStream.readUTF());

                        cv_getter(node_store);


                        for (int i = 0; i < get_keys.length; i++) {

                            if (get_starter[i].equals(store.get("current")) || get_starter[i].equals(ports.get(3)) || get_starter[i].equals(ports.get(4))) {

                                String[] placement = {"key", "value", "version_control", "belongs_to"};


                                Cursor c1 = db.query(TABLE_NAME, placement, query_condition(dbUtils), new String[]{get_keys[i]}, null, null, null);

                                ContentValues store_values = new ContentValues();
                                store_values.put("key", get_keys[i]);
                                store_values.put("value", get_vals[i]);
                                store_values.put("belongs_to", get_starter[i]);
                                store_values.put("version_control", get_versions[i]);

                                if (c1 != null && c1.getCount() > 0) {
                                    if (c1.moveToFirst()) {
                                        String VERSION_CONTROL = c1.getString(c1.getColumnIndex("version_control"));

                                        if (Long.parseLong(get_versions[i]) < Long.parseLong(VERSION_CONTROL)) {
                                            continue;
                                        } else {
                                            db.update(TABLE_NAME, store_values, "key = '" + get_keys[i] + "'", null);

                                        }

                                    }

                                } else {
                                    db.insert(TABLE_NAME, null, store_values);

                                }
                            }
                        }

                    } catch (Exception e) {
                        e.printStackTrace();
                        continue;
                    }

                }


            } catch (Exception e) {
                e.printStackTrace();
            }
            return null;
        }
    }

    private String query_condition(DBUtils sqlhelper) {
        String get_col = sqlhelper.COL_1 + "=?";
        return get_col;

    }

    private void cv_getter(Node_Store temp) {
        get_keys = temp.key.split("POINTBREAK");
        get_vals = temp.value.split("POINTBREAK");
        get_starter = temp.STARTER_ID.split("POINTBREAK");
        get_versions = temp.VERSION_CONTROL.split("POINTBREAK");

    }

    public String StringConstructor(Node_Store node) {
        String formation = node.CURRENT_ID + "@@" + node.HASHED_CURRENT_ID + "@@" + node.PREVIOUS_ID + "@@" + node.NEXT_1_ID + "@@" + node.NEXT_2_ID + "@@" + node.DECISION_MAKER + "@@" + node.key + "@@" + node.value + "@@" + node.STARTER_ID + "@@" + node.VERSION_CONTROL;

        return formation;


    }

    public Node_Store nodeCreator(String value) {
        Node_Store node_intermediary = new Node_Store();
        String[] msg = value.split("@@");

        node_intermediary.CURRENT_ID = msg[0];
        node_intermediary.HASHED_CURRENT_ID = msg[1];
        node_intermediary.PREVIOUS_ID = msg[2];
        node_intermediary.NEXT_1_ID = msg[3];
        node_intermediary.NEXT_2_ID = msg[4];
        node_intermediary.DECISION_MAKER = msg[5];
        node_intermediary.key = msg[6];
        node_intermediary.value = msg[7];
        node_intermediary.STARTER_ID = msg[8];
        node_intermediary.VERSION_CONTROL = msg[9];

        return node_intermediary;

    }

    private Socket socket_connector(String connection_string) throws IOException {
        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(connection_string) * 2);

        return socket;
    }


    public void server_rep(String send, DataOutputStream get_output_stream) throws IOException {
        Node_Store node_intermediary = nodeCreator(send);


        SQLiteDatabase db = dbUtils.getReadableDatabase();
        Cursor c1 = db.rawQuery("SELECT  * FROM " + TABLE_NAME, null);


        store.put("keys_alt", "");

        store.put("vals_alt", "");
        store.put("all_belongs_to", "");
        store.put("all_versions", "");


        if (c1 != null) {
            c1.moveToFirst();
            cursor_set(c1);


            while (!c1.isAfterLast()) {

                store.put("keys_alt", store.get("keys_alt") + c1.getString(store_int.get("key")) + "POINTBREAK");
                store.put("vals_alt", store.get("vals_alt") + c1.getString(store_int.get("value")) + "POINTBREAK");
                store.put("all_belongs_to", store.get("all_belongs_to") + c1.getString(store_int.get("belongs_to")) + "POINTBREAK");
                store.put("all_versions", store.get("all_versions") + c1.getString(store_int.get("version_control")) + "POINTBREAK");
                c1.moveToNext();

            }
        }

        node_intermediary.key = store.get("keys_alt");
        node_intermediary.value = store.get("vals_alt");
        node_intermediary.STARTER_ID = store.get("all_belongs_to");
        node_intermediary.VERSION_CONTROL = store.get("all_versions");


        get_output_stream.writeUTF(StringConstructor(node_intermediary));


    }


    public void client_i(String send) throws IOException {
        Node_Store node_intermediary = nodeCreator(send);
        String[] get_values = new String[]{node_intermediary.CURRENT_ID, node_intermediary.NEXT_1_ID, node_intermediary.NEXT_2_ID};

        for (int j = 0; j < get_values.length; j++) {


            socket_conn = socket_connector(get_values[j]);

            DataOutputStream dataOutputStream = new DataOutputStream(socket_conn.getOutputStream());
            try {
                dataOutputStream.writeUTF(send);

            } catch (Exception e) {
                e.printStackTrace();
            }


            DataInputStream dataInputStream = new DataInputStream(socket_conn.getInputStream());
            String get_confirmation;

            try {
                get_confirmation = dataInputStream.readUTF();
                if (get_confirmation.equals("MESSAGE_READ")) {
                    dataOutputStream.close();
                    socket_conn.close();
                }

            } catch (Exception e) {
                e.printStackTrace();
                continue;
            }

        }


    }


    public void server_i(String send) {
        Node_Store node_intermediary = nodeCreator(send);


        String[] placement = {"key", "value", "version_control", "belongs_to"};

        Cursor c1 = db.query(TABLE_NAME, placement, query_condition(dbUtils), new String[]{node_intermediary.key}, null, null, null);

        ContentValues store_values = new ContentValues();
        store_values.put("key", node_intermediary.key);
        store_values.put("value", node_intermediary.value);
        store_values.put("belongs_to", node_intermediary.CURRENT_ID);
        store_values.put("version_control", node_intermediary.VERSION_CONTROL);

        if (c1 != null && c1.getCount() > 0) {

            if (c1.moveToFirst()) {
                String VERSION_CONTROL = c1.getString(c1.getColumnIndex("version_control"));

                if (Long.parseLong(node_intermediary.VERSION_CONTROL) > Long.parseLong(VERSION_CONTROL)) {
                    db.update(TABLE_NAME, store_values, "key = '" + node_intermediary.key + "'", null);

                }
            }
        } else {
            db.insert(TABLE_NAME, null, store_values);

        }

    }


    public void client_u(String send) throws IOException {
        Node_Store node_intermediary = nodeCreator(send);


        String[] get_values = new String[]{node_intermediary.CURRENT_ID, node_intermediary.NEXT_1_ID, node_intermediary.NEXT_2_ID};
        String[] holding = new String[]{"FIRST", "FIRST"};

        for (int j = 0; j < get_values.length; j++) {

            socket_conn = socket_connector(get_values[j]);

            DataOutputStream dataOutputStream = new DataOutputStream(socket_conn.getOutputStream());
            try {
                dataOutputStream.writeUTF(send);

            } catch (Exception e) {
                e.printStackTrace();
            }


            DataInputStream dataInputStream = new DataInputStream(socket_conn.getInputStream());
            String result;

            try {
                dataInputStream.readUTF();


                result = dataInputStream.readUTF();


                String[] temp = result.split("POINTBREAK");

                if (!result.equals("NO_RESULT")) {
                    if (holding[0].equals("FIRST")) {
                        holding[0] = temp[0];
                        holding[1] = temp[1];

                    } else {
                        if (Long.parseLong(temp[1]) > Long.parseLong(holding[1])) {
                            holding[0] = temp[0];
                            holding[1] = temp[1];
                        }
                    }
                }

            } catch (Exception e) {
                e.printStackTrace();
                continue;
            }

            dataOutputStream.close();
            dataInputStream.close();
            socket_conn.close();
        }

        store.put("result", holding[0]);

    }

    public void server_u(String send, DataOutputStream get_output_stream) throws IOException {
        Node_Store node_intermediary = nodeCreator(send);


        String[] placement = {"key", "value", "version_control", "belongs_to"};
        SQLiteDatabase db = dbUtils.getReadableDatabase();


        Cursor c1 = db.query(TABLE_NAME, placement, query_condition(dbUtils), new String[]{node_intermediary.key}, null, null, null);

        String result = "NO_RESULT";


        if (c1 != null && c1.getCount() > 0) {


            if (c1.moveToFirst()) {

            }

            result = c1.getString(c1.getColumnIndex("value")) + "POINTBREAK" + c1.getString(c1.getColumnIndex("version_control"));
        }
        get_output_stream.writeUTF(result);


    }

    public void client_v(String send) throws IOException {
        Node_Store node_intermediary = nodeCreator(send);


        socket_conn = socket_connector(node_intermediary.STARTER_ID);

        DataOutputStream dataOutputStream = new DataOutputStream(socket_conn.getOutputStream());

        try {
            dataOutputStream.writeUTF(send);

        } catch (Exception e) {
            e.printStackTrace();
            ;
        }

        DataInputStream dataInputStream = new DataInputStream(socket_conn.getInputStream());
        String get_confirmation = dataInputStream.readUTF();
        if (get_confirmation.equals("MESSAGE_READ")) {
            dataOutputStream.close();
            dataInputStream.close();
            socket_conn.close();
        }


    }

    public void server_v(String send) {
        Node_Store node_intermediary = nodeCreator(send);
        store.put("result", node_intermediary.value);

    }

    public void client_g(String send) throws IOException {
        Node_Store node_intermediary = nodeCreator(send);


        store.put("get_all_keys", "");
        store.put("get_all_vals", "");

        for (int j = 1; j < ports.size(); j++) {


            socket_conn = socket_connector(ports.get(j));
            DataOutputStream dataOutputStream = new DataOutputStream(socket_conn.getOutputStream());
            try {

                dataOutputStream.writeUTF(send);

            } catch (Exception e) {
                e.printStackTrace();
            }


            DataInputStream dataInputStream = new DataInputStream(socket_conn.getInputStream());
            String get_values;

            try {
                dataInputStream.readUTF();


                get_values = dataInputStream.readUTF();


            } catch (Exception e) {
                e.printStackTrace();
                continue;
            }

            Node_Store temp = nodeCreator(get_values);


            store.put("get_all_keys", store.get("get_all_keys") + temp.key);

            store.put("get_all_vals", store.get("get_all_vals") + temp.value);

            dataOutputStream.close();
            dataInputStream.close();
            socket_conn.close();

        }


        store.put("gkeys", node_intermediary.key + store.get("get_all_keys"));

        store.put("gvals", node_intermediary.value + store.get("get_all_vals"));

        checkers.put("global_get_check", true);


    }


    public void server_g(String send, DataOutputStream get_output_stream) throws IOException {
        Node_Store node_intermediary = nodeCreator(send);

        SQLiteDatabase db = dbUtils.getReadableDatabase();
        Cursor c1 = db.rawQuery("SELECT  * FROM " + TABLE_NAME, null);


        store.put("keys_alt", "");
        store.put("vals_alt", "");

        if (c1 != null) {
            c1.moveToFirst();

            cursor_set(c1);

            while (!c1.isAfterLast()) {

                store.put("keys_alt", store.get("keys_alt") + c1.getString(store_int.get("key")) + "POINTBREAK");

                store.put("vals_alt", store.get("vals_alt") + c1.getString(store_int.get("value")) + "POINTBREAK");
                c1.moveToNext();

            }
        }

        Node_Store node_alter = new Node_Store(store.get("current"), null, store.get("prev"), store.get("next1"), store.get("next2"), "g");
        node_alter.STARTER_ID = node_intermediary.STARTER_ID;
        node_alter.key = store.get("keys_alt");
        node_alter.value = store.get("vals_alt");


        get_output_stream.writeUTF(StringConstructor(node_alter));


    }

    public void client_gf(String send) throws IOException {
        Node_Store node_intermediary = nodeCreator(send);

        socket_conn = socket_connector(node_intermediary.STARTER_ID);

        DataOutputStream dataOutputStream = new DataOutputStream(socket_conn.getOutputStream());

        try {
            dataOutputStream.writeUTF(send);

        } catch (Exception e) {
            e.printStackTrace();
            ;
        }

        DataInputStream dataInputStream = new DataInputStream(socket_conn.getInputStream());
        String get_confirmation = dataInputStream.readUTF();
        if (get_confirmation.equals("MESSAGE_READ")) {
            dataOutputStream.close();
            dataInputStream.close();
            socket_conn.close();
        }

    }


    public void server_gf(String send) {
        Node_Store node_intermediary = nodeCreator(send);

		                        /*Log.d(TAG, "************************************************************************");
                        Log.d("FOUND SERVER KEY :: ", Integer.toString(toSend.key.length()) + " PORT :: "+ store.get("current"));
                        Log.d("FOUND SERVER VALUE :: ", Integer.toString(toSend.value.length()) + " PORT :: "+ store.get("current"));
                        Log.d(TAG, "************************************************************************");
*/
        if (node_intermediary.key != null) {
            store.put("gkeys", node_intermediary.key);
            store.put("gvals", node_intermediary.value);
        } else {
            store.put("gkeys", "test");
            store.put("gvals", "test");
        }
        checkers.put("global_get_check", true);


    }

    public void client_d(String send) throws IOException {
        for (int j = 1; j < ports.size(); j++) {

            socket_conn = socket_connector(ports.get(j));


            DataOutputStream dataOutputStream = new DataOutputStream(socket_conn.getOutputStream());
            try {
                dataOutputStream.writeUTF(send);

            } catch (Exception e) {
                e.printStackTrace();
            }

            DataInputStream dataInputStream = new DataInputStream(socket_conn.getInputStream());
            String get_confirmation;

            try {
                dataInputStream.readUTF();


                get_confirmation = dataInputStream.readUTF();
                if (get_confirmation.equals("d_over")) {
                    dataOutputStream.close();
                    dataInputStream.close();
                    socket_conn.close();
                }

            } catch (Exception e) {
                e.printStackTrace();
                continue;
            }

        }

        checkers.put("delete_checker", true);


    }

    public void server_d(String send, DataOutputStream get_output_stream) throws IOException {
        Node_Store node_intermediary = nodeCreator(send);

        String get_col = dbUtils.COL_1 + " ='" + node_intermediary.key + "'";
        db.delete(TABLE_NAME, get_col, null);

        get_output_stream.writeUTF("d_over");


    }

    public void client_df(String send) throws IOException {
        Node_Store node_intermediary = nodeCreator(send);


        socket_conn = socket_connector(node_intermediary.STARTER_ID);

        DataOutputStream dataOutputStream = new DataOutputStream(socket_conn.getOutputStream());

        try {
            dataOutputStream.writeUTF(send);

        } catch (Exception e) {
            e.printStackTrace();
            ;
        }

        DataInputStream dataInputStream = new DataInputStream(socket_conn.getInputStream());
        String get_confirmation = dataInputStream.readUTF();
        if (get_confirmation.equals("MESSAGE_READ")) {
            dataOutputStream.close();
            dataInputStream.close();
            socket_conn.close();
        }


    }

    public void client_d_all(String send) throws IOException {
        Node_Store node_intermediary = nodeCreator(send);
        socket_conn = socket_connector(node_intermediary.NEXT_1_ID);

        DataOutputStream dataOutputStream = new DataOutputStream(socket_conn.getOutputStream());

        try {
            dataOutputStream.writeUTF(send);

        } catch (Exception e) {
            e.printStackTrace();
            ;
        }

        DataInputStream dataInputStream = new DataInputStream(socket_conn.getInputStream());
        String get_confirmation = dataInputStream.readUTF();
        if (get_confirmation.equals("MESSAGE_READ")) {
            dataOutputStream.close();
            dataInputStream.close();
            socket_conn.close();
        }

    }


    public void server_d_all(String send) {
        Node_Store node_intermediary = nodeCreator(send);

        store_int.put("number_of_deletes_performed", db.delete(TABLE_NAME, null, null));
        if (store.get("next1").equals(node_intermediary.STARTER_ID)) {

        } else {

            Node_Store node = new Node_Store(store.get("current"), null, store.get("prev"), store.get("next1"), store.get("next2"), "d_all");
            node.STARTER_ID = node_intermediary.STARTER_ID;
            node.value = Integer.toString(store_int.get("mydatabase.delete(TABLE_NAME, null, null)"));
            node.DECISION_MAKER = "d_all";


            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, StringConstructor(node));


        }
        //return 0;

    }
}
