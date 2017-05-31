package edu.buffalo.cse.cse486586.simpledht;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.StreamCorruptedException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Formatter;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.content.SharedPreferences;
import android.database.Cursor;
import android.database.CursorJoiner;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;
import android.widget.TextView;

public class SimpleDhtProvider extends ContentProvider {

    String DEVICE;
    String SUCC;
    String myPort;
    int casef = 0;

    String PRED;
    int set = 0;
    String NODE = "";
    String PORT = "";
    String ASK = "";
    String PORT1;
    static final String REMOTE_PORT0 = "11108";
    static final String REMOTE_PORT1 = "11112";
    static final String REMOTE_PORT2 = "11116";
    static final String REMOTE_PORT3 = "11120";
    static final String REMOTE_PORT4 = "11124";
    String CURSOR;
    Map STAR = new LinkedHashMap();


    Uri uri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");

    ArrayList avds = new ArrayList();

    int SERVER_PORT = 10000;

    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }


    public void SimpleDhtProvider1(Context context) {

        TelephonyManager tel = (TelephonyManager) context.getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = String.valueOf((Integer.parseInt(portStr) * 2));

        if (myPort.equals("11108")) {
            DEVICE = "5554";
        } else if (myPort.equals("11112")) {
            DEVICE = "5556";
        } else if (myPort.equals("11116")) {
            DEVICE = "5558";
        } else if (myPort.equals("11120")) {
            DEVICE = "5560";

        } else if (myPort.equals("11124")) {
            DEVICE = "5562";
        }
        Log.d(TAG, "MY DEVICE: " + DEVICE);
    }


    String TAG = "ContentProvider";
    private Context mcontext;
    public static final String STORAGE_NAME = "Storagefile";
    ArrayList origkey = new ArrayList();

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {

        SharedPreferences sp = mcontext.getSharedPreferences(STORAGE_NAME, 0);
        SharedPreferences.Editor edit = sp.edit();
        if (casef == 1) {
            String hashk = null;
            try {
                hashk = genHash(selection);
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
            edit.remove(hashk);
            origkey.remove(selection);
            edit.apply();
            return 0;

        } else if (casef == 2) {
            String hashk = null;
            ArrayList dup;
            dup = (ArrayList) avds.clone();
            try {
                Log.d(TAG, "DAVDs :   " + avds);
                Log.d(TAG, "DSEL: " + selection + dup);
                hashk = genHash(selection);
                dup.add(hashk);
                Collections.sort(dup);
                Log.d(TAG, "DUP: " + dup);

                for (int i = 0; i < dup.size(); i++) {
                    if (hashk.equals((String) dup.get(i))) {
                        ASK = (String) dup.get((i + 1) % dup.size());
                        break;
                    }

                }
                Log.d(TAG, "key n hash: " + selection + " h " + hashk);
                Log.d(TAG, "ASK: " + ASK);
                dup = null;
                if (ASK.equals(genHash("5554"))) {
                    PORT1 = "11108";

                } else if (ASK.equals(genHash("5556"))) {
                    PORT1 = "11112";

                } else if (ASK.equals(genHash("5558"))) {
                    PORT1 = "11116";

                } else if (ASK.equals(genHash("5560"))) {
                    PORT1 = "11120";

                } else if (ASK.equals(genHash("5562"))) {
                    PORT1 = "11124";

                }
                Log.d(TAG, "DUPPORT: " + PORT1);
            } catch (NoSuchAlgorithmException e) {

            }
            if (PORT1.equals(myPort)) {
                edit.remove(hashk);
                origkey.remove(selection);
                edit.apply();
                return 0;
            }


            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "delete", selection, PORT1);


        }
        return 0;


    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        if (avds.size() > 1) {
            casef = 2;
        }
        Log.d(TAG, "AVDS: " + avds);
        Log.d(TAG, "caseee: " + casef);
        //origkey.add(values.getAsString("key"));
        Log.d(TAG, "orig size: @@@@@@@@@@@@@@  " + origkey.size());
        String keys = values.getAsString("key");
        String ikey = values.getAsString("key");
        try {
            ikey = genHash(ikey);

        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        String ivalue = values.getAsString("value");
        this.mcontext = getContext();
        SharedPreferences sp = mcontext.getSharedPreferences(STORAGE_NAME, 0);

        if (casef == 1) {
            origkey.add(keys);
            SharedPreferences.Editor editor = sp.edit();
            editor.putString(ikey, ivalue);
            editor.commit();
            editor.apply();
            Log.d(TAG, "value inserted");
        }

        if (casef == 2) {
            if (set == 0 && !(DEVICE.equals("5554"))) {
                set = 1;
                try {
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "findsucc", "random", "11108").get();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    e.printStackTrace();
                }
                /*try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }*/
            }
            Log.d(TAG, "AVD LIST: " + avds);

            try {
                NODE = genHash(DEVICE);
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
            int j = 0;
            while (j < avds.size()) {
                if (NODE.equals(avds.get(j))) {
                    if (j != 0 && j != avds.size() - 1) {
                        SUCC = (String) avds.get(j + 1);
                        PRED = (String) avds.get(j - 1);
                    } else if (j == 0) {
                        SUCC = (String) avds.get(j + 1);
                        PRED = (String) avds.get(avds.size() - 1);

                    } else if (j == avds.size() - 1) {
                        SUCC = (String) avds.get(0);
                        PRED = (String) avds.get(j - 1);

                    }
                }
                j++;
            }
            try {
                if (SUCC.equals(genHash("5554"))) {
                    PORT = "11108";

                } else if (SUCC.equals(genHash("5556"))) {
                    PORT = "11112";

                } else if (SUCC.equals(genHash("5558"))) {
                    PORT = "11116";

                } else if (SUCC.equals(genHash("5560"))) {
                    PORT = "11120";

                } else if (SUCC.equals(genHash("5562"))) {
                    PORT = "11124";

                }

            } catch (NoSuchAlgorithmException e) {

            }


            if ((ikey.compareTo(NODE) > 0 && ikey.compareTo(PRED) > 0 && NODE.compareTo(PRED) < 0) || (ikey.compareTo(NODE) < 0 && ikey.compareTo(PRED) < 0 && NODE.compareTo(PRED) < 0) || (ikey.compareTo(NODE) <= 0 && ikey.compareTo(PRED) > 0)) {
                Log.d(TAG, "insert here " + values.getAsString("key") + " n " + NODE);
                SharedPreferences.Editor editor = sp.edit();
                origkey.add(keys);
                editor.putString(ikey, ivalue);
                editor.commit();
                editor.apply();
                Log.d(TAG, "inserted");
            } else {
                Log.d(TAG, "other " + values.getAsString("key") + " s " + SUCC);
                String msg = "insert" + ":" + myPort;
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, values.getAsString("key") + ":" + values.getAsString("value"), PORT);
                /*try {
                    Thread.sleep(50);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }*/

            }


        }

        return uri;


    }

    @Override
    public boolean onCreate() {

        Context context = getContext();
        SimpleDhtProvider1(context);
        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            serverSocket.setReuseAddress(true);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            Log.e(TAG, "Can't create a ServerSocket");
            e.printStackTrace();
            return false;
        }
        if (casef == 0) {

            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, DEVICE, String.valueOf(casef), "11108");
            /*try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }*/


        }

        return false;
    }


    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {
        SharedPreferences sp = mcontext.getSharedPreferences(STORAGE_NAME, 0);


        if (casef == 1) {

            if (selection.equals("*") || selection.equals("@")) {
                Map<String, ?> vals = sp.getAll();
                Log.d(TAG, "del: " + vals);
                MatrixCursor mcstar = new MatrixCursor(new String[]{"key", "value"});
                Iterator it = origkey.iterator();
                while (it.hasNext()) {
                    Log.d(TAG, "DEL  here");
                    MatrixCursor.RowBuilder br = mcstar.newRow();
                    String key = (String) it.next();
                    String keyh = "";
                    try {
                        keyh = genHash(key);
                    } catch (NoSuchAlgorithmException e) {
                        e.printStackTrace();
                    }
                    br.add("key", key);
                    br.add("value", vals.get(keyh));

                }
                return mcstar;

            } else {
                String default_val = "Key not found";
                MatrixCursor mc = new MatrixCursor(new String[]{"key", "value"});
                MatrixCursor.RowBuilder br = mc.newRow();
                String selection1 = "";
                try {
                    selection1 = genHash(selection);
                } catch (NoSuchAlgorithmException e) {
                    e.printStackTrace();
                }
                br.add("key", selection);
                String ivalue = sp.getString(selection1, default_val);
                br.add("value", ivalue);
                mc.setNotificationUri(getContext().getContentResolver(), uri);

                return mc;
            }
        } else if (casef == 2) {
            if (selection.equals("@")) {
                Map<String, ?> vals = sp.getAll();
                Log.d(TAG, vals.size() + "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@");
                MatrixCursor mcstar = new MatrixCursor(new String[]{"key", "value"});

                Iterator it = origkey.iterator();
                while (it.hasNext()) {
                    MatrixCursor.RowBuilder br = mcstar.newRow();
                    String key = (String) it.next();
                    String keyh = "";
                    try {
                        keyh = genHash(key);
                    } catch (NoSuchAlgorithmException e) {
                        e.printStackTrace();
                    }
                    br.add("key", key);
                    br.add("value", vals.get(keyh));

                }
                return mcstar;

            } else if (selection.equals("*")) {

                Map<String, ?> vals2 = sp.getAll();
                MatrixCursor mcstar2 = new MatrixCursor(new String[]{"key", "value"});

                Log.d(TAG, "avd: " + DEVICE);
                Iterator it = origkey.iterator();
                while (it.hasNext()) {
                    MatrixCursor.RowBuilder br = mcstar2.newRow();
                    String key = (String) it.next();
                    String keyh = "";
                    try {
                        keyh = genHash(key);
                    } catch (NoSuchAlgorithmException e) {
                        e.printStackTrace();
                    }
                    br.add("key", key);
                    br.add("value", vals2.get(keyh));

                }


                ArrayList remotePort1 = new ArrayList();
                Iterator itr = avds.iterator();
                while (itr.hasNext()) {
                    try {
                        String search = (String) itr.next();
                        if (search.equals(genHash("5554"))) {
                            PORT = "11108";
                            remotePort1.add(PORT);


                        } else if (search.equals(genHash("5556"))) {
                            PORT = "11112";
                            remotePort1.add(PORT);

                        } else if (search.equals(genHash("5558"))) {
                            PORT = "11116";
                            remotePort1.add(PORT);

                        } else if (search.equals(genHash("5560"))) {
                            PORT = "11120";
                            remotePort1.add(PORT);

                        } else if (search.equals(genHash("5562"))) {
                            PORT = "11124";
                            remotePort1.add(PORT);

                        }
                    } catch (NoSuchAlgorithmException e) {
                    }
                }
                int p = 0;
                Log.d(TAG, "PORTS: " + remotePort1 + " c " + p + " m " + myPort);
                while (p < remotePort1.size()) {
                    if (!(remotePort1.get(p).equals(myPort))) {
                        try {
                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "*", "random", String.valueOf(remotePort1.get(p))).get();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        } catch (ExecutionException e) {
                            e.printStackTrace();
                        }
                        Log.d(TAG, "PORThere: " + STAR.size());
                        Iterator itr2 = STAR.keySet().iterator();
                        while (itr2.hasNext()) {
                            MatrixCursor.RowBuilder br = mcstar2.newRow();
                            String key = (String) itr2.next();
                            br.add("key", key);
                            br.add("value", STAR.get(key));
                            Log.d(TAG, "RowFinal: " + br);
                        }


                    }
                    p++;
                }

                return mcstar2;


            } else {
                try {
                    ArrayList dup;
                    dup = (ArrayList) avds.clone();

                    Log.d(TAG, "AVDs :   " + avds);
                    Log.d(TAG, "QSEL: " + selection + dup);
                    String hashk = genHash(selection);
                    dup.add(hashk);
                    Collections.sort(dup);
                    Log.d(TAG, "DUP: " + dup);

                    for (int i = 0; i < dup.size(); i++) {
                        if (hashk.equals((String) dup.get(i))) {
                            ASK = (String) dup.get((i + 1) % dup.size());
                            break;
                        }

                    }
                    Log.d(TAG, "key n hash: " + selection + " h " + hashk);
                    Log.d(TAG, "ASK: " + ASK);
                    dup = null;
                    if (ASK.equals(genHash("5554"))) {
                        PORT1 = "11108";

                    } else if (ASK.equals(genHash("5556"))) {
                        PORT1 = "11112";

                    } else if (ASK.equals(genHash("5558"))) {
                        PORT1 = "11116";

                    } else if (ASK.equals(genHash("5560"))) {
                        PORT1 = "11120";

                    } else if (ASK.equals(genHash("5562"))) {
                        PORT1 = "11124";

                    }
                    Log.d(TAG, "DUPPORT: " + PORT1);

                    if (PORT1.equals(myPort)) {
                        Map<String, ?> vals = sp.getAll();


                        MatrixCursor mcstar = new MatrixCursor(new String[]{"key", "value"});


                        MatrixCursor.RowBuilder br = mcstar.newRow();

                        br.add("key", selection);
                        br.add("value", vals.get(genHash(selection)));
                        Log.d(TAG, "VAlue: " + vals.get(genHash(selection)));

                        return mcstar;

                    }


                    try {
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "query", selection, PORT1).get();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    } catch (ExecutionException e) {
                        e.printStackTrace();
                    }


                    Log.d(TAG, "HErewqwqwq : " + CURSOR);


                    MatrixCursor fmr = new MatrixCursor(new String[]{"key", "value"});


                    MatrixCursor.RowBuilder br = fmr.newRow();

                    br.add("key", selection);
                    br.add("value", CURSOR);
                    return fmr;

                } catch (NoSuchAlgorithmException e) {
                }


            }

        }
        return null;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
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


    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            int seqno = 0;
            Cursor curs;
            ServerSocket serverSocket = sockets[0];
            Log.d(TAG, "working");
            Map star = new LinkedHashMap();
            String node1 = "";
            while (true) {
                try {
                    Log.d(TAG, "Server side : ");
                    Socket soc = serverSocket.accept();
                    DataInputStream dis = new DataInputStream((soc.getInputStream()));
                    DataOutputStream dos1 = new DataOutputStream(soc.getOutputStream());
                    Log.d(TAG, "in server");
                    String msgs[] = dis.readUTF().split(":");
                    String msg = msgs[0];
                    Log.d(TAG, "!!!!!!!!!: " + msg);
                    if (msg.equals("delete")) {
                        String keyf = msgs[1];
                        Log.d(TAG, "KEYDEL: " + keyf);
                        casef = 2;
                        int res = delete(uri, keyf, null);
                        Log.d(TAG, "deleted: " + res);
                        dos1.writeUTF("ok");
                        dos1.flush();


                    } else if (msg.equals("*")) {

                        Log.d(TAG, "In star *");
                        casef = 2;

                        curs = query(uri, null, "@", null, null, null);
                        int keys = curs.getColumnIndex("key");
                        int inval = curs.getColumnIndex("value");
                        curs.moveToFirst();
                        Log.d(TAG, "POS: " + curs.getPosition());
                        while (!curs.isAfterLast()) {
                            star.put(curs.getString(keys), curs.getString(inval));
                            Log.d(TAG, "CURS: " + star);
                            curs.moveToNext();
                            if (curs.isAfterLast()) {
                                break;
                            }

                        }
                        ObjectOutputStream oos = new ObjectOutputStream(soc.getOutputStream());
                        oos.writeObject(star);
                        oos.flush();


                    } else if (msg.equals("query")) {
                        String keyf = msgs[1];
                        Log.d(TAG, "KEY: " + keyf);
                        casef = 2;
                        Cursor cur = query(uri, null, keyf, null, null, null);
                        int ind = cur.getColumnIndex("value");
                        Log.d(TAG, "INDEX: " + ind);
                        cur.moveToFirst();
                        String qval = cur.getString(ind);
                        dos1.writeUTF(qval);
                        dos1.flush();


                    } else if (msg.equals("insert")) {
                        Log.d(TAG, "here");
                        dos1.writeUTF("rec");
                        ContentValues conv = null;
                        String values[] = dis.readUTF().split(":");
                        ContentValues val = new ContentValues();
                        val.put("key", values[0]);
                        val.put("value", values[1]);
                        casef = 2;
                        Log.d(TAG, "VALS: " + val);
                        dos1.writeUTF("ok");
                        dos1.flush();
                        insert(uri, val);

                    } else if (!msg.equals("findsucc")) {
                        Log.d(TAG, "came here prob");
                        try {
                            node1 = genHash(msg);
                        } catch (NoSuchAlgorithmException e) {
                            e.printStackTrace();
                        }
                        avds.add(node1);
                        Collections.sort(avds);


                        dos1.writeUTF("Yes");
                        dos1.flush();


                    } else {
                        Log.d(TAG, "find");
                        ObjectOutputStream oos = new ObjectOutputStream(soc.getOutputStream());
                        oos.writeObject(avds);
                        Log.d(TAG, "flushed" + avds);
                        oos.flush();
                        if (dis.readUTF().equals("ok")) {
                            dos1.writeUTF("Yes");
                            dos1.flush();
                        }
                    }
                    soc.close();

                } catch (IOException e) {
                    e.printStackTrace();
                }


            }

        }
    }

    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {
            Log.d(TAG, "client starts");
            String remotePort = msgs[2];
            String msg = msgs[0];
            Log.d(TAG, "rem: " + remotePort);
            Log.d(TAG, msgs[0]);


            try {

                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(remotePort));


                DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
                DataInputStream dis1 = new DataInputStream(socket.getInputStream());
                if (msgs[0].equals("delete")) {
                    dos.writeUTF(msgs[0] + ":" + msgs[1]);
                    dos.flush();
                    String DELETE = dis1.readUTF();
                    Log.d(TAG, "DEL::: " + DELETE);

                } else if (msgs[0].equals("*")) {
                    Log.d(TAG, "Star :" + msgs[0]);
                    dos.writeUTF(msgs[0]);
                    dos.flush();
                    ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
                    try {
                        STAR = (LinkedHashMap) ois.readObject();
                    } catch (ClassNotFoundException e) {
                        e.printStackTrace();
                    }
                    Log.d(TAG, "CSQUERY::: " + STAR);

                } else if (msgs[0].equals("query")) {
                    Log.d(TAG, msgs[0] + ":" + msgs[1]);
                    dos.writeUTF(msgs[0] + ":" + msgs[1]);
                    dos.flush();
                    CURSOR = dis1.readUTF();
                    Log.d(TAG, "CURSORRR::: " + CURSOR);

                } else if (!(msg.split(":")[0].equals("insert"))) {

                    dos.writeUTF(msgs[0]);
                    dos.flush();
                    if (msgs[0].equals("findsucc")) {
                        ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
                        try {
                            avds = (ArrayList) ois.readObject();
                            dos.writeUTF("ok");
                            dos.flush();
                        } catch (ClassNotFoundException e) {
                            e.printStackTrace();
                        }

                    }
                    String read = "";
                    socket.setSoTimeout(500);
                    if ((read = dis1.readUTF()).equals("Yes")) {

                        casef = 2;
                        if (msgs[0].equals("5554")) {
                            casef = 1;
                        }

                        socket.close();
                    }
                } else {


                    dos.writeUTF("insert");
                    dos.flush();
                    Boolean ans = dis1.readUTF().equals("rec");
                    if (ans) {
                        dos.writeUTF(msgs[1]);
                        casef = 2;
                        if (dis1.readUTF().equals("ok"))
                            socket.close();

                    }

                }

            } catch (IOException e) {
                Log.e(TAG, "ClientTask socket IOException");
                casef = 1;

            }


            return null;
        }

    }

}


