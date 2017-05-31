package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.content.SharedPreferences;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider {


    String DEVICE;
    LinkedHashMap<String, String> miss = new LinkedHashMap<String, String>();
    String myPort;
    ArrayList<String> AVD = new ArrayList();
    //String CURSOR;
    Map STAR = new LinkedHashMap();
    static final String REMOTE_PORT0 = "11108";
    static final String REMOTE_PORT1 = "11112";
    static final String REMOTE_PORT2 = "11116";
    static final String REMOTE_PORT3 = "11120";
    static final String REMOTE_PORT4 = "11124";


    Uri uri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");
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
        //Log.d(TAG, "MY DEVICE: " + DEVICE);
    }


    String TAG = "ContentProvider";
    private Context mcontext;
    public static final String STORAGE_NAME = "Storagefile";

    ArrayList origkey = new ArrayList();


    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {


        File f = new File(
                "/data/data/edu.buffalo.cse.cse486586.simpledynamo/shared_prefs/Storagefile.xml");
        if (f.exists()) {
            //Log.d(TAG,"Context: "+mcontext);
            this.mcontext = getContext();
            //Log.d(TAG,"Context get: "+mcontext);
            SharedPreferences sp = mcontext.getSharedPreferences(STORAGE_NAME, 0);
            SharedPreferences.Editor edit = sp.edit();
            //if ((selection.split(":")).length > 1) {
            //Log.d(TAG, "insert here replica " + selection);
            //SharedPreferences.Editor editor = sp.edit();
            //String keys = selection.split(":")[0];
            //String ikey = selection.split(":")[0];
            //String ivalue = values.getAsString("value");
           /* try {
                ikey = genHash(ikey);

            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }*/
            //edit.remove(ikey);
            //origkey.remove(selection);
            //origkey.removeAll();


            if (selection.split(",").length > 1) {
                origkey.removeAll(origkey);
                edit.clear();
                //DEFL = 1;
                edit.commit();
                edit.apply();
            } else {
                //DEFL=1;
                String remotePort1[] = {REMOTE_PORT0, REMOTE_PORT1, REMOTE_PORT2, REMOTE_PORT3, REMOTE_PORT4};


                Log.d(TAG, "RECOVER!!!!: ");
                for (int i = 0; i < remotePort1.length; i++) {
                    //if (!remotePort1[i].equals(myPort)) {

                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "delete", "delll,2", remotePort1[i]);


                    //}
                }
            }
        }


        //editor.commit();
        return 0;

        //editor.apply();

            /*

            String hashk = null;
            ArrayList dup;
            dup = (ArrayList) AVD.clone();
            try {
                Log.d(TAG, "DAVDs :   " + AVD);
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


            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "delete", selection, PORT1);*/
        //return 0;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        String SUCC1 = null;
        String SUCC2 = null;
        String PORT1 = null;
        int POS = 0;
        String ASK = "";


        this.mcontext = getContext();
        SharedPreferences sp = mcontext.getSharedPreferences(STORAGE_NAME, 0);


        if ((values.getAsString("key").split(":")).length > 1 && (values.getAsString("key").split(":"))[1].equals("rec")) {
            Log.d(TAG, "insert here recovered !!" + values.getAsString("key") + " n ");
            SharedPreferences.Editor editor = sp.edit();
            String keys = values.getAsString("key").split(":")[0];
            String ikey = values.getAsString("key").split(":")[0];
            String ivalue = values.getAsString("value");
            try {
                ikey = genHash(ikey);

            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
            origkey.add(keys);
            editor.putString(keys, ivalue);
            editor.commit();
            editor.apply();
            Log.d(TAG, "inserted recovered ");


        } else if ((values.getAsString("key").split(":")).length > 1) {
            Log.d(TAG, "insert here replica !!" + values.getAsString("key") + " n ");
            SharedPreferences.Editor editor = sp.edit();
            String keys = values.getAsString("key").split(":")[0];
            String ikey = values.getAsString("key").split(":")[0];
            String ivalue = values.getAsString("value");
            try {
                ikey = genHash(ikey);

            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
            origkey.add(keys);
            editor.putString(keys, ivalue);
            editor.commit();
            editor.apply();
            Log.d(TAG, "inserted");


        } else {
            String keys = values.getAsString("key");

            String ikey = values.getAsString("key");
            try {
                ikey = genHash(ikey);

            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
            String ivalue = values.getAsString("value");


            //Log.d(TAG, "AVD LIST: " + AVD);

            ArrayList dup;
            dup = (ArrayList) AVD.clone();

            //Log.d(TAG, "AVDs :   " + AVD);
            //Log.d(TAG, "QSEL: " + dup);

            dup.add(ikey);
            Collections.sort(dup);
            Log.d(TAG, "DUP: " + keys + " " + dup);

            for (int i = 0; i < dup.size(); i++) {
                if (ikey.equals((String) dup.get(i))) {
                    ASK = (String) dup.get((i + 1) % dup.size());
                    POS = i;
                    break;
                }

            }

            Log.d(TAG, "ASK: " + ASK + " p ");
            dup = null;
            try {
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
            } catch (NoSuchAlgorithmException e) {
            }
            int su1, su2;
            String A2, A3;
            su1 = (POS + 1) % 5;
            su2 = (POS + 2) % 5;
            A2 = AVD.get(su1);
            A3 = AVD.get(su2);
            try {
                if (A2.equals(genHash("5554"))) {
                    SUCC1 = "11108";

                } else if (A2.equals(genHash("5556"))) {
                    SUCC1 = "11112";

                } else if (A2.equals(genHash("5558"))) {
                    SUCC1 = "11116";

                } else if (A2.equals(genHash("5560"))) {
                    SUCC1 = "11120";

                } else if (A2.equals(genHash("5562"))) {
                    SUCC1 = "11124";

                }
            } catch (NoSuchAlgorithmException e) {
            }
            try {
                if (A3.equals(genHash("5554"))) {
                    SUCC2 = "11108";

                } else if (A3.equals(genHash("5556"))) {
                    SUCC2 = "11112";

                } else if (A3.equals(genHash("5558"))) {
                    SUCC2 = "11116";

                } else if (A3.equals(genHash("5560"))) {
                    SUCC2 = "11120";

                } else if (A3.equals(genHash("5562"))) {
                    SUCC2 = "11124";

                }
            } catch (NoSuchAlgorithmException e) {
            }

            Log.d(TAG, "Replicas: " + PORT1 + " s1  " + SUCC1 + " s2 " + SUCC2);
            if (PORT1.equals(myPort) || SUCC1.equals(myPort) || SUCC2.equals(myPort)) {
                Log.d(TAG, "insert here original " + values.getAsString("key") + " n "+values.getAsString("value"));
                SharedPreferences.Editor editor = sp.edit();
                origkey.add(keys);
                editor.putString(keys, ivalue);
                editor.commit();
                editor.apply();
                Log.d(TAG, "inserted");
                String msg = "insert";
                Log.d(TAG,"GETTING INSERTTTT : "+ sp.getString(values.getAsString("key"),"no key neee")+"val: "+sp.getString(values.getAsString("value"),"noneee"));
                if (PORT1.equals(myPort)) {
                    Log.d(TAG, "###########" + SUCC2 + "   " + SUCC1 + values.getAsString("key"));

                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg + ":succ1", values.getAsString("key") + ":" + values.getAsString("value"), SUCC1);


                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg + ":succ2", values.getAsString("key") + ":" + values.getAsString("value"), SUCC2);

                } else if (SUCC1.equals(myPort)) {

                    Log.d(TAG, "@@@@@@@@@@@@@" + SUCC2 + "   " + PORT1 + " " + values.getAsString("key"));

                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg + ":port1", values.getAsString("key") + ":" + values.getAsString("value"), PORT1);


                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg + ":succ2", values.getAsString("key") + ":" + values.getAsString("value"), SUCC2);


                } else if (SUCC2.equals(myPort)) {

                    Log.d(TAG, "!!!!!!!!!!!!!!!!!!" + SUCC1 + "   " + PORT1 + " " + values.getAsString("key"));

                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg + ":port1", values.getAsString("key") + ":" + values.getAsString("value"), PORT1);


                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg + ":succ1", values.getAsString("key") + ":" + values.getAsString("value"), SUCC1);


                }


            } else {
                Log.d(TAG, "other avd" + values.getAsString("key") + " s ");
                String msg = "insert";
                Log.d(TAG, "%%%%%%%%%%%%%%%%%%%5" + SUCC1 + " " + SUCC2 + "   " + PORT1);


                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg + ":port1", values.getAsString("key") + ":" + values.getAsString("value"), PORT1);


                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg + ":succ1", values.getAsString("key") + ":" + values.getAsString("value"), SUCC1);

                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg + ":succ2", values.getAsString("key") + ":" + values.getAsString("value"), SUCC2);


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
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            Log.e(TAG, "Can't create a ServerSocket");
            e.printStackTrace();
            return false;
        }



        AVD.add("177ccecaec32c54b82d5aaafc18a2dadb753e3b1");
        AVD.add("208f7f72b198dadd244e61801abe1ec3a4857bc9");
        AVD.add("33d6357cfaaf0f72991b0ecd8c56da066613c089");
        AVD.add("abf0fd8db03e5ecb199a9b82929e9db79b909643");
        AVD.add("c25ddd596aa7c81fa12378fa725f706d54325d12");
        Log.d(TAG, "AVD LIST: " + AVD);


        SharedPreferences checkrestart = this.getContext().getSharedPreferences("isRestart", 0);
        Log.d(TAG, "Start1: " + checkrestart.getBoolean("isRestart", true));
        if (checkrestart.getBoolean("isRestart", true)) {

            Log.d(TAG, "STARTT!!!!!: ");
            checkrestart.edit().putBoolean("isRestart", false).commit();


        } else {

            String remotePort1[] = {REMOTE_PORT0, REMOTE_PORT1, REMOTE_PORT2, REMOTE_PORT3, REMOTE_PORT4};

            Log.d(TAG, "RECOVER!!!!: ");
            for (int i = 0; i < remotePort1.length; i++) {
                if (!remotePort1[i].equals(myPort)) {
                    try {
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "recover", "getmap", remotePort1[i]).get();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    } catch (ExecutionException e) {
                        e.printStackTrace();
                    }
                }
            }
            Log.d(TAG, "msg recovery done");



        }






        return false;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection,
                        String[] selectionArgs, String sortOrder) {



        String PORT1 = null;
        Log.d(TAG,"!!!!!!!!!!!!!!!!!!!!!!!!: "+ selection);
        String PORT = "";
        String ASK = "";
        this.mcontext = getContext();
        SharedPreferences sp = mcontext.getSharedPreferences(STORAGE_NAME, 0);

        if (selection.equals("@")) {
            Map<String, ?> vals = sp.getAll();
            Log.d(TAG, "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@" + vals.size());
            MatrixCursor mcstar = new MatrixCursor(new String[]{"key", "value"});
            Iterator it = vals.keySet().iterator();

            while (it.hasNext()) {
                //Log.d(TAG, "her!!!!!!!!!!!1");
                MatrixCursor.RowBuilder br = mcstar.newRow();
                String key = (String) it.next();
                String keyh = "";
                try {
                    keyh = genHash(key);
                } catch (NoSuchAlgorithmException e) {
                    e.printStackTrace();
                }
                br.add("key", key);
                br.add("value", vals.get(key));

            }
            return mcstar;

        } else if (selection.equals("*")) {

            Map<String, ?> vals2 = sp.getAll();
            MatrixCursor mcstar2 = new MatrixCursor(new String[]{"key", "value"});

            //Log.d(TAG, "avd: " + DEVICE);
            Iterator it = vals2.keySet().iterator();
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
                br.add("value", vals2.get(key));

            }


            ArrayList remotePort1 = new ArrayList();
            Iterator itr = AVD.iterator();
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
            //Log.d(TAG, "PORTS: " + remotePort1 + " c " + p + " m " + myPort);
            while (p < remotePort1.size()) {
                if (!(remotePort1.get(p).equals(myPort))) {
                    try {
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "*", "random", String.valueOf(remotePort1.get(p))).get();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    } catch (ExecutionException e) {
                        e.printStackTrace();
                    }
                    //Log.d(TAG, "PORThere: " + STAR.size());
                    Iterator itr2 = STAR.keySet().iterator();
                    while (itr2.hasNext()) {
                        MatrixCursor.RowBuilder br = mcstar2.newRow();
                        String key = (String) itr2.next();
                        br.add("key", key);
                        br.add("value", STAR.get(key));
                        //Log.d(TAG, "RowFinal: " + br);
                    }


                }
                p++;
            }

            return mcstar2;


        } else {
            /*try {
                Thread.sleep(350);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }*/
            try {
                String SUCC1 = "";
                String SUCC2 = "";
                int POS = 0;
                ArrayList dup;
                dup = (ArrayList) AVD.clone();

                if (selection.split(",").length > 1) {
                    Map<String, ?> vals = sp.getAll();
                    MatrixCursor mcstar = new MatrixCursor(new String[]{"key", "value"});
                    MatrixCursor.RowBuilder br = mcstar.newRow();
                    Log.d(TAG,"~~~~~~~~~~~~~~~~~keysss: "+selection);
                    br.add("key", selection);
                    br.add("value", vals.get(selection.split(",")[0]));
                    //Log.d(TAG, "VAlue: " + vals.get(genHash(selection)

                    return mcstar;


                }
                Log.d(TAG,"!!!!!!!!~~~~~~~~~~~~~~~~~keysss: "+selection);
                String hashk = genHash(selection);
                dup.add(hashk);
                Collections.sort(dup);
                //Log.d(TAG, "DUP: " + dup);

                for (int i = 0; i < dup.size(); i++) {
                    if (hashk.equals((String) dup.get(i))) {
                        ASK = (String) dup.get((i + 1) % dup.size());
                        POS = i;
                        break;
                    }

                }
                //Log.d(TAG, "key n hash: " + selection + " h " + hashk);
                //Log.d(TAG, "ASK: " + ASK);
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


                if (PORT1.equals("11108")) {
                    SUCC1 = "11116";
                    SUCC2 = "11120";

                } else if (PORT1.equals("11112")) {
                    SUCC1 = "11108";
                    SUCC2 = "11116";

                } else if (PORT1.equals("11116")) {
                    SUCC1 = "11120";
                    SUCC2 = "11124";

                } else if (PORT1.equals("11120")) {
                    SUCC1 = "11124";
                    SUCC2 = "11112";

                } else if (PORT1.equals("11124")) {
                    SUCC1 = "11112";
                    SUCC2 = "11108";

                }
                Log.d(TAG, "DUPPORT: " + PORT1 + " s " + SUCC1 + " s2 "+SUCC2);

                if (PORT1.equals(myPort) || SUCC1.equals(myPort) || SUCC2.equals(myPort)) {
                    Map<String, ?> vals = sp.getAll();
                    MatrixCursor mcstar = new MatrixCursor(new String[]{"key", "value"});
                    MatrixCursor.RowBuilder br = mcstar.newRow();

                    br.add("key", selection);
                    br.add("value", vals.get(selection));
                    //Log.d(TAG, "VAlue: " + vals.get(genHash(selection)));

                    return mcstar;

                }
                ArrayList <String>sdf = new ArrayList();
                sdf.add(PORT1);
                sdf.add(SUCC1);
                sdf.add(SUCC2);
                String CURS = "";
                String prev="";
                for(String s: sdf){

                    CURS = queryclient("query", selection + ",f1", s);
                    Log.d(TAG, "***********************QUERY ORIGINAL ************************************* : " + CURS);
                    Log.d(TAG, "***********************QUERY ORIGINAL ************************************* : " + prev);
                    if(CURS!=null && prev.equals(CURS)){
                        break;
                    }else if(CURS!=null){
                        prev=CURS;

                    }
                }
                //try {
                //CURS = queryclient("query", selection + ",f1", PORT1);
                //new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "query", selection, PORT1).get();
               /* } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    e.printStackTrace();
                }*/


                Log.d(TAG, "***********************QUERY ORIGINAL ************************************* : " + CURS);
                /*if (CURS == null) {
                    Log.d(TAG, "cursor null: " + SUCC1 + " p " + PORT1);
                    CURS = queryclient("query", selection + ",f1", SUCC1);

                    /*try {
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "query", selection + "," + "fl", SUCC1).get();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    } catch (ExecutionException e) {
                        e.printStackTrace();
                    }


                }*/

                Log.d(TAG, "***********************QUERY SUCCC************************************ : " + CURS);

                MatrixCursor fmr = new MatrixCursor(new String[]{"key", "value"});


                MatrixCursor.RowBuilder br = fmr.newRow();

                br.add("key", selection);
                br.add("value", CURS);
                return fmr;

            } catch (NoSuchAlgorithmException e) {
            }


        }


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


    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            int seqno = 0;
            Cursor curs;
            ServerSocket serverSocket = sockets[0];
            //Log.d(TAG, "working");
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
                    if (msg.equals("recover")) {
                        ObjectOutputStream oos = new ObjectOutputStream(soc.getOutputStream());
                        Log.d(TAG, "miss: " + miss);
                        oos.writeObject(miss);
                        oos.flush();
                        miss.clear();
                    } else if (msg.equals("delete")) {
                        String keyf = msgs[1];
                        Log.d(TAG, "KEYDEL: " + keyf);
                        //casef = 2;
                        int res = delete(uri, keyf, null);
                        Log.d(TAG, "deleted: " + res);
                        dos1.writeUTF("ok");
                        dos1.flush();


                    } else if (msg.equals("*")) {

                        Log.d(TAG, "In star *");
                        //casef = 2;

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
                        Log.d(TAG, "!!!!!!!!!!!#########33333KEY: " + keyf);
                        //casef = 2;
                        Cursor cur = query(uri, null, keyf, null, null, null);
                        int ind = cur.getColumnIndex("value");

                        cur.moveToFirst();
                        Log.d(TAG, "STATUS:   " + cur.isAfterLast() + " " + ind);
                        String qval = cur.getString(ind);
                        Log.d(TAG, "INDEX: " + ind + "keys: " + keyf + "vals: " + qval);
                        if (qval != null) {
                            dos1.writeUTF(qval);
                            dos1.flush();
                        }

                    } else if (msg.equals("insert")) {
                        Log.d(TAG, "here");
                        dos1.writeUTF("rec");
                        String values[] = dis.readUTF().split(":");
                        ContentValues val = new ContentValues();
                        val.put("key", values[0] + ":" + msgs[1]);
                        val.put("value", values[1]);
                        //casef = 2;
                        Log.d(TAG, "VALS: " + val);
                        insert(uri, val);
                        dos1.writeUTF("ok");
                        dos1.flush();
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
            Log.d(TAG, msg);


            try {


                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(remotePort));

                Log.d(TAG, "reached herer");
                DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
                DataInputStream dis1 = new DataInputStream(socket.getInputStream());
                if (msgs[0].equals("recover")) {
                    dos.writeUTF(msgs[0]);
                    dos.flush();
                    ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
                    LinkedHashMap<String, String> temp = new LinkedHashMap();

                    try {
                        temp = (LinkedHashMap) ois.readObject();
                        Log.d(TAG, "temp: " + temp + " s " + temp.size());
                    } catch (ClassNotFoundException e) {
                        e.printStackTrace();
                    }
                    ContentValues val = new ContentValues();
                    Iterator<String> it = temp.keySet().iterator();
                    while (it.hasNext()) {
                        String key = it.next();

                        val.put("key", key + ":rec");
                        val.put("value", temp.get(key));

                        Log.d(TAG, "VALS: " + val);
                        insert(uri, val);

                    }
                    //miss.clear();

                    //miss.putAll(temp);


                } else if (msgs[0].equals("delete")) {
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
                    //Log.d(TAG, "CSQUERY::: " + STAR);

                } else {
                    //socket.setSoTimeout(200);
                    Log.d(TAG, "INS!!!!!!!!!!!!!!!1");
                    dos.writeUTF(msgs[0]);

                    dos.flush();

                    Boolean ans = dis1.readUTF().equals("rec");
                    Log.d(TAG, "  r  " + remotePort + " a   " + ans);
                    if (ans) {
                        dos.writeUTF(msgs[1]);
                        Log.d(TAG, "  wrrrr  " + remotePort + "key" + msgs[1]);
                        //casef = 2;
                        if (dis1.readUTF().equals("ok"))
                            socket.close();


                    }

                }

            } catch (IOException e) {
                Log.e(TAG, "ClientTask socket IOException");
                if (msgs[0].contains("insert")) {
                    Log.d(TAG, "insert timeout: " + msgs[2]);

                    miss.put(msgs[1].split(":")[0], msgs[1].split(":")[1]);


                    Log.d(TAG, "asdsd: " + miss);
                }


                //casef = 1;

            }


            return null;
        }

    }

    public String queryclient(String... msgs) {
        Log.d(TAG, "client starts");
        String remotePort = msgs[2];
        String msg = msgs[0];
        Log.d(TAG, "rem: " + remotePort);
        Log.d(TAG, msg);
        String CURSOR;

        try {


            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                    Integer.parseInt(remotePort));

            Log.d(TAG, "reached herer");
            DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
            DataInputStream dis1 = new DataInputStream(socket.getInputStream());
            if (msgs[0].equals("query")) {
                CURSOR = "";
                Log.d(TAG, "Queryingg:" + remotePort + "   mmsgg   " + msgs[0] + ":" + msgs[1]);
                dos.writeUTF(msgs[0] + ":" + msgs[1]);
                dos.flush();
                //while(CURSOR.equals("")) {
                CURSOR = dis1.readUTF();
                Log.d(TAG, "CURSORRR::: " + CURSOR + "   port   " + remotePort + "  m    " + msgs[1]);
                //}
                Log.d(TAG, "CURSORRR::: " + CURSOR + "   port   " + remotePort + "  m    " + msgs[1]);
                return CURSOR;

            }


        } catch (EOFException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }


        return null;

    }


}
