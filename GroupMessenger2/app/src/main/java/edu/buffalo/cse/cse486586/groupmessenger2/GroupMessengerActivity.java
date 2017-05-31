package edu.buffalo.cse.cse486586.groupmessenger2;

import android.app.Activity;
import android.content.ContentResolver;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.os.Bundle;
import android.telephony.TelephonyManager;
import android.text.method.ScrollingMovementMethod;
import android.util.Log;
import android.view.Menu;
import android.view.View;
import android.widget.Button;
import android.widget.EditText;
import android.widget.TextView;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.StreamCorruptedException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.PriorityBlockingQueue;

/**
 * GroupMessengerActivity is the main Activity for the assignment.
 *
 * @author stevko
 */
public class GroupMessengerActivity extends Activity {
    static final String TAG = GroupMessengerActivity.class.getSimpleName();
    static final String REMOTE_PORT0 = "11108";
    static final String REMOTE_PORT1 = "11112";
    static final String REMOTE_PORT2 = "11116";
    static final String REMOTE_PORT3 = "11120";
    static final String REMOTE_PORT4 = "11124";
    static final int SERVER_PORT = 10000;
    int finc = 0;
    String PORT = null;
    int count;

    LinkedHashMap holdb = new LinkedHashMap();
    TreeMap sequencer = new TreeMap();
    private ContentResolver conres;
    ContentValues cv;
    private Uri uri;

    @Override
    protected void onCreate(Bundle savedInstanceState) {


        setContentView(R.layout.activity_group_messenger);
        super.onCreate(savedInstanceState);

        TelephonyManager tel = (TelephonyManager) this.getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));
        uri = Uri.parse("content://edu.buffalo.cse.cse486586.groupmessenger2.provider");
        try {

            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {

            Log.e(TAG, "Can't create a ServerSocket");
            return;
        }
        // final String[] msg = {""};

        /*
         * TODO: Use the TextView to display your messages. Though there is no grading component
         * on how you display the messages, if you implement it, it'll make your debugging easier.
         */
        final TextView tv = (TextView) findViewById(R.id.textView1);
        tv.setMovementMethod(new ScrollingMovementMethod());
        /*
         * Registers OnPTestClickListener for "button1" in the layout, which is the "PTest" button.
         * OnPTestClickListener demonstrates how to access a ContentProvider.
         */
        findViewById(R.id.button1).setOnClickListener(
                new OnPTestClickListener(tv, getContentResolver()));
        final EditText editText = (EditText) findViewById(R.id.editText1);
        final Button sendbutton = (Button) findViewById(R.id.button4);
        sendbutton.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                String msg = "";
                msg = editText.getText().toString() + "\n";
                editText.setText("");
                count += 1;
                SimpleDateFormat sd = new SimpleDateFormat("ddMMyyyyhhmmssSSS");
                msg = msg + ":" + myPort;
                // Log.d(TAG,msg);
                //tv.append(msg);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, myPort);


            }
        });


        /*
         * TODO: You need to register and implement an OnClickListener for the "Send" button.
         * In your implementation you need to get the message from the input box (EditText)
         * and send it to other AVDs.
         */
    }

    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.activity_group_messenger, menu);
        return true;
    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            int seqno = 0;
            ServerSocket serverSocket = sockets[0];
            ArrayList remo = new ArrayList();

            while (true) {
                try {
                    Socket soc = serverSocket.accept();
                    DataInputStream dis = new DataInputStream((soc.getInputStream()));
                    DataOutputStream dos1 = new DataOutputStream(soc.getOutputStream());
                    try {
                        String msg = dis.readUTF();
                        String[] mesg = msg.split(":");

                        if (PORT != null) {
                            Collection col = holdb.keySet();
                            Iterator ch = col.iterator();
                            while (ch.hasNext()) {
                                String del = (String) ch.next();
                                String spl[] = del.split(":");
                                if (spl[1].equals(PORT)) {
                                    remo.add(del);
                                    Log.d(TAG, "REM: " + del + " " + holdb.get(del));
                                }
                            }
                        }
                        if (mesg[3].equals("ud")) {
                            dos1.writeUTF(mesg[0] + ":" + String.valueOf(seqno));
                            Log.d(TAG, "Enter: " + mesg[0] + seqno);
                            holdb.put(mesg[0] + ":" + mesg[1], seqno + ":" + "ud");
                            dos1.flush();
                            seqno++;
                            //soc.close();
                        } else if (mesg[3].equals("d")) {
                            dos1.writeUTF("rec");
                            String order = (String) holdb.get(mesg[0] + ":" + mesg[1]);
                            String ods[] = order.split(":");
                            Log.d(TAG, "Message :" + mesg[0] + mesg[2]);
                            ods[0] = mesg[2];
                            ods[1] = mesg[3];
                            holdb.put(mesg[0] + ":" + mesg[1], ods[0] + ":" + ods[1]);

                            holdb.keySet().removeAll(remo);
                            Iterator it = holdb.keySet().iterator();
                            TreeMap seq = new TreeMap();
                            while (it.hasNext()) {
                                Object keys = it.next();
                                String ret = (String) holdb.get(keys);
                                String re[] = ret.split(":");
                                int sq = Integer.parseInt(re[0]);
                                seq.put(sq, keys + ":" + re[1]);
                            }
                            Log.d(TAG, "Tree: " + String.valueOf(seq));
                            while (true && !seq.isEmpty()) {
                                String val = (String) seq.get(seq.firstKey());
                                String br[] = val.split(":");
                                if (br[1].equals(PORT)) {
                                    seq.remove(val);
                                }
                                if (br[2].equals("d")) {
                                    publishProgress(seq.firstKey() + ":" + br[0]);
                                    Log.d(TAG, "remove: " + br[0] + seq.firstKey());
                                    //Log.d(TAG,seq.firstKey()+" " +br[0]);
                                    seq.remove(seq.firstKey());
                                    holdb.remove(br[0] + ":" + br[1]);

                                }
                                else {
                                    break;
                                }


                            }
                        }
                        soc.close();
                    } catch (EOFException e) {
                        e.printStackTrace();
                    }
                } catch (EOFException e) {
                    e.printStackTrace();

                } catch (Exception e) {
                    e.printStackTrace();
                }

            }

        }

        protected void onProgressUpdate(String... strings) {

            /*
             * The following code displays what is received in doInBackground().
             */

            String strReceived = strings[0].trim();
            String[] mesg = strReceived.split(":");
            // Log.d(TAG,"CP: "+mesg[0]+mesg[1]);
            int del = Integer.parseInt(mesg[0]);
            if (finc <= del) {
                String ikey = String.valueOf(finc);
                // Log.d(TAG, ikey);
                String ivalue = String.valueOf(mesg[1]);
                //Log.d(TAG, ivalue);
                conres = getContentResolver();
                //Log.d(TAG, conres.toString());
                cv = new ContentValues();
                cv.put("key", ikey);
                cv.put("value", ivalue);
                conres.insert(uri, cv);
                finc++;
            }
            //seq.put(mesg[1],mesg[0]);

            TextView remoteTextView = (TextView) findViewById(R.id.textView1);
            remoteTextView.append("\t" + mesg[1] + "\t\n");




            /*
             * The following code creates a file in the AVD's internal storage and stores a file.
             *
             * For more information on file I/O on Android, please take a look at
             * http://developer.android.com/training/basics/data-storage/files.html
             */

            String filename = "GroupMessengerOutput";
            String string = strReceived + "\n";
            FileOutputStream outputStream;

            try {
                outputStream = openFileOutput(filename, Context.MODE_PRIVATE);
                outputStream.write(string.getBytes());
                outputStream.close();
            } catch (Exception e) {
                Log.e(TAG, "File write failed");
            }

            return;
        }
    }


    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {
            // Log.d(TAG, "client starts");

            // String remotePort1[];
            String remotePort1[] = {REMOTE_PORT0, REMOTE_PORT1, REMOTE_PORT2, REMOTE_PORT3, REMOTE_PORT4};
            ArrayList seqno = new ArrayList();
            String flag = "ud";
            String msgToSend = null;
            for (int i = 0; i < remotePort1.length; i++) {
                try {

                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(remotePort1[i]));
                    msgToSend = msgs[0];
                    socket.setSoTimeout(500);
                    socket.setTcpNoDelay(true);
                    DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
                    DataInputStream dis1 = new DataInputStream(socket.getInputStream());
                    dos.writeUTF(msgToSend + ":" + remotePort1[i] + ":" + flag);
                    //Log.d(TAG,"send");
                    dos.flush();
                    //Log.d(TAG,"send1");
                    String sef = dis1.readUTF();
                    //Log.d(TAG,"send2");

                    String vals[] = sef.split(":");
                    String add = String.valueOf(vals[1]) + ":" + remotePort1[i];
                    seqno.add(add);
                    sequencer.put(vals[0], seqno);
                    //Log.d(TAG,"rec : "+sequencer);
                    socket.close();
                    //Log.d(TAG,"send4");

                } catch (UnknownHostException e) {
                    Log.e(TAG, "ClientTask UnknownHostException");
                } catch (SocketException e) {
                    Log.e(TAG, "Socket Exception");
                    e.printStackTrace();
                } catch (SocketTimeoutException e) {
                    Log.e(TAG, "SocketTimeout Exception");
                    e.printStackTrace();
                } catch (StreamCorruptedException e) {
                    Log.e(TAG, "Stream Currupt Exception");
                    e.printStackTrace();
                } catch (IOException e) {
                    Log.e(TAG, "ClientTask socket IOException");
                    PORT = remotePort1[i];
                    //e.printStackTrace();
                }
            }
            if (seqno.size() < 5) {

            }
            int max = -1, id = -1;

            flag = "d";
            for (int m = 0; m < seqno.size(); m++) {
                String ret = (String) seqno.get(m);
                String retr[] = ret.split(":");

                if (max < Integer.parseInt(retr[0])) {
                    max = Integer.parseInt(retr[0]);
                    id = Integer.parseInt(retr[1]);

                }
            }
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            for (int j = 0; j < remotePort1.length; j++) {
                try {

                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(remotePort1[j]));

                    DataOutputStream dos1 = new DataOutputStream(socket.getOutputStream());
                    dos1.writeUTF(msgToSend + ":" + max + id + ":" + flag);
                    //Log.d(TAG,"send seq");
                    dos1.flush();
                    //Log.d(TAG,maxseq+""+"sent sequence");
                    socket.setSoTimeout(500);
                    DataInputStream dis2 = new DataInputStream(socket.getInputStream());
                    String rec = dis2.readUTF();
                    if (rec.equals("rec")) {
                        socket.close();
                    }

                } catch (UnknownHostException e) {
                    Log.e(TAG, "ClientTask UnknownHostException");
                } catch (IOException e) {
                    Log.e(TAG, "ClientTask socket IOException");
                    //e.printStackTrace();
                    PORT = remotePort1[j];
                }
            }


            return null;
        }
    }

}

