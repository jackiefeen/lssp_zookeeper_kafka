package master.eit;

import javax.swing.*;
import javax.swing.event.ListSelectionEvent;
import javax.swing.event.ListSelectionListener;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.IOException;
import java.util.List;

/**
 * GUI CLASS
 *
 * This class build the graphical interface of the Client.
 * It connects the graphical components with the functionalities
 * needed for the project.
 */
public class ClientGUI extends JFrame {

    // Variables
    private Client client;
    private DefaultListModel listModelUsers = new DefaultListModel<String>();
    private DefaultListModel listModelChatrooms = new DefaultListModel<String>();
    private Boolean flagConnected;
    private Boolean flagRegistered;

    // GUI Attributes
    private JLabel serverLabel;
    private JPanel panelMain;
    private JButton loginBtn;
    public JTextField functionText;
    public JList listOnline;
    private JTextField msgText;
    private JButton sendBtn;
    public JTextArea textAreaMsg;
    private JButton signinButton;
    private JTextField connectionText;
    private JButton connectButton;
    private JLabel connectedLabel;
    public JLabel functionLabel;
    private JButton logoutButton;
    private JLabel onlineUsersLabel;
    public JLabel chatUserLabel;
    private JLabel registerLabel;
    public JList listChatrooms;
    private JLabel chatroomsLabel;
    private JButton quitButton;

    // Pointer to the Thread that Refresh the GUI
    private Thread refresh = null;
    public static String registrationstatus = "-1";

    /**
     * CONSTRUCTOR
     */
    public ClientGUI() {
        // Initialization
        add(panelMain);
        setTitle("NiceApp - Large Scale Systems Project 2018/2019");
        setSize(700, 500);
        setLocationRelativeTo(null);
        setResizable(false);
        setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);

        signinButton.setEnabled(false);
        sendBtn.setEnabled(false);
        loginBtn.setEnabled(false);
        logoutButton.setEnabled(false);
        chatroomsLabel.setEnabled(false);
        quitButton.setEnabled(false);

        functionText.setEnabled(false);
        msgText.setEnabled(false);
        onlineUsersLabel.setEnabled(false);
        chatUserLabel.setEnabled(false);

        listOnline.setEnabled(false);
        listChatrooms.setEnabled(false);
        textAreaMsg.setEnabled(false);
        textAreaMsg.setEditable(false);
        textAreaMsg.setLineWrap(true);
        textAreaMsg.setWrapStyleWord(true);

        registerLabel.setEnabled(false);

        // CONNECT BUTTON LISTENER
        connectButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                // ATTEMPT TO CONNECT TO THE SERVER
                try {
                    client = new Client(connectionText.getText().split(":")[0], connectionText.getText().split(":")[1]);
                    flagConnected = true;
                } catch (IOException | InterruptedException e1) {
                    e1.printStackTrace();
                }

                // IF CONNECTED permit to Users to register or go online
                if (flagConnected) {
                    connectedLabel.setText("You are connected to the server");
                    connectButton.setEnabled(false);
                    connectionText.setEnabled(false);

                    signinButton.setEnabled(true);
                    loginBtn.setEnabled(true);
                    functionText.setEnabled(true);

                    registerLabel.setEnabled(true);
                    functionLabel.setText("Please, register your username first or just Login if you are already registered!");
                } else {
                    connectedLabel.setText("Unable to connect! Try again!");
                }
            }
        });

        // REGISTER BUTTON LISTENER
        signinButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                // Try to register
                client.username = functionText.getText();
                client.register();
                flagRegistered = true;

                // Try to register the user
                long initTime = System.currentTimeMillis();
                boolean timeElapsed = true;
                while (timeElapsed){
                    signinButton.setEnabled(false);
                    if(System.currentTimeMillis() - initTime > 1000 || !registrationstatus.equals("-1")){
                        timeElapsed = false;
                    }
                }

                if (registrationstatus.equals("-1"))
                    functionLabel.setText("Unfortunately the server is not answering...Try again!");
                if (registrationstatus.equals("0"))
                    functionLabel.setText("Registration failed! Try again");
                if (registrationstatus.equals("1"))
                    functionLabel.setText("You are now registered in our system!");
                if (registrationstatus.equals("2"))
                    functionLabel.setText("You are registered already! Just Go Online");
                if (registrationstatus.equals("3"))
                    functionLabel.setText("You have tried to register multiple times. Please just Go Online.");

                // Initialize Registration feedback to Initial value
                registrationstatus = "-1";
                signinButton.setEnabled(true);
            }
        });

        // GO ONLINE BUTTON LISTENER
        loginBtn.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                // Try to GO ONLINE
                client.username = functionText.getText();

                // If the user can go online this means that he is registered
                if (client.goOnline() == 0) {
                    // Update the GUI providing new functionalities for messaging
                    updateOnlineUsers(client.getOnlineusers());
                    updateChatrooms(client.getOnlinechatrooms());
                    listOnline.setModel(listModelUsers);
                    listChatrooms.setModel(listModelChatrooms);

                    functionLabel.setText("You are now Online!");

                    logoutButton.setEnabled(true);
                    quitButton.setEnabled(true);
                    onlineUsersLabel.setEnabled(true);
                    chatUserLabel.setEnabled(true);
                    listOnline.setEnabled(true);
                    listChatrooms.setEnabled(true);
                    chatroomsLabel.setEnabled(true);

                    signinButton.setEnabled(false);
                    loginBtn.setEnabled(false);

                    functionText.setEnabled(false);
                    functionText.setEnabled(false);
                } else {
                    functionLabel.setText("You need to be registered first!");
                }
            }
        });

        // GO OFFLINE BUTTON LISTENER
        logoutButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                // Kill the Main Thread used for Refresh (if active)
                if (refresh != null)
                    while (refresh.isAlive())
                        refresh.interrupt();

                // Go Offline
                client.goOffline();

                // Update the GUI
                listModelUsers.clear();
                functionText.setText("");

                sendBtn.setEnabled(false);
                logoutButton.setEnabled(false);
                quitButton.setEnabled(false);
                msgText.setEnabled(false);
                onlineUsersLabel.setEnabled(false);
                chatUserLabel.setEnabled(false);
                chatroomsLabel.setEnabled(false);
                listOnline.setEnabled(false);
                listChatrooms.setEnabled(false);
                textAreaMsg.setEnabled(false);

                connectionText.setEnabled(true);
                connectButton.setEnabled(true);

                chatUserLabel.setText("Chat User");
            }
        });

        // ONLINE USERS LIST LISTENER
        // When a user is selected update the GUI
        listOnline.addListSelectionListener(new ListSelectionListener() {
            @Override
            public void valueChanged(ListSelectionEvent arg0) {
                if (!arg0.getValueIsAdjusting()) {
                    listChatrooms.clearSelection();
                    textAreaMsg.setEnabled(true);
                    msgText.setEnabled(true);
                    sendBtn.setEnabled(true);
                    textAreaMsg.setText("Admin: Welcome to the chat " + functionText.getText() + "!\n");

                    try {
                        // If there is a Thread refreshing some chat, kill it before starting a new one
                        if (refresh != null)
                            while (refresh.isAlive())
                                refresh.interrupt();

                        // Start a new Thread for Refresh the chat selected
                        chatUserLabel.setText(listOnline.getSelectedValue().toString());
                        refresh = client.readMessages(functionText.getText(), 2);
                        refresh.start();
                    } catch (Exception e) {
                        textAreaMsg.setText("");
                        textAreaMsg.setEnabled(false);
                        chatUserLabel.setText("Chat User");
                    }
                }
            }
        });

        // ONLINE CHATROOMS LISTENER
        listChatrooms.addListSelectionListener(new ListSelectionListener() {
            @Override
            public void valueChanged(ListSelectionEvent arg0) {
                listOnline.clearSelection();
                textAreaMsg.setEnabled(true);
                msgText.setEnabled(true);
                sendBtn.setEnabled(true);
                textAreaMsg.setText("Admin: Welcome to the chat " + functionText.getText() + "!\n");

                try {
                    // If there is a Thread refreshing some chat, kill it before starting a new one
                    if (refresh != null)
                        while (refresh.isAlive())
                            refresh.interrupt();

                    // Start a new Thread for Refresh the chat selected
                    chatUserLabel.setText(listChatrooms.getSelectedValue().toString());
                    refresh = client.readMessages("chatroom-"+chatUserLabel.getText(), 2);
                    refresh.start();
                } catch (Exception e) {
                    textAreaMsg.setText("");
                    textAreaMsg.setEnabled(false);
                    chatUserLabel.setText("Chat User");
                }
            }
        });

        // SEND MESSAGE BUTTON LISTENER
        sendBtn.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                // Send messages based on the selection (if you are a user or a chatroom
                if (!listOnline.isSelectionEmpty()) {
                    String label = chatUserLabel.getText().split(" ")[0];
                    // Send a copy of the message to sender topic
                    String msgsent = client.sendMessage("S", label, functionText.getText(), msgText.getText());
                    // Send the message to the final user
                    client.sendMessage("R", functionText.getText(), label, msgText.getText());
                    msgText.setText("");
                    textAreaMsg.append(msgsent+"\n");
                } else {
                    // Send the message to the chatroom topic
                    String topic = "chatroom-" + chatUserLabel.getText().split(" ")[0];
                    String msgsent = client.sendMessage("R", functionText.getText(), topic, msgText.getText());
                    msgText.setText("");
                    textAreaMsg.append(msgsent + "\n");
                }
            }
        });

        // QUIT BUTTON LISTENER
        quitButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                try {
                    // Kill all Threads
                    if (refresh != null)
                        while (refresh.isAlive())
                            refresh.interrupt();
                    // Quit
                    client.quit();

                    // Update the GUI
                    listModelUsers.clear();
                    listModelChatrooms.clear();
                    functionLabel.setText("User "+functionText.getText()+" Deleted! Register your username or login if you are already registered");
                    functionText.setText("");

                    sendBtn.setEnabled(false);
                    logoutButton.setEnabled(false);
                    quitButton.setEnabled(false);
                    msgText.setEnabled(false);
                    onlineUsersLabel.setEnabled(false);
                    chatUserLabel.setEnabled(false);
                    textAreaMsg.setText("");
                    chatroomsLabel.setEnabled(false);
                    listOnline.setEnabled(false);
                    listChatrooms.setEnabled(false);
                    textAreaMsg.setEnabled(false);

                    loginBtn.setEnabled(true);
                    signinButton.setEnabled(true);
                    registerLabel.setEnabled(true);
                    functionText.setEnabled(true);

                    chatUserLabel.setText("Chat User");
                } catch (Exception ex) {
                    System.out.println("Nothing to Quit!!"+ex);
                }
            }
        });
    }

    // ONLINE USERS LIST UPDATER
    public void updateOnlineUsers (List<String> onlineusers) {
        listModelUsers.clear();
        for (String user:onlineusers)
            if (client.username.equals(user))
                listModelUsers.addElement(user + " (Me)");

        for (String user:onlineusers)
            if (!client.username.equals(user))
                listModelUsers.addElement(user);
    }

    // ONLINE CHATROOMS LIST UPDATER
    public void updateChatrooms (List<String> chatrooms) {
        listModelChatrooms.clear();
        for (String room:chatrooms)
            if (room.contains("chatroom-"))
                listModelChatrooms.addElement(room.split("-")[1]);
    }
}
