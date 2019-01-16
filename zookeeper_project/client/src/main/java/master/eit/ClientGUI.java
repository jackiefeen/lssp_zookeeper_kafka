package master.eit;

import javax.swing.*;
import javax.swing.event.ListSelectionEvent;
import javax.swing.event.ListSelectionListener;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.IOException;
import java.util.List;

public class ClientGUI extends JFrame {

    // Variables
    private Client client;
    private DefaultListModel listModelUsers = new DefaultListModel<String>();
    private DefaultListModel listModelChatrooms = new DefaultListModel<String>();
    private Boolean flagConnected;
    private Boolean flagRegistered;
    private Boolean flagOnline;

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
    private JLabel functionLabel;
    private JButton logoutButton;
    private JLabel onlineUsersLabel;
    public JLabel chatUserLabel;
    private JLabel registerLabel;
    public JList listChatrooms;
    private JLabel chatroomsLabel;
    private JButton quitButton;

    private Thread refresh = null;

    public ClientGUI() {
        add(panelMain);
        setTitle("NiceApp - Built from GERITAs with Passion ");
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

        connectButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                try {
                    client = new Client(connectionText.getText().split(":")[0], connectionText.getText().split(":")[1]);
                    flagConnected = true;
                } catch (IOException | InterruptedException e1) {
                    e1.printStackTrace();
                }

                if (flagConnected) {
                    connectedLabel.setText("You are connected to the server");
                    connectButton.setEnabled(false);
                    connectionText.setEnabled(false);

                    signinButton.setEnabled(true);
                    loginBtn.setEnabled(true);
                    functionText.setEnabled(true);

                    registerLabel.setEnabled(true);
                    functionLabel.setText("Please, register your username first or just Login if you are already registered!");
                }
            }
        });

        signinButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                client.username = functionText.getText();
                client.register();
                flagRegistered = true;

                if (flagRegistered)
                    functionLabel.setText("You are registered!");
            }
        });

        loginBtn.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                client.username = functionText.getText();
                if (client.goOnline() == 0) {
                    flagOnline = true;
                    updateOnlineUsers(client.getOnlineusers());
                    updateChatrooms(client.getOnlinechatrooms());
                    listOnline.setModel(listModelUsers);
                    listChatrooms.setModel(listModelChatrooms);

                    if (flagOnline) {
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
                    }
                } else {
                    functionLabel.setText("You need to be registered first!");
                }
            }
        });


        logoutButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                if (refresh != null)
                    while (refresh.isAlive())
                        refresh.interrupt();

                client.goOffline();

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
                        if (refresh != null)
                            while (refresh.isAlive())
                                refresh.interrupt();
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

        listChatrooms.addListSelectionListener(new ListSelectionListener() {
            @Override
            public void valueChanged(ListSelectionEvent arg0) {
                listOnline.clearSelection();
                textAreaMsg.setEnabled(true);
                msgText.setEnabled(true);
                sendBtn.setEnabled(true);
                textAreaMsg.setText("Admin: Welcome to the chat " + functionText.getText() + "!\n");

                try {
                    if (refresh != null)
                        while (refresh.isAlive())
                            refresh.interrupt();
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

        sendBtn.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                String label = "";
                if (!listOnline.isSelectionEmpty()) {
                    label = chatUserLabel.getText().split(" ")[0];
                    String msgsent = client.sendMessage("S", label, functionText.getText(), msgText.getText());
                    client.sendMessage("R", functionText.getText(), label, msgText.getText());
                    msgText.setText("");
                    textAreaMsg.append(msgsent+"\n");
                } else {
                    String topic = "chatroom-" + chatUserLabel.getText().split(" ")[0];
                    String msgsent = client.sendMessage("R", functionText.getText(), topic, msgText.getText());
                    msgText.setText("");
                    textAreaMsg.append(msgsent + "\n");
                }
            }
        });

        quitButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                try {
                    if (refresh != null)
                        while (refresh.isAlive())
                            refresh.interrupt();
                    client.quit();

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

    public void updateOnlineUsers (List<String> onlineusers) {
        listModelUsers.clear();
        for (String user:onlineusers)
            if (client.username.equals(user))
                listModelUsers.addElement(user + " (Me)");

        for (String user:onlineusers)
            if (!client.username.equals(user))
                listModelUsers.addElement(user);
    }

    public void updateChatrooms (List<String> chatrooms) {
        listModelChatrooms.clear();
        for (String room:chatrooms)
            if (room.contains("chatroom-"))
                listModelChatrooms.addElement(room.split("-")[1]);
    }
}
