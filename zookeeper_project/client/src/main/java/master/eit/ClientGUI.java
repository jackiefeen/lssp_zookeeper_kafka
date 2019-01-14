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
    private DefaultListModel listModel = new DefaultListModel<String>();
    private Boolean flagConnected;
    private Boolean flagRegistered;
    private Boolean flagOnline;

    // GUI Attributes
    private JLabel serverLabel;
    private JPanel panelMain;
    private JButton loginBtn;
    private JTextField functionText;
    private JList listOnline;
    private JTextField msgText;
    private JButton sendBtn;
    private JTextArea textArea1;
    private JButton signinButton;
    private JTextField connectionText;
    private JButton connectButton;
    private JLabel connectedLabel;
    private JLabel functionLabel;
    private JButton logoutButton;
    private JLabel onlineUsersLabel;
    private JLabel chatUserLabel;
    private JLabel registerLabel;
    private JButton createChatroomBtn;
    private JTextField chatroomTextField;
    private JLabel createChatroomLabel;
    private JList listChatrooms;
    private JLabel chatroomsLabel;
    private JButton quitButton;

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
        createChatroomBtn.setEnabled(false);
        chatroomsLabel.setEnabled(false);
        quitButton.setEnabled(false);

        functionText.setEnabled(false);
        msgText.setEnabled(false);
        onlineUsersLabel.setEnabled(false);
        chatUserLabel.setEnabled(false);
        chatroomTextField.setEnabled(false);
        createChatroomLabel.setEnabled(false);

        listOnline.setEnabled(false);
        listChatrooms.setEnabled(false);
        textArea1.setEnabled(false);
        textArea1.setEditable(false);
        textArea1.setLineWrap(true);
        textArea1.setWrapStyleWord(true);

        registerLabel.setEnabled(false);

        connectButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                try {
                    client = new Client(connectionText.getText().split(":")[0], connectionText.getText().split(":")[1]);
                    flagConnected = true;
                } catch (IOException e1) {
                    e1.printStackTrace();
                } catch (InterruptedException e1) {
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
                    listOnline.setModel(listModel);

                    if (flagOnline) {
                        functionLabel.setText("You are now Online!");

                        logoutButton.setEnabled(true);
                        quitButton.setEnabled(true);
                        onlineUsersLabel.setEnabled(true);
                        chatUserLabel.setEnabled(true);
                        listOnline.setEnabled(true);
                        listChatrooms.setEnabled(true);
                        createChatroomLabel.setEnabled(true);
                        createChatroomBtn.setEnabled(true);
                        chatroomTextField.setEnabled(true);
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
                try {
                    client.goOffline();
                } catch (InterruptedException e1) {
                    e1.printStackTrace();
                }

                listModel.clear();
                functionText.setText("");

                sendBtn.setEnabled(false);
                logoutButton.setEnabled(false);
                createChatroomBtn.setEnabled(false);
                quitButton.setEnabled(false);
                msgText.setEnabled(false);
                onlineUsersLabel.setEnabled(false);
                chatUserLabel.setEnabled(false);
                chatroomsLabel.setEnabled(false);
                chatroomTextField.setText("");
                chatroomTextField.setEnabled(false);
                createChatroomLabel.setEnabled(false);
                listOnline.setEnabled(false);
                listChatrooms.setEnabled(false);
                textArea1.setEnabled(false);

                connectionText.setEnabled(true);
                connectButton.setEnabled(true);

                chatUserLabel.setText("Chat User");
            }
        });

        listOnline.addListSelectionListener(new ListSelectionListener() {
            @Override
            public void valueChanged(ListSelectionEvent arg0) {
                if (!arg0.getValueIsAdjusting()) {
                    textArea1.setEnabled(true);
                    msgText.setEnabled(true);
                    sendBtn.setEnabled(true);

                    try {
                        textArea1.setText("");
                        chatUserLabel.setText(listOnline.getSelectedValue().toString());
                        List<String> messages = client.readMessages(functionText.getText());
                        for (String msg:messages) {
                            if (msg.contains(functionText.getText()+"="+listOnline.getSelectedValue().toString().split(" ")[0])) {
                                if (msg.substring(0, 1).equals("S"))
                                    textArea1.append("Me:" + msg.split(":")[1]);
                                else
                                    textArea1.append(msg.split("=")[1]);
                            }
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                        System.out.println("No user selected");
                    }
                }
            }
        });

        sendBtn.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                String msgsent = "";
                msgsent = client.sendMessage("S", chatUserLabel.getText().split(" ")[0], functionText.getText(), msgText.getText());
                client.sendMessage("R", functionText.getText(), chatUserLabel.getText().split(" ")[0], msgText.getText());
                msgText.setText("");
                textArea1.append(msgsent+"\n");
            }
        });

        quitButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                try {
                    client.quit();

                    listModel.clear();
                    functionLabel.setText("User "+functionText.getText()+" Deleted! Register your username or login if you are already registered");
                    functionText.setText("");

                    sendBtn.setEnabled(false);
                    logoutButton.setEnabled(false);
                    createChatroomBtn.setEnabled(false);
                    quitButton.setEnabled(false);
                    msgText.setEnabled(false);
                    onlineUsersLabel.setEnabled(false);
                    chatUserLabel.setEnabled(false);
                    chatroomsLabel.setEnabled(false);
                    chatroomTextField.setText("");
                    chatroomTextField.setEnabled(false);
                    createChatroomLabel.setEnabled(false);
                    listOnline.setEnabled(false);
                    listChatrooms.setEnabled(false);
                    textArea1.setEnabled(false);

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
        listModel.clear();
        for (String user:onlineusers) {
            if (client.username.equals(user))
                listModel.addElement("Me (" + user + ")");
            else
                listModel.addElement(user);
        }
    }
}
