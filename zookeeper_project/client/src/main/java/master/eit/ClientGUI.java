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
    private JPanel panelMain;
    private JButton loginBtn;
    private JTextField registerText;
    private JList listOnline;
    private JTextField msgText;
    private JButton sendBtn;
    private JTextArea textArea1;
    private JButton signinButton;
    private JTextField connectionText;
    private JButton connectButton;
    private JTextField loginText;
    private JLabel connectedLabel;
    private JLabel registeredLabel;
    private JLabel onlineLabel;
    private JButton logoutButton;
    private JButton refreshButton;
    private JLabel onlineUsersLabel;
    private JLabel chatUserLabel;
    private JLabel registerLabel;
    private JLabel loginLabel;
    private JLabel serverLabel;
    private JButton createChatroomBtn;
    private JTextField chatroomTextField;
    private JLabel createChatroomLabel;
    private JList listChatrooms;
    private JLabel chatroomsLabel;

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
        refreshButton.setEnabled(false);
        logoutButton.setEnabled(false);
        createChatroomBtn.setEnabled(false);
        chatroomsLabel.setEnabled(false);

        registerText.setEnabled(false);
        loginText.setEnabled(false);
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
        loginLabel.setEnabled(false);

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
                    registerText.setEnabled(true);
                    loginText.setEnabled(true);

                    registerLabel.setEnabled(true);
                    loginLabel.setEnabled(true);
                    registeredLabel.setText("Please, register your username first");
                    onlineLabel.setText("Or just Login if you are already registered!");
                }
            }
        });

        signinButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                client.username = registerText.getText();
                client.register();
                flagRegistered = true;

                if (flagRegistered)
                    registeredLabel.setText("You are registered!");
            }
        });

        loginBtn.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                client.username = loginText.getText();
                if (client.goOnline() == 0) {
                    flagOnline = true;
                    updateOnlineUsers();
                    listOnline.setModel(listModel);

                    if (flagOnline) {
                        registeredLabel.setText("You are registered!");
                        onlineLabel.setText("You are now Online!");

                        refreshButton.setEnabled(true);
                        logoutButton.setEnabled(true);
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

                        registerText.setEnabled(false);
                        loginText.setEnabled(false);
                    }
                } else {
                    onlineLabel.setText("You need to be registered first!");
                }
            }
        });

        refreshButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                updateOnlineUsers();
                textArea1.setText("");
                chatUserLabel.setText("Chat User");
                textArea1.setEnabled(false);
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
                loginText.setText("");

                sendBtn.setEnabled(false);
                refreshButton.setEnabled(false);
                logoutButton.setEnabled(false);
                createChatroomBtn.setEnabled(false);
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
                        List<String> messages = client.readMessages(loginText.getText());
                        for (String msg:messages) {
                            if (msg.contains(loginText.getText()+"="+listOnline.getSelectedValue().toString().split(" ")[0])) {
                                if (msg.substring(0, 1).equals("S"))
                                    textArea1.append("Me:" + msg.split(":")[1]);
                                else
                                    textArea1.append(msg.split("=")[1]);
                            }
                        }
                    } catch (Exception e) {
                        System.out.println("No user selected");
                    }
                }
            }
        });

        sendBtn.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                String msgsent = "";
                msgsent = client.sendMessage("S", chatUserLabel.getText().split(" ")[0], loginText.getText(), msgText.getText());
                client.sendMessage("R", loginText.getText(), chatUserLabel.getText().split(" ")[0], msgText.getText());
                msgText.setText("");
                textArea1.append(msgsent+"\n");
            }
        });
    }

    private void updateOnlineUsers () {
        listModel.clear();
        for (String user:client.getOnlineusers()) {
            if (client.username.equals(user))
                listModel.addElement(user+" (Me)");
            else
                listModel.addElement(user);
        }
    }
}
