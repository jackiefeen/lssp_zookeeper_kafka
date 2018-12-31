package master.eit;

import javax.swing.*;

public class ClientGUI extends JFrame {
    private JPanel panelMain;
    private JButton loginBtn;
    private JTextField usernameTextField;
    private JList listOnline;
    private JTextField msgText;
    private JButton sendBtn;
    private JTextArea textArea1;
    private JButton signinButton;
    private JTextField textField2;
    private JButton connectButton;
    private JTextField textField1;

    public ClientGUI() {
        add(panelMain);
        setTitle("NiceApp - Built from GERITAs with Passion ");
        setSize(800, 500);
        setLocationRelativeTo(null);
        setResizable(false);
        setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
    }

}
