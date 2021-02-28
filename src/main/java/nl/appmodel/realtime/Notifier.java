package nl.appmodel.realtime;

import lombok.SneakyThrows;
import lombok.val;
import java.awt.*;
import java.awt.TrayIcon.MessageType;
public class Notifier {
    public static void main(String[] args) throws AWTException {
        if (SystemTray.isSupported()) {
            Notifier td = new Notifier();
            td.displayTray("Demo", MessageType.WARNING);
        } else {
            System.err.println("System tray not supported!");
        }
    }
    @SneakyThrows
    public void displayTray(String message, MessageType type) {
        //Obtain only one instance of the SystemTray object
        val tray = SystemTray.getSystemTray();
        //If the icon is a file
        val image = Toolkit.getDefaultToolkit().createImage("icon.png");
        //Alternative (if the icon is on the classpath):
        //Image image = Toolkit.getDefaultToolkit().createImage(getClass().getResource("icon.png"));
        var trayIcon = new TrayIcon(image, "Tray Demo");
        //Let the system resize the image if needed
        trayIcon.setImageAutoSize(true);
        //Set tooltip text for the tray icon
        trayIcon.setToolTip("System tray icon demo");
        tray.add(trayIcon);
        trayIcon.displayMessage("Backend attention", message, type);
    }
}
