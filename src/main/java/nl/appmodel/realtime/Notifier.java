package nl.appmodel.realtime;

import io.quarkus.runtime.Startup;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import javax.imageio.ImageIO;
import java.awt.*;
import java.awt.TrayIcon.MessageType;
@Slf4j
@Startup
public class Notifier {
    public static void main(String[] args) {
        if (SystemTray.isSupported()) {
            new Notifier().displayTray("Demo", "App start", MessageType.WARNING);
        } else {
            log.error("System tray not supported!");
        }
    }
    @SneakyThrows
    public void displayTray(String title, String message, MessageType type) {
        var menu = new PopupMenu("Menu");
        menu.add("Test-label");
        val tray     = SystemTray.getSystemTray();
        val image    = ImageIO.read(getClass().getClassLoader().getResource("logo64.png"));
        var trayIcon = new TrayIcon(image, message, menu);
        trayIcon.setImageAutoSize(true);
        tray.add(trayIcon);
        trayIcon.displayMessage(title, message, type);
    }
}
