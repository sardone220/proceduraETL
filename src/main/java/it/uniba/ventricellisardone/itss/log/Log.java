package it.uniba.ventricellisardone.itss.log;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Calendar;

public class Log {

    private static PrintWriter logFile;

    private Log(){
        //required to hide the public implicit constructor
    }

    public static void e(String tag, String message, Exception ex) {
        try {
            logFile = new PrintWriter(new FileOutputStream("log.txt", true));
            Calendar calendar = Calendar.getInstance();
            logFile.println(tag + " [EXCEPTION LOG] " + SimpleDateFormat.getInstance().format(calendar.getTime()));
            logFile.println("\t" + message);
            logFile.println("\t" + ex.getMessage());
            logFile.println();
            logFile.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    public static void i(String tag, String message) {
        try {
            logFile = new PrintWriter(new FileOutputStream("log.txt", true));
            Calendar calendar = Calendar.getInstance();
            logFile.println(tag + " [INFO LOG] " + SimpleDateFormat.getInstance().format(calendar.getTime()));
            logFile.println("\t" + message);
            logFile.println();
            logFile.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }
}
