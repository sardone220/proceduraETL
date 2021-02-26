package it.uniba.ventricellisardone.itss;

import java.io.File;

public class MainClass {

    public static void main(String[] args) {
        System.out.println(javax.swing.filechooser.FileSystemView.getFileSystemView().getHomeDirectory() + "" + File.separator + "Documents" + File.separator + "etl-authentication.json");
    }
}
