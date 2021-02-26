package it.uniba.ventricellisardone.itss.ui;

import javax.swing.*;

public class MainForm {
    private JPanel mainPanel;
    private JButton analysisButton;
    private JButton etlButton;


    public MainForm() {
        analysisButton.addActionListener(e -> {
            JFrame frame = new JFrame("Data analysis");
            frame.setContentPane(new DataAnalysisForm().getDataAnalysisPanel());
            frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
            frame.pack();
            frame.setSize(800, 600);
            frame.setVisible(true);
        });
        etlButton.addActionListener(e -> {
            JFrame frame = new JFrame("Etl tool");
            frame.setContentPane(new ETLForm().getEtlPanel());
            frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
            frame.pack();
            frame.setSize(800, 600);
            frame.setVisible(true);
        });
    }

    public static void main(String[] args) {
        MainForm mainForm = new MainForm();
        JFrame frame = new JFrame("ETL tool");
        frame.setContentPane(mainForm.mainPanel);
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        frame.pack();
        frame.setVisible(true);
    }
}
