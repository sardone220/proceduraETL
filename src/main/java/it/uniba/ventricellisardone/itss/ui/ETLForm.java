package it.uniba.ventricellisardone.itss.ui;

import it.uniba.ventricellisardone.itss.csv.CSVRecord;
import it.uniba.ventricellisardone.itss.csv.ecxception.CSVParsingException;
import it.uniba.ventricellisardone.itss.etl.Extraction;
import it.uniba.ventricellisardone.itss.etl.Loading;
import it.uniba.ventricellisardone.itss.etl.MatchBigQueryData;
import it.uniba.ventricellisardone.itss.etl.Transforming;
import it.uniba.ventricellisardone.itss.log.Log;
import org.apache.commons.io.FileUtils;

import javax.swing.*;
import javax.swing.text.DefaultCaret;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Locale;

public class ETLForm {
    private static final String TAG = "ETLForm.java";
    private static final String TABLE_NAME = "datawarehouse";

    private JPanel etlPanel;
    private JButton chooseButton;
    private JTextArea console;
    private JProgressBar uploadProgress;
    private JLabel progressValue;

    public ETLForm() {
        JTextAreaOutputStream outputStream = new JTextAreaOutputStream(console);
        DefaultCaret caret = (DefaultCaret) console.getCaret();
        caret.setUpdatePolicy(DefaultCaret.ALWAYS_UPDATE);
        System.setOut(new PrintStream(outputStream));
        System.setErr(new PrintStream(outputStream));

        chooseButton.addActionListener(e -> {
            console.setText("");
            JFileChooser chooser = new JFileChooser();
            chooser.setDialogTitle("Seleziona file da analizzare");
            int filePath = chooser.showOpenDialog(etlPanel);
            if (filePath == JFileChooser.APPROVE_OPTION) {
                try {
                    approvedOption(chooser);
                } catch (IOException | CSVParsingException ex) {
                    Log.e(TAG, "Eccezione sollevata: ", ex);
                }
            } else {
                System.out.println("Non è stato selezionato alcun file");
            }
        });
    }

    private void approvedOption(JFileChooser fileSource) throws IOException, CSVParsingException {
        JFileChooser destinationChooser = new JFileChooser();
        destinationChooser.setDialogTitle("Seleziona cartella di destinazione");
        destinationChooser.setAcceptAllFileFilterUsed(false);
        destinationChooser.setFileSelectionMode(JFileChooser.DIRECTORIES_ONLY);
        int directoryPath = destinationChooser.showOpenDialog(etlPanel);
        if (directoryPath == JFileChooser.APPROVE_OPTION) {
            transformMethod(destinationChooser, fileSource);
        } else {
            System.out.println("Non hai selezionato nessun file da analizzare");
        }
    }

    private void transformMethod(JFileChooser destinationChooser, JFileChooser sourceChooser) throws IOException, CSVParsingException {
        System.out.println("Hai selezionato il file: " + sourceChooser.getSelectedFile().getPath());
        String destinationPath = destinationChooser.getSelectedFile().getPath() + File.separator
                + new SimpleDateFormat("yyyy-MM-dd", Locale.getDefault()).format(Calendar.getInstance().getTime())
                + File.separator + "Transformed";
        System.out.println("Carico i dati in: " + destinationPath);
        File destinationFile = new File(destinationPath);
        if (!destinationFile.exists())
            FileUtils.forceMkdir(destinationFile);
        Extraction extraction = new Extraction(sourceChooser.getSelectedFile().getPath());
        List<CSVRecord> csvRecordList = extraction.getCsvRecordList();
        extraction.logParseErrorRecord(destinationPath, "parsing_error.csv");
        extraction.logNullRecord(destinationPath, "field_error.csv");
        MatchBigQueryData matchBigQueryData = null;
        System.out.println("Non ho trovato possibili duplicati");
        ExecuteTransform executeTransform = new ExecuteTransform(csvRecordList, destinationPath);
        executeTransform.execute();
    }

    public JPanel getEtlPanel() {
        return etlPanel;
    }

    private class ExecuteTransform extends SwingWorker<Boolean, Integer> {

        private final List<CSVRecord> csvRecordList;
        private final String destinationPath;


        public ExecuteTransform(List<CSVRecord> csvRecordList, String destinationPath) {
            this.csvRecordList = csvRecordList;
            this.destinationPath = destinationPath;
        }

        @Override
        protected Boolean doInBackground() {
            int transformedBlock = 0;
            ArrayList<CSVRecord> subList = new ArrayList<>();
            try {
                Loading loading = new Loading(this.destinationPath, TABLE_NAME);
                LoadData loadData = new LoadData(loading);
                Thread threadLoadData = new Thread(loadData);
                Transforming transforming = new Transforming(destinationPath, TABLE_NAME);
                for (int i = 0; i < csvRecordList.size(); i++) {
                    subList.add(csvRecordList.get(i));
                    System.out.println("I: " + i);
                    if (((i % 1000) == 0 || (i == (csvRecordList.size() - 1))) && (i != 0)) {
                        transforming.transformData(subList);
                        if (threadLoadData.isAlive())
                            threadLoadData.join();
                        loadData.setFileToLoad(transformedBlock);
                        threadLoadData = new Thread(loadData);
                        threadLoadData.start();
                        transformedBlock++;
                        publish(transformedBlock);
                        subList = new ArrayList<>();
                    }
                }
            } catch (Exception e) {
                System.err.println("Errore: " + e.getMessage());
            }
            return true;
        }

        @Override
        protected void process(List<Integer> chunks) {
            uploadProgress.setValue(chunks.get(chunks.size() - 1));
            progressValue.setText((chunks.get(chunks.size() - 1) + File.separator + csvRecordList.size() / 1000));
        }

        @Override
        protected void done() {
            super.done();
            System.out.println("Primo file generato");
        }
    }

    private static class LoadData implements Runnable {

        private int fileToLoad;
        private final Loading loadingInstance;
        private static final String TAG = "LoadDataRunnable";

        public LoadData(Loading loadingInstance) {
            fileToLoad = -1;
            this.loadingInstance = loadingInstance;
        }

        public void setFileToLoad(int fileToLoad) {
            this.fileToLoad = fileToLoad;
        }

        @Override
        public void run() {
            if (fileToLoad != -1) {
                try {
                    loadingInstance.startLoad(fileToLoad, (fileToLoad + 1));
                    Thread.currentThread().interrupt();
                } catch (IOException | InterruptedException e) {
                    Log.e(TAG, "Eccezione nel thread di caricamento", e);
                    System.err.println("Eccezione nel caricamento, verifica file di log");
                    Thread.currentThread().interrupt();
                }
            }
        }
    }
}
