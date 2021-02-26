/**
 * Questa classe permette il caricamento dei file .csv, generati da Transforming.java, contenenti i record rielaborati e
 * pronti per essere caricari nella piattaforma cloud di Google: BigQuery.
 */
package it.uniba.ventricellisardone.itss.etl;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.*;
import com.google.common.collect.Lists;
import it.uniba.ventricellisardone.itss.log.Log;

import java.io.*;
import java.nio.channels.Channels;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;

public class Loading {

    private static final  String TAG = "LoadData.class";
    private static final String DATASET = "dataset";
    private static final String LOCATION = "eu";

    private final String dataDirectory;
    private int actualLoad;
    private final BigQuery bigQuery;
    private final WriteChannelConfiguration writeChannelConfiguration;
    private final JobId jobId;

    /**
     * Il metodo costruttore imposta principalmente i parametri per il caricamento, inoltre setta le credenziali per lo
     * accesso al cloud, setta in maniera appropiata le impostazioni di progetto, imposta la tabella di riferimento, crea
     * il canale per l'invio dei file in scrittura e crea un macro lavoro con il quale verranno avviati i caricamenti dei
     * file.
     *
     * @param dataDirectory contiene la cartella all'interno della quale sono presenti i file da caricare.
     * @param tableName     contiene il nome della tabella del dataset nella quale caricare i dati.
     * @throws IOException può essere sollevata in caso di errore nell'accesso alla memoria della macchina.
     */
    public Loading(String dataDirectory, String tableName) throws IOException {
        File directory = new File(dataDirectory);
        if (!directory.exists())
            throw new IOException("Directory not found");
        else {
            this.dataDirectory = dataDirectory;
            GoogleCredentials credentials = GoogleCredentials.fromStream(new FileInputStream(javax.swing.filechooser.FileSystemView.getFileSystemView().getHomeDirectory() + File.separator + "Documents" + File.separator + "etl-authentication.json"))
                    .createScoped(Lists.newArrayList("https://www.googleapis.com/auth/cloud-platform"));
            bigQuery = BigQueryOptions.newBuilder().setProjectId("biproject-itss").setCredentials(credentials).build().getService();
            TableId tableId = TableId.of(DATASET, tableName);
            writeChannelConfiguration = WriteChannelConfiguration.newBuilder(tableId).setFormatOptions(FormatOptions.csv()).setAutodetect(true).build();
            jobId = JobId.newBuilder().setLocation(LOCATION).setProject("biproject-itss").build();
        }
    }

    /**
     * Questo metodo imposta il thread per il caricamento del file nel cloud, leggendo il file da caricare dalla memoria,
     * inviando al canale di scrittura una copia del file e salvando in un file a parte il punto in cui il caricamento è
     * arrivato.
     *
     * @param startFrom contiene il valore intero che contraddistingue il file da caricare da cui iniziare.
     * @param endTo     contiene il valore intero che contraddistingue il file da caricare con cui ultimare
     * @throws IOException          può essere sollevata in caso di problemi con l'accesso alla memoria dell'elaboratore.
     * @throws InterruptedException maneggiando thread separati il metodo potrebbe dover gestire interruzioni inaspettate
     *                              del thread di caricamento.
     */
    public void startLoad(int startFrom, int endTo) throws IOException, InterruptedException {
        this.actualLoad = startFrom;
        saveOperation();
        Thread thread = null;
        for (int i = startFrom; i < endTo; i++) {
            String file = dataDirectory + File.separator + "load_data_" + i + ".csv";
            TableDataWriteChannel writer = bigQuery.writer(jobId, writeChannelConfiguration);
            try (OutputStream stream = Channels.newOutputStream(writer)) {
                Files.copy(Paths.get(file), stream);
            } catch (IOException | NullPointerException e) {
                System.out.println("[EXECUTING] Eccezione path: " + e.getMessage());
                Log.e(TAG, "ERRORE OUTPUT STREAM", e);
            }
            if (thread != null)
                thread.join();
            thread = new Thread(new UploadCSVFile(writer));
            thread.start();
            this.actualLoad = i;
            saveOperation();
        }
    }

    /**
     * Questo metodo permette di salvare su di un file in numero dell'ultimo file caricato, così in caso di interruzione
     * imprevista potranno essere effettuate delle operazioni preliminari sui dati per ripristinare l'esecuzione.
     *
     * @throws IOException può essere sollevata in caso di problemi con l'accesso alla memoria del calcolatore.
     */
    private void saveOperation() throws IOException {
        try (ObjectOutputStream outputStream =
                     new ObjectOutputStream(new FileOutputStream(new SimpleDateFormat("yyyy-MM-dd", Locale.getDefault())
                             .format(Calendar.getInstance().getTime())))) {
            outputStream.writeInt(actualLoad);
        }
    }

    /**
     * Questo metodo permette di leggere il file di salvataggio di cui prima.
     *
     * @param date essezialmente il nome del file di salvataggio.
     * @return il numero di file caricati.
     * @throws IOException può essere sollevata in caso di problemi con l'accesso alla memoria del calcolatore.
     */
    public int getLastLoad(Date date) throws IOException {
        try (ObjectInputStream inputStream =
                     new ObjectInputStream(new FileInputStream(new SimpleDateFormat("yyyy-MM-dd", Locale.getDefault())
                             .format(date)))) {
            return inputStream.readInt();
        }
    }

    /**
     * Questa classe implementa Runnable e quindi permette l'esecuzione del codice contenuto nel metodo run() su un thread
     * differente rispetto a quello principale. Si è adottata questa tecnica per recuperare un po' di tempo desequenzializzando
     * la lettura del file e il suo caricamento, parallelizzando questi due processi mentre un file viene caricato sul cloud,
     * il successivo viene copiato nel canale di scrittura, massimizzando l'efficienza dei due processi.
     */
    private static class UploadCSVFile implements Runnable {

        private final TableDataWriteChannel writer;

        public UploadCSVFile(TableDataWriteChannel writer) {
            this.writer = writer;
        }

        @Override
        public void run() {
            Job job = writer.getJob();
            try {
                job = job.waitFor();
                if (job.getStatus().getError() != null) {
                    System.err.println("[EXECUTING] Eccezione job: " + job.getStatus().getError());
                    JobStatistics.LoadStatistics statistics = job.getStatistics();
                    System.out.println("[EXECUTING] CARICATI: " + statistics.getOutputRows() + " RECORDS");
                }
            } catch (InterruptedException e) {
                Log.e(TAG, "ERRORE IN RUNNABLE", e);
                System.err.println("[EXECUTING] ERRORE IN RUNNABLE: " + e.getMessage());
                Thread.currentThread().interrupt();
            }
        }
    }
}
