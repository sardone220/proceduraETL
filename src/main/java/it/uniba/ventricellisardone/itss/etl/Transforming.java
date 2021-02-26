/**
 * Questa classe ha il compito di leggere la lista di record ottenuta da Extraction e trasformarla in una serie di file
 * .csv da caricare in BigQuery, trasformado i dati e aggiungendo campi strategici per l'analisi dei dati successiva.
 */
package it.uniba.ventricellisardone.itss.etl;

import it.uniba.ventricellisardone.itss.cloud.data.CloudData;
import it.uniba.ventricellisardone.itss.csv.CSVRecord;
import it.uniba.ventricellisardone.itss.log.Log;
import org.apache.commons.io.FileUtils;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Locale;

public class Transforming {
    private static final String CSV_EXTENSION = ".csv";
    // Il nuovo header che i file .csv avranno per essere caricati nel cloud.
    private static final String HEADER = "ordine_id_carrello,ordine_data,ordine_giorno_nome,ordine_giorno_dell_anno," +
            "ordine_mese_nome,ordine_anno_valore,ordine_mese_valore,ordine_trimestre,ordine_periodo," +
            "ordine_trimestre_anno,ordine_mese_anno,ordine_feriale_non,ordine_festivo_non,ordine_codice_stato," +
            "ordine_stato_nome,ordine_sesso_acquirente,ordine_quantita,ordine_prezzo_pagato,ordine_sconto," +
            "ordine_outlet,ordine_brand,ordine_collezione,ordine_colore,ordine_sesso_articolo," +
            "ordine_metodo_pagamento,ordine_taglia,ordine_categoria,ordine_macro_categoria";

    private static final String TAG = "Transform.class";
    private static final List<String> COLOR;

    // Lista dei colori ammessi tra i record.
    static {
        COLOR = List.of("ROSSO", "MULTICOLOR", "BLU", "NO COLOR", "GIALLO", "ROSA",
                "VERDE", "NERO", "GRIGIO", "MARRONE", "NEUTRO", "BIANCO", "VIOLA",
                "ARANCIONE", "FANTASIA");
    }

    private final String savingPath;
    private Integer lastFileCreated;
    private final String targetTable;

    /**
     * Il costruttore crea la directory dove salvare i file .csv generati.
     *
     * @param savingPath contiene il percorso della cartella nella quale andranno salvati i file creati.
     * @throws IOException viene sollevata se il metodo non riesce ad accedere alla memoria del calcolatore.
     */
    public Transforming(String savingPath, String targetTable) throws IOException {
        this.savingPath = savingPath;
        FileUtils.forceMkdir(new File(savingPath));
        this.lastFileCreated = 0;
        this.targetTable = targetTable;
    }

    /**
     * Questo metodo seleziona record per record i valori ottenuti da Extraction e avvia così la trasformazione sui dati e la
     * aggiunta dei record necessari alle analisi successive.
     * Contemporaneamente genera i file .csv dopo la trasformazione e tiene il conto dei file generati.
     *
     * @param csvRecordList contiene la lista dei record da trasformare.
     * @throws ParseException viene sollevata se si cerca di trasformare un record non trasformabile.
     * @throws IOException    viene sollevata se il sistema ha problemi ad accedere alla memoria del calcolatore.
     */
    public void transformData(List<CSVRecord> csvRecordList) throws ParseException, IOException {
        PrintWriter csvFile = new PrintWriter(new FileOutputStream(savingPath + File.separator + "load_data_" + lastFileCreated
                + CSV_EXTENSION), true, StandardCharsets.UTF_8);
        csvFile.println(HEADER);
        int i = 0;
        Date checkingDate = null;
        if (csvRecordList.size() > 0)
            checkingDate = csvRecordList.get(i).getOrderDate();
        List<CSVRecord> datePackage = new ArrayList<>();
        while (i < csvRecordList.size()) {
            try {
                if (!checkingDate.equals(csvRecordList.get(i).getOrderDate()) || i == (csvRecordList.size() - 1)) {
                    if ((csvRecordList.size() - 1) == i)
                        datePackage.add(csvRecordList.get(i));
                    MatchBigQueryData matchBigQueryData = new MatchBigQueryData(datePackage, this.targetTable);
                    checkMatching(csvFile, datePackage, matchBigQueryData);
                    datePackage = new ArrayList<>();
                } else
                    datePackage.add(csvRecordList.get(i));
                checkingDate = csvRecordList.get(i).getOrderDate();
                i++;
            } catch (NullPointerException | SQLException | InterruptedException ex) {
                Log.e(TAG, "Exception in transforming", ex);
            }
        }
        csvFile.close();
        lastFileCreated++;
        PrintWriter logTransform = new PrintWriter(savingPath + File.separator + "log_transform.log");
        logTransform.append(lastFileCreated.toString());
        logTransform.close();
    }

    /**
     * Questo è il metodo delegato alla verifica della conflittualità del record
     *
     * @param csvFile           il file su cui si scrivono i record rivelatisi genuini.
     * @param datePackage       contiene il pacchetto di record raggruppati per data.
     * @param matchBigQueryData contiene la classe java che effettua il match record per record.
     * @throws ParseException        può essere sollevata quando si cerca di trasformare un record che non può essere trasformato
     * @throws InterruptedException  può essere sollevata quando si verifica online la genuinità del record.
     * @throws FileNotFoundException può essere sollevata quando il calcolatore ha difficoltà ad accedere alla macchina.
     */
    private void checkMatching(PrintWriter csvFile, List<CSVRecord> datePackage, MatchBigQueryData matchBigQueryData)
            throws ParseException, InterruptedException, FileNotFoundException {
        if (!matchBigQueryData.isRecordMatching()) {
            System.out.println("Scrivo su file pacchetto di record ammesso");
            writeOnFile(csvFile, datePackage, false);
        } else {
            System.out.println("Salvo pacchetto di record non ammesso");
            PrintWriter duplicateRecords = saveDuplicateRecords();
            writeOnFile(duplicateRecords, datePackage, true);
        }
    }

    /**
     * Questo metodo permette di aprire un file writer apposito per salvare il pacco di record che hanno generato un doppione
     * nel cloud.
     *
     * @return restituisce il writer appena creato.
     * @throws FileNotFoundException viene sollevata quando si hanno problemi ad accedere alla memoria.
     */
    private PrintWriter saveDuplicateRecords() throws FileNotFoundException {
        System.out.println("Il pacchetto di record contiene record duplicati, verificare al termine del caricamento");
        String path = savingPath + File.separator + "duplicate_data" + CSV_EXTENSION;
        return new PrintWriter(new FileOutputStream(path), true, StandardCharsets.UTF_8);
    }


    /**
     * Questo metodo avvia il thread di scrittura sul file in modo da permettere che transform data prepari un altro
     * package di record da verificare e eventualmente scrivere sul file.
     * In questo metodo è presente un interruttore per impedire che i test falliscano, in quanto il multi-threading
     * non attende la scrittura del file e il test avvia la verifica su un file non ancora scritto.
     *
     * @param csvFile contiene il riferimento al file che si sta scrivendo.
     * @param records contiene il package di record da trasformare e scrivere.
     */
    private void writeOnFile(PrintWriter csvFile, List<CSVRecord> records, boolean duplicate) {
        Thread writer = new Thread(new WritePackageOnFile(csvFile, records, duplicate));
        writer.start();
        try {
            if (this.targetTable.equals("test_tabella"))
                writer.join();
        } catch (InterruptedException e) {
            Log.e(TAG, "Eccezione sollevata", e);
        }
    }

    /**
     * Questo metodo agisce come il precedente solo che effettua la trasformazione senza creare il file .csv ma restituendo
     * una lista di stringhe contenenti i nuovi record.
     *
     * @param recordList lista di record da trasformare.
     * @return lista di stringhe che rappresentano i nuovi record
     * @throws ParseException vine sollevata quando si cerca di trasformare un record non trasformabile.
     */
    public static List<String> getTransformedRecord(List<CSVRecord> recordList) throws ParseException {
        ArrayList<String> stringRecordList = new ArrayList<>();
        for (CSVRecord csvRecord : recordList) {
            CloudData cloudData = new CloudData(csvRecord.getOrderDate());
            stringRecordList.add(buildCloudRecord(csvRecord, cloudData).toString());
        }
        return stringRecordList;
    }

    /**
     * Questo metodo si occupa della creazione della stringa che rappresenta il nuovo record.
     *
     * @param record    contiene il record da scrivere.
     * @param cloudData contiene le informazioni aggiuntive sulla data calcolate.
     * @return uno stringBuilder contenente la stringa generata trasformando il record CSV da OLTP a OLAP.
     * @throws ParseException vine sollevata se si cerca di trasformare un record non trasformabile.
     */
    private static StringBuilder buildCloudRecord(CSVRecord record, CloudData cloudData) throws ParseException {
        StringBuilder bigQueryRecord = new StringBuilder();
        bigQueryRecord.append(record.getIdOrder());
        bigQueryRecord.append(",");
        bigQueryRecord.append(cloudData.getDateString());
        bigQueryRecord.append(",");
        bigQueryRecord.append(cloudData.getDayName().toUpperCase());
        bigQueryRecord.append(",");
        bigQueryRecord.append(cloudData.getDayNumber());
        bigQueryRecord.append(",");
        bigQueryRecord.append(cloudData.getMonthName().toUpperCase());
        bigQueryRecord.append(",");
        bigQueryRecord.append(cloudData.getYearValue());
        bigQueryRecord.append(",");
        bigQueryRecord.append(cloudData.getMonthValue());
        bigQueryRecord.append(",");
        bigQueryRecord.append(cloudData.getQuarter());
        bigQueryRecord.append(",");
        bigQueryRecord.append(cloudData.getSeason().toUpperCase());
        bigQueryRecord.append(",");
        bigQueryRecord.append(cloudData.getSeasonYear());
        bigQueryRecord.append(",");
        bigQueryRecord.append(cloudData.getMonthYear());
        bigQueryRecord.append(",");
        bigQueryRecord.append(cloudData.getWeekday());
        bigQueryRecord.append(",");
        bigQueryRecord.append(cloudData.getHoliday());
        bigQueryRecord.append(",");
        bigQueryRecord.append(record.getCountryCode());
        bigQueryRecord.append(",");
        bigQueryRecord.append(new Locale("IT", record.getCountryCode()).getDisplayCountry().toUpperCase());
        bigQueryRecord.append(",");
        bigQueryRecord.append(record.getCustomerGender());
        bigQueryRecord.append(",");
        bigQueryRecord.append(record.getQuantity());
        bigQueryRecord.append(",");
        bigQueryRecord.append(String.format(Locale.ROOT, "%.1f", record.getPayedPrice()));
        bigQueryRecord.append(",");
        bigQueryRecord.append(record.getDiscount());
        bigQueryRecord.append(",");
        bigQueryRecord.append(record.isOutlet() ? "OUTLET" : "NON OUTLET");
        bigQueryRecord.append(",");
        bigQueryRecord.append(record.getNomeBrand().toUpperCase());
        bigQueryRecord.append(",");
        bigQueryRecord.append(record.getCollection().toUpperCase());
        bigQueryRecord.append(",");
        bigQueryRecord.append(checkColor(record.getColor()));
        bigQueryRecord.append(",");
        bigQueryRecord.append(record.getItemGender().toUpperCase());
        bigQueryRecord.append(",");
        bigQueryRecord.append(record.getPaymentMethod().toUpperCase());
        bigQueryRecord.append(",");
        bigQueryRecord.append(record.getSize().toUpperCase());
        bigQueryRecord.append(",");
        bigQueryRecord.append(record.getCategory().toUpperCase());
        bigQueryRecord.append(",");
        bigQueryRecord.append(record.getMacroCategory().toUpperCase());
        return bigQueryRecord;
    }

    /**
     * Verifica che i colore riportato nel record sia ammesso.
     */
    private static String checkColor(String color) throws ParseException {
        if (COLOR.contains(color.toUpperCase()))
            return color.toUpperCase();
        else
            throw new ParseException("Colore " + color + " non ammesso", color.length());
    }

    /**
     * Questa classe permette di salvare in maniera asincrona i record che devono essere trasformati, siano essi in
     * conflitto o meno, infatti grazie al paramentro PrintWriter riesce a differenziare la scrittura dei record.
     */
    private static class WritePackageOnFile implements Runnable {
        private final PrintWriter csvFile;
        private final List<CSVRecord> records;
        private final boolean duplicate;

        public WritePackageOnFile(PrintWriter csvFile, List<CSVRecord> records, boolean duplicate) {
            this.csvFile = csvFile;
            this.records = records;
            this.duplicate = duplicate;
        }

        @Override
        public void run() {
            for (CSVRecord record : records) {
                try {
                    CloudData cloudData = new CloudData(record.getOrderDate());
                    StringBuilder bigQueryRecord = buildCloudRecord(record, cloudData);
                    csvFile.println(bigQueryRecord.toString());
                } catch (ParseException e) {
                    Log.e(TAG, "Eccezione sollevata", e);
                }
            }
            if (duplicate)
                csvFile.close();
        }
    }

} //end class
