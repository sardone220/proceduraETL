/**
 * Questa classe avvia permette la lettura dei file .csv in ingresso formando la lista di record contenuta nel file e scartando
 * i record non idonei, permettendo poi il savlataggio degli stessi per individuare le cause dello scarto e poter eventualmente
 * intervenire per "aggiustare" i record non aderenti al protocollo di comunicazione accordato.
 */

package it.uniba.ventricellisardone.itss.etl;

import it.uniba.ventricellisardone.itss.csv.CSVRecord;
import it.uniba.ventricellisardone.itss.csv.ecxception.CSVNullFieldsException;
import it.uniba.ventricellisardone.itss.csv.ecxception.CSVParsingException;
import it.uniba.ventricellisardone.itss.log.Log;
import org.apache.commons.io.FileUtils;

import java.io.*;
import java.nio.file.Paths;
import java.text.ParseException;
import java.util.*;

public class Extraction {

    protected static final Map<String, Integer> HEADER_FILE;
    private static final String TAG = "CSVFile.class";
    private static final String CSV_EXTENSION = ".csv";
    private static final String RESULTS_DIR = "" + File.separator + "Results" + File.separator + "";

    //  L'header file che i file in input devono avere per essere ritenuti idonei.
    static {
        HEADER_FILE = Map.ofEntries(Map.entry("IdOrdine", 0),
                Map.entry("DataOrdine", 1),
                Map.entry("CodStatoFattura", 2),
                Map.entry("SexAcquirente", 3),
                Map.entry("Quantita", 4),
                Map.entry("PrezzoPagato", 5),
                Map.entry("Sconto", 6),
                Map.entry("Outlet", 7),
                Map.entry("NomeBrand", 8),
                Map.entry("Collezione", 9),
                Map.entry("Colore", 10),
                Map.entry("SexArticolo", 11),
                Map.entry("PagamentoOrdine", 12),
                Map.entry("ValoreTagliaEffettivo", 13),
                Map.entry("NomeCategoria", 14),
                Map.entry("MacroCategoria", 15));

    }

    private Map<String, Integer> headerFile;
    private Scanner fileScanner;
    private List<CSVRecord> csvRecordList;
    private List<String> nullRecordList;
    private List<String> parseErrorList;

    public Extraction(String dataPath) throws CSVParsingException{
        try {
            dataPath = Paths.get(dataPath).toString();
            fileScanner = new Scanner(new File(dataPath));
            setHeaderFile();
            if(!this.headerFile.equals(HEADER_FILE))
                throw new CSVParsingException("Gli header dei file non coincidono", 0);
            else
                setCsvRecordList();
        } catch (FileNotFoundException e) {
            Log.e(TAG, "Constructor exception: ", e);
        }
    }

    public Map<String, Integer> getHeaderFile() {
        return headerFile;
    }

    /**
     * Questo metodo genera una mappa che contiene l'header del file in lettura per consentire il confronto con l'header
     * file standard
     */
    private void setHeaderFile() {
        String[] headerString = fileScanner.nextLine().split(";");
        headerFile = new HashMap<>();
        for (int i = 0; i < headerString.length; i++)
            headerFile.put(headerString[i], i);
    }

    public List<CSVRecord> getCsvRecordList() {
        return csvRecordList;
    }

    /**
     * Questo metodo permette di intercettare i record che persentano difetti strutturali e memorizzarli in liste differenti,
     * mentre crea la lista dei record principale.
     */
    private void setCsvRecordList() {
        csvRecordList = new ArrayList<>();
        while(fileScanner.hasNextLine()){
            String fileLine = fileScanner.nextLine();
            String[] stringRecord = fileLine.split(";");
            try {
                CSVRecord csvRecord = new CSVRecord(stringRecord);
                csvRecordList.add(csvRecord);
            }catch (CSVNullFieldsException ex){
                if(nullRecordList == null)
                    nullRecordList = new ArrayList<>();
                nullRecordList.add(fileLine);
            }catch (ParseException | NumberFormatException ex){
                if(parseErrorList == null)
                    parseErrorList = new ArrayList<>();
                parseErrorList.add(fileLine);
            }
        }
    }

    public List<String> getNullRecordList() {
        return nullRecordList;
    }

    public List<String> getParseErrorList(){
        return parseErrorList;
    }

    /**
     * Questo metodo permette di salvare su di un file apparte tutti i record che hanno presentato buchi nella loro lista
     * di attributi e che quindi non possono essere caricati nel datawarehouse.
     * @param pathDirectory contiene il percorso in cui si desidera salvare il file di log
     * @param fileName contiene il nome che si vuole dare al file di log
     * @throws IOException viene sollevata se non viene individuato il file o non si riesce a scriverci all'interno.
     */
    public void logNullRecord(String pathDirectory, String fileName) throws IOException {
        pathDirectory += RESULTS_DIR;
        pathDirectory = Paths.get(pathDirectory).toString();
        fileName = checkDirectoryAndFileName(pathDirectory, fileName, CSV_EXTENSION);
        try{
            if(nullRecordList != null)
                writeLog(pathDirectory, fileName, nullRecordList);
        }catch (FileNotFoundException e){
            Log.e(TAG, "Exception log null record", e);
        }
    }

    /**
     * Questo metodo permette di salvare su di un file apparte tutti i record che hanno presentato degli errori nei valori
     * dei loro campi
     * @param pathDirectory contiene il percorso in cui si desidera salvare il file di log
     * @param fileName contiene il nome che si vuole dare al file di log
     * @throws IOException viene sollevata se non viene individuato il file o non si riesce a scriverci all'interno.
     */
    public void logParseErrorRecord(String pathDirectory, String fileName) throws IOException {
        pathDirectory += RESULTS_DIR;
        pathDirectory = Paths.get(pathDirectory).toString();
        fileName = checkDirectoryAndFileName(pathDirectory, fileName, CSV_EXTENSION);
        try{
            if(parseErrorList != null)
                writeLog(pathDirectory, fileName, parseErrorList);
        }catch (FileNotFoundException e){
            Log.e(TAG, "Exception log null record", e);
        }
    }

    /**
     * Questo metodo verifica che nei nomi dei file indicati siano presenti le estensioni csv e xml senza le quali il file
     * potrebbe subire delle alterazioni durante il salvataggio.
     * @param pathDirectory contiene il percorso in cui si desidera salvare il file di log
     * @param fileName contiene il nome che si vuole dare al file di log
     * @param extension contiene l'estensione che sto verificando: .csv o .xml
     * @return ritorna il nome eventualmente modificato per aggiungere le estensioni dei file.
     * @throws IOException può sollevare un eccezione di IO nel caso il percorso non conduca a nessuna directory.
     */
    private String checkDirectoryAndFileName(String pathDirectory , String fileName, String extension) throws IOException {
        Log.i(TAG, "Directory: " + pathDirectory);
        Log.i(TAG, "File name: " + fileName);
        FileUtils.forceMkdir(new File(pathDirectory));
        if (!fileName.contains(extension)) {
            fileName = fileName.concat(extension);
            System.out.println("[INFO] Added "+ extension + " file extension");
        }
        return fileName;
    }

    /**
     * Questo metodo è quello che accede direttamente ai file in scrittura durante le operazioni di log.
     * @param pathDirectory contiene il percorso in cui si desidera salvare il file di log
     * @param fileName contiene il nome che si vuole dare al file di log
     * @param recordList contiene la lista di record di cui si vuole fare il log: nullRecordList o parseErrorList
     * @throws FileNotFoundException solleva questa eccezione quando ci sono problemi ad accedere alla memoria dell'elaboratore.
     */
    private static void writeLog(String pathDirectory, String fileName, List<String> recordList) throws FileNotFoundException {
        PrintWriter writer = new PrintWriter(new FileOutputStream(pathDirectory + File.separator + fileName, false));
        Log.i(TAG, "List size: " + recordList.size());
        for (String string : recordList) {
            writer.println(string);
        }
        writer.close();
    }
}
