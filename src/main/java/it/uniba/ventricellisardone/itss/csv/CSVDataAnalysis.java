/**
 * Questa classe genera dei report su i campi che sono stati individuati come i più dinamici nella fase di ricognizione
 * degli archivi, fornendo le liste dei valori che i campi assumono, più il numero di occorrenze dei valori negli archivi.
 * Questo tool è stato creato per agevolare il miglioramento degli archivi iniziali e individuare i cambiamenti da apportare
 * nel DB OLTP.
 */
package it.uniba.ventricellisardone.itss.csv;

import it.uniba.ventricellisardone.itss.log.Log;
import org.apache.commons.io.FileUtils;

import java.io.*;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CSVDataAnalysis {

    private static final Map<String, Integer> ANALYSIS_HEADER;
    private final List<CSVRecord> csvRecordList;
    private static final String TAG = "DataAnalysis.class";
    private static final String XML_EXTENSION = ".xml";

    static {
        ANALYSIS_HEADER = Map.of("Colore", 0,
                "CodStatoFattura", 1,
                "NomeBrand", 2,
                "Collezione", 3,
                "PagamentoOrdine", 4,
                "NomeCategoria", 5,
                "MacroCategoria", 6);
    }

    public CSVDataAnalysis(List<CSVRecord> csvRecordList) {
        this.csvRecordList = csvRecordList;
    }

    public Map<String, Map<String, Integer>> performDataAnalysis(){
        Map<String, Map<String, Integer>> dataMap = new HashMap<>();
        for(Map.Entry<String, Integer> entry : ANALYSIS_HEADER.entrySet()){
            dataMap.put(entry.getKey(), new HashMap<>());
        }
        for(CSVRecord record : csvRecordList){
            String[] stringsRecord = CSVRecord.reverseCSVRecordForAnalysis(record);
            dataMap.replaceAll((k, v) -> checkField(stringsRecord[ANALYSIS_HEADER.get(k)], v));
        }
        return dataMap;
    }

    private Map<String, Integer> checkField(String fieldValue, Map<String, Integer> fieldMap){
        if(fieldMap.containsKey(fieldValue))
            fieldMap.put(fieldValue, fieldMap.get(fieldValue) + 1);
        else
            fieldMap.put(fieldValue, 1);
        return fieldMap;
    }

    public void logDataAnalysis(Map<String, Map<String, Integer>> analysisReport, String pathDirectory, String fileName) throws IOException {
        pathDirectory += "" + File.separator + "Analysis" + File.separator + "";
        pathDirectory = Paths.get(pathDirectory).toString();
        fileName = checkDirectoryAndFileName(pathDirectory, fileName);
        try {
            PrintWriter writer = new PrintWriter(new FileOutputStream(pathDirectory + File.separator + fileName, false));
            writer.println("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
            if(fileName.contains("test"))
                writer.println("<data type=\"test\">");
            else
                writer.println("<data type=\"execution\">");
            for(Map.Entry<String, Map<String, Integer>> category : analysisReport.entrySet()){
                writer.println("\t<category key=\"" + category.getKey() + "\">");
                for(Map.Entry<String, Integer> entry : category.getValue().entrySet())
                    writer.println("\t\t<entry key=\"" + entry.getKey() + "\" value=\"" + entry.getValue() + "\"/>");
                writer.println("\t</category>");
            }
            writer.println("</data>");
            writer.close();
        } catch (FileNotFoundException e) {
            Log.e(TAG, "Log data analysis", e);
        }
    }

    private String checkDirectoryAndFileName(String pathDirectory, String fileName) throws IOException {
        FileUtils.forceMkdir(new File(pathDirectory));
        if (!fileName.contains(CSVDataAnalysis.XML_EXTENSION)) {
            fileName = fileName.concat(CSVDataAnalysis.XML_EXTENSION);
            System.out.println("[INFO] Added "+ CSVDataAnalysis.XML_EXTENSION + " file extension");
        }
        return fileName;
    }
}
