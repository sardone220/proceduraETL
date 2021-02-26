/**
 * Questa classe permette di effettuare un confronto approfondito tra i dati già presenti nel cloud e gli eventuali dati
 * di refresh, infatti dopo aver prelevato la data con valore più elevato dal cloud si può verificare la presenza di quella data
 * nei nuovi dati di refresh e, se presente, controllare che i dati di refresh non collidano con quelli già presenti in remoto.
 */
package it.uniba.ventricellisardone.itss.etl;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.common.collect.Lists;
import it.uniba.ventricellisardone.itss.csv.CSVRecord;
import it.uniba.ventricellisardone.itss.log.Log;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;

public class MatchBigQueryData {

    private static final BigQuery BIG_QUERY;
    private static final String TAG = "MatchingBigQueryData";

    private String bigQueryDate;
    private final List<CSVRecord> recordList;
    private final String targetTable;

    static {
        GoogleCredentials googleCredentials;
        BigQuery bigQuery = null;
        try {
            googleCredentials = GoogleCredentials.fromStream(new FileInputStream(javax.swing.filechooser
                    .FileSystemView.getFileSystemView().getHomeDirectory() + File.separator + "Documents"
                    + File.separator + "etl-authentication.json"))
                    .createScoped(Lists.newArrayList("https://www.googleapis.com/auth/cloud-platform"));
            bigQuery = BigQueryOptions.newBuilder().setProjectId("biproject-itss").setCredentials(googleCredentials).build().getService();
        } catch (IOException e) {
            Log.e(TAG, "Eccezione caricamento credenziali", e);
        }
        BIG_QUERY = bigQuery;
    }

    /**
     * Il costruttore ha il compito di prelevare la data di valore più elevato presente nel cloud e impostarla come variabile
     * d'istanza assieme alle altre.
     * @param recordList contiene la lista dei record da controllare.
     * @param targetTable contiene la tabella su cui effettuare le query.
     * @throws InterruptedException viene sollevata quando il thread che esegue la query subisce un iterruzione non prevista
     * @throws SQLException viene sollevata quando il risultato della query non è quello atteso
     */
    public MatchBigQueryData(List<CSVRecord> recordList, String targetTable) throws InterruptedException, SQLException {
        this.recordList = recordList;
        this.targetTable = targetTable;
        QueryJobConfiguration jobConfiguration = QueryJobConfiguration
                .newBuilder("SELECT MAX(ordine_data) AS ordine_ultima_data FROM `biproject-itss.dataset."
                        + this.targetTable +"`").build();
        try {
            for (FieldValueList row : BIG_QUERY.query(jobConfiguration).iterateAll()) {
                if (row.size() > 1)
                    throw new SQLException("Errore nel selezionare il massimo dalla tabella.");
                else
                    this.bigQueryDate = row.get(0).getStringValue();
            }
        }catch (NullPointerException ex){
            Log.e(TAG, "Eccezione sollevata, provo a non interrompere il programma", ex);
            this.bigQueryDate = null;
        }
    }

    /**
     * Questo metodo verifica che le date del cloud e dei dati di refresh coincidano.
     * @return true quando coincidono, false altrimenti.
     */
    public boolean isDateMatching() {
        if(this.bigQueryDate != null)
            try {
                Date parseGoogleDate = new SimpleDateFormat("yyyy-MM-dd", Locale.getDefault())
                        .parse(this.bigQueryDate);
                Date recordDate = recordList.get(0).getOrderDate();
                return parseGoogleDate.before(recordDate) || parseGoogleDate.equals(recordDate);
            } catch (ParseException e) {
                Log.e(TAG, "Eccezione sollevata: ", e);
                return true;
            }
        else
            return false;
    }

    /**
     * Questo metodo basandosi sul risultato di isMatching() verifica che i record con la data incriminata siano almeno
     * differenti da quelli già presenti nel cloud
     * @return ritorna il numero di record che hanno colliso con i dati del cloud.
     * @throws ParseException viene sollevata quando si cerca di trasformare un record che non può essere trasformato
     * @throws InterruptedException viene sollevato quando il thread di esecuzione della query viene interrotto in maniera
     *                              non prevista.
     */
    public int getConflictRowNumber() throws ParseException, InterruptedException {
        int rowCounter = 0;
        if(isDateMatching()){
            QueryJobConfiguration jobConfiguration = QueryJobConfiguration
                    .newBuilder("SELECT * FROM `biproject-itss.dataset."
                            + this.targetTable +"` WHERE ordine_data = '" + this.bigQueryDate + "'").build();
            List<String> transformedRecord = Transforming.getTransformedRecord(this.recordList);
            for(FieldValueList row : BIG_QUERY.query(jobConfiguration).iterateAll()){
                for(String record : transformedRecord){
                    if(record.equals(buildStringFromFieldValueList(row)))
                        rowCounter++;
                }
            }
        }
        return rowCounter;
    }

    /**
     * Questo metodo al contrario del precedente non restituisce il numero di conflitti, ma verifica che la lista di
     * record che si vuole caricare non sia già presente in BigQuery.
     * @return restituisce true se viene trovata una corrispondenza tra il record da inserire e il datawarehouse in cloud.
     *         restituisce false se non viene trovata corrispondenza tra il record da inserire e il datawarehouse in cloud.
     * @throws ParseException può essere sollevata se il record che stiamo verificando non può essere trasformato.
     * @throws InterruptedException può essere sollevata se durante l'esecuzione della query il thread viene interrotto
     *                              inaspettatamente.
     */
    public boolean isRecordMatching() throws ParseException, InterruptedException {
        boolean match = false;
        if(isDateMatching()){
            QueryJobConfiguration jobConfiguration = QueryJobConfiguration
                    .newBuilder("SELECT * FROM `biproject-itss.dataset."
                            + this.targetTable +"` WHERE ordine_data = '" + this.bigQueryDate + "'").build();
            Iterator<FieldValueList> resultSet = BIG_QUERY.query(jobConfiguration).iterateAll().iterator();
            while(resultSet.hasNext() && !match){
                Iterator<String> transformedRecord = Transforming.getTransformedRecord(this.recordList).iterator();
                String cloudRecord = buildStringFromFieldValueList(resultSet.next());
                while(transformedRecord.hasNext() && !match){
                    if(transformedRecord.next().equals(cloudRecord))
                        match = true;
                }
            }
        }
        return match;
    }

    public String getBigQueryDate() {
        return bigQueryDate;
    }

    private String buildStringFromFieldValueList(FieldValueList row){
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < row.size() ; i++) {
            builder.append(row.get(i).getStringValue());
            if(i < row.size() - 1)
                builder.append(",");
        }
        return builder.toString();
    }
}
