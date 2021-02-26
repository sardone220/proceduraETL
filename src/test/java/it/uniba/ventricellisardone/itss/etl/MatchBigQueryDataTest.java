package it.uniba.ventricellisardone.itss.etl;

import it.uniba.ventricellisardone.itss.csv.CSVRecord;
import it.uniba.ventricellisardone.itss.csv.ecxception.CSVParsingException;
import org.junit.jupiter.api.Test;

import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

public class MatchBigQueryDataTest {
    
    private static final String TABLE_NAME = "test_tabella";

    @Test
    public void initTest() throws SQLException, InterruptedException, CSVParsingException, URISyntaxException {
        System.out.println("[INFO] InitTest");
        List<CSVRecord> csvRecords = new Extraction(Paths.get(Objects.requireNonNull(MatchBigQueryDataTest.class.getClassLoader()
                .getResource("etl/matching/conflict_matching_data.csv")).toURI()).toString()).getCsvRecordList();
        MatchBigQueryData matchBigQueryData = new MatchBigQueryData(csvRecords, TABLE_NAME);
        assert (matchBigQueryData.getBigQueryDate().equals(new SimpleDateFormat("yyyy-MM-dd", Locale.getDefault())
                .format(csvRecords.get(0).getOrderDate()))) : "Le date non coincidono";
    }

    @Test
    public void conflictRowMatchTest() throws SQLException, InterruptedException, ParseException, URISyntaxException {
        System.out.println("[INFO] ConflictRowTest");
        List<CSVRecord> csvRecords = new Extraction(Paths.get(Objects.requireNonNull(MatchBigQueryDataTest.class.getClassLoader()
                .getResource("etl/matching/conflict_matching_data.csv")).toURI()).toString()).getCsvRecordList();
        MatchBigQueryData matchBigQueryData = new MatchBigQueryData(csvRecords, TABLE_NAME);
        assert (matchBigQueryData.isDateMatching()) : "Le date nel cloud non coincidono con quelle estratte";
        assert (matchBigQueryData.getConflictRowNumber() == 4) : "Il conteggio delle righe non ha funzionato: " + matchBigQueryData.getConflictRowNumber();
    }

    @Test
    public void conflictRowNotMatchTest() throws SQLException, InterruptedException, ParseException, URISyntaxException {
        System.out.println("[INFO] ConflictRowNotMatchTest");
        List<CSVRecord> csvRecords = new Extraction(Paths.get(Objects.requireNonNull(MatchBigQueryDataTest.class.getClassLoader()
                .getResource("etl/matching/conflict_not_matching_data.csv")).toURI()).toString()).getCsvRecordList();
        MatchBigQueryData matchBigQueryData = new MatchBigQueryData(csvRecords, TABLE_NAME);
        assert (matchBigQueryData.isDateMatching()) : "Le date nel cloud non coincidono con quelle estratte";
        assert (matchBigQueryData.getConflictRowNumber() == 0) : "Il conteggio delle righe non ha funzionato: " + matchBigQueryData.getConflictRowNumber();
    }
    
    @Test
    public void recordMatchTest() throws ParseException, SQLException, InterruptedException, URISyntaxException {
        System.out.println("[INFO] RecordMatchTest");
        List<CSVRecord> csvRecords = new Extraction(Paths.get(Objects.requireNonNull(MatchBigQueryDataTest.class.getClassLoader()
                .getResource("etl/matching/record_match_data.csv")).toURI()).toString()).getCsvRecordList();
        MatchBigQueryData matchBigQueryData = new MatchBigQueryData(csvRecords, TABLE_NAME);
        assert (matchBigQueryData.isRecordMatching()) : "I record non hanno fatto match";
    }
    
    @Test
    public void recordNotMatchTest() throws SQLException, InterruptedException, ParseException, URISyntaxException {
        System.out.println("[INFO] RecordNotMatchTest");
        List<CSVRecord> csvRecords = new Extraction(Paths.get(Objects.requireNonNull(MatchBigQueryDataTest.class.getClassLoader()
                .getResource("etl/matching/not_record_match_data.csv")).toURI()).toString()).getCsvRecordList();
        MatchBigQueryData matchBigQueryData = new MatchBigQueryData(csvRecords, TABLE_NAME);
        assert (!matchBigQueryData.isRecordMatching()) : "I record hanno fatto match";
    }
}
