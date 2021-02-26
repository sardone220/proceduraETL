package it.uniba.ventricellisardone.itss.etl;

import it.uniba.ventricellisardone.itss.csv.ecxception.CSVParsingException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.text.ParseException;
import java.util.Objects;
import java.util.Scanner;

public class TransformingTest {

    private static final String TABLE_NAME = "test_tabella";

    @Test
    public void correctTest() throws IOException, ParseException, URISyntaxException {
        System.out.println("[INFO] CorrectTest");
        Extraction extraction = new Extraction(Paths.get(Objects.requireNonNull(TransformingTest.class.getClassLoader().getResource("etl/transforming/transform_data.csv")).toURI()).toString());
        Transforming transforming = new Transforming(javax.swing.filechooser.FileSystemView.getFileSystemView().getHomeDirectory() + "/Desktop/TEST", TABLE_NAME);
        transforming.transformData(extraction.getCsvRecordList());
        Scanner transformedFile = new Scanner(new File(javax.swing.filechooser.FileSystemView.getFileSystemView().getHomeDirectory() + "/Desktop/TEST/load_data_0.csv"));
        Scanner testFile = new Scanner(new File(Objects.requireNonNull(TransformingTest.class.getClassLoader().getResource("etl/transforming/transformed_data.csv")).getPath()));
        int i = 0;
        while (transformedFile.hasNextLine()) {
            assert (testFile.nextLine().trim().equals(transformedFile.nextLine().trim())) : "Errore alla linea: " + i;
            i++;
        }
    }

    @Test
    public void parseErrorTest() throws IOException, CSVParsingException, URISyntaxException {
        System.out.println("[INFO] ParseErrorTest");
        Extraction extraction = new Extraction(Paths.get(Objects.requireNonNull(TransformingTest.class.getClassLoader().getResource("etl/transforming/transform_data_error.csv")).toURI()).toString());
        Transforming transforming = new Transforming(javax.swing.filechooser.FileSystemView.getFileSystemView().getHomeDirectory() + "/Desktop/TEST", TABLE_NAME);
        Assertions.assertThrows(ParseException.class, () -> transforming.transformData(extraction.getCsvRecordList()), "Eccezione non sollevata");
    }

    @Test
    public void chooseFileTest() throws IOException, ParseException, URISyntaxException {
        System.out.println("[INFO] ChooseFileTest");
        Extraction extraction = new Extraction(Paths.get(Objects.requireNonNull(TransformingTest.class.getClassLoader().getResource("etl/transforming/transform_data.csv")).toURI()).toString());
        Transforming transforming = new Transforming(javax.swing.filechooser.FileSystemView.getFileSystemView().getHomeDirectory() + "/Desktop/TEST", TABLE_NAME);
        transforming.transformData(extraction.getCsvRecordList());
        extraction = new Extraction(Paths.get(Objects.requireNonNull(TransformingTest.class.getClassLoader().getResource("etl/transforming/transform_data_1.csv")).toURI()).toString());
        transforming.transformData(extraction.getCsvRecordList());
        Assertions.assertDoesNotThrow(() -> new Scanner(new File(javax.swing.filechooser.FileSystemView.getFileSystemView().getHomeDirectory() + "/Desktop/TEST/load_data_0.csv")));
        Assertions.assertDoesNotThrow(() -> new Scanner(new File(javax.swing.filechooser.FileSystemView.getFileSystemView().getHomeDirectory() + "/Desktop/TEST/load_data_1.csv")));
    }

    @Test
    public void conflictLoadTest() throws ParseException, IOException, URISyntaxException {
        System.out.println("[INFO] ConflictLoadTest");
        Extraction extraction = new Extraction(Paths.get(Objects.requireNonNull(TransformingTest.class.getClassLoader().getResource("etl/transforming/transform_data_conflict.csv")).toURI()).toString());
        Transforming transforming = new Transforming(javax.swing.filechooser.FileSystemView.getFileSystemView().getHomeDirectory() + "/Desktop/TEST", TABLE_NAME);
        transforming.transformData(extraction.getCsvRecordList());
        Scanner duplicateData = new Scanner(new File(javax.swing.filechooser.FileSystemView.getFileSystemView().getHomeDirectory() + "/Desktop/TEST/duplicate_data.csv"));
        Scanner testFileData = new Scanner(new File(Objects.requireNonNull(TransformingTest.class.getClassLoader().getResource("etl/loading/load_data_0.csv")).getPath()));
        //Salto l'header file del file di test
        testFileData.nextLine();
        while(testFileData.hasNextLine())
            assert (testFileData.nextLine().equals(duplicateData.nextLine()));
    }
}
