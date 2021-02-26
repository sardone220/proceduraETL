package it.uniba.ventricellisardone.itss.csv;

import it.uniba.ventricellisardone.itss.csv.ecxception.CSVParsingException;
import it.uniba.ventricellisardone.itss.etl.Extraction;
import it.uniba.ventricellisardone.itss.etl.ExtractionTest;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Objects;
import java.util.Scanner;

public class CSVDataAnalysisTest {

    @Test
    public void analysisTest() throws IOException, CSVParsingException, URISyntaxException {
        System.out.println("[INFO] AnalysisTest");
        Extraction extraction = new Extraction(Paths.get(Objects.requireNonNull(CSVDataAnalysisTest.class.getClassLoader().getResource("data_analysis/right_data.csv")).toURI()).toString());
        CSVDataAnalysis CSVDataAnalysis = new CSVDataAnalysis(extraction.getCsvRecordList());

        Map<String, Map<String, Integer>> map = CSVDataAnalysis.performDataAnalysis();
        assert (map.equals(CSVStaticTestModel.getTestMap())) : "[ERROR] Incorrect data analysis";
        CSVDataAnalysis.logDataAnalysis(map, javax.swing.filechooser.FileSystemView.getFileSystemView().getHomeDirectory() + "/Desktop/TEST", "test_result_analysis.xml");

        Scanner testFile = new Scanner(new File(Objects.requireNonNull(ExtractionTest.class.getClassLoader().getResource("data_analysis/data_analysis_test.xml")).getPath()));
        String resultPath =  javax.swing.filechooser.FileSystemView.getFileSystemView().getHomeDirectory() + "/Desktop/TEST/Analysis/test_result_analysis.xml";
        Scanner resultFile = new Scanner(new File(resultPath));
        int i = 0;
        while (resultFile.hasNextLine()){
            assert (resultFile.nextLine().trim().equals(testFile.nextLine().trim())) : "[ERROR] Data analysis line " + i + " not match";
            i++;
        }
    }

}
