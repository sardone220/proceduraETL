package it.uniba.ventricellisardone.itss.cloud.data;

import com.google.cloud.Date;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.text.ParseException;
import java.text.SimpleDateFormat;

public class CloudDataTest {

    @Test
    public void holidayTest() throws ParseException {
        System.out.println("[INFO] HolidayTest");
        CloudData cloudData = new CloudData(new SimpleDateFormat("dd/MM/yy").parse("01/05/20"));
        assert (cloudData.getDateString().equals("2020-05-01")) : "Errore data string: " + cloudData.getDateString();
        assert (cloudData.getGoogleData().equals(
                Date.fromYearMonthDay(2020, 5, 1))) : "Data errata: " + cloudData.getGoogleData().toString();
        //Questo scempio è stato aggiunto perché Windows non riesce a fare un confronto come si deve per colpa della 'Ì'
        char[] systemChar = cloudData.getDayName().toCharArray();
        char[] handWroteChar = "VENERDÌ".toCharArray();
        for(int i = 0; i < systemChar.length; i++)
           assert (Character.getNumericValue(systemChar[i]) == Character.getNumericValue(handWroteChar[i]));
        assert (cloudData.getDayNumber() == 122) : "Errato giorno dell'anno: " + cloudData.getDayNumber();
        assert (cloudData.getMonthName().equals("MAGGIO")) : "Errato nome mese: " + cloudData.getMonthName();
        assert (cloudData.getYearValue() == 2020) : "Errato anno: " + cloudData.getYearValue();
        assert (cloudData.getMonthValue() == 5) : "Errato mese valore: " + cloudData.getMonthValue();
        assert (cloudData.getQuarter().equals("T2")) : "Errato trimestre: " + cloudData.getQuarter();
        assert (cloudData.getSeason().equals("FESTA DEL LAVORO")) : "Errato periodo: " + cloudData.getSeason();
        assert (cloudData.getSeasonYear().equals("T2-2020")) : "Errato trimestre anno: " + cloudData.getSeasonYear();
        assert (cloudData.getMonthYear().equals("5-2020")) : "Errato mese-anno: " + cloudData.getMonthYear();
        assert (cloudData.getWeekday().equals("NON FERIALE")) : "Errato feriale: " + cloudData.getWeekday();
        assert (cloudData.getHoliday().equals("FESTIVO")) : "Errato festivo: " + cloudData.getHoliday();
    }

    @Test
    public void notHolidayTest() throws ParseException{
        System.out.println("[INFO] NotHolidayTest");
        CloudData cloudData = new CloudData(new SimpleDateFormat("dd/MM/yy").parse("02/05/20"));
        assert (cloudData.getDateString().equals("2020-05-02")) : "Errore data string: " + cloudData.getDateString();
        assert (cloudData.getGoogleData().equals(
                Date.fromYearMonthDay(2020, 5, 2))) : "Data errata: " + cloudData.getGoogleData().toString();
        assert (cloudData.getDayName().equals("SABATO")) : "Errore nome giorno: " + cloudData.getDayName();
        assert (cloudData.getDayNumber() == 123) : "Errato giorno dell'anno: " + cloudData.getDayNumber();
        assert (cloudData.getMonthName().equals("MAGGIO")) : "Errato nome mese: " + cloudData.getMonthName();
        assert (cloudData.getYearValue() == 2020) : "Errato anno: " + cloudData.getYearValue();
        assert (cloudData.getMonthValue() == 5) : "Errato mese valore: " + cloudData.getMonthValue();
        assert (cloudData.getQuarter().equals("T2")) : "Errato trimestre: " + cloudData.getQuarter();
        assert (cloudData.getSeason().equals("NESSUNO")) : "Errato periodo: " + cloudData.getSeason();
        assert (cloudData.getSeasonYear().equals("T2-2020")) : "Errato trimestre anno: " + cloudData.getSeasonYear();
        assert (cloudData.getMonthYear().equals("5-2020")) : "Errato mese-anno: " + cloudData.getMonthYear();
        assert (cloudData.getWeekday().equals("FERIALE")) : "Errato feriale: " + cloudData.getWeekday();
        assert (cloudData.getHoliday().equals("NON FESTIVO")) : "Errato festivo: " + cloudData.getHoliday();
    }

    @Test
    public void sundayTest() throws ParseException{
        System.out.println("[INFO] SundayTest");
        CloudData cloudData = new CloudData(new SimpleDateFormat("dd/MM/yy").parse("03/05/20"));
        assert (cloudData.getDateString().equals("2020-05-03")) : "Errore data string: " + cloudData.getDateString();
        assert (cloudData.getGoogleData().equals(
                Date.fromYearMonthDay(2020, 5, 3))) : "Data errata: " + cloudData.getGoogleData().toString();
        assert (cloudData.getDayName().equals("DOMENICA")) : "Errore nome giorno: " + cloudData.getDayName();
        assert (cloudData.getDayNumber() == 124) : "Errato giorno dell'anno: " + cloudData.getDayNumber();
        assert (cloudData.getMonthName().equals("MAGGIO")) : "Errato nome mese: " + cloudData.getMonthName();
        assert (cloudData.getYearValue() == 2020) : "Errato anno: " + cloudData.getYearValue();
        assert (cloudData.getMonthValue() == 5) : "Errato mese valore: " + cloudData.getMonthValue();
        assert (cloudData.getQuarter().equals("T2")) : "Errato trimestre: " + cloudData.getQuarter();
        assert (cloudData.getSeason().equals("NESSUNO")) : "Errato periodo: " + cloudData.getSeason();
        assert (cloudData.getSeasonYear().equals("T2-2020")) : "Errato trimestre anno: " + cloudData.getSeasonYear();
        assert (cloudData.getMonthYear().equals("5-2020")) : "Errato mese-anno: " + cloudData.getMonthYear();
        assert (cloudData.getWeekday().equals("NON FERIALE")) : "Errato feriale: " + cloudData.getWeekday();
        assert (cloudData.getHoliday().equals("NON FESTIVO")) : "Errato festivo: " + cloudData.getHoliday();
    }

    @Test
    public void wrongConstructorTest(){
        System.out.println("[INFO] WrongConstructorTest");
        Assertions.assertThrows(ParseException.class, () -> new CloudData(new SimpleDateFormat("dd/MM/yy").parse("ciccio/05/20")));
    }
}
