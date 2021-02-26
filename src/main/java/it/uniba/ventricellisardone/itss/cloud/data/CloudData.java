/**
 * Questa classe permette di trasformare le ordinarie date prelevate dal DB OLTP nel formato gg/mm/aa e trasformarle in
 * modo tale da ottenere informazioni su: nome del giorno di riferimento, trimestre, periodo dell'anno, la presenza o la
 * assenza di festività nella data selezionata.
 * Per far ciò la classe fa riferimento a delle API online che fornito un anno restituisce la lista delle festività per
 * quell'anno così da riconoscere se una data cade in una festività
 */

package it.uniba.ventricellisardone.itss.cloud.data;

import com.google.cloud.Date;
import com.google.common.io.CharStreams;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import it.uniba.ventricellisardone.itss.log.Log;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.lang.reflect.Type;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.List;
import java.util.Locale;

public class CloudData{

    private static final String TAG = "Cloud data";
    private static final String NOTHING = "NESSUNO";

    private final Date googleData;

    private final String dayName;
    private final int dayNumber;
    private final String monthName;
    private final int yearValue;
    private final int monthValue;
    private final String quarter;
    private final String season;
    private final String seasonYear;
    private final String monthYear;
    private final String weekday;
    private final String holiday;
    private final String dateString;

    public CloudData(java.util.Date date) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        this.dateString = new SimpleDateFormat("yyyy-MM-dd", Locale.getDefault()).format(date);
        this.googleData = Date.fromJavaUtilDate(date);
        this.dayName = calendar.getDisplayName(Calendar.DAY_OF_WEEK, Calendar.LONG, Locale.getDefault()).toUpperCase();
        this.dayNumber = calendar.get(Calendar.DAY_OF_YEAR);
        this.monthName = calendar.getDisplayName(Calendar.MONTH, Calendar.LONG, Locale.getDefault()).toUpperCase();
        this.yearValue = calendar.get(Calendar.YEAR);
        //calendar numera i mesi da 0, aggiungo 1 per evitare ambiguità
        this.monthValue = calendar.get(Calendar.MONTH) + 1;
        this.quarter = setQuarter();
        this.season = setSeason(date);
        this.seasonYear = setQuarter() + "-" + this.yearValue;
        this.monthYear = this.monthValue + "-" + this.yearValue;
        this.weekday = setWeekday();
        this.holiday = setHoliday();
    }

    public String getDateString() {
        return dateString;
    }

    public Date getGoogleData() {
        return googleData;
    }

    public String getDayName() {
        return dayName;
    }

    public int getDayNumber() {
        return dayNumber;
    }

    public String getMonthName() {
        return monthName;
    }

    public int getYearValue() {
        return yearValue;
    }

    public int getMonthValue() {
        return monthValue;
    }

    public String getQuarter() {
        return quarter;
    }

    public String getSeason() {
        return season;
    }

    public String getSeasonYear() {
        return seasonYear;
    }

    public String getMonthYear() {
        return monthYear;
    }

    public String getWeekday() {
        return weekday;
    }

    public String getHoliday() {
        return holiday;
    }

    private String setQuarter() {
        if (this.monthValue >= 1 && this.monthValue <= 3)
            return "T1";
        else if (this.monthValue >= 4 && this.monthValue <= 6)
            return "T2";
        else if (this.monthValue >= 7 && this.monthValue <= 9)
            return "T3";
        else
            return "T4";
    }

    /**
     * Questo metodo avvia una richiesta http all'API su rapidAPI che restituisce la lista di festività e gestisce le
     * eccezioni di IO e di parsing della data
     * @param date contiene la data di cui necessitiamo le informazioni
     * @return restituisce NESSUNO: se la data non ricade in nessuna festività;
     *                     NOME_FESTIVITA: se la data ricade in una festività;
     */
    private String setSeason(java.util.Date date) {
        String stringResponse = NOTHING;
        try {
            CloseableHttpClient client = HttpClients.custom().build();
            HttpUriRequest request = RequestBuilder.get()
                    .setUri("https://public-holiday.p.rapidapi.com/" + this.yearValue + "/IT")
                    .addHeader("x-rapidapi-host", "public-holiday.p.rapidapi.com")
                    .addHeader("x-rapidapi-key", "2ffe94d15fmsh77f096ee6ae83e2p1e6163jsn8d8d6c79a2c2")
                    .build();
            CloseableHttpResponse response = client.execute(request);
            Reader reader = new InputStreamReader(response.getEntity().getContent());
            stringResponse = CharStreams.toString(reader);
            Gson gson = new Gson();
            Type listType = new TypeToken<List<HolidaysDate>>() {}.getType();
            List<HolidaysDate> holidaysDateList = gson.fromJson(stringResponse, listType);
            stringResponse = NOTHING;
            stringResponse = matchDate(date, stringResponse, holidaysDateList);
            response.close();
            request.abort();
        } catch (IOException e) {
            Log.e(TAG, "Eccezione in CloudDate, richesta API: ", e);
        }
        return stringResponse;
    }

    private String matchDate(java.util.Date date, String stringResponse, List<HolidaysDate> holidaysDateList) {
        try {
            for (HolidaysDate holidaysDate : holidaysDateList) {
                if (holidaysDate.getDate().equals(date))
                    stringResponse = holidaysDate.getLocalName().toUpperCase();
            }
        }catch (NullPointerException ex) {
            System.err.println("Eccezione in CloudDate, vedi log file");
            Log.e(TAG, "Eccezione in CloudDate, richesta API: " + this.yearValue, ex);
            throw ex;
        }
        return stringResponse;
    }

    private String setWeekday() {
        if (this.season.equals(NOTHING) && !this.dayName.equals("DOMENICA"))
            return "FERIALE";
        else
            return "NON FERIALE";
    }

    private String setHoliday() {
        if (this.season.equals(NOTHING))
            return "NON FESTIVO";
        else
            return "FESTIVO";
    }

    private static class HolidaysDate {
        private java.util.Date date;
        private String localName;

        public HolidaysDate() {
            // Empty method required
            // necessario per deserializzazione JSON.
        }

        public java.util.Date getDate() {
            return date;
        }

        public String getLocalName() {
            return localName;
        }
    }
}
