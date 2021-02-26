/**
 * Questa classe gestisce i record letti dal file .csv dato in input al software ETL, inoltre verifica che il record sia
 * ben formato e non presenti errori di alcun tipo.
 */
package it.uniba.ventricellisardone.itss.csv;

import it.uniba.ventricellisardone.itss.csv.ecxception.CSVNullFieldsException;
import it.uniba.ventricellisardone.itss.csv.ecxception.CSVParsingException;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Locale;

public class CSVRecord {

    private long idOrder;
    private Date orderDate;
    private String countryCode;
    private char customerGender;
    private int quantity;
    private double payedPrice;
    private int discount;
    private boolean outlet;
    private String nomeBrand;
    private String collection;
    private String color;
    private String itemGender;
    private String paymentMethod;
    private String size;
    private String category;
    private String macroCategory;

    /**
     * Per prima cosa il costruttore verifica che sia composto dal giusto numero di record, dopo di che avvia i comandi
     * per creare l'istanza del record in memoria.
     * @param strings è un array di stringhe che contiene lo split di una riga del file .csv.
     * @throws ParseException: viene sollevata se nella creazione del record vengono rilevati valori non concordati nel
     *                          protocollo di caricamento.
     * @throws CSVNullFieldsException: viene sollevata se nel conteggio dei campi del record risulta un record assente o
     *                                  magari superfluo.
     */
    public CSVRecord(String[] strings) throws ParseException, CSVNullFieldsException {
        if (strings.length == 16) {
            for (int i = 0; i < 16; i++) {
                if(strings[i].isEmpty())
                    throw new CSVParsingException("Valore non impostato correttamente", i);
            }
            init(strings);
        } else
            throw new CSVNullFieldsException("Il numero dei campi non corrisponde a quanto stabilito: " + strings.length);
    }

    /**
     * Questo metodo si occupa della corretta assegnazione e del controllo sui campi del record, verificando che i valori
     * di tali campi rispettino le direttive.
     */
    private void init(String[] strings) throws ParseException {
        this.idOrder = Long.parseLong(strings[0]);
        this.orderDate = new SimpleDateFormat("dd/MM/yy", Locale.getDefault()).parse(strings[1]);
        this.countryCode = strings[2];
        if (strings[3].charAt(0) == 'M')
            this.customerGender = 'M';
        else if (strings[3].charAt(0) == 'F')
            this.customerGender = 'F';
        else
            throw new CSVParsingException("Il sesso inserito non esiste", 3);
        this.quantity = Integer.parseInt(strings[4]);
        this.payedPrice = Double.parseDouble(strings[5]);
        if (this.payedPrice < 0)
            throw new IllegalArgumentException("Il prezzo non può essere negativo");
        this.discount = Integer.parseInt(strings[6]);
        if (strings[7].equals("0")) {
            this.outlet = false;
        } else if (strings[7].equals("1")) {
            this.outlet = true;
        } else
            throw new CSVParsingException("Il valore non è un booleano", 7);
        this.nomeBrand = strings[8];
        this.collection = strings[9];
        this.color = strings[10];
        this.itemGender = strings[11];
        this.paymentMethod = strings[12];
        this.size = strings[13];
        this.category = strings[14];
        this.macroCategory = strings[15];
    }

    public long getIdOrder() {
        return idOrder;
    }

    public Date getOrderDate() {
        return orderDate;
    }

    public String getCountryCode() {
        return countryCode;
    }


    public char getCustomerGender() {
        return customerGender;
    }

    public int getQuantity() {
        return quantity;
    }

    public void setQuantity(int quantity) {
        this.quantity = quantity;
    }

    public double getPayedPrice() {
        return payedPrice;
    }

    public int getDiscount() {
        return discount;
    }

    public void setDiscount(int discount) {
        this.discount = discount;
    }

    public boolean isOutlet() {
        return outlet;
    }

    public void setOutlet(boolean outlet) {
        this.outlet = outlet;
    }

    public String getNomeBrand() {
        return nomeBrand;
    }

    public String getCollection() {
        return collection;
    }

    public void setCollection(String collection) {
        this.collection = collection;
    }

    public String getColor() {
        return color;
    }

    public String getItemGender() {
        return itemGender;
    }

    public String getPaymentMethod() {
        return paymentMethod;
    }

    public String getSize() {
        return size;
    }

    public String getCategory() {
        return category;
    }

    public String getMacroCategory() {
        return macroCategory;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof CSVRecord) {
            CSVRecord record = (CSVRecord) obj;
            String[] otherRecord = CSVRecord.reverseCSVRecord(record);
            String[] thisRecord = CSVRecord.reverseCSVRecord(this);
            boolean result = false;
            for (int i = 0; i < thisRecord.length; i++)
                result = otherRecord[i].equals(thisRecord[i]);
            return result;
        } else
            return false;
    }

    @Override
    public int hashCode() {
        return (int) this.getIdOrder();
    }

    /**
     * Questo metodo permette di passare da un record gestito dal programma ad una stringa .csv che potrà essere utilizzata
     * in seguito, questo metodo è specifico per la parte di tool che fa l'analisi degli archivi.
     * @param record contiene il record in formato manipolabile.
     * @return un array di stringhe costituenti i vari campi dell'array
     */
    public static String[] reverseCSVRecordForAnalysis(CSVRecord record) {
        String[] strings = new String[7];
        strings[0] = record.getColor();
        strings[1] = record.getCountryCode();
        strings[2] = record.getNomeBrand();
        strings[3] = record.getCollection();
        strings[4] = record.getPaymentMethod();
        strings[5] = record.getCategory();
        strings[6] = record.getMacroCategory();
        return strings;
    }

    /**
     * Anche questo metodo genera da un record manipolabile dal software un'array di stringhe contenenti i campi del record
     * solo che in questo caso il record viene cosiderato nella sua interezza.
     * @param record contiene il record in formato manipolabile
     * @return restutisce l'array di stringhe contenenti i vari campi del record.
     */
    public static String[] reverseCSVRecord(CSVRecord record) {
        String[] strings = new String[16];
        strings[0] = Long.toString(record.getIdOrder());
        strings[1] = new SimpleDateFormat("dd/MM/yy", Locale.getDefault()).format(record.getOrderDate());
        strings[2] = record.getCountryCode();
        strings[3] = String.valueOf(record.getCustomerGender());
        strings[4] = Integer.toString(record.getQuantity());
        strings[5] = Double.toString(record.getPayedPrice());
        strings[6] = Integer.toString(record.getDiscount());
        strings[7] = Boolean.toString(record.isOutlet());
        strings[8] = record.getNomeBrand();
        strings[9] = record.getCollection();
        strings[10] = record.getColor();
        strings[11] = record.getItemGender();
        strings[12] = record.getPaymentMethod();
        strings[13] = record.getSize();
        strings[14] = record.getCategory();
        strings[15] = record.getMacroCategory();
        return strings;
    }

    @Override
    public String toString() {
        return Arrays.toString(CSVRecord.reverseCSVRecord(this)) + "\n";
    }
}
