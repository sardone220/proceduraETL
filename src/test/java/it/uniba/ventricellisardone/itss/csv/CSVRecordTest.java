package it.uniba.ventricellisardone.itss.csv;

import it.uniba.ventricellisardone.itss.csv.ecxception.CSVNullFieldsException;
import it.uniba.ventricellisardone.itss.csv.ecxception.CSVParsingException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Locale;

public class CSVRecordTest {

    @Test
    public void rightConstructorTest() throws ParseException, CSVNullFieldsException {
        System.out.println("[INFO] RightConstructorTest");
        String[] strings = {"11", "07/09/10", "IT", "M", "1", "89", "0", "0","FRECCE TRICOLORI", "Primavera - Estate 2011",
                "Celeste", "Uomo", "PayPal", "UNICA", "Orologi", "Gioielli Moda"};
        CSVRecord csvRecord = new CSVRecord(strings);
        assert (csvRecord.getIdOrder() == 11);
        assert (csvRecord.getOrderDate().equals(new SimpleDateFormat("dd/MM/yy",
                Locale.getDefault()).parse("07/09/10")));
        assert (csvRecord.getCountryCode().equals("IT"));
        assert (csvRecord.getCustomerGender() == 'M');
        assert (csvRecord.getQuantity() == 1);
        assert (csvRecord.getPayedPrice() == 89);
        assert (csvRecord.getDiscount() == 0);
        assert (!csvRecord.isOutlet());
        assert (csvRecord.getNomeBrand().equals("FRECCE TRICOLORI"));
        assert (csvRecord.getCollection().equals("Primavera - Estate 2011"));
        assert (csvRecord.getColor().equals("Celeste"));
        assert (csvRecord.getItemGender().equals("Uomo"));
        assert (csvRecord.getPaymentMethod().equals("PayPal"));
        assert (csvRecord.getSize().equals("UNICA"));
        assert (csvRecord.getCategory().equals("Orologi"));
        assert (csvRecord.getMacroCategory().equals("Gioielli Moda"));
    }

    @Test
    public void orderIdErrorTest(){
        System.out.println("[INFO] OrderIdErrorTest");
        String[] strings = {"ciccio", "07/09/10", "IT", "M", "1", "89", "0", "0","FRECCE TRICOLORI", "Primavera - Estate 2011",
                "Celeste", "Uomo", "PayPal", "UNICA", "Orologi", "Gioielli Moda"};
        Assertions.assertThrows(NumberFormatException.class, () -> new CSVRecord(strings));
    }

    @Test
    public void dateErrorTest(){
        System.out.println("[INFO] RightConstructorTest");
        String[] strings = {"11", "ciccio", "IT", "M", "1", "89", "0", "0","FRECCE TRICOLORI", "Primavera - Estate 2011",
                "Celeste", "Uomo", "PayPal", "UNICA", "Orologi", "Gioielli Moda"};
        Assertions.assertThrows(ParseException.class, () -> new CSVRecord(strings));
    }

    @Test
    public void customerGenderErrorTest(){
        System.out.println("[INFO] CustomerGenderErrorTest");
        String[] strings = {"11", "07/09/10", "IT", "G", "1", "89", "0", "0","FRECCE TRICOLORI", "Primavera - Estate 2011",
                "Celeste", "Uomo", "PayPal", "UNICA", "Orologi", "Gioielli Moda"};
        Assertions.assertThrows(CSVParsingException.class, () -> new CSVRecord(strings));
    }

    @Test
    public void payedPriceParseTest(){
        System.out.println("[INFO] PayedPriceTest");
        String[] strings = {"11", "07/09/10", "IT", "M", "1", "ciccio", "0", "0","FRECCE TRICOLORI", "Primavera - Estate 2011",
                "Celeste", "Uomo", "PayPal", "UNICA", "Orologi", "Gioielli Moda"};
        Assertions.assertThrows(NumberFormatException.class, () -> new  CSVRecord(strings));
    }

    @Test
    public void payedPriceNegativeTest(){
        System.out.println("[INFO] PayedPriceNegativeTest");
        String[] strings = {"11", "07/09/10", "IT", "M", "1", "-89", "0", "0","FRECCE TRICOLORI", "Primavera - Estate 2011",
                "Celeste", "Uomo", "PayPal", "UNICA", "Orologi", "Gioielli Moda"};
        Assertions.assertThrows(IllegalArgumentException.class, () -> new CSVRecord(strings));
    }

    @Test
    public void outletParseErrorTest(){
        System.out.println("[INFO] OutletParseErrorTest");
        String[] strings = {"11", "07/09/10", "IT", "M", "1", "89", "0", "ciccio","FRECCE TRICOLORI", "Primavera - Estate 2011",
                "Celeste", "Uomo", "PayPal", "UNICA", "Orologi"};
        Assertions.assertThrows(CSVNullFieldsException.class, () -> new CSVRecord(strings));
    }
}
