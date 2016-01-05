package weka.classifiers.timeseries;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;

public class PredictionsToCSV
{
    private static final String CSV_TEST_NON_MISSING_DATA =
            "D:\\workspace\\SASUniversityEdition\\myfolders\\sasuser.v94\\Rossmann\\NonMissing\\test\\csv";
    private static final String CSV_TEST_MISSING_DATA =
            "D:\\workspace\\SASUniversityEdition\\myfolders\\sasuser.v94\\Rossmann\\Missing\\test\\csv";

    private static final String[] header = {"Id","Store","Date","DayOfWeek","Open","Promo","StateHoliday","SchoolHoliday","StoreType","Assortment","CompetitionDistance","CompetitionOpenSince_Days","Promo2","PromoInterval","Promo2Since_Days","Sales"};

    public List<File> loadAllCSVFiles()
    {
        File csvTestNonMissingDir = new File(CSV_TEST_NON_MISSING_DATA);
        System.out.println("[INFO] Loading CSV files from " + csvTestNonMissingDir);
        List<File> nonMissingFiles = Arrays.asList(csvTestNonMissingDir.listFiles());

        File csvTestMissingDir = new File(CSV_TEST_MISSING_DATA);
        System.out.println("[INFO] Loading CSV files from " + csvTestMissingDir);
        List<File> missingFiles = Arrays.asList(csvTestMissingDir.listFiles());

        List<File> file = new ArrayList<>();
        file.addAll(nonMissingFiles);
        file.addAll(missingFiles);
        return file;
    }

    public void update(String storeName, List<String> predictions)
    {
        File csvFile = loadFile(storeName);
        if( csvFile != null)
        {
            update(csvFile, predictions);
        }
    }

    private void update(File csvFile, List<String> predictions)
    {
        try
        {
            Reader in = new FileReader(csvFile);
            final CSVParser parser = new CSVParser(in, CSVFormat.EXCEL.withHeader());
            FileWriter fileWriter = new FileWriter(csvFile);
            final CSVPrinter printer = CSVFormat.DEFAULT.withHeader(header).print(fileWriter);
            List<CSVRecord> csvRecords = parser.getRecords();
            for(int i=0; i< csvRecords.size(); i++)
            {
                CSVRecord record = csvRecords.get(i);
                List<String> columnList = new ArrayList<>(header.length);
                for (int j = 0; j < header.length - 1; j++)
                {
                    columnList.add(record.get(header[j]));
                }
                columnList.add(predictions.get(i));
                printer.printRecord(columnList);
            }
            parser.close();
            in.close();
            fileWriter.close();
            printer.close();
            System.out.println("[INFO] Predicted values saved to " + csvFile.getName());

        }
        catch(Exception e)
        {
            e.printStackTrace();
        }
    }

    public File loadFile(String storeName)
    {
        List<File> files = loadAllCSVFiles();
        for (File file : files)
        {
            String storeIdFromFile = file.getName().replaceAll("[^0-9]", "");
            String storeIdFromName = storeName.replaceAll("[^0-9]", "");
            if (storeIdFromName.equals(storeIdFromFile))
            {
                return file;
            }
        }
        return null;
    }

    private static List<String> getDummyPrediction()
    {
        List<String> predictedValues = new ArrayList<>();
        predictedValues.add("4498.275830120248");
        predictedValues.add("144.2267895834862");
        predictedValues.add("5711.443200971612");
        predictedValues.add("5117.676835822561");
        predictedValues.add("5016.7733647619");
        predictedValues.add("5263.916115576019");
        predictedValues.add("5343.320345113131");
        predictedValues.add("4691.667417832552");
        predictedValues.add("399.88839412711235");
        predictedValues.add("4735.091672883739");
        predictedValues.add("4072.7705352034786");
        predictedValues.add("3842.639081649949");
        predictedValues.add("4010.644999227856");
        predictedValues.add("4135.879406196579");
        predictedValues.add("399.88839412711235");
        predictedValues.add("4735.091672883739");
        predictedValues.add("4072.7705352034786");
        predictedValues.add("3842.639081649949");
        predictedValues.add("4010.644999227856");
        predictedValues.add("4135.879406196579");
        predictedValues.add("4740.316307983622");
        predictedValues.add("4740.316307983622");
        predictedValues.add("258.7360180610074");
        predictedValues.add("5840.73035336306");
        predictedValues.add("5260.013406941502");
        predictedValues.add("4993.28235821097");
        predictedValues.add("5056.604028416014");
        predictedValues.add("5262.261857551541");
        predictedValues.add("4661.872115552211");
        predictedValues.add("263.53921516192895");
        predictedValues.add("4547.113040348941");
        predictedValues.add("4006.3434067292205");
        predictedValues.add("3703.3214037303383");
        predictedValues.add("3856.4281078686063");
        predictedValues.add("3935.573560320958");
        predictedValues.add("4554.541709817486");
        predictedValues.add("200.42325749193165");
        predictedValues.add("5729.889350656138");
        predictedValues.add("5158.253741223393");
        predictedValues.add("4974.211604639487");
        predictedValues.add("5074.867410079474");
        predictedValues.add("5190.403271541585");
        predictedValues.add("5729.889350656138");
        predictedValues.add("5729.889350656138");
        predictedValues.add("5729.889350656138");
        predictedValues.add("5729.889350656138");
        predictedValues.add("5729.889350656138");
        predictedValues.add("5729.889350656138");
        predictedValues.add("4593.894560131054");
        predictedValues.add("193.99111479091144");
        predictedValues.add("4434.270350641372");
        return predictedValues;
    }

    public static void main(String[] args)
    {

    }

}
