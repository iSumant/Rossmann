package weka.classifiers.timeseries;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import weka.core.Attribute;
import weka.core.Instance;
import weka.core.Instances;
import weka.core.converters.ArffLoader;
import weka.core.converters.ArffSaver;
import weka.core.converters.CSVLoader;
import weka.filters.Filter;
import weka.filters.unsupervised.attribute.Add;
import weka.filters.unsupervised.attribute.Remove;
import weka.filters.unsupervised.attribute.RenameNominalValues;

public class CSV2Arff
{
    private static final String CSV_TRAIN_NON_MISSING_DATA =
            "D:\\workspace\\SASUniversityEdition\\myfolders\\sasuser.v94\\Rossmann\\NonMissing\\train\\csv";
    private static final String CSV_TEST_NON_MISSING_DATA =
            "D:\\workspace\\SASUniversityEdition\\myfolders\\sasuser.v94\\Rossmann\\NonMissing\\test\\csv";
    private static final String ARFF_TRAIN_NON_MISSING_DATA =
            "D:\\workspace\\SASUniversityEdition\\myfolders\\sasuser.v94\\Rossmann\\NonMissing\\train\\arff";
    private static final String ARFF_TEST_NON_MISSING_DATA =
            "D:\\workspace\\SASUniversityEdition\\myfolders\\sasuser.v94\\Rossmann\\NonMissing\\test\\arff";

    private static final String CSV_TRAIN_MISSING_DATA =
            "D:\\workspace\\SASUniversityEdition\\myfolders\\sasuser.v94\\Rossmann\\Missing\\train\\csv";
    private static final String CSV_TEST_MISSING_DATA =
            "D:\\workspace\\SASUniversityEdition\\myfolders\\sasuser.v94\\Rossmann\\Missing\\test\\csv";
    private static final String ARFF_TRAIN_MISSING_DATA =
            "D:\\workspace\\SASUniversityEdition\\myfolders\\sasuser.v94\\Rossmann\\Missing\\train\\arff";
    private static final String ARFF_TEST_MISSING_DATA =
            "D:\\workspace\\SASUniversityEdition\\myfolders\\sasuser.v94\\Rossmann\\Missing\\test\\arff";

    private static final String FILE_SEPARATOR = "\\";
    private static final String TRAIN = "_train";
    private static final String TEST = "_test";


    public static void main(String[] args) throws Exception
    {
        CSV2Arff csv2Arff = new CSV2Arff();
        File csvTrainNonMissingDir = new File(CSV_TRAIN_NON_MISSING_DATA);
        System.out.println("[Info] Loading CSV files from " + csvTrainNonMissingDir);
//        csv2Arff.loadFiles(csvTrainNonMissingDir);
        File arffTrainNonMissingDir = new File(ARFF_TRAIN_NON_MISSING_DATA);
        System.out.println("[Info] Loading ARFF files from " + arffTrainNonMissingDir);
//        csv2Arff.resetARFFHeader(arffTrainNonMissingDir);

        File csvTrainMissingDir = new File(CSV_TRAIN_MISSING_DATA);
        System.out.println("[Info] Loading CSV files from " + csvTrainMissingDir);
//        csv2Arff.loadFiles(csvTrainMissingDir);
        File arffTrainMissingDir = new File(ARFF_TRAIN_MISSING_DATA);
        System.out.println("[Info] Loading ARFF files from " + arffTrainMissingDir);
//        csv2Arff.resetARFFHeader(arffTrainMissingDir);

        File csvTestNonMissingDir = new File(CSV_TEST_NON_MISSING_DATA);
        System.out.println("[Info] Loading CSV files from " + csvTestNonMissingDir);
        csv2Arff.loadFiles(csvTestNonMissingDir);
        File arffTestNonMissingDir = new File(ARFF_TEST_NON_MISSING_DATA);
        System.out.println("[Info] Loading ARFF files from " + arffTestNonMissingDir);
        csv2Arff.resetARFFHeader(arffTestNonMissingDir);

        File csvTestMissingDir = new File(CSV_TEST_MISSING_DATA);
        System.out.println("[Info] Loading CSV files from " + csvTestMissingDir);
//        csv2Arff.loadFiles(csvTestMissingDir);
        File arffTestMissingDir = new File(ARFF_TEST_MISSING_DATA);
        System.out.println("[Info] Loading ARFF files from " + arffTestMissingDir);
//        csv2Arff.resetARFFHeader(arffTestMissingDir);
    }

    public void loadFiles(File dir)
    {
        List<File> files = Arrays.asList(dir.listFiles());
        int count=0;
        boolean flag=false;
        for (File csvFile : files)
        {
            long sizeInKiloBytes = csvFile.length()/1024;
            if (sizeInKiloBytes > 1L)
            {
                count++;
                if(CSV_TRAIN_NON_MISSING_DATA.equals(dir.getPath()) )
                {
                    flag = true;
                    Thread thread = new Thread(){
                        public void run()
                        {
                            convertTrain(csvFile, ARFF_TRAIN_NON_MISSING_DATA);
                        }
                    };
                    thread.start();
                }
                else if(CSV_TRAIN_MISSING_DATA.equals(dir.getPath()) && false)
                {
                    flag = true;
                    Thread thread = new Thread(){
                        public void run()
                        {
                            convertTrain(csvFile, ARFF_TRAIN_MISSING_DATA);
                        }
                    };
                    thread.start();
                }
                else if(CSV_TEST_NON_MISSING_DATA.equals(dir.getPath()) )
                {
                    flag = true;
                    Thread thread = new Thread(){
                        public void run()
                        {
                            convertTest(csvFile, ARFF_TEST_NON_MISSING_DATA);
                        }
                    };
                    thread.start();
                }
                else if(CSV_TEST_MISSING_DATA.equals(dir.getPath()) && false)
                {
                    flag = true;
                    Thread thread = new Thread()
                    {
                        public void run()
                        {
                            convertTest(csvFile, ARFF_TEST_MISSING_DATA);
                        }
                    };
                    thread.start();
                }
            }
            else
            {
                continue;
            }

        }
        if(flag)
        {
            System.out.println("[Info] ----- Total Files Converted: " + count + " -----");
        }
    }

    public void convertTrain(File csvFile, String outputPath)
    {
        try
        {
            String fileName = FilenameUtils.removeExtension(csvFile.getName()) + TRAIN;
            String arffFile = fileName + ArffLoader.FILE_EXTENSION;
            // LOAD CSV
            CSVLoader loader = new CSVLoader();
            loader.setSource(csvFile);
            loader.setNominalAttributes("1,3,5,6,7,8,9,10,13,14");
            loader.setDateFormat("yyyy-MM-dd");
            loader.setDateAttributes("2");
            loader.setNumericAttributes("11,12,15");
            Instances data = loader.getDataSet();
            data.setRelationName(fileName);

            // SAVE ARFF
            ArffSaver saver = new ArffSaver();
            saver.setInstances(data);
            saver.setFile(new File(outputPath + FILE_SEPARATOR + arffFile));
            saver.writeBatch();
            System.out.println("[Info] " + arffFile + " file saved.\tTotal instances: " + data.size());
        }
        catch (IOException exp)
        {
            System.err.println("[Error]" + exp.getLocalizedMessage());
        }

    }

    public void convertTest(File csvFile, String outputPath)
    {
        try
        {
            String fileName = FilenameUtils.removeExtension(csvFile.getName()) + TEST;
            String arffFile = fileName + ArffLoader.FILE_EXTENSION;
            // LOAD CSV
            CSVLoader loader = new CSVLoader();
            loader.setSource(csvFile);
            loader.setNominalAttributes("1,2,4,5,6,7,8,9,10,13,14");
            loader.setDateFormat("yyyy-MM-dd");
            loader.setDateAttributes("3");
            loader.setNumericAttributes("11,12,15");
            Instances data = loader.getDataSet();
            data.setRelationName(fileName);

            //Adding Customer to ARFF
            Instances newData = new Instances(data);
            newData.insertAttributeAt(new Attribute("Customers"), 4);
            newData.insertAttributeAt(new Attribute("Sales"), newData.numAttributes());

            //Removing ID from ARFF
            Remove remove = new Remove();
            remove.setAttributeIndices("1");
            remove.setInvertSelection(false);
            remove.setInputFormat(newData);
            newData = Filter.useFilter(newData, remove);

            // SAVE ARFF
            ArffSaver saver = new ArffSaver();
            saver.setInstances(newData);
            saver.setFile(new File(outputPath + FILE_SEPARATOR + arffFile));
            saver.writeBatch();
            System.out.println("[Info] " + arffFile + " file saved.\tTotal instances: " + data.size());
        }
        catch (Exception exp)
        {
            System.err.println("[Error]" + exp.getLocalizedMessage());
        }

    }

    public void resetARFFHeader(File dir)
    {
        List<File> files = Arrays.asList(dir.listFiles());
        for (File arffFile : files)
        {
            try
            {
                List<String> lines = FileUtils.readLines(arffFile);
                for (int i=3; i<=17; i++)
                {
                    lines.remove(3);
                }
                lines.set(3, getDefaultARFFHeader());
                FileUtils.writeLines(arffFile, lines);
                System.out.println("[Info] " + arffFile.getName() + " header modified");
            }
            catch (Exception e)
            {
                e.printStackTrace();
            }
        }
    }

    private static String getDefaultARFFHeader()
    {
        StringBuilder arffHeader = new StringBuilder();
        arffHeader.append("@attribute Date date yyyy-MM-dd\n" +
                "@attribute DayOfWeek {2,3,4,5,6,7,1}\n" +
                "@attribute Customers numeric\n" +
                "@attribute Open {0,1}\n" +
                "@attribute Promo {0,1}\n" +
                "@attribute StateHoliday {a,0,b,c}\n" +
                "@attribute SchoolHoliday {0,1}\n" +
                "@attribute StoreType {a,b,c,d}\n" +
                "@attribute Assortment {a,b,c}\n" +
                "@attribute CompetitionDistance numeric\n" +
                "@attribute CompetitionOpenSince_Days numeric\n" +
                "@attribute Promo2 {0,1}\n" +
                "@attribute PromoInterval {'Jan,Apr,Jul,Oct','Mar,Jun,Sept,Dec','Feb,May,Aug,Nov'}\n" +
                "@attribute Promo2Since_Days numeric\n" +
                "@attribute Sales numeric\n");
        return arffHeader.toString();
    }
}



