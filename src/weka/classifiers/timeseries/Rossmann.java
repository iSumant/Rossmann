package weka.classifiers.timeseries;

import java.io.File;
import java.io.SyncFailedException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.codec.language.DoubleMetaphone;
import org.apache.commons.io.FilenameUtils;
import weka.classifiers.evaluation.NumericPrediction;
import weka.classifiers.functions.LinearRegression;
import weka.classifiers.functions.SMOreg;
import weka.classifiers.functions.supportVector.PolyKernel;
import weka.classifiers.functions.supportVector.RegSMOImproved;
import weka.classifiers.timeseries.core.TSLagMaker.Periodicity;
import weka.classifiers.timeseries.eval.TSEvaluation;
import weka.core.Instances;
import weka.core.converters.ConverterUtils.DataSource;
public class Rossmann
{
    private static final String ARFF_TRAIN_NON_MISSING_DATA =
            "D:\\workspace\\SASUniversityEdition\\myfolders\\sasuser.v94\\Rossmann\\NonMissing\\train\\arff";
    private static final String ARFF_TEST_NON_MISSING_DATA =
            "D:\\workspace\\SASUniversityEdition\\myfolders\\sasuser.v94\\Rossmann\\NonMissing\\test\\arff";
    private static final String ARFF_TRAIN_MISSING_DATA =
            "D:\\workspace\\SASUniversityEdition\\myfolders\\sasuser.v94\\Rossmann\\Missing\\train\\arff";
    private static final String ARFF_TEST_MISSING_DATA =
            "D:\\workspace\\SASUniversityEdition\\myfolders\\sasuser.v94\\Rossmann\\Missing\\test\\arff";
   private static final String ARFF_EXT = ".arff";

   public static void main(String[] args)
   {
       Rossmann ross = new Rossmann();
       File arffTrainNonMissingDir = new File(ARFF_TRAIN_NON_MISSING_DATA);
       File arffTestNonMissingDir = new File(ARFF_TEST_NON_MISSING_DATA);
       File arffTrainMissingDir = new File(ARFF_TRAIN_MISSING_DATA);
       File arffTestMissingDir = new File(ARFF_TEST_MISSING_DATA);

       List<File> nonMissingTrainFiles = Arrays.asList(arffTrainNonMissingDir.listFiles());
       List<File> nonMissingTestFiles = Arrays.asList(arffTestNonMissingDir.listFiles());
       List<File> missingTrainFiles = Arrays.asList(arffTrainMissingDir.listFiles());
       List<File> missingTestFiles = Arrays.asList(arffTestMissingDir.listFiles());

       if(true)
       {
           for (File nonMissingTrainFile : nonMissingTrainFiles)
           {
               for (File nonMissingTestFile : nonMissingTestFiles)
               {
                   String trainStoreID = nonMissingTrainFile.getName().replaceAll("[^0-9]", "");
                   String testStoreID = nonMissingTestFile.getName().replaceAll("[^0-9]", "");
                   if(trainStoreID.equals(testStoreID) && trainStoreID.equals("1"))
                   {
                       ross.loadTrainARFF(nonMissingTrainFile, nonMissingTestFile);
                   }
               }
           }
       }
       else
       {
           for (File missingTrainFile : missingTrainFiles)
           {
               for (File missingTestFile : missingTestFiles)
               {
                   String trainStoreID = missingTrainFile.getName().replaceAll("[^0-9]", "");
                   String testStoreID = missingTestFile.getName().replaceAll("[^0-9]", "");
                   if(trainStoreID.equals(testStoreID))
                   {
                       ross.loadTrainARFF(missingTrainFile, missingTestFile);
                   }
               }
           }
       }
   }

   public void loadTrainARFF(File trainingARFF, File testARFF)
   {
       try
       {
           Instances trainData = readARFF(trainingARFF.getPath());
           Instances testData = readARFF(testARFF.getPath());
           WekaForecaster forecaster = buildModel();
           if (trainData != null)
           {
               forecaster.buildForecaster(trainData, System.out);
               forecaster.primeForecaster(trainData);
               System.out.println("Scheme:\t\t" + forecaster.getAlgorithmName());
               System.out.println("Relation:\t" + trainData.relationName());
               System.out.println("Instances:\t" + trainData.numInstances());
               System.out.println("Attributes:\t" + trainData.numAttributes());
//               List<List<NumericPrediction>> forecast = forecaster.forecast(48, testData, System.out);
//               List<String> predictedValues = new ArrayList<>();
//               for (int i = 0; i < 48; i++)
//               {
//                   List<NumericPrediction> predsAtStep = forecast.get(i);
//                   for (int j = 0; j < 1; j++)
//                   {
//                       NumericPrediction predForTarget = predsAtStep.get(j);
//                       System.out.print("" + predForTarget.predicted() + " ");
//                       predictedValues.add(Double.toString(predForTarget.predicted()));
//                   }
//                   System.out.println();
//               }
               List<String> predictedValues = evaluate(forecaster, trainData, testData);
               if (predictedValues.size() == 48)
               {
                   PredictionsToCSV predictionToCSV = new PredictionsToCSV();
                   String storeName = FilenameUtils.removeExtension(trainingARFF.getName());
//                   predictionToCSV.update(storeName, predictedValues);
               }
           }
       }
       catch (Exception e)
       {
           e.printStackTrace();
       }
   }

   public Instances readARFF(String fileName)
   {
      Instances storeData = null;
      try
      {
         DataSource source = new DataSource(fileName);
         storeData = source.getDataSet();
         if (storeData.classIndex() == -1)
         {
            storeData.setClassIndex(storeData.numAttributes() - 1);
         }
      }
      catch (Exception e)
      {
         e.printStackTrace();
      }
      return storeData;
   }

   public WekaForecaster buildModel()
   {
      WekaForecaster forecaster = new WekaForecaster();
      try
      {
         forecaster.setFieldsToForecast("Sales");
         SMOreg smoReg = new SMOreg();
         smoReg.setKernel(new PolyKernel());
         smoReg.setRegOptimizer(new RegSMOImproved());
         forecaster.setBaseForecaster(smoReg);
         forecaster.getTSLagMaker().setTimeStampField("Date");
         forecaster.getTSLagMaker().setMinLag(1);
         forecaster.getTSLagMaker().setMaxLag(48);
         forecaster.getTSLagMaker().setPeriodicity(Periodicity.DAILY);
         forecaster.setOverlayFields("Open,Promo,StateHoliday,SchoolHoliday,StoreType,Assortment,"
                                        + "CompetitionDistance,CompetitionOpenSince_Days,"
                                        + "Promo2,PromoInterval,Promo2Since_Days");
      }
      catch (Exception e)
      {
         e.printStackTrace();
      }
      return forecaster;
   }

   public List<String> evaluate(WekaForecaster forecaster, Instances trainingData, Instances testData)
   {
       List<String> predictions = new ArrayList<>();
       try
      {
         int holdoutSize = testData.size();
         int totalSize = trainingData.size();
         TSEvaluation eval = new TSEvaluation(trainingData, testData);
         eval.setHorizon(holdoutSize);             //number of steps to forecast into the future
         eval.setPrimeWindowSize(totalSize);     //number of historical instances to be presented to the forecaster
         eval.setEvaluationModules("RMSPE,MAPE");
         eval.setForecastFuture(Boolean.TRUE);
         eval.setEvaluateOnTrainingData(Boolean.TRUE);
         eval.setEvaluateOnTestData(Boolean.TRUE);
         eval.evaluateForecaster(forecaster);
         System.out.println(eval.printFutureTrainingForecast(forecaster));
         System.out.printf(eval.toSummaryString());
         List<List<NumericPrediction>> m_trainingFuture =  eval.m_trainingFuture;
          for (List<NumericPrediction> numericPredictions : m_trainingFuture) {
              for (NumericPrediction numericPrediction : numericPredictions)
              {
                  predictions.add(Double.toString(numericPrediction.predicted()));
              }
          }

      }
      catch (Exception e)
      {
         e.printStackTrace();
      }
       return predictions;
   }

}
