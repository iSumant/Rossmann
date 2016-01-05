package weka.classifiers.timeseries.eval;

import java.util.ArrayList;
import java.util.List;

import weka.classifiers.evaluation.NumericPrediction;
import weka.core.Utils;

/**
 * An evaluation module that computes the root mean squared percentage error of forecasted values.
 * Refer: https://www.kaggle.com/c/rossmann-store-sales/details/evaluation
 *
 * @author Sumant Sharma (sumant@mail.usf.edu)
 */
public class RMSPEModule extends RMSEModule
{
   @Override
   public String getEvalName()
   {
      return "RMSPE";
   }

   @Override
   public String getDescription()
   {
      return "Root mean squared percentage error";
   }

   @Override
   public String getDefinition()
   {
      return "sqrt(sum(((actual - predicted)/actual)^2) / N)";
   }

   @Override
   public double[] calculateMeasure() throws Exception
   {
      double[] result = new double[m_targetFieldNames.size()];
      for (int i = 0; i < result.length; i++)
      {
         result[i] = Utils.missingValue();
      }

      for (int i = 0; i < m_targetFieldNames.size(); i++)
      {
         double sumRootMeanSquare = 0;
         List<NumericPrediction> preds = m_predictions.get(i);

         int count = 0;
         for (NumericPrediction p : preds)
         {
            if (!Utils.isMissingValue(p.error()) && Math.abs(p.actual()) > 0)
            {
               sumRootMeanSquare += Math.pow(p.error() / p.actual(), 2);
               count++;
            }
         }

         if (count > 0)
         {
            sumRootMeanSquare /= count;
            result[i] = Math.sqrt(sumRootMeanSquare) * 100;
         }
         else
         {
            result[i] = Utils.missingValue();
         }
      }
      return result;
   }

   public String calculateTotalRMSPE(int holdoutSize) throws Exception
   {
      double result, sumRootMeanSquare = 0;
      for (int i = 0; i < m_targetFieldNames.size(); i++)
      {
         List<NumericPrediction> preds = m_predictions.get(i);

         for (NumericPrediction p : preds)
         {
            if (!Utils.isMissingValue(p.error()) && Math.abs(p.actual()) > 0)
            {
               sumRootMeanSquare += Math.pow(p.error() / p.actual(), 2);
            }
         }
      }
      sumRootMeanSquare /= holdoutSize;
      result = Math.sqrt(sumRootMeanSquare) * 100;
      StringBuilder out = new StringBuilder();
      out.append("=== Total " + getDescription() + " ===");
      out.append(result);
      return out.toString();
   }

}
