/**
 * SAS program for predicting sales for Rossmann Store
 * Rossmann.sas Date: 10/27/15
 * @author <a href="mailto:sumant@mail.usf.edu">Sumant Sharma</a>
 */
 
LIBNAME projLib '/folders/myfolders/sasuser.v94/Rossmann/';

/* Importing the datasets */
%importCSV("/folders/myfolders/sasuser.v94/Rossmann/train.csv", rossmann_train);
%importCSV("/folders/myfolders/sasuser.v94/Rossmann/store.csv", rossmann_store);
%importCSV("/folders/myfolders/sasuser.v94/Rossmann/test.csv", rossmann_test);

/* Sorting the datasets according to Store */
%sortData(rossmann_train, Store);
%sortData(rossmann_store, Store);
%sortData(rossmann_test, Store);

/* Pre-processing attributes in rossmann_store*/
DATA rossmann_store_final(DROP=	CompetitionOpenSinceMonth 
							CompetitionOpenSinceYear 
							Promo2SinceWeek 
							Promo2SinceYear 
							CompetitionOpenSinceDate 
							Promo2SinceDate 
							PredictionStartDate);
	SET rossmann_store;
	/* Merging the CompetitionOpenSinceMonth and CompetitionOpenSinceYear into CompetitionOpenSince */
	CompetitionOpenSinceDate=MDY(CompetitionOpenSinceMonth, 1, CompetitionOpenSinceYear);
	FORMAT CompetitionOpenSinceDate WORDDATE12.;
	
	/* Merging the Promo2SinceWeek and Promo2SinceYear into Promo2Since */
	Promo2SinceDate=intnx('week', MDY(1,1,Promo2SinceYear), Promo2SinceWeek-1);
	FORMAT Promo2SinceDate WORDDATE12.;
	
	PredictionStartDate = '1aug2015'd;
	/* Converting Date predictor variables to relative measure */
	CompetitionOpenSince_Days = PredictionStartDate - CompetitionOpenSinceDate;
	Promo2Since_Days = PredictionStartDate - Promo2SinceDate;	
RUN;

/* Creating the subsets of the train dataset based on stores with and without missing sales */
%exploreCategorical(rossmann_train, Store, freq_Store);

DATA nonMissingStores(DROP=Percent Count) missingStores(DROP=Percent Count);
	SET freq_Store;
	IF COUNT EQ 942 THEN OUTPUT nonMissingStores;
	ELSE OUTPUT missingStores;
RUN;

/* %printData(nonMissingStores, 1000); */
/* %printData(missingStores, 1000); */

DATA train_nonmissing(DROP=Percent Count) train_missing(DROP=Percent Count);
	MERGE rossmann_train freq_Store;
	BY Store;	
	IF COUNT EQ 942 THEN OUTPUT train_nonmissing;
	ELSE OUTPUT train_missing;
RUN;

DATA test_nonmissing(DROP=Percent Count) test_missing(DROP=Percent Count);
	MERGE rossmann_test freq_Store;
	BY Store;	
	IF COUNT EQ 942 THEN OUTPUT test_nonmissing;
	ELSE OUTPUT test_missing;
RUN;

/*----------------------------------------------------------------------------------------------*/
/*     Merging train data and store data and creating a separate csv for each store     		*/
/*---------------------------------------------------------------------------------------------*/

%macro subsetNonMissingTrainStores;
	%local i;
/* "/folders/myfolders/sasuser.v94/Rossmann/NonMissing/train/csv/Rossmann_Store&storeId..csv"  */
/* 	%do i=1 %to 1115; */
/* 		%mergeDataByStore(train_nonmissing, &i); */
		%mergeDataByStore(train_nonmissing, 616);
		%mergeDataByStore(train_nonmissing, 639);
		%mergeDataByStore(train_nonmissing, 848);
		%mergeDataByStore(train_nonmissing, 1076);
		
/* 	%end; */
%mend;

%macro subsetMissingTrainStores;
	%local i;
/* "/folders/myfolders/sasuser.v94/Rossmann/Missing/train/csv/Rossmann_Store&storeId..csv"  */
/* 	%do i=1 %to 1115; */
/* 		%mergeDataByStore(train_missing, &i); */
		%mergeDataByStore(train_missing, 616);
		%mergeDataByStore(train_missing, 639);
		%mergeDataByStore(train_missing, 848);
		%mergeDataByStore(train_missing, 1076);
/* 	%end; */
%mend;

%macro subsetNonMissingTestStores;
	%local i;
/* "/folders/myfolders/sasuser.v94/Rossmann/NonMissing/test/csv/Rossmann_Store&storeId..csv"  */
/* 	%do i=1 %to 1115; */
/* 		%mergeTestDataByStore(test_nonmissing, &i); */
		%mergeDataByStore(test_nonmissing, 616);
		%mergeDataByStore(test_nonmissing, 639);
		%mergeDataByStore(test_nonmissing, 848);
		%mergeDataByStore(test_nonmissing, 1076);
/* 	%end; */
%mend;

%macro subsetMissingTestStores;
	%local i;
/* "/folders/myfolders/sasuser.v94/Rossmann/Missing/test/csv/Rossmann_Store&storeId..csv"  */
/* 	%do i=1 %to 1115; */
/* 		%mergeTestDataByStore(test_missing, &i); */
/* 	%end; */
	%mergeDataByStore(test_missing, 616);
		%mergeDataByStore(test_missing, 639);
		%mergeDataByStore(test_missing, 848);
		%mergeDataByStore(test_missing, 1076);
%mend;

OPTIONS nomerror noserror;
%subsetNonMissingTrainStores;
%subsetMissingTrainStores;
%subsetNonMissingTestStores;
%subsetMissingTestStores;

%MACRO mergeDataByStore(dataIn, storeId);
DATA trainData(WHERE=(Store=&storeId));
	SET &dataIn;
RUN;

%sortData(trainData, Date);

DATA Rossmann_Store&storeId;
	MERGE trainData rossmann_store_final(WHERE=(Store=&storeId));
	BY STORE;
	FORMAT Date YYMMDDd10.;
RUN;

DATA Rossmann_Store&storeId;
	RETAIN Store Date DayOfWeek Customers Open Promo StateHoliday SchoolHoliday StoreType Assortment CompetitionDistance CompetitionOpenSince_Days Promo2 PromoInterval Promo2Since_Days Sales;
	SET Rossmann_Store&storeId;
RUN;

%exportStoreToCSV(Rossmann_Store&storeId, &storeId);

/* Delete the dataset after each iteration to save memory */
PROC DELETE DATA=WORK.TRAINDATA WORK.Rossmann_Store&storeId (GENNUM=all);
RUN;
%MEND;

%MACRO mergeTestDataByStore(dataIn, storeId);
DATA testData(WHERE=(Store=&storeId));
	SET &dataIn;
RUN;

%sortData(testData, Date);

DATA Rossmann_Store&storeId;
	MERGE testData rossmann_store_final(WHERE=(Store=&storeId));
	BY STORE;
	FORMAT Date YYMMDDd10.;
RUN;

DATA Rossmann_Store&storeId;
	RETAIN Id Store Date DayOfWeek Open Promo StateHoliday SchoolHoliday StoreType Assortment CompetitionDistance CompetitionOpenSince_Days Promo2 PromoInterval Promo2Since_Days Sales;
	SET Rossmann_Store&storeId;
RUN;

%exportStoreToCSV(Rossmann_Store&storeId, &storeId);

/* Delete the dataset after each iteration to save memory */
PROC DELETE DATA=WORK.TRAINDATA WORK.Rossmann_Store&storeId (GENNUM=all);
RUN;
%MEND;


/*--------------------------------------------------*/
/* 					Exploration  					*/
/*--------------------------------------------------*/


%exploreCategorical(rossmann_store, StoreType, freq_StoreType);
%exploreCategorical(rossmann_store, Assortment, freq_Assortment);
%exploreCategorical(rossmann_store, PromoInterval, freq_PromoInterval);
%printData(rossmann_train, 10);
%printData(rossmann_store_final, 10);
%printData(rossmann_test, 10);




/* MACROS */
%MACRO importCSV(inputFile, dataOut);
	PROC IMPORT datafile=&inputFile
	     out=&dataOut
	     dbms=csv
	     replace;
	     getnames=yes;
	RUN;
%MEND;

%MACRO exportCSV(dataIn, outputFile);
	PROC EXPORT data=&dataIn
	     outfile=&outputFile
	     dbms=csv
	     replace;
	RUN;
%MEND;

%MACRO exportStoreToCSV(dataIn, storeId, pathToFile);
	PROC EXPORT data=&dataIn
	     outfile="/folders/myfolders/sasuser.v94/Rossmann/Missing/test/csv/Rossmann_Store&storeId..csv"
	     dbms=csv
	     replace;
	RUN;
%MEND;

%MACRO printData(inputData, noOfObs);
	PROC PRINT DATA=&inputData(OBS=&noOfObs);
	RUN;
%MEND;

%MACRO sortData(inputData, sortVariable);
	PROC SORT DATA=&inputData; 
 		BY &sortVariable; 
	RUN;
%MEND;

%MACRO exploreContinuous(dataIn, varName);
	PROC SORT DATA=&dataIn;
		BY &varName;
	RUN;
	
	ODS SELECT BasicMeasures ExtremeObs MissingValues;
	PROC UNIVARIATE;
		VAR &varName;
	 	OUTPUT out=dataOut n=n mean=var_mean median=var_median std=var_sd nmiss=var_miss;
	RUN;
	
	PROC SGPLOT DATA=&dataIn;
		HISTOGRAM &varName;
		DENSITY &varName/ type=kernel;
		TITLE "&varName Distribution";
	RUN;
%MEND;

%MACRO exploreCategorical(dataIn, varName, dataOut);
	PROC SORT DATA=&dataIn;
		 BY &varName;
	RUN;
	
	PROC FREQ DATA=&dataIn noprint;
		 TABLES &varName / out=&dataOut;
	RUN;
		
	PROC PRINT DATA=&dataOut;
		TITLE "Frequency Table for &varName variable";	
	RUN;
%MEND;

%MACRO plotTimeSeriesPerStore(inputData, storeID);
	ODS graphics on / IMAGEFMT=PNG WIDTH=1400px;
	PROC SGPLOT DATA=&inputData (WHERE=(Store=&storeID));
		SERIES X=Date Y=Sales;
		LOESS X=Date Y=Sales / SMOOTH=0.1 NOMARKERS NOLEGFIT DEGREE=2 LINEATTRS=(color=black);
		TITLE "Sales trend for Store: &storeID";
	QUIT;
%MEND;

%MACRO plotTimeSeriesPerYear(inputData, storeID);
	ODS graphics on / IMAGEFMT=PNG WIDTH=1400px;
	PROC SGPLOT DATA=&inputData (WHERE=(Store=&storeID));
		SERIES X=Date Y=Sales;
		LOESS X=Date Y=Sales / SMOOTH=0.1 NOMARKERS NOLEGFIT DEGREE=2 LINEATTRS=(color=black);
		TITLE "Sales trend for Store: &storeID";
	QUIT;
%MEND;

/*----------------------------------------------*/
/*     Plot Sale Trends for all the stores      */
/*----------------------------------------------*/

%macro plotSaleTrends;
	%local i;
	%do i=1 %to 1115;
		%plotTimeSeries(projLib.train, &i);
	%end;
%mend;

/* %plotSaleTrends; */

/*----------------------------------------------*/
/*     				Obsolete		  	        */
/*----------------------------------------------*/
PROC FORMAT;
   value formatCompDistance	low-<100 = 'Very Near'
                 			100-<500 = 'Moderate Near'
                 			500-<1000 = 'Moderate Far'
                 			1000-high = 'Very Far';
RUN;





