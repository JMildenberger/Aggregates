%let program="ULC-Agg.sas";
%let programversion="0";

libname LPAll "J:\SAS Testing\Labor Productivity in SAS\LP All Sectors\Libraries\Intermediate";

/*Add program and version to ProgramVersion table */
Proc sql;
	insert into LPAll.ProgramVersions
	values (&program, &programversion);
quit;
/*	This piece of code is extracting the detailed ULC series used by the program. When this program goes live, this
	piece of code will be changed to pull from the dataset produced by the detailed ULC programs */

data work.ULCSourceData;
	set LPAll.LP_Append;
run;

Proc sql;
	Create table 	work.ManfUlcDet as 
	select 			Distinct IndustryID, DataSeriesID, DataArrayID, YearID, CensusPeriodID, Value
	from 			work.ULCSourceData
	where 			DataSeriesID in ("U25")
	order by 		IndustryID, DataSeriesID, YearID;
quit;

data work.ManfUlcDet;
	set work.ManfUlcDet;
	YearNo=input(substr(YearID,5,1),1.);
run;


/*	This code code assigns annual compensation dataseriescodeid's. U25 = AnnCmp*/
Proc sql;
	Create table	work.AssignAggMap as
	Select			Distinct IndustryID, ArrayCodeIndustryID, DigitID, CensusPeriodID, "U25" as	DataSeriesID
	from			LPAll.AggregateConcordance
	where			IndustrySeriesID="LaborComp"
	order by		IndustryID, CensusPeriodID, ArrayCodeIndustryID;
quit;

Proc sql;
	Create table	work.ConfigDistinct as
	Select 			Distinct IndustryID, IndustrySeriesID, CensusPeriodID, Program, Method
	from 			LPAll.ProgramMethodControlTable
	where 			IndustrySeriesID="LaborComp" and Program="ULC-Agg.sas";
quit;

/* The Lab-Agg industries are paired with the aggregation map*/
Proc sql;
	Create table	work.JoinConfigMapwithAggMap as
	Select			a.IndustryID, b.ArrayCodeIndustryID, b.DataSeriesID, a.CensusPeriodID, a.Method
	from			work.ConfigDistinct a
	left join		work.AssignAggMap b
	on				a.IndustryID=b.IndustryID and a.CensusPeriodID=b.CensusPeriodID;
quit;

/* The YearIDs for each industry are brought in */
Proc sql;
	Create table	work.JoinYearMap as
	Select			a.IndustryID, a.ArrayCodeIndustryID, a.DataSeriesID, a.CensusPeriodID, a.Method, b.YearID
	from			work.JoinConfigMapwithAggMap a
	left join		LPAll.SAS_IndustrySeriesYears b
	on				a.IndustryID=b.IndustryID and a.CensusPeriodID=b.CensusPeriodID
	where			b.IndustrySeriesID="LaborComp";
quit;

/* This code brings in the ULC data for each detailed industry into the AssignAggMap dataset */
Proc sql;
	Create table	work.AnnCmpDataSet as
	Select			a.IndustryID, a.ArrayCodeIndustryID, b.DataSeriesID, b.YearID, b.CensusPeriodID, b.YearNo, a.Method, b.Value
	from			work.JoinYearMap a
	inner join		work.ManfUlcDet b
	on				(a.ArrayCodeIndustryID=b.IndustryID) and (a.DataSeriesID=b.DataSeriesID) and (a.YearID=b.YearID);
quit;

/* This code sums ULC data for each detailed industry*/
proc sql;
	Create table	work.ULCAggCalculatedVariables as
	Select			IndustryID, DataSeriesID, "0000" as DataArrayID, YearID, CensusPeriodID, sum(Value) as Value
	from			work.AnnCmpDataset
	group by 		IndustryID, YearID, DataSeriesID, CensusPeriodID
	order by		IndustryID, YearID;

	Create table 	LPAll.LP_Append as
	Select			IndustryID, DataSeriesID, DataArrayID, YearID, CensusPeriodID, Value 			from work.ULCAggCalculatedVariables union all
	Select			IndustryID, DataSeriesID, DataArrayID, YearID, CensusPeriodID, Value 			from work.ULCSourceData
	order by		IndustryID, DataSeriesID, DataArrayID, YearID;

quit;

proc datasets library=work kill noprint;
run;
quit;

proc catalog c=work.sasmacr kill force;
run;
quit;
