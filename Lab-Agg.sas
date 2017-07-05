%let program="Lab-Agg.sas";
%let programversion="0";

libname LPAll "J:\SAS Testing\Labor Productivity in SAS\LP All Sectors\Libraries\Intermediate";

/*Add program and version to ProgramVersion table */
Proc sql;
	insert into LPAll.ProgramVersions
	values (&program, &programversion);
quit;


data work.LaborSourceData;
	set LPAll.LP_Append;
run;

/* The detailed data is temporarily pulled from IPS. When incorporated into batch, the pull will come from the detailed labor programs */

Proc sql;
	Create table 	work.LabDet as 
	select 			Distinct IndustryID, DataSeriesID, DataArrayID, YearID, CensusPeriodID, Value
	from 			work.LaborSourceData
	where 			DataSeriesID in ("L21", "L22", "L23", "L24", "L25", "W21", "W22", "W23", "W24", "W25");
quit;


Proc sql;
	Create table	work.ConfigDistinct as
	Select 			Distinct IndustryID, IndustrySeriesID, CensusPeriodID, Program, Method
	from 			LPAll.ProgramMethodControlTable
	where 			IndustrySeriesID="LaborHours" and Program="Lab-Agg.sas";
quit;


/* The aggregate map is whittled down to the distinct list of IndustryID/ArrayCodeIndustryID/CensusPeriodID combinations */
Proc sql;
	Create table	work.DistinctAggMap as
	Select			Distinct IndustryID, ArrayCodeIndustryID, CensusPeriodID
	from			LPALL.AggregateConcordance
	where			IndustrySeriesID="LaborHours";
quit;

/* The Lab-Agg industries are paired with the aggregation map*/
Proc sql;
	Create table	work.JoinConfigMapwithAggMap as
	Select			a.IndustryID, b.ArrayCodeIndustryID, a.CensusPeriodID, a.Method
	from			work.ConfigDistinct a
	left join		work.DistinctAggMap b
	on				a.IndustryID=b.IndustryID and a.CensusPeriodID=b.CensusPeriodID;
quit;

/* The YearIDs for each industry are brought in */
Proc sql;
	Create table	work.JoinYearMap as
	Select			a.IndustryID, a.ArrayCodeIndustryID, a.CensusPeriodID, a.Method, b.YearID
	from			work.JoinConfigMapwithAggMap a
	left join		LPAll.SAS_IndustrySeriesYears b
	on				a.IndustryID=b.IndustryID and a.CensusPeriodID=b.CensusPeriodID
	where			b.IndustrySeriesID="LaborHours";
quit;

/*	A dataset is required that lists out the 10 source DataSeriesIDs required for labor aggregation.
	L21=PWHrs; L22=NPWHrs; L23=SEHrs; L24=UPFHrs; L25=AnnHrs; W21=PWEmp; W22=NPWEmp; W23=SEEmp; W24=NPFEmp; W25=AnnWrk */
data work.DetailDataSeriesID;
input DataSeriesID $4.;
datalines;
L21
L22
L23
L24
L25
W21
W22
W23
W24
W25
;
run;

data work.AEDataSeriesID;
input DataSeriesID $4.;
datalines;
L25
W25
;
run;

/*The DataSeriesIDs are merged with the aggregate maps */
Proc sql;
	Create table	work.AssignAggMapAE as
	Select			a.IndustryID, a.ArrayCodeIndustryID, a.CensusPeriodID, a.Method, a.YearID, b.DataSeriesID
	from			work.JoinYearMap a,work.AEDataSeriesID b
	where			a.Method="AEOnly";

	Create table	work.AssignAggMapStandard as
	Select			a.IndustryID, a.ArrayCodeIndustryID, a.CensusPeriodID, a.Method,  a.YearID, b.DataSeriesID
	from			work.JoinYearMap a,work.DetailDataSeriesID b
	where			a.Method="Standard";

	Create table	work.AssignAggMapAll as
	Select			* from work.AssignAggMapAE union all
	Select			* from work.AssignAggMapStandard;
quit;


Proc sql;
	/*The aggregate map is finally merged with the detailed values */
	Create table	work.PopulateAggMap as
	Select			a.*, b.Value
	from			work.AssignAggMapAll a
	left join		work.LabDet b
	on				(a.ArrayCodeIndustryID=b.IndustryID) and (a.YearID=b.YearID) and (a.DataSeriesID=b.DataSeriesID);

	/* Calculating values. If a single detailed component has a null value then the summation will return a null value */
	Create table	work.SumSeries as
	Select			IndustryID, DataSeriesID, CensusPeriodID, YearID, 
					case when nmiss(value)=0 then sum(value) else . end as Value
	from			work.PopulateAggMap
	group by		IndustryID, DataSeriesID, CensusPeriodID, YearID;
quit;

Proc sql;
/* Merging calculated variables together */
	Create table 	work.LabAggCalculatedVariables as
	Select 			IndustryID, DataSeriesID, "0000" as DataArrayID, YearID, CensusPeriodID, Value 	from work.SumSeries
	order by		IndustryID, DataSeriesID, YearID;


	Create table 	LPAll.LP_Append as
	Select			IndustryID, DataSeriesID, DataArrayID, YearID, CensusPeriodID, Value 			from work.LabAggCalculatedVariables union all
	Select			IndustryID, DataSeriesID, DataArrayID, YearID, CensusPeriodID, Value 			from work.LaborSourceData
	order by		IndustryID, DataSeriesID, DataArrayID, YearID;
quit;

proc datasets library=work kill noprint;
run;
quit;

proc catalog c=work.sasmacr kill force;
run;
quit;
