use [AUS_Dataset];
Go

-- Query to transfer data from [dbo].[Aus_Suburb_Postcode] dataset to City Table through Import Data

select distinct City from [dbo].[Aus_Suburb_Postcode]
order by City ASC

select * from City

-- Query to transfer data from [dbo].[Aus_Suburb_Postcode] dataset to State Table through Import Data

select distinct State, State_Code from [dbo].[Aus_Suburb_Postcode]
order by State ASC

select * from State

-- Query to transfer data from [dbo].[Aus_Suburb_Postcode] dataset to Suburb Table through Import Data

select S.Suburb, S.Postcode, S.City, S.State, S.State_Code, ct.City_id, st.State_id, S.Latitude, S.Longitude from [dbo].[Aus_Suburb_Postcode] S
join City ct on ct.City_Name = S.City
join State st on st.State_Code = S.State_Code
order by S.Suburb Asc

select * from Suburb

-- Query to transfer Property data from [dbo].[NSW_PropertyValue] dataset to Property Table through Import Data

select p.Property_Median_Value, p.Date, p.Updated_Year, p.Updated_Month, p.DateKey, s.Suburb_id from [dbo].[NSW_PropertyValue] p
join Suburb s on s.Suburb_Name = p.Suburb
join City c on c.City_Name = p.City
join State st on st.State_Code = p.State_Code
order by p.Suburb ASC


select * from Property

-- Query to transfer Station data from [dbo].[NSW_PropertyValue] dataset to Property Table through Import Data

select distinct Entrance_Type from [dbo].[NSW_StationEntrances_2018]
order by Entrance_Type ASC

select * from [dbo].[NSW_StationEntrances_2018]


select stn.Train_Station, stn.Street_Name, stn.Street_Type, se_type.Entrance_id, stn.LAT, stn.LONG, 
stn.Exit_Number, stn.Address, s.Suburb_id from [dbo].[NSW_StationEntrances_2018] stn
join Suburb s on s.Suburb_Name = stn.Train_Station
join Station_EntranceType se_type on se_type.Entrance_Type = stn.Entrance_Type
where s.State_id = 2
order by stn.Train_Station ASC


SELECT * FROM Station

-- Query to transfer School data from [dbo].[NSW_Public_Schools] dataset to School Table through Import Data

select distinct Level_of_Schooling from [dbo].[NSW_Public_Schools]
order by Level_of_Schooling ASC

select distinct School_Gender from [dbo].[NSW_Public_Schools]
order by School_Gender ASC

select distinct School_Subtype from [dbo].[NSW_Public_Schools]
order by School_Subtype ASC


SELECT ps.School_Code, ps.School_Name, ps.Number_of_Students, ps.AgeID, ps.ICSEA_Value, sgt.Gender_id, ps.Street_Name,
sub.Suburb_id, ps.Suburb, ps.postcode, ps.Phone, ps.Fax, ps.Email, ps.indigenous_pct, ps.lbote_pct, sct.SchoolType_id, sst.School_SubType_id,
ps.Selective_School, ps.late_opening_school, ps.Date_1st_Teacher, dt.DateKey, ps.lga, ps.electorate, ps.fed_electorate, 
ps.operational_directorate, ps.Latitude, ps.Longitude,ps.Date_Extracted

FROM [dbo].[NSW_Public_Schools] ps
join Suburb sub on sub.Suburb_Name = ps.Suburb
join SchoolGender_Type sgt on sgt.Gender_Type = ps.School_Gender
join School_Type sct on sct.Level_of_Schooling = ps.Level_of_Schooling
join School_SubType sst on sst.School_SubType = ps.School_Subtype
join Date dt on dt.DateKey = ps.DateKey
where sub.State_id = 2 and sub.Postcode = ps.postcode
order by ps.Suburb ASC


-- Create Date Table


IF (EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'dbo' AND  TABLE_NAME = 'Date'))
BEGIN
DROP TABLE Date
END
go

GO
CREATE TABLE	[dbo].[Date]
	(	[DateKey] INT primary key, 
		[Date] DATETIME,
		[FullDate] CHAR(10),-- Date in MM-dd-yyyy format
		[DayOfMonth] VARCHAR(2), -- Field will hold day number of Month
		[DayName] VARCHAR(9), -- Contains name of the day, Sunday, Monday 
		[DayOfWeek] CHAR(1),-- First Day Sunday=1 and Saturday=7
		[DayOfWeekInMonth] VARCHAR(2), --1st Monday or 2nd Monday in Month
		[DayOfWeekInYear] VARCHAR(2),
		[DayOfQuarter] VARCHAR(3),
		[DayOfYear] VARCHAR(3),
		[WeekOfMonth] VARCHAR(1),-- Week Number of Month 
		[WeekOfQuarter] VARCHAR(2), --Week Number of the Quarter
		[WeekOfYear] VARCHAR(2),--Week Number of the Year
		[Month] VARCHAR(2), --Number of the Month 1 to 12
		[MonthName] VARCHAR(9),--January, February etc
		[MonthOfQuarter] VARCHAR(2),-- Month Number belongs to Quarter
		[Quarter] CHAR(1),
		[QuarterName] VARCHAR(9),--First,Second..
		[Year] CHAR(4),-- Year value of Date stored in Row
		[YearName] CHAR(7), --CY 2012,CY 2013
		[MonthYear] CHAR(10), --Jan-2013,Feb-2013
		[MMYYYY] CHAR(6),
		[FirstDayOfMonth] DATE,
		[LastDayOfMonth] DATE,
		[FirstDayOfQuarter] DATE,
		[LastDayOfQuarter] DATE,
		[FirstDayOfYear] DATE,
		[LastDayOfYear] DATE
	)
GO


select min(Date_1st_Teacher), max(Date_1st_Teacher) from [dbo].[NSW_Public_Schools]

/********************************************************************************************/
--Specify Start Date and End date here
--Value of Start Date Must be Less than Your End Date 

DECLARE @StartDate DATETIME = '01/01/1848' --Starting value of Date Range
DECLARE @EndDate DATETIME = '01/01/2023' --End Value of Date Range

--Temporary Variables To Hold the Values During Processing of Each Date of Year
DECLARE
	@DayOfWeekInMonth INT,
	@DayOfWeekInYear INT,
	@DayOfQuarter INT,
	@WeekOfMonth INT,
	@CurrentYear INT,
	@CurrentMonth INT,
	@CurrentQuarter INT

/*Table Data type to store the day of week count for the month and year*/
DECLARE @DayOfWeek TABLE (DOW INT, MonthCount INT, QuarterCount INT, YearCount INT)

INSERT INTO @DayOfWeek VALUES (1, 0, 0, 0)
INSERT INTO @DayOfWeek VALUES (2, 0, 0, 0)
INSERT INTO @DayOfWeek VALUES (3, 0, 0, 0)
INSERT INTO @DayOfWeek VALUES (4, 0, 0, 0)
INSERT INTO @DayOfWeek VALUES (5, 0, 0, 0)
INSERT INTO @DayOfWeek VALUES (6, 0, 0, 0)
INSERT INTO @DayOfWeek VALUES (7, 0, 0, 0)

--Extract and assign various parts of Values from Current Date to Variable

DECLARE @CurrentDate AS DATETIME = @StartDate
SET @CurrentMonth = DATEPART(MM, @CurrentDate)
SET @CurrentYear = DATEPART(YY, @CurrentDate)
SET @CurrentQuarter = DATEPART(QQ, @CurrentDate)

/********************************************************************************************/
--Proceed only if Start Date(Current date ) is less than End date you specified above

WHILE @CurrentDate < @EndDate
BEGIN
 
/*Begin day of week logic*/

         /*Check for Change in Month of the Current date if Month changed then 
          Change variable value*/
	IF @CurrentMonth != DATEPART(MM, @CurrentDate) 
	BEGIN
		UPDATE @DayOfWeek
		SET MonthCount = 0
		SET @CurrentMonth = DATEPART(MM, @CurrentDate)
	END

        /* Check for Change in Quarter of the Current date if Quarter changed then change 
         Variable value*/

	IF @CurrentQuarter != DATEPART(QQ, @CurrentDate)
	BEGIN
		UPDATE @DayOfWeek
		SET QuarterCount = 0
		SET @CurrentQuarter = DATEPART(QQ, @CurrentDate)
	END
       
        /* Check for Change in Year of the Current date if Year changed then change 
         Variable value*/
	

	IF @CurrentYear != DATEPART(YY, @CurrentDate)
	BEGIN
		UPDATE @DayOfWeek
		SET YearCount = 0
		SET @CurrentYear = DATEPART(YY, @CurrentDate)
	END
	
        -- Set values in table data type created above from variables 

	UPDATE @DayOfWeek
	SET 
		MonthCount = MonthCount + 1,
		QuarterCount = QuarterCount + 1,
		YearCount = YearCount + 1
	WHERE DOW = DATEPART(DW, @CurrentDate)

	SELECT
		@DayOfWeekInMonth = MonthCount,
		@DayOfQuarter = QuarterCount,
		@DayOfWeekInYear = YearCount
	FROM @DayOfWeek
	WHERE DOW = DATEPART(DW, @CurrentDate)
	
/*End day of week logic*/


/* Populate Your Dimension Table with values*/
	
	INSERT INTO [dbo].[Date]
	SELECT
		
		CONVERT (char(8),@CurrentDate,112) as DateKey,
		@CurrentDate AS Date,
		CONVERT (char(10),@CurrentDate,101) as FullDate,
		DATEPART(DD, @CurrentDate) AS DayOfMonth,
		DATENAME(DW, @CurrentDate) AS DayName,
		DATEPART(DW, @CurrentDate) AS DayOfWeek,
		@DayOfWeekInMonth AS DayOfWeekInMonth,
		@DayOfWeekInYear AS DayOfWeekInYear,
		@DayOfQuarter AS DayOfQuarter,
		DATEPART(DY, @CurrentDate) AS DayOfYear,
		DATEPART(WW, @CurrentDate) + 1 - DATEPART(WW, CONVERT(VARCHAR, 
		DATEPART(MM, @CurrentDate)) + '/1/' + CONVERT(VARCHAR,
		DATEPART(YY, @CurrentDate))) AS WeekOfMonth,
		(DATEDIFF(DD, DATEADD(QQ, DATEDIFF(QQ, 0, @CurrentDate), 0), 
		@CurrentDate) / 7) + 1 AS WeekOfQuarter,
		DATEPART(WW, @CurrentDate) AS WeekOfYear,
		DATEPART(MM, @CurrentDate) AS Month,
		DATENAME(MM, @CurrentDate) AS MonthName,
		CASE
			WHEN DATEPART(MM, @CurrentDate) IN (1, 4, 7, 10) THEN 1
			WHEN DATEPART(MM, @CurrentDate) IN (2, 5, 8, 11) THEN 2
			WHEN DATEPART(MM, @CurrentDate) IN (3, 6, 9, 12) THEN 3
			END AS MonthOfQuarter,
		DATEPART(QQ, @CurrentDate) AS Quarter,
		CASE DATEPART(QQ, @CurrentDate)
			WHEN 1 THEN 'First'
			WHEN 2 THEN 'Second'
			WHEN 3 THEN 'Third'
			WHEN 4 THEN 'Fourth'
			END AS QuarterName,
		DATEPART(YEAR, @CurrentDate) AS Year,
		'CY ' + CONVERT(VARCHAR, DATEPART(YEAR, @CurrentDate)) AS YearName,
		LEFT(DATENAME(MM, @CurrentDate), 3) + '-' + CONVERT(VARCHAR, 
		DATEPART(YY, @CurrentDate)) AS MonthYear,
		RIGHT('0' + CONVERT(VARCHAR, DATEPART(MM, @CurrentDate)),2) + 
		CONVERT(VARCHAR, DATEPART(YY, @CurrentDate)) AS MMYYYY,
		CONVERT(DATETIME, CONVERT(DATE, DATEADD(DD, - (DATEPART(DD, 
		@CurrentDate) - 1), @CurrentDate))) AS FirstDayOfMonth,
		CONVERT(DATETIME, CONVERT(DATE, DATEADD(DD, - (DATEPART(DD,
		(DATEADD(MM, 1, @CurrentDate)))), DATEADD(MM, 1,
		@CurrentDate)))) AS LastDayOfMonth,
		DATEADD(QQ, DATEDIFF(QQ, 0, @CurrentDate), 0) AS FirstDayOfQuarter,
		DATEADD(QQ, DATEDIFF(QQ, -1, @CurrentDate), -1) AS LastDayOfQuarter,
		CONVERT(DATETIME, '01/01/' + CONVERT(VARCHAR, DATEPART(YY, 
		@CurrentDate))) AS FirstDayOfYear,
		CONVERT(DATETIME, '12/31/' + CONVERT(VARCHAR, DATEPART(YY, 
		@CurrentDate))) AS LastDayOfYear

	SET @CurrentDate = DATEADD(DD, 1, @CurrentDate)
END

/********************************************************************************************/
go 
SELECT * FROM [dbo].[Date]



SELECT * FROM School

select * from Suburb

select * from Station

select * from Property



