USE [Perfcol]
GO

if exists (Select * from sys.tables where name = 'Auswertung')
Drop Table [Auswertung]
Go



CREATE TABLE [dbo].[Auswertung](
[SqlSystem] [varchar](255) NULL,
[snapshot_time_stamp] [int] NULL,
[snapshot_time] [datetime] NULL,
[avg_CPU] [float] NULL,
[instance_name] [varchar](256) NULL
) ON [PRIMARY]

GO


if exists (Select * from sys.tables where name = 'AuswertungTemp')
Drop Table [AuswertungTemp]
Go


CREATE TABLE [dbo].[AuswertungTemp](
[SqlSystem] [varchar](255) NULL,
[snapshot_time_stamp] [int] NULL,
[snapshot_time] [datetime] NULL,
[avg_CPU] [float] NULL
) ON [PRIMARY]

--##load auswertungsdaten...

declare @snapshot_time_id int
declare @instance_name varchar(256) 
declare @snapshottime datetime2
declare  c cursor for 
Select Distinct instance_name from core.snapshots

open c
fetch next from c into  @instance_name
while @@fetch_status = 0
begin

truncate table AuswertungTemp

Insert Into AuswertungTemp
EXEC[snapshots].[rpt_cpu_usage]
@instance_name = @instance_name


Insert Into Auswertung Select *,@instance_name from AuswertungTemp

fetch next from c into   @instance_name
end 
close c
deallocate c

go

Create Table LogicalCores ([column 0] varchar(255), [column 1] int)

Insert Into LogicalCores values ('vie-lukast10', 4),('vie-lukast10\SQL2017', 4)

Select * from logicalCores
Select * from MonteCarloSimulation
Select * from CPUCumulativeMaterialized


if exists (Select * from sys.objects where name = 'CPUTime')
Drop View CPUTime
GO
Create View CPUTime
as
Select *,avg_CPU/100*CAST([Column 1] as int) as  CoresUsed  from Auswertung a join LogicalCores l on a.instance_name = UPPER(l.[Column 0])

GO


if exists (Select * from sys.objects where name = 'Cores')
Drop View Cores
GO
Create View Cores
as
Select  0 as Core UNION Select 1 UNION Select 2 UNION Select 3 UNION Select 4 UNION Select 5
 UNION Select 6 UNION Select 7 UNION Select 8 UNION Select 9 UNION Select 10 
 UNION Select 11 UNION Select 12 UNION Select 13 UNION Select 14 UNION Select 15
 UNION Select 16 UNION Select 17 UNION Select 18 UNION Select 19 UNION Select 20 
 UNION Select 21 UNION Select 22 UNION Select 23 UNION Select 24

GO


if exists (Select * from sys.objects where name = 'CPUDensity')
Drop View CPUDensity
GO
Create View CPUDensity
as
Select instance_name,ROUND(CoresUsed,0) as CoresUsed,COUNT(*) as Frequency from CPUTime 
where SqlSystem = 'SQL Server'
group by instance_name, ROUND(CoresUsed,0)
UNION 
Select *,0 from (
Select Distinct instance_name,Core from CPUTime cross Join Cores
Except 
Select instance_name,ROUND(CoresUsed,0) as CoresUsed from CPUTime 
where SqlSystem = 'SQL Server'
group by instance_name, ROUND(CoresUsed,0))x
GO

if exists (Select * from sys.objects where name = 'CPUDensityPercent')
Drop View CPUDensityPercent
GO
Create View CPUDensityPercent
as
Select instance_name,CoresUsed,Frequency, Frequency*1.0/SUM(Frequency) OVER (PARTITION BY instance_name) as Percentage
from CPUDensity
GO

Select * from CPUDensity

if exists (Select * from sys.objects where name = 'CPUCumulative')
Drop View CPUCumulative
GO
Create View CPUCumulative
as
Select *,SUM(Percentage)  OVER (PARTITION BY instance_name order by coresUsed)as CumulativeProb  from CPUDensityPercent
go

if exists (Select * from sys.tables where name = 'CPUCumulativeMaterialized')
Drop Table CPUCumulativeMaterialized
Go


Select * into CPUCumulativeMaterialized  from CPUCumulative


if exists (Select * from sys.objects where name = 'getSimulatedLogicalCPUUtilization')
Drop Function getSimulatedLogicalCPUUtilization


go
Create Function getSimulatedLogicalCPUUtilization(@instance varchar(255), @r float)
 returns int
AS
begin
declare @coresused int
Select @coresused = c1.CoresUsed
 from CPUCumulativeMaterialized c1 left 
 join CPUCumulativeMaterialized c2 on c1.instance_name = c2.instance_name and c1.coresused = c2.CoresUsed  +1
where c1.instance_name = @instance
and @r between Coalesce(c2.CumulativeProb,0) and c1.CumulativeProb
RETURN @coresused
end

go


if exists (Select * from sys.objects where name = 'MonteCarloSimulation')
Drop Table MonteCarloSimulation
GO
Create Table MonteCarloSimulation (ID int primary key identity, SimulationID int, Instance varchar(255),CPUCount int)

GO

declare @simulation int 
set @simulation = 0
while @simulation < 10000
begin

Insert Into MonteCarloSimulation
Select @simulation,[Column 0],COALESCE(dbo.getSimulatedLogicalCPUUtilization(UPPER([Column 0]),RAND(CHECKSUM(NEWID())) ),0)
from LogicalCores 
set @simulation += 1
end

GO
if exists (Select * from sys.objects where name = 'SimulationResultDetailed')
Drop View SimulationResultDetailed
go
Create View SimulationResultDetailed
as
Select Top 100000000 SimulationID,SUM(CPUCount) as 'CPUCount' from MonteCarloSimulation group by SimulationID
order by SimulationID
go
if exists (Select * from sys.objects where name = 'SimulationResultDetailed_50PercentShared')
Drop View SimulationResultDetailed_50PercentShared
go
Create View SimulationResultDetailed_50PercentShared
as
Select SimulationID,SUM(CPUCount*0.5) as 'CPUCount' from MonteCarloSimulation group by SimulationID

GO


--Returns worst case AVG and Maximum CPU Requirement
Select AVG(CPUCount) DurchschnittlicherCPUCount,MAX(CPUCount) from SimulationResultDetailed
--Returns AVG and Maximum CPU Requirement if 50% of the workload from each Machine is added together.
Select AVG(CPUCount) DurchschnittlicherCPUCount,MAX(CPUCount) from SimulationResultDetailed_50PercentShared

--Returns detailed information:
Select * from SimulationResultDetailed
--Appendix
--Test the cummulative Probability Function
--Parameter 1) Instanz, Parameter 2) gleichverteilter Wert zwischen 0 und 1  
--Returnwert: Je nach zufällig gewähltem Wert 
 Select dbo.getSimulatedLogicalCPUUtilization('vie-lukast10',0.001)
 Select dbo.getSimulatedLogicalCPUUtilization('vie-lukast10\SQL2017',0.01)



 Select * from LogicalCores

