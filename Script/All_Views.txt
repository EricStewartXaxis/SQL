 
-
SalesForceModel
text
----
 
-
XaxisETL
text
----

CREATE VIEW [dbo].[Beligum_Company__c] AS
SELECT DISTINCT dbo.ReplaceExtraChars(LTRIM(RTRIM(eb.[F3]))) AS 'Name'
	  ,'Agency' AS Type__c
	  ,'Active' AS Status__c
	  ,'Belgium' AS Market__c
	  ,sf.Id
FROM XaxisETL.[dbo].[Extract_Belgium] eb
	LEFT JOIN (SELECT DISTINCT NAME, Id
			  FROM GlobalQA.dbo.Company__c 
			  WHERE type__c = 'Agency'
			  AND Market__c LIKE '%Belgium%') sf
	ON  sf.NAME LIKE '%'+dbo.ReplaceExtraChars(LTRIM(RTRIM(eb.[F3])))+'%'
WHERE eb.[F3] IS NOT NULL
	AND eb.[F5] IS NOT NULL
	AND dbo.ReplaceExtraChars(LTRIM(RTRIM(eb.[F3]))) <> ''
	AND eb.id > 8

UNION ALL

--Find new Advertiser

SELECT DISTINCT dbo.ReplaceExtraChars(LTRIM(RTRIM(eb.[F5]))) AS 'Name'
	,'Advertiser' AS Type__c
	,'Active' AS Status__c
	,NULL AS Market__c
	,sf.Id
FROM XaxisETL.[dbo].[Extract_Belgium] eb
	LEFT JOIN (SELECT DISTINCT NAME, Id
			  FROM GlobalQA.dbo.Company__c
			  WHERE type__c = 'Advertiser') sf
	ON  dbo.ReplaceExtraChars(LTRIM(RTRIM(eb.[F5]))) = sf.NAME
WHERE eb.[F3] IS NOT NULL
	AND eb.[F5] IS NOT NULL
	AND dbo.ReplaceExtraChars(LTRIM(RTRIM(eb.[F5]))) <> ''
	AND eb.id > 8
--	AND sf.NAME IS NULL





 
-
Nordics
text
----

CREATE VIEW [dbo].[Sell_Lines_Booking] AS
SELECT DISTINCT op.Id
	  ,LTRIM(RTRIM('NordicOPP - ' + sl.[Client Name] + ' - ' + sl.Brand + ' ('+
                                        CASE WHEN sl.[Client Country] = 'DK' Then 'Denmark'
										     WHEN sl.[Client Country] = 'NO' Then 'Norway'
											 WHEN sl.[Client Country] = 'SE' Then 'Sweden'
											 ELSE 'Nordic' END +')' + ' - ' + sl.CampaignName))
				 AS MathcingMatch
	  ,sl.OrderId
	  ,sl.PlacementName
	  ,sl.StartDate
	  ,sl.EndDate
	  ,sl.BudgetNet
	  ,sl.Unit
	  ,sl.Comment
	  ,sl.MediaName
	  ,sl.[Client Name]
	  ,op.Agency_Market__c
	  ,re.Id AS RecordTypeID
	  ,sl.Volume

  FROM [Nordics].[dbo].[SQL_Sell_Lines] sl
	LEFT JOIN (SELECT REPLACE(cc.Name, ',', ' ') AS Name
					,cc.id
			   FROM Nordics.dbo.Company__c cc
			   WHERE cc.Type__c = 'Advertiser') adv
		ON sl.[Client Name] = adv.Name

	LEFT JOIN (SELECT REPLACE(cc.Name, ',', ' ') AS Name
					,cc.id
			   FROM Nordics.dbo.Company__c cc
			   WHERE cc.Type__c = 'Agency') age
		ON sl.brand + ' ('+CASE WHEN [Client Country] = 'DK' Then 'Denmark'
							    WHEN [Client Country] = 'NO' Then 'Norway'
								WHEN [Client Country] = 'SE' Then 'Sweden'
								ELSE 'Nordic' END +')' = age.Name
	
	LEFT JOIN Nordics.dbo.Account acct
		ON adv.id = acct.Advertiser__c
	   AND age.Id = acct.Agency__c

	LEFT JOIN Nordics.dbo.Opportunity op
		ON LTRIM(RTRIM(LEFT( 'NordicOPP - ' + sl.[Client Name] + ' - ' + sl.Brand + ' ('+
                                        CASE WHEN sl.[Client Country] = 'DK' Then 'Denmark'
										     WHEN sl.[Client Country] = 'NO' Then 'Norway'
											 WHEN sl.[Client Country] = 'SE' Then 'Sweden'
											 ELSE 'Nordic' END +')' + ' - ' + sl.CampaignName
				,LEN(RTRIM(LTRIM(op.Name)))))) = LTRIM(RTRIM(op.Name))
		AND adv.Id = op.Advertiser__c
		AND age.Id = op.Agency__c
		AND acct.id = op.AccountId
	LEFT JOIN(
				SELECT DeveloperName
									,Id
							FROM Nordics.dbo.RecordType
							WHERE SobjectType = 'Opportunity'
							AND DeveloperName IN ('Denmark', 'Norway' ,'Sweden')
							) re
					ON op.Agency_Market__c = re.DeveloperName


CREATE VIEW [dbo].[Sell_Lines_Order] AS

SELECT DISTINCT tt.Id AS Opportunity__c
	  ,'OrderID - ' + CAST(tt.OrderID AS VARCHAR) AS External_ID__c
	  ,tt.PlacementName AS Product_Detail__c
	  ,mm.StartDate AS Start_Date__c
	  ,ma.EndDate AS End_Date__c
	  ,su.Gross AS Gross_Cost__c
--	  ,tt.Unit AS Planned_Units__c
--	  ,co.Comment AS Comments__c
--	  ,tt.PlacementName AS Placement_Name__c
	  ,tt.MediaName AS Buy_Name_txt__c
--	  ,tt.Unit AS Deliver_Final_Billable_Units__c
	  ,'Net Cost (Calc Margin)' AS Imputing_Margin_or_Net__c
--	  ,su.Gross AS Planned_Cost__c
	  ,'MediaTrader' AS PackageType__c
	  ,tt.RecordTypeID
	  ,tt.[Client Name] AS Supplier_Name__c
	  ,CASE WHEN su.Volume = 0 THEN 100 ELSE su.Volume END AS Buy_Volume__c
	  ,ISNULL(su.Gross, 0) AS Media_Net_Cost__c
	  ,'Sell Volume / Gross Cost' AS Input_Mode__c
	  ,'Triggers' AS Audience_Tier__c
FROM Nordics.dbo.Sell_Lines_Booking tt
	INNER JOIN (SELECT OrderID
					  ,MIN(StartDate) AS StartDate
				FROM Nordics.dbo.Sell_Lines_Booking
				GROUP BY OrderID) mm
		ON tt.orderId = mm.OrderID
	INNER JOIN (SELECT OrderID
					  ,MAX(StartDate) AS EndDate
				FROM Nordics.dbo.Sell_Lines_Booking
				GROUP BY OrderID) ma
		ON tt.orderId = ma.OrderID
	INNER JOIN (SELECT OrderID
					  ,SUM(ISNULL(BudgetNet, 0)) AS Gross
					  ,SUM(ISNULL(Volume, 0)) AS Volume
				FROM Nordics.dbo.Sell_Lines_Booking
				GROUP BY OrderID) su
		ON tt.orderId = su.OrderID

	INNER JOIN (SELECT DISTINCT OrderId
						,Comment
						,ROW_NUMBER() OVER (PARTITION BY OrderId ORDER BY Len(Comment) DESC) AS max_len
				FROM Nordics.dbo.Sell_Lines_Booking
				) co
		ON tt.orderId = co.OrderID
		AND max_len = 1
--WHERE tt.id IS NOT NULL



 
-
GlobalQA
text
----

CREATE VIEW [dbo].[Beligum_Accounts] AS


WITH w_Account AS (
SELECT DISTINCT F5 +' - '+ F3 +' (Belgium)' AS Name
	  ,acct.Id
	  ,acct.Agency__c AS Agency__c
	  ,F3 AS Agency_Name
	  ,acct.Advertiser__c AS Advertiser__c
	  ,F5 AS Advertiser_Name
FROM XaxisETL.[dbo].[Extract_Belgium] eb
	LEFT JOIN (SELECT acct.Id
					  ,acct.Advertiser__c
					  ,acct.Agency__c
					  ,sf_ag.NAME AS Agency_Name
					  ,sf_ad.NAME AS Advertiser_Name
				FROM Account acct
					INNER JOIN (SELECT ag.NAME
									 ,ag.Id
							   FROM XaxisETL.dbo.Beligum_Company__c ag
							   WHERE ag.type__c = 'Agency'
							   AND Market__c LIKE '%Belgium%') sf_ag
						ON acct.Agency__c = sf_ag.Id
					INNER JOIN (SELECT ad.NAME
									  ,ad.Id
								FROM XaxisETL.dbo.Beligum_Company__c ad
								WHERE ad.type__c = 'Advertiser') sf_ad
						ON acct.Advertiser__c = sf_ad.Id
				WHERE acct.Advertiser__c IS NOT NULL
				  AND acct.Agency__c IS NOT NULL
				  AND sf_ag.NAME IS NOT NULL
				  AND sf_ad.NAME IS NOT NULL
				  ) acct
	ON acct.Advertiser_Name = eb.F5
   AND acct.Agency_Name = eb.F3
WHERE eb.id > 8
 -- AND acct.id IS NULL
  AND [F3] IS NOT NULL
  AND [F5] IS NOT NULL
  AND LTRIM(RTRIM([F3])) <> ''
  AND LTRIM(RTRIM([F5])) <> ''
  
)
, Adv_Count AS (
	SELECT bc.id
		  ,COUNT(ba.Id) AS a_count
	FROM XaxisETL.dbo.Beligum_Company__c bc
		INNER JOIN GlobalQA.dbo.Account ba
			ON bc.Id = ba.Advertiser__c
	WHERE bc.Type__c = 'Advertiser'
	GROUP BY bc.Id)
, Adv_Created AS (
	SELECT bc.id
		  ,bc.CreatedDate
	FROM Company__c bc
	WHERE bc.Type__c = 'Advertiser')
,  Ag_Created AS (
	SELECT bc.id
		  ,bc.CreatedDate
	FROM Company__c bc
	WHERE bc.Type__c = 'Agency')
, Acct_Count AS (
	SELECT AccountId
		  ,COUNT(Id) AS account_count
	FROM GlobalQA.dbo.Opportunity
	GROUP BY AccountId
	)
, with_rank AS (
	SELECT DISTINCT wa.Name
		  ,wa.Id
		  ,sf_ad.id AS Advertiser__c
		  ,sf_ag.id AS Agency__c
		  ,'Xaxis' AS Business_Unit__c
		  ,'Signed' AS Account_Opt_In_Status__c
		  ,(SELECT TOP 1 [Id] FROM RecordType WHERE SobjectType = 'Account' AND DeveloperName = 'Xaxis_Media_Buying_EMEA') AS RecordTypeId
		  ,DENSE_RANK() OVER (PARTITION BY wa.Name ORDER BY Adv_Count.a_count, Adv_Created.CreatedDate, Ag_Created.CreatedDate, Acct_Count.account_count DESC, wa.Id) AS Ranky
		  ,Adv_Count.a_count, Adv_Created.CreatedDate AS adv_crd, Ag_Created.CreatedDate, Acct_Count.account_count
	FROM w_Account wa
		INNER JOIN(
				   SELECT ag.NAME
						 ,ag.Id
				   FROM XaxisETL.dbo.Beligum_Company__c ag
				   WHERE ag.type__c = 'Agency'
					 AND ag.Market__c LIKE '%Belgium%'
					 ) sf_ag
			ON wa.Agency_Name = sf_ag.Name
		   AND wa.Agency__c = sf_ag.Id
		INNER JOIN( 
				   SELECT ad.NAME
						,ad.Id
				   FROM XaxisETL.dbo.Beligum_Company__c ad
				   WHERE ad.type__c = 'Advertiser') sf_ad
			ON wa.Advertiser_Name = sf_ad.Name
		   AND wa.Advertiser__c = sf_ad.Id
		INNER JOIN Adv_Count
			ON Adv_Count.Id = sf_ad.id
		INNER JOIN Adv_Created
			ON Adv_Created.Id = sf_ad.id
		INNER JOIN Ag_Created
			ON Ag_Created.Id = sf_ag.id
		LEFT JOIN Acct_Count
			ON Acct_Count.AccountId = wa.Id

--	WHERE wa.Agency__c IS NULL
--	OR wa.Advertiser__c IS NULL
	)

SELECT Name
	  ,Id
	  ,Advertiser__c
	  ,Agency__c
	  ,Business_Unit__c
	  ,Account_Opt_In_Status__c
	  ,RecordTypeId
FROM with_rank
WHERE Ranky = 1



CREATE VIEW [dbo].[Beligum_Opp] AS
SELECT DISTINCT CASE WHEN F17 IS NULL THEN 'Speculative'
					 WHEN F17 LIKE 'Signed' THEN 'Contract Pending'
					 WHEN F17 LIKE 'Signed+Matos' THEN 'Closed Won'
--					 WHEN F17 LIKE 'Not Agreed' THEN 'Closed Lost'
					 ELSE 'Contacted / Prospecting'
				END AS StageName
--	  ,CONVERT(VARCHAR(8), CONVERT(DATE,F8,3),3) AS CloseDate
	  ,CONVERT(DATE,F8,3) AS CloseDate
	  ,acct.Advertiser__c AS Advertiser__c
	  ,acct.Agency__c AS Agency__c
	  ,(SELECT TOP 1 [Id] FROM [RecordType] WHERE SobjectType = 'Opportunity' AND DeveloperName = 'Belgium') AS RecordTypeId
	  ,acct.Id AS AccountId
	  ,'BelgiumOpp-'+F2 AS External_Id__c
	  ,dbo.ReplaceExtraChars(LTRIM(RTRIM([F6]))) AS 'Name'
	  ,sf_opp.Id
FROM XaxisETL.[dbo].[Extract_Belgium] eb
	LEFT JOIN Beligum_Accounts acct
		LEFT JOIN Company__c adv
			ON acct.Advertiser__c = adv.Id
		LEFT JOIN Company__c ag
			ON acct.Agency__c = ag.Id
		ON ag.Name LIKE dbo.ReplaceExtraChars(eb.F3) + '%(Belgium)%'
	    AND dbo.ReplaceExtraChars(eb.F5) = adv.Name
	LEFT JOIN Opportunity sf_opp
		ON  dbo.ReplaceExtraChars(LTRIM(RTRIM(eb.[F6]))) = sf_opp.Name
		  OR 'NewBeligumOpp-' +dbo.ReplaceExtraChars(LTRIM(RTRIM(eb.[F6]))) = 'NewBeligumOpp-' +sf_opp.Name
	   AND sf_opp.AccountId = acct.Id
WHERE eb.id > 8
  AND [F8] IS NOT NULL
  AND dbo.ReplaceExtraChars(LTRIM(RTRIM([F8]))) <> ''
  


CREATE VIEW Beligum_Sell_Lines_PreSplit AS
		SELECT bo.Id AS Opportunity__c
			  ,'BelgiumOpp-'+F2 AS External_ID__c
			  ,eb.F10 AS Product_Detail__c
			  ,CAST(CONVERT(DATE,eb.F7,3) AS DATE) AS Start_Date__c
			  ,CAST(CONVERT(DATE,eb.F8,3) AS DATE) AS End_Date__c
--			  ,CONVERT(MONEY, REPLACE(eb.F24, ',','')) AS Gross_Cost__c
			  ,dbo.ReplaceExtraChars(eb.F6) AS Buy_Name_txt__c
			  ,'Net Cost (Calc Margin)' AS Imputing_Margin_or_Net__c
			  ,'MediaTrader' AS	PackageType__c
			  ,(SELECT TOP 1 Id FROM GLobalQA.dbo.RecordType WHERE SobjectType = 'Opportunity_Buy__c' AND DeveloperName = 'Belgium') AS RecordTypeId
			  ,eb.F4 AS Supplier_Name__c
			  ,CONVERT(MONEY, REPLACE(eb.F21, ',','')) AS Buy_Volume__c_FR
			  ,CONVERT(MONEY, REPLACE(eb.F22, ',','')) AS Buy_Volume__c_NL
			  ,CONVERT(MONEY, REPLACE(eb.F25, ',','')) AS Gross_Cost__c_FR
			  ,CONVERT(MONEY, REPLACE(eb.F26, ',','')) AS Gross_Cost__c_NL
			  ,CONVERT(MONEY, REPLACE(eb.F23, ',','')) AS Rate__c
			  ,CASE WHEN LTRIM(RTRIM(eb.F28)) = '' THEN NULL ELSE LTRIM(RTRIM(eb.F28)) END AS Target_Gender__c
			  ,CASE WHEN LTRIM(RTRIM(eb.F29)) = '' THEN NULL ELSE LTRIM(RTRIM(eb.F29)) END AS Target_Age__c
			  ,LTRIM(RTRIM(CONCAT(ISNULL(eb.F29, ''), ' ', ISNULL(eb.F30, ''), ' ', ISNULL(eb.F31, ''), ' ', ISNULL(eb.F32, ''), ' ', ISNULL(eb.F38, ''))))  AS Opp_Buy_Description__c
			  ,'Externally Managed' AS Input_Mode__c
			  ,'Triggers' AS Audience_Tier__c
			  ,EB.F18 AS FR_line
			  ,EB.F19 AS NL_line
		  FROM [GlobalQA].[dbo].[Beligum_Opp] bo
			INNER JOIN XaxisETL.dbo.Extract_Belgium eb
				ON REPLACE( External_Id__c, 'BelgiumOpp-', '') = eb.F2
		  WHERE eb.id > 8
		  AND [F8] IS NOT NULL
		  AND dbo.ReplaceExtraChars(LTRIM(RTRIM([F8]))) <> ''


CREATE VIEW [dbo].[Beligum_Sell_Lines] AS

SELECT Opportunity__c
	  ,External_ID__c
      ,Product_Detail__c
      ,Start_Date__c
      ,End_Date__c
 --     ,Gross_Cost__c
      ,Buy_Name_txt__c
      ,Imputing_Margin_or_Net__c
      ,PackageType__c
      ,RecordTypeId
      ,Supplier_Name__c
      ,Buy_Volume__c_FR AS Buy_Volume__c
      ,Gross_Cost__c_FR AS Gross_Cost__c
      ,Rate__c
      ,Target_Gender__c
      ,Target_Age__c
      ,Opp_Buy_Description__c
      ,Input_Mode__c
      ,Audience_Tier__c
FROM GlobalQA.dbo.Beligum_Sell_Lines_PreSplit
WHERE FR_line IS NOT NULL
   OR LTRIM(RTRIM(FR_line)) <> ''
UNION ALL
SELECT Opportunity__c
	  ,External_ID__c
      ,Product_Detail__c
      ,Start_Date__c
      ,End_Date__c
--      ,Gross_Cost__c
      ,Buy_Name_txt__c
      ,Imputing_Margin_or_Net__c
      ,PackageType__c
      ,RecordTypeId
      ,Supplier_Name__c
      ,Buy_Volume__c_NL AS Buy_Volume__c
      ,Gross_Cost__c_NL AS Gross_Cost__c
      ,Rate__c
      ,Target_Gender__c
      ,Target_Age__c
      ,Opp_Buy_Description__c
      ,Input_Mode__c
      ,Audience_Tier__c
FROM GlobalQA.dbo.Beligum_Sell_Lines_PreSplit
WHERE FR_line IS NOT NULL
   OR LTRIM(RTRIM(NL_line)) <> ''



 
-
GLStaging
text
----


CREATE VIEW [dbo].[Beligum_Company] AS
SELECT DISTINCT dbo.ReplaceExtraChars(LTRIM(RTRIM(eb.[F3]))) AS 'Name'
	  ,'Agency' AS Type__c
	  ,'Active' AS Status__c
	  ,'Belgium' AS Market__c
	  ,sf.Id
FROM XaxisETL.[dbo].[Extract_Belgium] eb
	LEFT JOIN (SELECT DISTINCT NAME, Id
			  FROM GLStaging.dbo.Company__c 
			  WHERE type__c = 'Agency'
			  AND Market__c LIKE '%Belgium%') sf
	ON  sf.NAME LIKE '%'+dbo.ReplaceExtraChars(LTRIM(RTRIM(eb.[F3])))+'%'
WHERE eb.[F3] IS NOT NULL
	AND eb.[F5] IS NOT NULL
	AND dbo.ReplaceExtraChars(LTRIM(RTRIM(eb.[F3]))) <> ''
	AND eb.id > 8

UNION ALL

--Find new Advertiser

SELECT DISTINCT dbo.ReplaceExtraChars(LTRIM(RTRIM(eb.[F5]))) AS 'Name'
	,'Advertiser' AS Type__c
	,'Active' AS Status__c
	,NULL AS Market__c
	,sf.Id
FROM XaxisETL.[dbo].[Extract_Belgium] eb
	LEFT JOIN (SELECT DISTINCT NAME, Id
			  FROM GLStaging.dbo.Company__c
			  WHERE type__c = 'Advertiser') sf
	ON  dbo.ReplaceExtraChars(LTRIM(RTRIM(eb.[F5]))) = sf.NAME
WHERE eb.[F3] IS NOT NULL
	AND eb.[F5] IS NOT NULL
	AND dbo.ReplaceExtraChars(LTRIM(RTRIM(eb.[F5]))) <> ''
	AND eb.id > 8
--	AND sf.NAME IS NULL








CREATE VIEW [dbo].[BeligumNew_Accounts] AS

WITH w_Account AS (
SELECT DISTINCT F5 +' - '+ F3 +' (Belgium)' AS Name
	  ,acct.Agency__c AS Agency__c
	  ,F3 AS Agency_Name
	  ,acct.Advertiser__c AS Advertiser__c
	  ,F5 AS Advertiser_Name
FROM XaxisETL.[dbo].[Extract_Belgium] eb
	LEFT JOIN (SELECT acct.Id
					  ,acct.Advertiser__c
					  ,acct.Agency__c
					  ,sf_ag.NAME AS Agency_Name
					  ,sf_ad.NAME AS Advertiser_Name
				FROM Account acct
					INNER JOIN (SELECT ag.NAME
									 ,ag.Id
							   FROM Beligum_Company ag
							   WHERE ag.type__c = 'Agency'
							   AND Market__c LIKE '%Belgium%') sf_ag
						ON acct.Agency__c = sf_ag.Id
					INNER JOIN (SELECT ad.NAME
									  ,ad.Id
								FROM Beligum_Company ad
								WHERE ad.type__c = 'Advertiser') sf_ad
						ON acct.Advertiser__c = sf_ad.Id
				WHERE acct.Advertiser__c IS NOT NULL
				  AND acct.Agency__c IS NOT NULL
				  AND sf_ag.NAME IS NOT NULL
				  AND sf_ad.NAME IS NOT NULL
				  ) acct
	ON acct.Advertiser_Name = eb.F5
   AND acct.Agency_Name = eb.F3
WHERE eb.id > 8
 -- AND acct.id IS NULL
  AND [F3] IS NOT NULL
  AND [F5] IS NOT NULL
  AND LTRIM(RTRIM([F3])) <> ''
  AND LTRIM(RTRIM([F5])) <> ''
  
)
, Adv_Count AS (
	SELECT bc.id
		  ,ISNULL(COUNT(ba.Id), 0) AS a_count
	FROM Beligum_Company bc
		LEFT JOIN Account ba
			ON bc.Id = ba.Advertiser__c
	WHERE bc.Type__c = 'Advertiser'
	GROUP BY bc.Id)
, Adv_Created AS (
	SELECT bc.id
		  ,bc.CreatedDate
	FROM Company__c bc
	WHERE bc.Type__c = 'Advertiser')
,  Ag_Created AS (
	SELECT bc.id
		  ,bc.CreatedDate
	FROM Company__c bc
	WHERE bc.Type__c = 'Agency')
, with_rank AS (
	SELECT wa.Name
		  ,sf_ad.id AS Advertiser__c
		  ,sf_ag.id AS Agency__c
		  ,'Xaxis' AS Business_Unit__c
		  ,'Signed' AS Account_Opt_In_Status__c
		  ,(SELECT TOP 1 [Id] FROM RecordType WHERE SobjectType = 'Account' AND DeveloperName = 'Xaxis_Media_Buying_EMEA') AS RecordTypeId
		  ,DENSE_RANK() OVER (PARTITION BY wa.Name ORDER BY Adv_Count.a_count, Adv_Created.CreatedDate, Ag_Created.CreatedDate) AS Ranky
	FROM w_Account wa
		INNER JOIN(
				   SELECT ag.NAME
						 ,ag.Id
				   FROM Beligum_Company ag
				   WHERE ag.type__c = 'Agency'
					 AND ag.Market__c LIKE '%Belgium%'
					 ) sf_ag
			ON wa.Agency_Name = sf_ag.Name
		INNER JOIN( 
				   SELECT ad.NAME
						,ad.Id
				   FROM Beligum_Company ad
				   WHERE ad.type__c = 'Advertiser') sf_ad
			ON wa.Advertiser_Name = sf_ad.Name
		INNER JOIN Adv_Count
			ON Adv_Count.Id = sf_ad.id
		INNER JOIN Adv_Created
			ON Adv_Created.Id = sf_ad.id
		INNER JOIN Ag_Created
			ON Ag_Created.Id = sf_ag.id

	WHERE wa.Agency__c IS NULL
	OR wa.Advertiser__c IS NULL
	)
SELECT Name
	  ,Advertiser__c
	  ,Agency__c
	  ,Business_Unit__c
	  ,Account_Opt_In_Status__c
	  ,RecordTypeId
FROM with_rank
WHERE Ranky = 1


CREATE VIEW [dbo].[BeligumNew_Opps] AS
SELECT [StageName]
      ,[CloseDate]
      ,[Advertiser__c]
      ,[Agency__c]
      ,[RecordTypeId]
      ,[AccountId]
      ,[External_Id__c]
      ,'NewBelgiumOpp-' + [Name] AS [Name]
      ,CASE WHEN StageName = 'Closed Lost' THEN'Campaign not running' ELSE NULL END AS Reason_Lost__c   
  FROM [Beligum_Opp]
WHERE id is null





CREATE VIEW [dbo].[Beligum_Opp] AS
SELECT DISTINCT CASE WHEN F17 IS NULL THEN 'Speculative'
					 WHEN F17 LIKE 'Signed' THEN 'Contract Pending'
					 WHEN F17 LIKE 'Signed+Matos' THEN 'Closed Won'
--					 WHEN F17 LIKE 'Not Agreed' THEN 'Closed Lost'
					 ELSE 'Contacted / Prospecting'
				END AS StageName
--	  ,CONVERT(VARCHAR(8), CONVERT(DATE,F8,3),3) AS CloseDate
	  ,CONVERT(DATE,F8,3) AS CloseDate
	  ,acct.Advertiser__c AS Advertiser__c
	  ,acct.Agency__c AS Agency__c
	  ,(SELECT TOP 1 [Id] FROM [RecordType] WHERE SobjectType = 'Opportunity' AND DeveloperName = 'Belgium') AS RecordTypeId
	  ,acct.Id AS AccountId
	  ,'BelgiumOpp-'+F2 AS External_Id__c
	  ,dbo.ReplaceExtraChars(LTRIM(RTRIM([F6]))) AS 'Name'
	  ,sf_opp.Id
FROM XaxisETL.[dbo].[Extract_Belgium] eb
	LEFT JOIN Beligum_Accounts acct
		LEFT JOIN Company__c adv
			ON acct.Advertiser__c = adv.Id
		LEFT JOIN Company__c ag
			ON acct.Agency__c = ag.Id
		ON ag.Name LIKE dbo.ReplaceExtraChars(eb.F3) + '%(Belgium)%'
	    AND dbo.ReplaceExtraChars(eb.F5) = adv.Name
	LEFT JOIN Opportunity sf_opp
		ON  dbo.ReplaceExtraChars(LTRIM(RTRIM(eb.[F6]))) = sf_opp.Name
		  OR 'NewBelgiumOpp-' +dbo.ReplaceExtraChars(LTRIM(RTRIM(eb.[F6]))) = sf_opp.Name
	   AND sf_opp.AccountId = acct.Id
WHERE eb.id > 8
  AND [F8] IS NOT NULL
  AND dbo.ReplaceExtraChars(LTRIM(RTRIM([F8]))) <> ''
  






CREATE VIEW [dbo].[Beligum_Sell_Lines_PreSplit] AS
		SELECT bo.Id AS Opportunity__c
			  ,'BelgiumOpp-'+F2 AS External_ID__c
			  ,dbo.ReplaceExtraChars(LTRIM(RTRIM(eb.F10))) AS Product_Detail__c
			  ,CAST(CONVERT(DATE,eb.F7,3) AS DATE) AS Start_Date__c
			  ,CAST(CONVERT(DATE,eb.F8,3) AS DATE) AS End_Date__c
--			  ,CONVERT(MONEY, REPLACE(eb.F24, ',','')) AS Gross_Cost__c
			  ,dbo.ReplaceExtraChars(eb.F6) AS Buy_Name_txt__c
			  ,'Net Cost (Calc Margin)' AS Imputing_Margin_or_Net__c
			  ,'MediaTrader' AS	PackageType__c
			  ,(SELECT TOP 1 Id FROM RecordType WHERE SobjectType = 'Opportunity_Buy__c' AND DeveloperName = 'Belgium') AS RecordTypeId
			  ,CASE WHEN dbo.ReplaceExtraChars(LTRIM(RTRIM(eb.F4))) = ''
				    THEN NULL
					ELSE dbo.ReplaceExtraChars(LTRIM(RTRIM(eb.F4)))
				END AS Supplier_Name__c
			  ,CONVERT(MONEY, REPLACE(eb.F21, ',','')) AS Buy_Volume__c_FR
			  ,CONVERT(MONEY, REPLACE(eb.F22, ',','')) AS Buy_Volume__c_NL
			  ,CONVERT(MONEY, REPLACE(eb.F25, ',','')) AS Gross_Cost__c_FR
			  ,CONVERT(MONEY, REPLACE(eb.F26, ',','')) AS Gross_Cost__c_NL
			  ,CONVERT(MONEY, REPLACE(eb.F23, ',','')) AS Rate__c
			  ,0 AS Media_Net_Cost__c
			  ,CASE WHEN LTRIM(RTRIM(eb.F28)) = '' THEN NULL ELSE LTRIM(RTRIM(eb.F28)) END AS Target_Gender__c
			  ,CASE WHEN LTRIM(RTRIM(eb.F29)) = '' THEN NULL ELSE LTRIM(RTRIM(eb.F29)) END AS Target_Age__c
			  ,CASE WHEN dbo.ReplaceExtraChars(LTRIM(RTRIM(CONCAT(ISNULL(eb.F34, ''), ' ', ISNULL(eb.F35, ''), ' ', ISNULL(eb.F36, ''), ' ', ISNULL(eb.F37, ''), ' ', ISNULL(eb.F43, ''))))) = ''
					THEN NULL
					ELSE dbo.ReplaceExtraChars(LTRIM(RTRIM(CONCAT(ISNULL(eb.F34, ''), ' ', ISNULL(eb.F35, ''), ' ', ISNULL(eb.F36, ''), ' ', ISNULL(eb.F37, ''), ' ', ISNULL(eb.F43, '')))))
			   END AS Opp_Buy_Description__c
			  ,'Externally Managed' AS Input_Mode__c
			  ,'Triggers' AS Audience_Tier__c
			  ,CASE WHEN dbo.ReplaceExtraChars(LTRIM(RTRIM(EB.F18))) = '' THEN NULL ELSE dbo.ReplaceExtraChars(LTRIM(RTRIM(EB.F18))) END AS FR_line
			  ,CASE WHEN dbo.ReplaceExtraChars(LTRIM(RTRIM(EB.F19))) = '' THEN NULL ELSE dbo.ReplaceExtraChars(LTRIM(RTRIM(EB.F19))) END AS NL_line
		  FROM Beligum_Opp bo
			INNER JOIN XaxisETL.dbo.Extract_Belgium eb
				ON REPLACE( External_Id__c, 'BelgiumOpp-', '') = eb.F2
		  WHERE eb.id > 8
		  AND [F8] IS NOT NULL
		  AND dbo.ReplaceExtraChars(LTRIM(RTRIM([F8]))) <> ''



CREATE VIEW [dbo].[Beligum_Sell_Lines] AS

SELECT Opportunity__c
	  ,External_ID__c + '-FR' AS External_ID__c
      ,Product_Detail__c
      ,Start_Date__c
      ,End_Date__c
 --     ,Gross_Cost__c
      ,Buy_Name_txt__c
      ,Imputing_Margin_or_Net__c
      ,PackageType__c
      ,RecordTypeId
      ,Supplier_Name__c
      ,Buy_Volume__c_FR AS Buy_Volume__c
      ,Gross_Cost__c_FR AS Gross_Cost__c
	  ,Media_Net_Cost__c
      ,Rate__c
      ,Target_Gender__c
      ,Target_Age__c
      ,Opp_Buy_Description__c
      ,Input_Mode__c
      ,Audience_Tier__c
FROM Beligum_Sell_Lines_PreSplit
WHERE FR_line IS NOT NULL
   OR LTRIM(RTRIM(FR_line)) <> ''
UNION ALL
SELECT Opportunity__c
	  ,External_ID__c + '-NL' AS External_ID__c
      ,Product_Detail__c
      ,Start_Date__c
      ,End_Date__c
--      ,Gross_Cost__c
      ,Buy_Name_txt__c
      ,Imputing_Margin_or_Net__c
      ,PackageType__c
      ,RecordTypeId
      ,Supplier_Name__c
      ,Buy_Volume__c_NL AS Buy_Volume__c
      ,Gross_Cost__c_NL AS Gross_Cost__c
	  ,Media_Net_Cost__c
      ,Rate__c
      ,Target_Gender__c
      ,Target_Age__c
      ,Opp_Buy_Description__c
      ,Input_Mode__c
      ,Audience_Tier__c
FROM Beligum_Sell_Lines_PreSplit
WHERE FR_line IS NOT NULL
   OR LTRIM(RTRIM(NL_line)) <> ''






CREATE VIEW [dbo].[Spain_Company] AS
SELECT DISTINCT dbo.ReplaceExtraChars(LTRIM(RTRIM(eb.[Field 9]))) AS 'Name'
	  ,'Agency' AS Type__c
	  ,'Active' AS Status__c
	  ,'Spain' AS Market__c
	  ,sf.Id
FROM XaxisETL.[dbo].[Extract_Spain] eb
	LEFT JOIN (SELECT DISTINCT NAME, Id
			  FROM GLStaging.dbo.Company__c 
			  WHERE type__c = 'Agency'
			  AND Market__c LIKE '%Spain%') sf
	ON  sf.NAME LIKE '%'+dbo.ReplaceExtraChars(LTRIM(RTRIM(eb.[Field 9])))+'%'
WHERE eb.[Field 9] IS NOT NULL
	AND eb.[Field 9] <> 'Agencia'
	AND eb.[Field 2] IS NOT NULL
	AND dbo.ReplaceExtraChars(LTRIM(RTRIM(eb.[Field 9]))) <> ''

UNION ALL

--Find new Advertiser

SELECT DISTINCT dbo.ReplaceExtraChars(LTRIM(RTRIM(eb.[Field 11]))) AS 'Name'
	,'Advertiser' AS Type__c
	,'Active' AS Status__c
	,NULL AS Market__c
	,sf.Id
FROM XaxisETL.[dbo].[Extract_Spain] eb
	LEFT JOIN (SELECT DISTINCT NAME, Id
			  FROM GLStaging.dbo.Company__c
			  WHERE type__c = 'Advertiser') sf
	ON  dbo.ReplaceExtraChars(LTRIM(RTRIM(eb.[Field 11]))) = sf.NAME
WHERE eb.[Field 11] IS NOT NULL
  AND eb.[Field 11] <> 'Cliente'
  AND dbo.ReplaceExtraChars(LTRIM(RTRIM(eb.[Field 11]))) <> ''
  AND eb.[Field 2] IS NOT NULL



CREATE VIEW [dbo].[SpainNew_Accounts] AS

WITH w_Account AS (
SELECT DISTINCT [Field 11] +' - '+ [Field 9] +' (Spain)' AS Name
	  ,acct.Agency__c AS Agency__c
	  ,[Field 9] AS Agency_Name
	  ,acct.Advertiser__c AS Advertiser__c
	  ,[Field 11] AS Advertiser_Name
FROM XaxisETL.[dbo].[Extract_Spain] eb
	LEFT JOIN (SELECT acct.Id
					  ,acct.Advertiser__c
					  ,acct.Agency__c
					  ,sf_ag.NAME AS Agency_Name
					  ,sf_ad.NAME AS Advertiser_Name
				FROM Account acct
					INNER JOIN (SELECT ag.NAME
									 ,ag.Id
							   FROM Spain_Company ag
							   WHERE ag.type__c = 'Agency'
							   AND Market__c LIKE '%Spain%') sf_ag
						ON acct.Agency__c = sf_ag.Id
					INNER JOIN (SELECT ad.NAME
									  ,ad.Id
								FROM Spain_Company ad
								WHERE ad.type__c = 'Advertiser') sf_ad
						ON acct.Advertiser__c = sf_ad.Id
				WHERE acct.Advertiser__c IS NOT NULL
				  AND acct.Agency__c IS NOT NULL
				  AND sf_ag.NAME IS NOT NULL
				  AND sf_ad.NAME IS NOT NULL
				  ) acct
	ON acct.Advertiser_Name = eb.[Field 11]
   AND acct.Agency_Name = eb.[Field 9]
WHERE eb.[Field 11] IS NOT NULL
  AND eb.[Field 11] <> 'Cliente'
 -- AND acct.id IS NULL
  AND [Field 9] IS NOT NULL
  AND [Field 11] IS NOT NULL
  AND LTRIM(RTRIM([Field 9])) <> ''
  AND LTRIM(RTRIM([Field 11])) <> ''
  
)
, Adv_Count AS (
	SELECT bc.id
		  ,ISNULL(COUNT(ba.Id), 0) AS a_count
	FROM Spain_Company bc
		LEFT JOIN Account ba
			ON bc.Id = ba.Advertiser__c
	WHERE bc.Type__c = 'Advertiser'
	GROUP BY bc.Id)
, Adv_Created AS (
	SELECT bc.id
		  ,bc.CreatedDate
	FROM Company__c bc
	WHERE bc.Type__c = 'Advertiser')
,  Ag_Created AS (
	SELECT bc.id
		  ,bc.CreatedDate
	FROM Company__c bc
	WHERE bc.Type__c = 'Agency')
, with_rank AS (
	SELECT wa.Name
		  ,sf_ad.id AS Advertiser__c
		  ,sf_ag.id AS Agency__c
		  ,'Xaxis' AS Business_Unit__c
		  ,'Signed' AS Account_Opt_In_Status__c
		  ,(SELECT TOP 1 [Id] FROM RecordType WHERE SobjectType = 'Account' AND DeveloperName = 'Xaxis_Media_Buying_EMEA') AS RecordTypeId
		  ,DENSE_RANK() OVER (PARTITION BY wa.Name ORDER BY Adv_Count.a_count, Adv_Created.CreatedDate, Ag_Created.CreatedDate) AS Ranky
	FROM w_Account wa
		INNER JOIN(
				   SELECT ag.NAME
						 ,ag.Id
				   FROM Spain_Company ag
				   WHERE ag.type__c = 'Agency'
					 AND ag.Market__c LIKE '%Spain%'
					 ) sf_ag
			ON wa.Agency_Name = sf_ag.Name
		INNER JOIN( 
				   SELECT ad.NAME
						,ad.Id
				   FROM Spain_Company ad
				   WHERE ad.type__c = 'Advertiser') sf_ad
			ON wa.Advertiser_Name = sf_ad.Name
		INNER JOIN Adv_Count
			ON Adv_Count.Id = sf_ad.id
		INNER JOIN Adv_Created
			ON Adv_Created.Id = sf_ad.id
		INNER JOIN Ag_Created
			ON Ag_Created.Id = sf_ag.id

	WHERE wa.Agency__c IS NULL
	OR wa.Advertiser__c IS NULL
	)
SELECT Name
	  ,Advertiser__c
	  ,Agency__c
	  ,Business_Unit__c
	  ,Account_Opt_In_Status__c
	  ,RecordTypeId
FROM with_rank
WHERE Ranky = 1




CREATE VIEW [dbo].[Spain_Accounts] AS


WITH w_Account AS (
SELECT DISTINCT [Field 11] +' - '+ [Field 9] +' (Spain)' AS Name
	  ,acct.Id
	  ,acct.Agency__c AS Agency__c
	  ,[Field 9] AS Agency_Name
	  ,acct.Advertiser__c AS Advertiser__c
	  ,[Field 11] AS Advertiser_Name
FROM XaxisETL.[dbo].[Extract_Spain] eb
	LEFT JOIN (SELECT acct.Id
					  ,acct.Advertiser__c
					  ,acct.Agency__c
					  ,sf_ag.NAME AS Agency_Name
					  ,sf_ad.NAME AS Advertiser_Name
				FROM Account acct
					INNER JOIN (SELECT ag.NAME
									 ,ag.Id
							   FROM Spain_Company ag
							   WHERE ag.type__c = 'Agency'
							   AND Market__c LIKE '%Spain%') sf_ag
						ON acct.Agency__c = sf_ag.Id
					INNER JOIN (SELECT ad.NAME
									  ,ad.Id
								FROM Spain_Company ad
								WHERE ad.type__c = 'Advertiser') sf_ad
						ON acct.Advertiser__c = sf_ad.Id
				WHERE acct.Advertiser__c IS NOT NULL
				  AND acct.Agency__c IS NOT NULL
				  AND sf_ag.NAME IS NOT NULL
				  AND sf_ad.NAME IS NOT NULL
				  ) acct
	ON acct.Advertiser_Name = eb.[Field 11]
   AND acct.Agency_Name = eb.[Field 9]
WHERE eb.[Field 11] IS NOT NULL
  AND eb.[Field 11] <> 'Cliente'
 -- AND acct.id IS NULL
  AND [Field 9] IS NOT NULL
  AND [Field 11] IS NOT NULL
  AND LTRIM(RTRIM([Field 9])) <> ''
  AND LTRIM(RTRIM([Field 11])) <> ''
  
)
, Adv_Count AS (
	SELECT bc.id
		  ,COUNT(ba.Id) AS a_count
	FROM Spain_Company bc
		INNER JOIN Account ba
			ON bc.Id = ba.Advertiser__c
	WHERE bc.Type__c = 'Advertiser'
	GROUP BY bc.Id)
, Adv_Created AS (
	SELECT bc.id
		  ,bc.CreatedDate
	FROM Company__c bc
	WHERE bc.Type__c = 'Advertiser')
,  Ag_Created AS (
	SELECT bc.id
		  ,bc.CreatedDate
	FROM Company__c bc
	WHERE bc.Type__c = 'Agency')
, Acct_Count AS (
	SELECT AccountId
		  ,COUNT(Id) AS account_count
	FROM Opportunity
	GROUP BY AccountId
	)
, with_rank AS (
	SELECT DISTINCT wa.Name
		  ,wa.Id
		  ,sf_ad.id AS Advertiser__c
		  ,sf_ag.id AS Agency__c
		  ,'Xaxis' AS Business_Unit__c
		  ,'Signed' AS Account_Opt_In_Status__c
		  ,(SELECT TOP 1 [Id] FROM RecordType WHERE SobjectType = 'Account' AND DeveloperName = 'Xaxis_Media_Buying_EMEA') AS RecordTypeId
		  ,DENSE_RANK() OVER (PARTITION BY wa.Name ORDER BY Adv_Count.a_count, Adv_Created.CreatedDate, Ag_Created.CreatedDate, Acct_Count.account_count DESC, wa.Id) AS Ranky
		  ,Adv_Count.a_count, Adv_Created.CreatedDate AS adv_crd, Ag_Created.CreatedDate, Acct_Count.account_count
	FROM w_Account wa
		INNER JOIN(
				   SELECT ag.NAME
						 ,ag.Id
				   FROM Spain_Company ag
				   WHERE ag.type__c = 'Agency'
					 AND ag.Market__c LIKE '%Spain%'
					 ) sf_ag
			ON wa.Agency_Name = sf_ag.Name
		   AND wa.Agency__c = sf_ag.Id
		INNER JOIN( 
				   SELECT ad.NAME
						,ad.Id
				   FROM Spain_Company ad
				   WHERE ad.type__c = 'Advertiser') sf_ad
			ON wa.Advertiser_Name = sf_ad.Name
		   AND wa.Advertiser__c = sf_ad.Id
		INNER JOIN Adv_Count
			ON Adv_Count.Id = sf_ad.id
		INNER JOIN Adv_Created
			ON Adv_Created.Id = sf_ad.id
		INNER JOIN Ag_Created
			ON Ag_Created.Id = sf_ag.id
		LEFT JOIN Acct_Count
			ON Acct_Count.AccountId = wa.Id

--	WHERE wa.Agency__c IS NULL
--	OR wa.Advertiser__c IS NULL
	)

SELECT Name
	  ,Id
	  ,Advertiser__c
	  ,Agency__c
	  ,Business_Unit__c
	  ,Account_Opt_In_Status__c
	  ,RecordTypeId
FROM with_rank
WHERE Ranky = 1







CREATE VIEW [dbo].[Beligum_Accounts] AS


WITH w_Account AS (
SELECT DISTINCT F5 +' - '+ F3 +' (Belgium)' AS Name
	  ,acct.Id
	  ,acct.Agency__c AS Agency__c
	  ,F3 AS Agency_Name
	  ,acct.Advertiser__c AS Advertiser__c
	  ,F5 AS Advertiser_Name
FROM XaxisETL.[dbo].[Extract_Belgium] eb
	LEFT JOIN (SELECT acct.Id
					  ,acct.Advertiser__c
					  ,acct.Agency__c
					  ,sf_ag.NAME AS Agency_Name
					  ,sf_ad.NAME AS Advertiser_Name
				FROM Account acct
					INNER JOIN (SELECT ag.NAME
									 ,ag.Id
							   FROM Beligum_Company ag
							   WHERE ag.type__c = 'Agency'
								 AND ag.Market__c LIKE '%Belgium%') sf_ag
						ON acct.Agency__c = sf_ag.Id
					INNER JOIN (SELECT ad.NAME
									  ,ad.Id
								FROM Beligum_Company ad
								WHERE ad.type__c = 'Advertiser') sf_ad
						ON acct.Advertiser__c = sf_ad.Id
				WHERE acct.Advertiser__c IS NOT NULL
				  AND acct.Agency__c IS NOT NULL
				  AND sf_ag.NAME IS NOT NULL
				  AND sf_ad.NAME IS NOT NULL
				  ) acct
	ON acct.Advertiser_Name = eb.F5
   AND acct.Agency_Name = eb.F3
WHERE eb.id > 8
 -- AND acct.id IS NULL
  AND [F3] IS NOT NULL
  AND [F5] IS NOT NULL
  AND LTRIM(RTRIM([F3])) <> ''
  AND LTRIM(RTRIM([F5])) <> ''
  
)
, Adv_Count AS (
	SELECT bc.id
		  ,COUNT(ba.Id) AS a_count
	FROM Beligum_Company bc
		INNER JOIN Account ba
			ON bc.Id = ba.Advertiser__c
	WHERE bc.Type__c = 'Advertiser'
	GROUP BY bc.Id)
, Adv_Created AS (
	SELECT bc.id
		  ,bc.CreatedDate
	FROM Company__c bc
	WHERE bc.Type__c = 'Advertiser')
,  Ag_Created AS (
	SELECT bc.id
		  ,bc.CreatedDate
	FROM Company__c bc
	WHERE bc.Type__c = 'Agency')
, Acct_Count AS (
	SELECT AccountId
		  ,COUNT(Id) AS account_count
	FROM Opportunity
	GROUP BY AccountId
	)
, with_rank AS (
	SELECT DISTINCT wa.Name
		  ,wa.Id
		  ,sf_ad.id AS Advertiser__c
		  ,sf_ag.id AS Agency__c
		  ,'Xaxis' AS Business_Unit__c
		  ,'Signed' AS Account_Opt_In_Status__c
		  ,(SELECT TOP 1 [Id] FROM RecordType WHERE SobjectType = 'Account' AND DeveloperName = 'Xaxis_Media_Buying_EMEA') AS RecordTypeId
		  ,DENSE_RANK() OVER (PARTITION BY wa.Name ORDER BY Adv_Count.a_count, Adv_Created.CreatedDate, Ag_Created.CreatedDate, Acct_Count.account_count DESC, wa.Id) AS Ranky
		  ,Adv_Count.a_count, Adv_Created.CreatedDate AS adv_crd, Ag_Created.CreatedDate, Acct_Count.account_count
	FROM w_Account wa
		INNER JOIN(
				   SELECT ag.NAME
						 ,ag.Id
				   FROM Beligum_Company ag
				   WHERE ag.type__c = 'Agency'
					 AND ag.Market__c LIKE '%Belgium%'
					 ) sf_ag
			ON wa.Agency_Name = sf_ag.Name
		   AND wa.Agency__c = sf_ag.Id
		INNER JOIN( 
				   SELECT ad.NAME
						,ad.Id
				   FROM Beligum_Company ad
				   WHERE ad.type__c = 'Advertiser') sf_ad
			ON wa.Advertiser_Name = sf_ad.Name
		   AND wa.Advertiser__c = sf_ad.Id
		INNER JOIN Adv_Count
			ON Adv_Count.Id = sf_ad.id
		INNER JOIN Adv_Created
			ON Adv_Created.Id = sf_ad.id
		INNER JOIN Ag_Created
			ON Ag_Created.Id = sf_ag.id
		LEFT JOIN Acct_Count
			ON Acct_Count.AccountId = wa.Id

--	WHERE wa.Agency__c IS NULL
--	OR wa.Advertiser__c IS NULL
	)

SELECT Name
	  ,Id
	  ,Advertiser__c
	  ,Agency__c
	  ,Business_Unit__c
	  ,Account_Opt_In_Status__c
	  ,RecordTypeId
FROM with_rank
WHERE Ranky = 1





CREATE VIEW [dbo].[SpainNew_Opps] AS
SELECT [StageName]
      ,[CloseDate]
      ,[Advertiser__c]
      ,[Agency__c]
      ,[RecordTypeId]
      ,[AccountId]
--      ,[External_Id__c]
      ,'NewSpainOpp-' + [Name] AS [Name]
      ,CASE WHEN StageName = 'Closed Lost' THEN'Campaign not running' ELSE NULL END AS Reason_Lost__c   
  FROM [Spain_Opp]
WHERE id is null






CREATE VIEW [dbo].[Spain_Opp] AS
SELECT DISTINCT CASE WHEN [Field 5] IS NULL THEN 'Speculative'
					 WHEN [Field 5] LIKE 'Pendiente activación' THEN 'Contract Pending'
					 WHEN [Field 5] LIKE 'Pendiente planificación' THEN 'Contract Pending'
					 ELSE 'Contacted / Prospecting'
				END AS StageName
--	  ,CONVERT(VARCHAR(8), CONVERT(DATE,F8,3),3) AS CloseDate
	  ,CONVERT(DATE,[Field 8],101) AS CloseDate
	  ,acct.Advertiser__c AS Advertiser__c
	  ,acct.Agency__c AS Agency__c
	  ,(SELECT TOP 1 [Id] FROM [RecordType] WHERE SobjectType = 'Opportunity' AND DeveloperName = 'Spain') AS RecordTypeId
	  ,acct.Id AS AccountId
--	  ,'SpainOpp-'+[Field 4] AS External_Id__c
	  ,dbo.ReplaceExtraChars(LTRIM(RTRIM([Field 12]))) AS 'Name'
	  ,sf_opp.Id
FROM XaxisETL.[dbo].[Extract_Spain] eb
	LEFT JOIN Spain_Accounts acct
		LEFT JOIN Company__c adv
			ON acct.Advertiser__c = adv.Id
		LEFT JOIN Company__c ag
			ON acct.Agency__c = ag.Id
		ON ag.Name LIKE dbo.ReplaceExtraChars(LTRIM(RTRIM(eb.[Field 9]))) + '%' 
	    AND dbo.ReplaceExtraChars(LTRIM(RTRIM(eb.[Field 11]))) = adv.Name
		AND ag.Market__c = 'Spain'
	LEFT JOIN Opportunity sf_opp
		ON  dbo.ReplaceExtraChars(LTRIM(RTRIM(eb.[Field 12]))) = sf_opp.Name
		  OR 'NewSpainOpp-' +dbo.ReplaceExtraChars(LTRIM(RTRIM(eb.[Field 12]))) = sf_opp.Name
	   AND sf_opp.AccountId = acct.Id
WHERE eb.[Field 11] IS NOT NULL
  AND eb.[Field 11] <> 'Cliente'
  AND dbo.ReplaceExtraChars(LTRIM(RTRIM(eb.[Field 11]))) <> ''
  AND eb.[Field 2] IS NOT NULL
  








CREATE VIEW [dbo].[Spain_Sell_Lines] AS
		
		SELECT DISTINCT sf_opp.Id AS Opportunity__c
			  ,'SpainOpp-'+[Field 4] AS External_ID__c
			  ,dbo.ReplaceExtraChars(LTRIM(RTRIM(eb.[Field 13]))) AS Product_Detail__c
			  ,CAST(CONVERT(DATE,eb.[Field 7],101) AS DATE) AS Start_Date__c
			  ,CAST(CONVERT(DATE,eb.[Field 8],101) AS DATE) AS End_Date__c
--			  ,CONVERT(MONEY, REPLACE(eb.F24, ',','')) AS Gross_Cost__c
			  ,dbo.ReplaceExtraChars(ISNULL(eb.[Field 23], '') + ' ' + ISNULL(eb.[Field 25], '')) AS Buy_Name_txt__c
			  ,'Net Cost (Calc Margin)' AS Imputing_Margin_or_Net__c
			  ,'MediaTrader' AS	PackageType__c
			  ,(SELECT TOP 1 Id FROM RecordType WHERE SobjectType = 'Opportunity_Buy__c' AND DeveloperName = 'Spain') AS RecordTypeId
			  ,CASE WHEN dbo.ReplaceExtraChars(LTRIM(RTRIM(eb.[Field 11]))) = ''
				    THEN NULL
					ELSE dbo.ReplaceExtraChars(LTRIM(RTRIM(eb.[Field 11])))
				END AS Supplier_Name__c
			  ,CONVERT(MONEY, REPLACE(dbo.ReplaceExtraChars(eb.[Field 19]), ',','')) AS Buy_Volume__c
			  ,CONVERT(MONEY, REPLACE(dbo.ReplaceExtraChars(eb.[Field 21]), ',','')) AS Gross_Cost__c
			  ,CONVERT(MONEY, REPLACE(dbo.ReplaceExtraChars(eb.[Field 20]), ',','')) AS Rate__c
			  ,ISNULL(sum_media.m_cost, 0) AS Media_Net_Cost__c
			  ,dbo.ReplaceExtraChars(LTRIM(RTRIM(eb.[Field 18]))) AS Buy_Type__c
			  ,CASE WHEN dbo.ReplaceExtraChars(LTRIM(RTRIM(eb.[Field 22]))) = '' THEN 'Triggers'
					WHEN dbo.ReplaceExtraChars(LTRIM(RTRIM(eb.[Field 22]))) IS NULL THEN 'Triggers'
					ELSE dbo.ReplaceExtraChars(LTRIM(RTRIM(eb.[Field 22])))
					END  AS Audience_Tier__c
			  ,CONVERT(MONEY, REPLACE(REPLACE(dbo.ReplaceExtraChars(eb.[Field 27]), ',',''), '%',''))/100  AS Current_Margin__c
			  ,'From spreadsheet: ' + eb.[Tag: Filename] AS Current_Margin_Explanation__c
--			  ,CASE WHEN LTRIM(RTRIM(eb.F28)) = '' THEN NULL ELSE LTRIM(RTRIM(eb.F28)) END AS Target_Gender__c
--			  ,CASE WHEN LTRIM(RTRIM(eb.F29)) = '' THEN NULL ELSE LTRIM(RTRIM(eb.F29)) END AS Target_Age__c
			  ,CASE WHEN dbo.ReplaceExtraChars(LTRIM(RTRIM(CONCAT(ISNULL(eb.[Field 23], ''), ' ', ISNULL(eb.[Field 24], ''), ' ', ISNULL(eb.[Field 25], ''))))) = ''
					THEN NULL
					ELSE dbo.ReplaceExtraChars(LTRIM(RTRIM(CONCAT(ISNULL(eb.[Field 23], ''), ' ', ISNULL(eb.[Field 24], ''), ' ', ISNULL(eb.[Field 25], '')))))
			   END AS Opp_Buy_Description__c
			  ,'Externally Managed' AS Input_Mode__c
--			  ,'Triggers' AS Audience_Tier__c
--			  ,CASE WHEN [Field 5] IS NULL THEN 'Speculative'
--					 WHEN [Field 5] LIKE 'Pendiente activación' THEN 'Contract Pending'
--					 WHEN [Field 5] LIKE 'Pendiente planificación' THEN 'Proposal Sent'
--					 ELSE 'Contacted / Prospecting'
--				END AS Sell_Line_Status__c
FROM XaxisETL.[dbo].[Extract_Spain] eb
	LEFT JOIN Spain_Accounts acct
		LEFT JOIN Company__c adv
			ON acct.Advertiser__c = adv.Id
		LEFT JOIN Company__c ag
			ON acct.Agency__c = ag.Id
		ON ag.Name LIKE dbo.ReplaceExtraChars(LTRIM(RTRIM(eb.[Field 9]))) + '%' 
	    AND dbo.ReplaceExtraChars(LTRIM(RTRIM(eb.[Field 11]))) = adv.Name
		AND ag.Market__c = 'Spain'
	LEFT JOIN Opportunity sf_opp
		ON  dbo.ReplaceExtraChars(LTRIM(RTRIM(eb.[Field 12]))) = sf_opp.Name
		  OR 'NewSpainOpp-' +dbo.ReplaceExtraChars(LTRIM(RTRIM(eb.[Field 12]))) = sf_opp.Name
	   AND sf_opp.AccountId = acct.Id
			INNER JOIN (
						SELECT [Field 4] AS id
							  ,SUM(ISNULL(CONVERT(MONEY, REPLACE(dbo.ReplaceExtraChars([Field 21]), ',','')), 0)) AS m_cost
						FROM XaxisETL.dbo.Extract_Spain
						WHERE LTRIM(RTRIM([Field 1])) = 'Compra'
						  AND LTRIM(RTRIM(REPLACE(dbo.ReplaceExtraChars([Field 21]), ',',''))) <> ''
						GROUP BY [Field 4]
						) sum_media
				ON eb.[Field 4] = sum_media.id
		  WHERE  eb.[Field 11] IS NOT NULL
			  AND eb.[Field 11] <> 'Cliente'
			 -- AND acct.id IS NULL
			  AND [Field 9] IS NOT NULL
			  AND [Field 11] IS NOT NULL
			  AND LTRIM(RTRIM([Field 9])) <> ''
			  AND LTRIM(RTRIM([Field 11])) <> ''
			  
AND LTRIM(RTRIM([Field 1])) = 'Venta'






CREATE VIEW [dbo].[Spain_Buy_Placement] AS

SELECT sl.External_Id__c AS Name
	 ,sl.Id AS Sell_Line__c
	 ,CASE WHEN dbo.ReplaceExtraChars(LTRIM(RTRIM(eb.[Field 16]))) = '' THEN NULL
		   ELSE dbo.ReplaceExtraChars(LTRIM(RTRIM(eb.[Field 16]))) 
	  END AS Creative_Format__c
	 ,CONVERT(MONEY, REPLACE(dbo.ReplaceExtraChars(eb.[Field 21]), ',','')) AS Actual_Cost__c
	 ,CAST(CONVERT(DATE,eb.[Field 7],101) AS DATE) AS Start_Date__c
	 ,CAST(CONVERT(DATE,eb.[Field 8],101) AS DATE) AS End_Date__c
FROM XaxisETL.[dbo].[Extract_Spain] eb
	LEFT JOIN GLStaging.dbo.Opportunity_Buy__c sl
		ON CAST([Field 4] AS VARCHAR) = CAST(REPLACE(sl.External_Id__c, 'SpainOpp-', '') AS VARCHAR)
WHERE  eb.[Field 11] IS NOT NULL
	AND eb.[Field 11] <> 'Cliente'
	-- AND acct.id IS NULL
	AND [Field 9] IS NOT NULL
	AND [Field 11] IS NOT NULL
	AND LTRIM(RTRIM([Field 9])) <> ''
	AND LTRIM(RTRIM([Field 11])) <> ''
	AND LTRIM(RTRIM([Field 1])) = 'Compra'







