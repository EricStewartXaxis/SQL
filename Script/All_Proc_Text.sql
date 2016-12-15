 
-
SalesForceModel
text
----
 
-
XaxisETL
text
----
 
-
Nordics
text
----
 
-
GlobalQA
text
----
 
-
GLStaging
text
----


CREATE PROCEDURE [dbo].[NordicsNew_Accounts_sp] AS

IF OBJECT_ID('tempdb..#sf_adv') IS NOT NULL DROP TABLE #sf_adv;
IF OBJECT_ID('tempdb..#sf_ag') IS NOT NULL DROP TABLE #sf_ag;
IF OBJECT_ID('tempdb..#w_Account') IS NOT NULL DROP TABLE #w_Account;


--WITH sf_adv AS (

					SELECT DISTINCT client_name_clean AS Name
						   ,'Advertiser' AS Type__c
						   ,'Active' AS Status__c
					--	   , NULL AS Market__c
						   ,sf.Id
						   ,sf.Name_Full
					--	   ,master.dbo.Levenshtein(dbo.ReplaceExtraChars([client name]),Name_full, DEFAULT) AS dis
						   ,DENSE_RANK() OVER(PARTITION BY client_name_clean 
											  ORDER BY ISNULL(master.dbo.Levenshtein(client_name_clean
																					, Name_full
																					, DEFAULT)
															,LEN(client_name_clean)), sf.Id) AS Ranky
				    INTO #sf_adv
					FROM XaxisETL.dbo.Extract_Nordics en
						LEFT JOIN (SELECT DISTINCT CASE WHEN CHARINDEX(' ', Name) < 8 THEN Name
													ELSE LEFT(Name,CHARINDEX(' ', NAME) - 1) 
													END AS Name
											, Id
											,Name AS Name_Full
									FROM Company__c
									WHERE type__c = 'Advertiser') sf
						ON CASE WHEN CHARINDEX(' ', client_name_clean) < 8 THEN client_name_clean
										ELSE LEFT(client_name_clean,CHARINDEX(' ', client_name_clean) - 1)
					--								   END LIKE '%' + REPLACE(REPLACE(sf.Name,'[',''), ']', '') + '%'
										END LIKE REPLACE(REPLACE(sf.Name,'[',''), ']', '') + '%'
						AND ISNULL( 1- (CAST(master.dbo.Levenshtein(client_name_clean
																	,Name_full
																	,DEFAULT) AS DECIMAL)
									/LEN(client_name_clean)), 1) >= .2
					WHERE client_name_clean IS NOT NULL
--				)
--, sf_ag AS (
			SELECT DISTINCT dbo.ReplaceExtraChars(brand) AS Name
					  ,'Agency' AS Type__c
					  ,'Active' AS Status__c
					  ,CASE WHEN [Client Country] = 'DK' Then 'Denmark'
							WHEN [Client Country] = 'NO' Then 'Norway'
							WHEN [Client Country] = 'SE' Then 'Sweden'
						ELSE 'Nordic'END AS Market__c
					  ,sf.Id
					  ,sf.Name_full
					  ,master.dbo.Levenshtein(dbo.ReplaceExtraChars(brand),Name_full, LEN(dbo.ReplaceExtraChars(brand))-1) AS dis
					  ,DENSE_RANK() OVER(PARTITION BY dbo.ReplaceExtraChars(brand), [Client Country] 
									   ORDER BY ISNULL(master.dbo.Levenshtein(dbo.ReplaceExtraChars(brand),Name_full, LEN(dbo.ReplaceExtraChars(brand))-1)
													  ,LEN(dbo.ReplaceExtraChars(brand))), sf.Id) AS Ranky
		        INTO #sf_ag
				FROM XaxisETL.dbo.Extract_Nordics en
					LEFT JOIN (SELECT DISTINCT CASE WHEN CHARINDEX(' ', Name) = 0 THEN Name
													ELSE LEFT(Name,CHARINDEX(' ', NAME) - 1) 
											   END AS Name
									 ,Id
									 ,Market__c
									 ,Name AS Name_Full
							   FROM Company__c 
							   WHERE type__c = 'Agency'
							   AND (Market__c LIKE '%Denmark%'
								 OR Market__c LIKE '%Norway%'
								 OR Market__c LIKE '%Sweden%'
								 OR Market__c LIKE '%Nordic%')) sf
						ON CASE WHEN CHARINDEX(' ', brand) = 0 THEN brand
							ELSE LEFT(brand,CHARINDEX(' ', brand) - 1)
						   END LIKE '%' + REPLACE(sf.Name, [Client Country]+':', '') + '%'
						AND CASE WHEN [Client Country] = 'DK' Then 'Denmark'
							WHEN [Client Country] = 'NO' Then 'Norway'
							WHEN [Client Country] = 'SE' Then 'Sweden'
							ELSE 'Nordic'END LIKE '%' + sf.Market__c + '%'
				WHERE brand IS NOT NULL
--		   )

--, w_Account AS (
--				SELECT DISTINCT dbo.ReplaceExtraChars(eb.[client name]) +' - '+ dbo.ReplaceExtraChars(eb.brand) +' (Nordics)' AS Name
				SELECT DISTINCT eb.client_name_clean +' - '+ dbo.ReplaceExtraChars(eb.brand) AS Name
					  ,acct.Id
					  ,acct.Agency__c AS Agency__c
					  ,dbo.ReplaceExtraChars(eb.brand) AS Agency_Name
					  ,acct.Advertiser__c AS Advertiser__c
					  ,client_name_clean AS Advertiser_Name
--					  ,acct.ag_Ranky
--					  ,acct.adv_Ranky
				INTO #w_Account
				FROM XaxisETL.[dbo].[Extract_Nordics] eb

					LEFT JOIN (SELECT acct.Id
									  ,acct.Advertiser__c
									  ,acct.Agency__c
									  ,sf_ag.NAME AS Agency_Name
									  ,sf_adv.NAME AS Advertiser_Name
									  ,sf_ag.Market__c
									  ,sf_ag.Ranky AS ag_Ranky
									  ,sf_adv.Ranky AS adv_Ranky
								FROM Account acct
									INNER JOIN #sf_ag sf_ag
										ON acct.Agency__c = sf_ag.Id
									INNER JOIN #sf_adv sf_adv
										ON acct.Advertiser__c = sf_adv.Id		   
								WHERE acct.Advertiser__c IS NOT NULL
								  AND acct.Agency__c IS NOT NULL
								  AND sf_ag.NAME IS NOT NULL
								  AND sf_adv.NAME IS NOT NULL
								  ) acct
					ON acct.Advertiser_Name = eb.client_name_clean
				   AND acct.Agency_Name = dbo.ReplaceExtraChars(eb.brand)
				WHERE  dbo.ReplaceExtraChars(eb.brand) IS NOT NULL
				  AND eb.client_name_clean IS NOT NULL 
--				)
;
WITH Adv_Count AS (
	SELECT bc.id
		  ,ISNULL(COUNT(ba.Id), 0) AS a_count
	FROM #sf_adv bc
		LEFT JOIN Account ba
			ON bc.Id = ba.Advertiser__c
	WHERE bc.Type__c = 'Advertiser'
	  AND bc.Ranky = 1
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
	SELECT CASE WHEN sf_ag.Market__c IS NULL THEN wa.Name + ' (Nordic)'
				ELSE wa.Name + ' (' +sf_ag.Market__c + ')'
		   END AS Name
		  ,sf_ad.id AS Advertiser__c
		  ,sf_ag.id AS Agency__c
		  ,'Xaxis' AS Business_Unit__c
		  ,'Signed' AS Account_Opt_In_Status__c
		  ,(SELECT TOP 1 [Id] FROM RecordType WHERE SobjectType = 'Account' AND DeveloperName = 'Xaxis_Media_Buying_EMEA') AS RecordTypeId
		  ,DENSE_RANK() OVER (PARTITION BY wa.Name ORDER BY Adv_Count.a_count, Adv_Created.CreatedDate, Ag_Created.CreatedDate) AS Ranky
		  ,wa.Agency__c AS Ag_Name
		   ,wa.Advertiser__c AS Adv_Name
--		   , wa.adv_Ranky
--		   ,wa.ag_Ranky
	FROM #w_Account wa
		INNER JOIN(
				   SELECT ag.NAME
						 ,ag.Id
						 ,ag.Market__c
				   FROM #sf_ag ag
				   WHERE ag.type__c = 'Agency'
				     AND ag.Ranky = 1
					 --AND ag.Market__c LIKE '%Nordic%'
					 ) sf_ag
			ON wa.Agency_Name = sf_ag.Name
		INNER JOIN( 
				   SELECT ad.NAME
						,ad.Id
				   FROM #sf_adv ad
				   WHERE ad.type__c = 'Advertiser'
				     AND ad.Ranky = 1) sf_ad
			ON wa.Advertiser_Name = sf_ad.Name
		INNER JOIN Adv_Count
			ON Adv_Count.Id = sf_ad.id
		INNER JOIN Adv_Created
			ON Adv_Created.Id = sf_ad.id
		INNER JOIN Ag_Created
			ON Ag_Created.Id = sf_ag.id

	WHERE wa.Agency__c     IS NULL
	OR    wa.Advertiser__c IS NULL
	)
SELECT Name
	  ,Advertiser__c
	  ,Agency__c
	  ,Business_Unit__c
	  ,Account_Opt_In_Status__c
	  ,RecordTypeId
FROM with_rank
WHERE Ranky = 1









CREATE PROCEDURE [dbo].[Nordics_Opp_sp] AS

IF OBJECT_ID('tempdb..#sf_adv') IS NOT NULL DROP TABLE #sf_adv;
IF OBJECT_ID('tempdb..#sf_ag') IS NOT NULL DROP TABLE #sf_ag;
IF OBJECT_ID('tempdb..#w_Account') IS NOT NULL DROP TABLE #w_Account;

--WITH sf_adv AS (
				SELECT DISTINCT client_name_clean AS Name
						,'Advertiser' AS Type__c
						,'Active' AS Status__c
				--	   , NULL AS Market__c
						,sf.Id
						,sf.Name_Full
						,en.DefaultCurrency
				--	   ,master.dbo.Levenshtein(dbo.ReplaceExtraChars([client name]),Name_full, DEFAULT) AS dis
						,DENSE_RANK() OVER(PARTITION BY client_name_clean 
											ORDER BY ISNULL(master.dbo.Levenshtein(client_name_clean
																				, Name_full
																				, DEFAULT)
														,LEN(client_name_clean)), sf.Id) AS Ranky
				INTO #sf_adv
				FROM XaxisETL.dbo.Extract_Nordics en
					LEFT JOIN (SELECT DISTINCT CASE WHEN CHARINDEX(' ', Name) < 8 THEN Name
												ELSE LEFT(Name,CHARINDEX(' ', NAME) - 1) 
												END AS Name
										, Id
										,Name AS Name_Full
								FROM GLStaging.dbo.Company__c
								WHERE type__c = 'Advertiser') sf
					ON CASE WHEN CHARINDEX(' ', client_name_clean) < 8 THEN client_name_clean
									ELSE LEFT(client_name_clean,CHARINDEX(' ', client_name_clean) - 1)
				--								   END LIKE '%' + REPLACE(REPLACE(sf.Name,'[',''), ']', '') + '%'
									END LIKE REPLACE(REPLACE(sf.Name,'[',''), ']', '') + '%'
					AND ISNULL( 1- (CAST(master.dbo.Levenshtein(client_name_clean
																,Name_full
																,DEFAULT) AS DECIMAL)
								/LEN(client_name_clean)), 1) >= .2
				WHERE client_name_clean IS NOT NULL
				ORDER BY 1
--				)
--, sf_ag AS (
			SELECT DISTINCT dbo.ReplaceExtraChars(brand) AS Name
					  ,'Agency' AS Type__c
					  ,'Active' AS Status__c
					  ,CASE WHEN [Client Country] = 'DK' Then 'Denmark'
							WHEN [Client Country] = 'NO' Then 'Norway'
							WHEN [Client Country] = 'SE' Then 'Sweden'
						ELSE 'Nordic'END AS Market__c
					  ,sf.Id
					  ,sf.Name_full
					  ,master.dbo.Levenshtein(dbo.ReplaceExtraChars(brand),Name_full, LEN(dbo.ReplaceExtraChars(brand))-1) AS dis
					  ,DENSE_RANK() OVER(PARTITION BY dbo.ReplaceExtraChars(brand), [Client Country] 
									   ORDER BY ISNULL(master.dbo.Levenshtein(dbo.ReplaceExtraChars(brand),Name_full, LEN(dbo.ReplaceExtraChars(brand))-1)
													  ,LEN(dbo.ReplaceExtraChars(brand))), sf.Id) AS Ranky
				INTO #sf_ag
				FROM XaxisETL.dbo.Extract_Nordics en
					LEFT JOIN (SELECT DISTINCT CASE WHEN CHARINDEX(' ', Name) = 0 THEN Name
													ELSE LEFT(Name,CHARINDEX(' ', NAME) - 1) 
											   END AS Name
									 ,Id
									 ,Market__c
									 ,Name AS Name_Full
							   FROM GLStaging.dbo.Company__c 
							   WHERE type__c = 'Agency'
							   AND (Market__c LIKE '%Denmark%'
								 OR Market__c LIKE '%Norway%'
								 OR Market__c LIKE '%Sweden%'
								 OR Market__c LIKE '%Nordic%')) sf
						ON CASE WHEN CHARINDEX(' ', brand) = 0 THEN brand
							ELSE LEFT(brand,CHARINDEX(' ', brand) - 1)
						   END LIKE '%' + REPLACE(sf.Name, [Client Country]+':', '') + '%'
						AND CASE WHEN [Client Country] = 'DK' Then 'Denmark'
							WHEN [Client Country] = 'NO' Then 'Norway'
							WHEN [Client Country] = 'SE' Then 'Sweden'
							ELSE 'Nordic'END LIKE '%' + sf.Market__c + '%'
				WHERE brand IS NOT NULL
--		   )

--, w_Account AS (
				SELECT DISTINCT eb.client_name_clean +' - '+ dbo.ReplaceExtraChars(eb.brand) AS Name
					  ,acct.Id
					  ,acct.Agency__c AS Agency__c
					  ,dbo.ReplaceExtraChars(eb.brand) AS Agency_Name
					  ,acct.Advertiser__c AS Advertiser__c
					  ,client_name_clean AS Advertiser_Name
					  ,acct.ag_Ranky
					  ,acct.adv_Ranky
					  ,acct.DefaultCurrency
--					  ,eb.OrderID
					  ,eb.CampaignName
					  ,eb.PlacementName
					  ,eb.Unit
					  ,eb.MediaName
					  ,eb.BudgetNet 
					  ,eb.[Client
 Name]
					  ,eb.EndDate
--					  ,eb.OrderID
	--				  ,eb.BookingID
				INTO #w_Account
				FROM XaxisETL.[dbo].[Extract_Nordics] eb
					LEFT JOIN (SELECT acct.Id
									  ,acct.Advertiser__c
									  ,acct.Agency__c
									  ,sf_ag.NAME AS Agency_Name
									  ,sf_adv.NAME AS Advertiser_Name
									  ,sf_ag.Market__c
									  ,sf_ag.Ranky AS ag_Ranky
									  ,sf_adv.Ranky AS adv_Ranky
									  ,sf_adv.DefaultCurrency
								FROM Account acct
									INNER JOIN #sf_ag sf_ag
										ON acct.Agency__c = sf_ag.Id
									INNER JOIN #sf_adv sf_adv
										ON acct.Advertiser__c = sf_adv.Id		   
								WHERE acct.Advertiser__c IS NOT NULL
								  AND acct.Agency__c IS NOT NULL
								  AND sf_ag.NAME IS NOT NULL
								  AND sf_adv.NAME IS NOT NULL
								  ) acct
						ON acct.Advertiser_Name = eb.client_name_clean
					   AND acct.Agency_Name = dbo.ReplaceExtraChars(eb.brand)
					   AND acct.DefaultCurrency = eb.DefaultCurrency
				WHERE  dbo.ReplaceExtraChars(eb.brand) IS NOT NULL
				  AND eb.client_name_clean IS NOT NULL 
--				)
;

WITH Adv_Count AS (
	SELECT bc.id
		  ,ISNULL(COUNT(ba.Id), 0) AS a_count
	FROM #sf_adv bc
		LEFT JOIN Account ba
			ON bc.Id = ba.Advertiser__c
	WHERE bc.Type__c = 'Advertiser'
	  AND bc.Ranky = 1
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
	SELECT DISTINCT 
			CASE WHEN sf_ag.Market__c IS NULL THEN wa.Name + ' (Nordic)'
				ELSE wa.Name + ' (' +sf_ag.Market__c + ')'
		   END AS Name
		  ,wa.Agency_Name AS Ag_Name
		  ,wa.Id
		  ,wa.DefaultCurrency
		  ,sf_ad.id AS Advertiser__c
		  ,sf_ag.id AS Agency__c
		  ,'Xaxis' AS Business_Unit__c
		  ,'Signed' AS Account_Opt_In_Status__c
		  ,(SELECT TOP 1 [Id] FROM RecordType WHERE SobjectType = 'Opportunity' AND DeveloperName = sf_ag.Market__c) AS RecordTypeId
		  ,DENSE_RANK() OVER (PARTITION BY wa.Name ORDER BY Adv_Count.a_count DESC, Adv_Created.CreatedDate, Ag_Created.CreatedDate, wa.Id) AS Ranky
--		  ,wa.Agency__c AS Ag_Name
--		   ,wa.Advertiser__c AS Adv_Name
		   ,wa.adv_Ranky
		   ,wa.ag_Ranky
		   ,wa.Advertiser_Name
--			,wa.OrderID
			,wa.CampaignName
--			,wa.PlacementName
			,wa.Unit
--			,wa.MediaName
--			,sum_net.sum_BudgetNet AS BudgetNet
			,sum_net.EndDate AS EndDate
--			,wa.[Client Name]
--			,wa.OrderID


	FROM #w_Account wa
		INNER JOIN(
				   SELECT ag.NAME
						 ,ag.Id
						 ,ag.Market__c
				   FROM #sf_ag ag
				   WHERE ag.type__c = 'Agency'
				     AND ag.Ranky = 1
					 --AND ag.Market__c LIKE '%Nordic%'
					 ) sf_ag
			ON wa.Agency_Name = sf_ag.Name
		INNER JOIN( 
				   SELECT ad.NAME
						,ad.Id
				   FROM #sf_adv ad
				   WHERE ad.type__c = 'Advertiser'
				     AND ad.Ranky = 1) sf_ad
			ON wa.Advertiser_Name = sf_ad.Name
		INNER JOIN Adv_Count
			ON Adv_Count.Id = sf_ad.id
		INNER JOIN Adv_Created
			ON Adv_Created.Id = sf_ad.id
		INNER JOIN Ag_Created
			ON Ag_Created.Id = sf_ag.id
		INNER JOIN (SELECT Id	
						  ,CampaignName
--						  ,OrderID
						  ,Advertiser__c
						  ,Agency__c

						  ,SUM(ISNULL(BudgetNet, 0)) AS sum_BudgetNet
						  ,MAX(ISNULL(EndDate, '1900-01-01 00:00:00.0000000')) AS EndDate					  
				    FROM #w_Account w_Account
					GROUP BY Id
							,CampaignName
							,Advertiser__c
							,Agency__c
--							,OrderID

					) sum_net
			ON wa.Id = sum_net.Id
		   AND wa.CampaignName = sum_net.CampaignName
		   And wa.Advertiser__c = sum_net.Advertiser__c
		   AND wa.Agency__c = sum_net.Agency__c		
--		   AND wa.OrderID = sum_net.OrderID		


--	WHERE wa.Agency__c     IS NULL
--	OR    wa.Advertiser__c IS NULL
	)
SELECT DISTINCT wr.Business_Unit__c
	   ,wr.Advertiser__c
	   ,wr.Agency__c
	   ,wr.RecordTypeId
	   ,wr.Id AS 
AccountId
	   ,wr.DefaultCurrency AS CurrencyIsoCode
/*	   ,CASE WHEN LEN(wr.Advertiser_Name + ' - ' + dbo.ReplaceExtraChars(CampaignName) + ' OrderID:' +Cast(OrderID AS VARCHAR)) > 120
				  AND LEN(dbo.ReplaceExtraChars(CampaignName) + ' OrderID:' +Cast(OrderID AS VARCHAR)) > 120
			 THEN  ' OrderID:' +Cast(OrderID AS VARCHAR)
			 WHEN LEN( wr.Advertiser_Name + ' - ' + dbo.ReplaceExtraChars(CampaignName) + ' OrderID:' +Cast(OrderID AS VARCHAR)) >120
			 THEN  dbo.ReplaceExtraChars(CampaignName) + ' OrderID:' +Cast(OrderID AS VARCHAR)
			 ELSE  wr.Advertiser_Name + ' - ' + dbo.ReplaceExtraChars(CampaignName) + ' OrderID:' +Cast(OrderID AS VARCHAR) 
		END AS Name
*/
	   ,CASE WHEN LEN(wr.Ag_Name + ' - ' + wr.Advertiser_Name + ' - ' + dbo.ReplaceExtraChars(CampaignName)) > 120 AND cn.name_count > 1
			 THEN dbo.ReplaceExtraChars(wr.Ag_Name) + ' - ' + dbo.ReplaceExtraChars(CampaignName)
			 WHEN LEN(wr.Ag_Name + ' - ' + wr.Advertiser_Name + ' - ' + dbo.ReplaceExtraChars(CampaignName)) > 120 AND cn.name_count = 1
--			 THEN dbo.ReplaceExtraChars(wr.Ag_Name) + ' - ' + dbo.ReplaceExtraChars(wr.Advertiser_Name) + ' - ' + dbo.ReplaceExtraChars(CampaignName)
			 THEN dbo.ReplaceExtraChars(wr.Advertiser_Name) + ' - ' + dbo.ReplaceExtraChars(CampaignName)
			 WHEN LEN(wr.Ag_Name + ' - ' + wr.Advertiser_Name + ' - ' + dbo.ReplaceExtraChars(CampaignName)) < 121 AND cn.name_count > 1
			 THEN dbo.ReplaceExtraChars(wr.Ag_Name) + ' - ' + dbo.ReplaceExtraChars(wr.Advertiser_Name) + ' - ' + dbo.ReplaceExtraChars(CampaignName)
			 WHEN LEN(wr.Ag_Name + ' - ' + wr.Advertiser_Name + ' - ' + dbo.ReplaceExtraChars(CampaignName)) < 121 AND cn.name_count = 1
			 THEN dbo.ReplaceExtraChars(wr.Advertiser_Name) + ' - ' + dbo.ReplaceExtraChars(CampaignName)
			 ELSE dbo.ReplaceExtraChars(wr.Advertiser_Name) + ' - ' + dbo.ReplaceExtraChars(CampaignName)
		END AS Name
	   ,CONVERT(DATE,ISNULL(EndDate, '1900-01-01 00:00:00.0000000'),102) AS CloseDate
	   ,'Closed Won' AS StageName	   
--	   ,' OrderID:' +Cast(OrderID AS VARCHAR) AS External_Id__c
	   ,op.Id
FROM with_rank wr
	INNER JOIN (SELECT wr2.Advertiser_Name + ' - ' + wr2.CampaignName AS Name
					  ,COUNT(DISTINCT wr2.Advertiser_Name + ' - ' + wr2.CampaignName + ' - ' + wr2.Ag_Name) AS name_count
				FROM with_rank wr2
				GROUP BY wr2.Advertiser_Name + ' - ' + wr2.CampaignName
				) cn
		ON wr.Advertiser_Name + ' - ' + wr.CampaignName = cn.Name
	LEFT JOIN Opportunity op
		ON CASE WHEN LEN(wr.Ag_Name + ' - ' + wr.Advertiser_Name + ' - ' + dbo.ReplaceExtraChars(CampaignName)) > 120 AND cn.name_count > 1
			 THEN dbo.ReplaceExtraChars(wr.Ag_Name) + ' - ' + dbo.ReplaceExtraChars(CampaignName)
			 WHEN LEN(wr.Ag_Name + ' - ' + wr.Advertiser_Name + ' - ' + dbo.ReplaceExtraChars(CampaignName)) > 120 AND cn.name_count = 1
--			 THEN dbo.ReplaceExtraChars(wr.Ag_Name) + ' - ' + dbo.ReplaceExtraChars(wr.Advertiser_Name) + ' - ' + dbo.ReplaceExtraChars(CampaignName)
			 THEN dbo.ReplaceExtraChars(wr.Advertiser_Name) + ' - ' + dbo.ReplaceExtraChars(CampaignName)
			 WHEN LEN(wr.Ag_Name + ' - ' + wr.Advertiser_Name + ' - ' + dbo.ReplaceExtraChars(CampaignName)) < 121 AND cn.name_count > 1
			 THEN dbo.ReplaceExtraChars(wr.Ag_Name) + ' - ' + dbo.ReplaceExtraChars(wr.Advertiser_Name) + ' - ' + dbo.ReplaceExtraChars(CampaignName)
			 WHEN LEN(wr.Ag_Name + ' - ' + wr.Advertiser_Name + ' - ' + dbo.ReplaceExtraChars(CampaignName)) < 121 AND cn.name_count = 1
			 THEN dbo.ReplaceExtraChars(wr.Advertiser_Name) + ' - ' + dbo.ReplaceExtraChars(CampaignName)
			 ELSE dbo.ReplaceExtraChars(wr.Advertiser_Name) + ' - ' + dbo.ReplaceExtraChars(CampaignName)
		END = op.Name
	   AND wr.Id = op.AccountId
WHERE Ranky = 1
--  AND adv_Ranky = 1
--  AND ag_Ranky = 1

--  AND op.Id IS NULL






CREATE PROCEDURE [dbo].[NordicsNew_Opp_sp] AS

IF OBJECT_ID('tempdb..#temp_opp') IS NOT NULL DROP TABLE #temp_opp;

CREATE TABLE #temp_opp(
	[Business_Unit__c]			[nvarchar](218) NULL,
	[Advertiser__c]				[nvarchar](218) NULL,
	[Agency__c]					[nvarchar](222) NULL,
	[RecordTypeId]				[nvarchar](242) NULL,
	[AccountId]					[nvarchar](218) NULL,
	[CurrencyIsoCode]			[nvarchar](10) NULL,
	[Name]						[nvarchar](218) NULL,
	[CloseDate]					[date] NULL,
	[StageName]					[nvarchar](233) NULL,
	[Id]						[nvarchar](250) NULL
) ON [PRIMARY];

INSERT INTO #temp_opp
EXECUTE [dbo].Nordics_Opp_sp;

SELECT [Business_Unit__c]	
	   ,[Advertiser__c]		
	   ,[Agency__c]			
	   ,[RecordTypeId]		
	   ,[AccountId]
	   ,[CurrencyIsoCode]				
	   ,[Name]		
	   ,[CloseDate]			
	   ,[StageName]					
FROM #temp_opp
WHERE Id IS NULL


CREATE PROCEDURE [dbo].[Turkey_Opps_sp] AS
SELECT DISTINCT 'Opportunity Closed Won' StageName
--	  ,CONVERT(VARCHAR(8), CONVERT(DATE,F8,3),3) AS CloseDate
	  ,dd.CloseDate
	  ,acct.Advertiser__c AS Advertiser__c
	  ,acct.Agency__c AS Agency__c
	  ,(SELECT TOP 1 [Id] FROM [RecordType] WHERE SobjectType = 'Opportunity' AND DeveloperName = 'Turkey') AS RecordTypeId
	  ,acct.Id AS AccountId
--	  ,'SpainOpp-'+[Field 4] AS External_Id__c
	  ,dbo.ReplaceExtraChars(LTRIM(RTRIM([Field 11]))) AS 'Name'
	  ,sf_opp.Id
FROM XaxisETL.[dbo].[Extract_Turkey] eb
	INNER JOIN (
				SELECT 'MX' AS [Turk], 'Maxus (Turkey)' AS [SF] UNION
				SELECT 'MEC' , 'MEC (Turkey)'		UNION
				SELECT 'MC' , 'Mediacom (Turkey)'	UNION
				SELECT 'MS' , 'Mindshare (Turkey)'
				) ags	
		ON eb.[Field 9] = ags.Turk
	INNER JOIN (SELECT dbo.ReplaceExtraChars(LTRIM(RTRIM([Field 11]))) AS 'Name'
					  ,MAX(CONVERT(DATE,[Field 7], 101)) AS CloseDate
			    FROM XaxisETL.dbo.Extract_Turkey
				WHERE [Field 9] IS NOT NULL
				  AND [Field 9] <> 'Agency'
				  AND [Field 9] <> 'Needs mapping to Turkish Agencies int the system'
				  AND [Field 9] <> 'Campaign Details'
				GROUP BY dbo.ReplaceExtraChars(LTRIM(RTRIM([Field 11])))
				) dd
			ON dbo.ReplaceExtraChars(LTRIM(RTRIM(eb.[Field 11]))) = dd.Name
	LEFT JOIN Turkey_Accounts acct
		LEFT JOIN Company__c adv
			ON acct.Advertiser__c = adv.Id
		LEFT JOIN Company__c ag
			ON acct.Agency__c = ag.Id
		ON ag.Name = ags.SF
	    AND master.dbo.InitCap(LTRIM(RTRIM(LOWER([Field 10])))) = adv.Name
		AND ag.Market__c = 'Turkey'
	LEFT JOIN Opportunity sf_opp
		ON  dbo.ReplaceExtraChars(LTRIM(RTRIM(eb.[Field 11]))) = sf_opp.Name
	   AND sf_opp.AccountId = acct.Id
WHERE [Field 9] IS NOT NULL
  AND [Field 9] <> 'Agency'
  AND [Field 9] <> 'Needs mapping to Turkish Agencies int the system'
  AND [Field 9] <> 'Campaign Details'












CREATE PROCEDURE [dbo].[TurkeyNew_Opps_sp] AS
SELECT DISTINCT 'Opportunity Closed Won' StageName
--	  ,CONVERT(VARCHAR(8), CONVERT(DATE,F8,3),3) AS CloseDate
	  ,dd.CloseDate
	  ,acct.Advertiser__c AS Advertiser__c
	  ,acct.Agency__c AS Agency__c
	  ,(SELECT TOP 1 [Id] FROM [RecordType] WHERE SobjectType = 'Opportunity' AND DeveloperName = 'Turkey') AS RecordTypeId
	  ,acct.Id AS AccountId
--	  ,'SpainOpp-'+[Field 4] AS External_Id__c
	  ,dbo.ReplaceExtraChars(LTRIM(RTRIM([Field 11]))) AS 'Name'
--	  ,sf_opp.Id
FROM XaxisETL.[dbo].[Extract_Turkey] eb
	INNER JOIN (
				SELECT 'MX' AS [Turk], 'Maxus (Turkey)' AS [SF] UNION
				SELECT 'MEC' , 'MEC (Turkey)'		UNION
				SELECT 'MC' , 'Mediacom (Turkey)'	UNION
				SELECT 'MS' , 'Mindshare (Turkey)'
				) ags	
		ON eb.[Field 9] = ags.Turk
	INNER JOIN (SELECT dbo.ReplaceExtraChars(LTRIM(RTRIM([Field 11]))) AS 'Name'
					  ,MAX(CONVERT(DATE,[Field 7], 101)) AS CloseDate
			    FROM XaxisETL.dbo.Extract_Turkey
				WHERE [Field 9] IS NOT NULL
				  AND [Field 9] <> 'Agency'
				  AND [Field 9] <> 'Needs mapping to Turkish Agencies int the system'
				  AND [Field 9] <> 'Campaign Details'
				GROUP BY dbo.ReplaceExtraChars(LTRIM(RTRIM([Field 11])))
				) dd
			ON dbo.ReplaceExtraChars(LTRIM(RTRIM(eb.[Field 11]))) = dd.Name
	LEFT JOIN Turkey_Accounts acct
		LEFT JOIN Company__c adv
			ON acct.Advertiser__c = adv.Id
		LEFT JOIN Company__c ag
			ON acct.Agency__c = ag.Id
		ON ag.Name = ags.SF
	    AND master.dbo.InitCap(LTRIM(RTRIM(LOWER([Field 10])))) = adv.Name
		AND ag.Market__c = 'Turkey'
	LEFT JOIN Opportunity sf_opp
		ON  dbo.ReplaceExtraChars(LTRIM(RTRIM(eb.[Field 11]))) = sf_opp.Name
	   AND sf_opp.AccountId = acct.Id
WHERE [Field 9] IS NOT NULL
  AND [Field 9] <> 'Agency'
  AND [Field 9] <> 'Needs mapping to Turkish Agencies int the system'
  AND [Field 9] <> 'Campaign Details'
  AND sf_opp.Id IS NULL













CReate PROCEDURE [dbo].[NordicsNew_Sell_Line_sp2] AS

WITH sf_adv AS (
				SELECT DISTINCT client_name_clean AS Name
						,'Advertiser' AS Type__c
						,'Active' AS Status__c
				--	   , NULL AS Market__c
						,sf.Id
						,sf.Name_Full
				--	   ,master.dbo.Levenshtein(dbo.ReplaceExtraChars([client name]),Name_full, DEFAULT) AS dis
						,DENSE_RANK() OVER(PARTITION BY client_name_clean 
											ORDER BY ISNULL(master.dbo.Levenshtein(client_name_clean
																				, Name_full
																				, DEFAULT)
														,LEN(client_name_clean)), sf.Id) AS Ranky
				FROM XaxisETL.dbo.Extract_Nordics en
					LEFT JOIN (SELECT DISTINCT CASE WHEN CHARINDEX(' ', Name) < 8 THEN Name
												ELSE LEFT(Name,CHARINDEX(' ', NAME) - 1) 
												END AS Name
										, Id
										,Name AS Name_Full
								FROM GLStaging.dbo.Company__c
								WHERE type__c = 'Advertiser') sf
					ON CASE WHEN CHARINDEX(' ', client_name_clean) < 8 THEN client_name_clean
									ELSE LEFT(client_name_clean,CHARINDEX(' ', client_name_clean) - 1)
				--								   END LIKE '%' + REPLACE(REPLACE(sf.Name,'[',''), ']', '') + '%'
									END LIKE REPLACE(REPLACE(sf.Name,'[',''), ']', '') + '%'
					AND ISNULL( 1- (CAST(master.dbo.Levenshtein(client_name_clean
																,Name_full
																,DEFAULT) AS DECIMAL)
								/LEN(client_name_clean)), 1) >= .2
				WHERE client_name_clean IS NOT NULL
				)
, sf_ag AS (
			SELECT DISTINCT dbo.ReplaceExtraChars(brand) AS Name
					  ,'Agency' AS Type__c
					  ,'Active' AS Status__c
					  ,CASE WHEN [Client Country] = 'DK' Then 'Denmark'
							WHEN [Client Country] = 'NO' Then 'Norway'
							WHEN [Client Country] = 'SE' Then 'Sweden'
						ELSE 'Nordic'END AS Market__c
					  ,sf.Id
					  ,sf.Name_full
					  ,master.dbo.Levenshtein(dbo.ReplaceExtraChars(brand),Name_full, LEN(dbo.ReplaceExtraChars(brand))-1) AS dis
					  ,DENSE_RANK() OVER(PARTITION BY dbo.ReplaceExtraChars(brand), [Client Country] 
									   ORDER BY ISNULL(master.dbo.Levenshtein(dbo.ReplaceExtraChars(brand),Name_full, LEN(dbo.ReplaceExtraChars(brand))-1)
													  ,LEN(dbo.ReplaceExtraChars(brand))), sf.Id) AS Ranky
				FROM XaxisETL.dbo.Extract_Nordics en
					LEFT JOIN (SELECT DISTINCT CASE WHEN CHARINDEX(' ', Name) = 0 THEN Name
													ELSE LEFT(Name,CHARINDEX(' ', NAME) - 1) 
											   END AS Name
									 ,Id
									 ,Market__c
									 ,Name AS Name_Full
							   FROM GLStaging.dbo.Company__c 
							   WHERE type__c = 'Agency'
							   AND (Market__c LIKE '%Denmark%'
								 OR Market__c LIKE '%Norway%'
								 OR Market__c LIKE '%Sweden%'
								 OR Market__c LIKE '%Nordic%')) sf
						ON CASE WHEN CHARINDEX(' ', brand) = 0 THEN brand
							ELSE LEFT(brand,CHARINDEX(' ', brand) - 1)
						   END LIKE '%' + sf.Name + '%'
						AND CASE WHEN [Client Country] = 'DK' Then 'Denmark'
							WHEN [Client Country] = 'NO' Then 'Norway'
							WHEN [Client Country] = 'SE' Then 'Sweden'
							ELSE 'Nordic'END LIKE '%' + sf.Market__c + '%'
				WHERE brand IS NOT NULL
		   )

, w_Account AS (
				SELECT DISTINCT eb.client_name_clean +' - '+ dbo.ReplaceExtraChars(eb.brand) AS Name
					  ,acct.Id
					  ,acct.Agency__c AS Agency__c
					  ,dbo.ReplaceExtraChars(eb.brand) AS Agency_Name
					  ,acct.Advertiser__c AS Advertiser__c
					  ,client_name_clean AS Advertiser_Name
					  ,acct.ag_Ranky
					  ,acct.adv_Ranky

--					  ,eb.OrderID
					  ,eb.CampaignName
					  ,eb.PlacementName
					  ,eb.Unit
					  ,eb.MediaName
					  ,eb.BudgetNet 
					  ,eb.[Client Name]
					  ,eb.EndDate
					  ,eb.OrderID
					  ,eb.BookingID

				FROM XaxisETL.[dbo].[Extract_Nordics] eb
					LEFT JOIN (SELECT acct.Id
									  ,acct.Advertiser__c
									  ,acct.Agency__c
									  ,sf_ag.NAME AS Agency_Name
									  ,sf_adv.NAME AS Advertiser_Name
									  ,sf_ag.Market__c
									  ,sf_ag.Ra
nky AS ag_Ranky
									  ,sf_adv.Ranky AS adv_Ranky
								FROM Account acct
									INNER JOIN sf_ag
										ON acct.Agency__c = sf_ag.Id
									INNER JOIN sf_adv
										ON acct.Advertiser__c = sf_adv.Id		   
								WHERE acct.Advertiser__c IS NOT NULL
								  AND acct.Agency__c IS NOT NULL
								  AND sf_ag.NAME IS NOT NULL
								  AND sf_adv.NAME IS NOT NULL
								  ) acct
						ON acct.Advertiser_Name = eb.client_name_clean
					   AND acct.Agency_Name = dbo.ReplaceExtraChars(eb.brand)
				WHERE  dbo.ReplaceExtraChars(eb.brand) IS NOT NULL
				  AND eb.client_name_clean IS NOT NULL 
				)
, Adv_Count AS (
	SELECT bc.id
		  ,ISNULL(COUNT(ba.Id), 0) AS a_count
	FROM sf_adv bc
		LEFT JOIN Account ba
			ON bc.Id = ba.Advertiser__c
	WHERE bc.Type__c = 'Advertiser'
	  AND bc.Ranky = 1
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
	SELECT DISTINCT 
			CASE WHEN sf_ag.Market__c IS NULL THEN wa.Name + ' (Nordic)'
				ELSE wa.Name + ' (' +sf_ag.Market__c + ')'
		   END AS Name
		  ,wa.Id
		  ,sf_ad.id AS Advertiser__c
		  ,sf_ag.id AS Agency__c
		  ,'Xaxis' AS Business_Unit__c
		  ,'Signed' AS Account_Opt_In_Status__c
		  ,(SELECT TOP 1 [Id] FROM RecordType WHERE SobjectType = 'Opportunity' AND DeveloperName = sf_ag.Market__c) AS RecordTypeId
		  ,DENSE_RANK() OVER (PARTITION BY wa.Name ORDER BY Adv_Count.a_count DESC, Adv_Created.CreatedDate, Ag_Created.CreatedDate, wa.Id) AS Ranky
		  ,wa.Agency__c AS Ag_Name
		   ,wa.Advertiser__c AS Adv_Name
		   ,wa.adv_Ranky
		   ,wa.ag_Ranky
		   ,wa.Advertiser_Name
--			,wa.OrderID
			,wa.CampaignName
--			,wa.PlacementName
			,wa.Unit
--			,wa.MediaName
--			,sum_net.sum_BudgetNet AS BudgetNet
			,sum_net.EndDate AS EndDate
--			,wa.[Client Name]
			,wa.OrderID
			,wa.BookingID

	FROM w_Account wa
		INNER JOIN(
				   SELECT ag.NAME
						 ,ag.Id
						 ,ag.Market__c
				   FROM sf_ag ag
				   WHERE ag.type__c = 'Agency'
				     AND ag.Ranky = 1
					 --AND ag.Market__c LIKE '%Nordic%'
					 ) sf_ag
			ON wa.Agency_Name = sf_ag.Name
		INNER JOIN( 
				   SELECT ad.NAME
						,ad.Id
				   FROM sf_adv ad
				   WHERE ad.type__c = 'Advertiser'
				     AND ad.Ranky = 1) sf_ad
			ON wa.Advertiser_Name = sf_ad.Name
		INNER JOIN Adv_Count
			ON Adv_Count.Id = sf_ad.id
		INNER JOIN Adv_Created
			ON Adv_Created.Id = sf_ad.id
		INNER JOIN Ag_Created
			ON Ag_Created.Id = sf_ag.id
		INNER JOIN (SELECT Id	
						  ,CampaignName
						  ,OrderID
						  ,Advertiser__c
						  ,Agency__c
						  ,BookingID
						  ,SUM(ISNULL(BudgetNet, 0)) AS sum_BudgetNet
						  ,MAX(ISNULL(EndDate, '1900-01-01 00:00:00.0000000')) AS EndDate					  
				    FROM w_Account
					GROUP BY Id
							,CampaignName
							,Advertiser__c
							,Agency__c
							,OrderID
							,BookingID
					) sum_net
			ON wa.Id = sum_net.Id
		   AND wa.CampaignName = sum_net.CampaignName
		   And wa.Advertiser__c = sum_net.Advertiser__c
		   AND wa.Agency__c = sum_net.Agency__c		
		   AND wa.OrderID = sum_net.OrderID		
		   AND wa.BookingID = sum_net.BookingID


--	WHERE wa.Agency__c     IS NULL
--	OR    wa.Advertiser__c IS NULL
	)
SELECT DISTINCT wr.Business_Unit__c
	   ,wr.Advertiser__c
	   ,wr.Agency__c
	   ,wr.RecordTypeId
	   ,wr.Id AS AccountId
/*	   ,CASE WHEN LEN(wr.Advertiser_Name + ' - ' + dbo.ReplaceExtraChars(CampaignName) + ' BookingID:' +Cast(BookingID AS VARCHAR)) > 120
				  AND LEN(dbo.ReplaceExtraChars(CampaignName) + ' BookingID:' +Cast(BookingID AS VARCHAR)) > 120
			 THEN  ' BookingID:' +Cast(BookingID AS VARCHAR)
			 WHEN LEN( wr.Advertiser_Name + ' - ' + dbo.ReplaceExtraChars(CampaignName) + ' BookingID:' +Cast(BookingID AS VARCHAR)) >120
			 THEN  dbo.ReplaceExtraChars(Campa
ignName) + ' BookingID:' +Cast(BookingID AS VARCHAR)
			 ELSE  wr.Advertiser_Name + ' - ' + dbo.ReplaceExtraChars(CampaignName) + ' BookingID:' +Cast(BookingID AS VARCHAR) 
		END AS Name*/
	   ,CONVERT(DATE,ISNULL(EndDate, '1900-01-01 00:00:00.0000000'),102) AS CloseDate
	   ,'Closed Won' AS StageName	   
	   ,' BookingID:' +Cast(BookingID AS VARCHAR) AS External_Id__c
	   ,op.AccountId
FROM with_rank wr
	LEFT JOIN Opportunity op
		ON CASE WHEN LEN(wr.Advertiser_Name + ' - ' + dbo.ReplaceExtraChars(CampaignName) + ' OrderID:' +Cast(OrderID AS VARCHAR)) > 120
				  AND LEN(dbo.ReplaceExtraChars(CampaignName) + ' OrderID:' +Cast(OrderID AS VARCHAR)) > 120
			 THEN  ' OrderID:' +Cast(OrderID AS VARCHAR)
			 WHEN LEN( wr.Advertiser_Name + ' - ' + dbo.ReplaceExtraChars(CampaignName) + ' OrderID:' +Cast(OrderID AS VARCHAR)) >120
			 THEN  dbo.ReplaceExtraChars(CampaignName) + ' OrderID:' +Cast(OrderID AS VARCHAR)
			 ELSE  wr.Advertiser_Name + ' - ' + dbo.ReplaceExtraChars(CampaignName) + ' OrderID:' +Cast(OrderID AS VARCHAR) 
		END = op.Name
	   AND wr.Id = op.AccountId
WHERE Ranky = 1
  AND adv_Ranky = 1
  AND ag_Ranky = 1
  AND op.Id IS NULL






CREATE PROCEDURE [dbo].[Nordics_Sell_Line_sp] AS

IF OBJECT_ID('tempdb..#sf_adv') IS NOT NULL DROP TABLE #sf_adv;
IF OBJECT_ID('tempdb..#sf_ag') IS NOT NULL DROP TABLE #sf_ag;
IF OBJECT_ID('tempdb..#w_Account') IS NOT NULL DROP TABLE #w_Account;

--WITH sf_adv AS (
				SELECT DISTINCT client_name_clean AS Name
						,'Advertiser' AS Type__c
						,'Active' AS Status__c
				--	   , NULL AS Market__c
						,sf.Id
						,sf.Name_Full
						,en.DefaultCurrency
				--	   ,master.dbo.Levenshtein(dbo.ReplaceExtraChars([client name]),Name_full, DEFAULT) AS dis
						,DENSE_RANK() OVER(PARTITION BY client_name_clean 
											ORDER BY ISNULL(master.dbo.Levenshtein(client_name_clean
																				, Name_full
																				, DEFAULT)
														,LEN(client_name_clean)), sf.Id) AS Ranky
				INTO #sf_adv
				FROM XaxisETL.dbo.Extract_Nordics en
					LEFT JOIN (SELECT DISTINCT CASE WHEN CHARINDEX(' ', Name) < 8 THEN Name
												ELSE LEFT(Name,CHARINDEX(' ', NAME) - 1) 
												END AS Name
										, Id
										,Name AS Name_Full
								FROM GLStaging.dbo.Company__c
								WHERE type__c = 'Advertiser') sf
					ON CASE WHEN CHARINDEX(' ', client_name_clean) < 8 THEN client_name_clean
									ELSE LEFT(client_name_clean,CHARINDEX(' ', client_name_clean) - 1)
				--								   END LIKE '%' + REPLACE(REPLACE(sf.Name,'[',''), ']', '') + '%'
									END LIKE REPLACE(REPLACE(sf.Name,'[',''), ']', '') + '%'
					AND ISNULL( 1- (CAST(master.dbo.Levenshtein(client_name_clean
																,Name_full
																,DEFAULT) AS DECIMAL)
								/LEN(client_name_clean)), 1) >= .2
				WHERE client_name_clean IS NOT NULL
	
--, sf_ag AS (
			SELECT DISTINCT dbo.ReplaceExtraChars(brand) AS Name
					  ,'Agency' AS Type__c
					  ,'Active' AS Status__c
					  ,CASE WHEN [Client Country] = 'DK' Then 'Denmark'
							WHEN [Client Country] = 'NO' Then 'Norway'
							WHEN [Client Country] = 'SE' Then 'Sweden'
						ELSE 'Nordic'END AS Market__c
					  ,sf.Id
					  ,sf.Name_full
					  ,master.dbo.Levenshtein(dbo.ReplaceExtraChars(brand),Name_full, LEN(dbo.ReplaceExtraChars(brand))-1) AS dis
					  ,DENSE_RANK() OVER(PARTITION BY dbo.ReplaceExtraChars(brand), [Client Country] 
									   ORDER BY ISNULL(master.dbo.Levenshtein(dbo.ReplaceExtraChars(brand),Name_full, LEN(dbo.ReplaceExtraChars(brand))-1)
													  ,LEN(dbo.ReplaceExtraChars(brand))), sf.Id) AS Ranky
				INTO #sf_ag
				FROM XaxisETL.dbo.Extract_Nordics en
					LEFT JOIN (SELECT DISTINCT CASE WHEN CHARINDEX(' ', Name) = 0 THEN Name
													ELSE LEFT(Name,CHARINDEX(' ', NAME) - 1) 
											   END AS Name
									 ,Id
									 ,Market__c
									 ,Name AS Name_Full
							   FROM GLStaging.dbo.Company__c 
							   WHERE type__c = 'Agency'
							   AND (Market__c LIKE '%Denmark%'
								 OR Market__c LIKE '%Norway%'
								 OR Market__c LIKE '%Sweden%'
								 OR Market__c LIKE '%Nordic%')) sf
						ON CASE WHEN CHARINDEX(' ', brand) = 0 THEN brand
							ELSE LEFT(brand,CHARINDEX(' ', brand) - 1)
						   END LIKE '%' + REPLACE(sf.Name, [Client Country]+':', '') + '%'
						AND CASE WHEN [Client Country] = 'DK' Then 'Denmark'
							WHEN [Client Country] = 'NO' Then 'Norway'
							WHEN [Client Country] = 'SE' Then 'Sweden'
							ELSE 'Nordic'END LIKE '%' + sf.Market__c + '%'
				WHERE brand IS NOT NULL
		   

--, w_Account AS (
				SELECT DISTINCT eb.client_name_clean +' - '+ dbo.ReplaceExtraChars(eb.brand) AS Name
					  ,acct.Id
					  ,acct.Agency__c AS Agency__c
					  ,dbo.ReplaceExtraChars(eb.brand) AS Agency_Name
					  ,acct.Advertiser__c AS Advertiser__c
					  ,client_name_clean AS Advertiser_Name
					  ,acct.ag_Ranky
					  ,acct.adv_Ranky
					  ,acct.DefaultCurrency
--					  ,eb.OrderID
					  ,eb.CampaignName
					  ,eb.PlacementName
					  ,eb.Unit
					  ,eb.MediaName
					  ,eb.BudgetNet 
					  ,eb.ActualsNet
					  ,eb.[Cl
ient Name]
					  ,eb.EndDate
					  ,eb.StartDate
					  ,eb.OrderID
--					  ,eb.BookingID
				INTO #w_Account
				FROM XaxisETL.[dbo].[Extract_Nordics] eb
					LEFT JOIN (SELECT acct.Id
									  ,acct.Advertiser__c
									  ,acct.Agency__c
									  ,sf_ag.NAME AS Agency_Name
									  ,sf_adv.NAME AS Advertiser_Name
									  ,sf_ag.Market__c
									  ,sf_ag.Ranky AS ag_Ranky
									  ,sf_adv.Ranky AS adv_Ranky
									  ,sf_adv.DefaultCurrency
								FROM Account acct
									INNER JOIN #sf_ag sf_ag
										ON acct.Agency__c = sf_ag.Id
									INNER JOIN #sf_adv sf_adv
										ON acct.Advertiser__c = sf_adv.Id		   
								WHERE acct.Advertiser__c IS NOT NULL
								  AND acct.Agency__c IS NOT NULL
								  AND sf_ag.NAME IS NOT NULL
								  AND sf_adv.NAME IS NOT NULL
								  ) acct
						ON acct.Advertiser_Name = eb.client_name_clean
					   AND acct.Agency_Name = dbo.ReplaceExtraChars(eb.brand)
					   AND acct.DefaultCurrency = eb.DefaultCurrency
				WHERE  dbo.ReplaceExtraChars(eb.brand) IS NOT NULL
				  AND eb.client_name_clean IS NOT NULL 

;				
WITH Adv_Count AS (
	SELECT bc.id
		  ,ISNULL(COUNT(ba.Id), 0) AS a_count
	FROM #sf_adv bc
		LEFT JOIN Account ba
			ON bc.Id = ba.Advertiser__c
	WHERE bc.Type__c = 'Advertiser'
	  AND bc.Ranky = 1
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
	SELECT DISTINCT 
			CASE WHEN sf_ag.Market__c IS NULL THEN wa.Name + ' (Nordic)'
				ELSE wa.Name + ' (' +sf_ag.Market__c + ')'
		   END AS Name
		  ,wa.Id
		  ,sf_ad.id AS Advertiser__c
		  ,sf_ag.id AS Agency__c
		  ,'Xaxis' AS Business_Unit__c
		  ,'Signed' AS Account_Opt_In_Status__c
		  ,(SELECT TOP 1 [Id] FROM RecordType WHERE SobjectType = 'Opportunity_Buy__c' AND DeveloperName = sf_ag.Market__c) AS RecordTypeId
		  ,DENSE_RANK() OVER (PARTITION BY wa.Name ORDER BY Adv_Count.a_count DESC, Adv_Created.CreatedDate, Ag_Created.CreatedDate, wa.Id) AS Ranky
		  ,wa.Agency_Name AS Ag_Name
		   ,wa.Advertiser__c AS Adv_Name
		   ,wa.adv_Ranky
		   ,wa.ag_Ranky
		   ,wa.Advertiser_Name
--			,wa.OrderID
			,wa.CampaignName
--			,wa.PlacementName
			,wa.Unit
			,wa.MediaName
			,sum_net.sum_BudgetNet AS BudgetNet
--			,sum_net.sum_ActualNet AS ActualNet
			,sum_net.EndDate AS EndDate
			,sum_net.StartDate AS StartDate
--			,wa.[Client Name]
			,wa.OrderID
--			,wa.BookingID
		    ,place.PlacementName
			,ISNULL(place.Ranky, 1) AS place_ranky

	FROM #w_Account wa
		INNER JOIN(
				   SELECT ag.NAME
						 ,ag.Id
						 ,ag.Market__c
				   FROM #sf_ag ag
				   WHERE ag.type__c = 'Agency'
				     AND ag.Ranky = 1
					 --AND ag.Market__c LIKE '%Nordic%'
					 ) sf_ag
			ON wa.Agency_Name = sf_ag.Name
		INNER JOIN( 
				   SELECT ad.NAME
						,ad.Id
				   FROM #sf_adv ad
				   WHERE ad.type__c = 'Advertiser'
				     AND ad.Ranky = 1) sf_ad
			ON wa.Advertiser_Name = sf_ad.Name
		INNER JOIN Adv_Count
			ON Adv_Count.Id = sf_ad.id
		INNER JOIN Adv_Created
			ON Adv_Created.Id = sf_ad.id
		INNER JOIN Ag_Created
			ON Ag_Created.Id = sf_ag.id
		INNER JOIN (SELECT Id	
						  ,CampaignName
						  ,OrderID
						  ,Advertiser__c
						  ,Agency__c
--						  ,BookingID
						  ,SUM(ISNULL(BudgetNet, 0)) AS sum_BudgetNet
						  ,SUM(ISNULL(ActualsNet, 0)) AS sum_ActualNet
						  ,MAX(ISNULL(EndDate,  '2020-01-01 00:00:00.0000000')) AS EndDate	
						  ,MIN(ISNULL(StartDate,'1900-01-01 00:00:00.0000000')) AS StartDate				  
				    FROM #w_Account
--					WHERE ActualsNet IS NOT NULL
					WHERE BudgetNet IS NOT NULL
					GROUP BY Id
							,CampaignName
							,Advertiser__c
							,Agency__c
							,OrderID
--							,BookingID
					) sum_net
			ON wa.Id = sum_net.Id
		   AND wa.CampaignName =
 sum_net.CampaignName
		   And wa.Advertiser__c = sum_net.Advertiser__c
		   AND wa.Agency__c = sum_net.Agency__c		
		   AND wa.OrderID = sum_net.OrderID		
--		   AND wa.BookingID = sum_net.BookingID

		LEFT JOIN (SELECT DISTINCT CampaignName
						  ,PlacementName
						  ,DENSE_RANK() OVER (PARTITION BY CampaignName ORDER BY LEN(PlacementName) DESC, OrderId) AS Ranky
					FROM XaxisETL.[dbo].[Extract_Nordics] 
					WHERE PlacementName IS NOT NULL
					) place
				ON wa.CampaignName = place.CampaignName
--WHERE ISNULL(place.Ranky, 1) = 1
			   
		   


--	WHERE wa.Agency__c     IS NULL
--	OR    wa.Advertiser__c IS NULL
	)

, with_rank_org AS (
	SELECT DISTINCT 
			CASE WHEN sf_ag.Market__c IS NULL THEN wa.Name + ' (Nordic)'
				ELSE wa.Name + ' (' +sf_ag.Market__c + ')'
		   END AS Name
		  ,wa.Agency_Name AS Ag_Name
		  ,wa.Id
		  ,wa.DefaultCurrency
		  ,sf_ad.id AS Advertiser__c
		  ,sf_ag.id AS Agency__c
		  ,'Xaxis' AS Business_Unit__c
		  ,'Signed' AS Account_Opt_In_Status__c
		  ,(SELECT TOP 1 [Id] FROM RecordType WHERE SobjectType = 'Opportunity' AND DeveloperName = sf_ag.Market__c) AS RecordTypeId
		  ,DENSE_RANK() OVER (PARTITION BY wa.Name ORDER BY Adv_Count.a_count DESC, Adv_Created.CreatedDate, Ag_Created.CreatedDate, wa.Id) AS Ranky
--		  ,wa.Agency__c AS Ag_Name
--		   ,wa.Advertiser__c AS Adv_Name
		   ,wa.adv_Ranky
		   ,wa.ag_Ranky
		   ,wa.Advertiser_Name
--			,wa.OrderID
			,wa.CampaignName
--			,wa.PlacementName
			,wa.Unit
--			,wa.MediaName
--			,sum_net.sum_BudgetNet AS BudgetNet
			,sum_net.EndDate AS EndDate
--			,wa.[Client Name]
--			,wa.OrderID


	FROM #w_Account wa
		INNER JOIN(
				   SELECT ag.NAME
						 ,ag.Id
						 ,ag.Market__c
				   FROM #sf_ag ag
				   WHERE ag.type__c = 'Agency'
				     AND ag.Ranky = 1
					 --AND ag.Market__c LIKE '%Nordic%'
					 ) sf_ag
			ON wa.Agency_Name = sf_ag.Name
		INNER JOIN( 
				   SELECT ad.NAME
						,ad.Id
				   FROM #sf_adv ad
				   WHERE ad.type__c = 'Advertiser'
				     AND ad.Ranky = 1) sf_ad
			ON wa.Advertiser_Name = sf_ad.Name
		INNER JOIN Adv_Count
			ON Adv_Count.Id = sf_ad.id
		INNER JOIN Adv_Created
			ON Adv_Created.Id = sf_ad.id
		INNER JOIN Ag_Created
			ON Ag_Created.Id = sf_ag.id
		INNER JOIN (SELECT Id	
						  ,CampaignName
--						  ,OrderID
						  ,Advertiser__c
						  ,Agency__c

						  ,SUM(ISNULL(BudgetNet, 0)) AS sum_BudgetNet
						  ,MAX(ISNULL(EndDate, '1900-01-01 00:00:00.0000000')) AS EndDate					  
				    FROM #w_Account w_Account
					GROUP BY Id
							,CampaignName
							,Advertiser__c
							,Agency__c
--							,OrderID

					) sum_net
			ON wa.Id = sum_net.Id
		   AND wa.CampaignName = sum_net.CampaignName
		   And wa.Advertiser__c = sum_net.Advertiser__c
		   AND wa.Agency__c = sum_net.Agency__c		
--		   AND wa.OrderID = sum_net.OrderID		


--	WHERE wa.Agency__c     IS NULL
--	OR    wa.Advertiser__c IS NULL
	)



SELECT DISTINCT wr.RecordTypeId
--	   ,wr.Id AS AccountId
	   ,wr.BudgetNet AS Gross_Cost__c
/*	   ,CASE WHEN LEN(wr.Advertiser_Name + ' - ' + dbo.ReplaceExtraChars(CampaignName) + ' BookingID:' +Cast(BookingID AS VARCHAR)) > 120
				  AND LEN(dbo.ReplaceExtraChars(CampaignName) + ' BookingID:' +Cast(BookingID AS VARCHAR)) > 120
			 THEN  ' BookingID:' +Cast(BookingID AS VARCHAR)
			 WHEN LEN( wr.Advertiser_Name + ' - ' + dbo.ReplaceExtraChars(CampaignName) + ' BookingID:' +Cast(BookingID AS VARCHAR)) >120
			 THEN  dbo.ReplaceExtraChars(CampaignName) + ' BookingID:' +Cast(BookingID AS VARCHAR)
			 ELSE  wr.Advertiser_Name + ' - ' + dbo.ReplaceExtraChars(CampaignName) + ' BookingID:' +Cast(BookingID AS VARCHAR) 
		END AS Name*/
	   ,CONVERT(DATE,ISNULL(StartDate, '1900-01-01 00:00:00.0000000'),102) AS Start_Date__c
	   ,CONVERT(DATE,ISNULL(EndDate, '1900-01-01 00:00:00.0000000'),102) AS End_Date__c
	   ,MediaName AS Buy_Name_txt__c
	   ,'Net Cost (Calc Margin)' AS Imputing_Margin_or
_Net__c
	   ,'MediaTrader' AS PackageType__c
	   ,CASE WHEN CHARINDEX('mobile', LOWER(MediaName))
				 +CHARINDEX('mobil', LOWER(MediaName))
				 +CHARINDEX('doubleclick', LOWER(MediaName))
				 +CHARINDEX('adform', LOWER(MediaName)) > 0 THEN 'Xaxis Mobile'
		     WHEN CHARINDEX('programmatic', LOWER(MediaName))
				+CHARINDEX('tv', LOWER(MediaName))
				+CHARINDEX('video', LOWER(MediaName)) > 0 THEN 'Xaxis TV'
	    ELSE 'Xaxis Display' END AS product_detail__c

	  ,CASE WHEN CHARINDEX('mobile', LOWER(MediaName))
				 +CHARINDEX('mobil', LOWER(MediaName))
				 +CHARINDEX('doubleclick', LOWER(MediaName))
				 +CHARINDEX('adform', LOWER(MediaName)) > 0 THEN 'Mobile'
		     WHEN CHARINDEX('programmatic', LOWER(MediaName))
				+CHARINDEX('tv', LOWER(MediaName))
				+CHARINDEX('video', LOWER(MediaName)) > 0 THEN 'Video'
	    ELSE 'Display' END AS Media_Code__c

--	   ,'Closed Won' AS StageName	   
	   ,'NordicOrderID:' +Cast(OrderID AS VARCHAR) AS External_Id__c
	   ,dbo.ReplaceExtraChars(wr.PlacementName) AS Opp_Buy_Description__c
	   ,'Externally Managed' AS Input_Mode__c
	   ,op.Id AS [Opportunity__c]
	   ,sl.Id
	   ,0.0 AS Media_Net_Cost__c
	   ,'Triggers' AS Audience_Tier__c

FROM with_rank wr
	INNER JOIN (SELECT wr2.Advertiser_Name + ' - ' + wr2.CampaignName AS Name
					  ,COUNT(DISTINCT wr2.Advertiser_Name + ' - ' + wr2.CampaignName + ' - ' + wr2.Ag_Name) AS name_count
				FROM with_rank_org wr2
				GROUP BY wr2.Advertiser_Name + ' - ' + wr2.CampaignName
				) cn
		ON wr.Advertiser_Name + ' - ' + wr.CampaignName = cn.Name
	LEFT JOIN Opportunity op
		ON CASE WHEN LEN(wr.Ag_Name + ' - ' + wr.Advertiser_Name + ' - ' + dbo.ReplaceExtraChars(CampaignName)) > 120 AND cn.name_count > 1
			 THEN dbo.ReplaceExtraChars(wr.Ag_Name) + ' - ' + dbo.ReplaceExtraChars(CampaignName)
			 WHEN LEN(wr.Ag_Name + ' - ' + wr.Advertiser_Name + ' - ' + dbo.ReplaceExtraChars(CampaignName)) > 120 AND cn.name_count = 1
--			 THEN dbo.ReplaceExtraChars(wr.Ag_Name) + ' - ' + dbo.ReplaceExtraChars(wr.Advertiser_Name) + ' - ' + dbo.ReplaceExtraChars(CampaignName)
			 THEN dbo.ReplaceExtraChars(wr.Advertiser_Name) + ' - ' + dbo.ReplaceExtraChars(CampaignName)
			 WHEN LEN(wr.Ag_Name + ' - ' + wr.Advertiser_Name + ' - ' + dbo.ReplaceExtraChars(CampaignName)) < 121 AND cn.name_count > 1
			 THEN dbo.ReplaceExtraChars(wr.Ag_Name) + ' - ' + dbo.ReplaceExtraChars(wr.Advertiser_Name) + ' - ' + dbo.ReplaceExtraChars(CampaignName)
			 WHEN LEN(wr.Ag_Name + ' - ' + wr.Advertiser_Name + ' - ' + dbo.ReplaceExtraChars(CampaignName)) < 121 AND cn.name_count = 1
			 THEN dbo.ReplaceExtraChars(wr.Advertiser_Name) + ' - ' + dbo.ReplaceExtraChars(CampaignName)
			 ELSE dbo.ReplaceExtraChars(wr.Advertiser_Name) + ' - ' + dbo.ReplaceExtraChars(CampaignName)
		END = op.Name
	   AND wr.Id = op.AccountId
	LEFT JOIN Opportunity_Buy__c sl
		ON 'NordicOrderID:' +Cast(OrderID AS VARCHAR) = sl.External_Id__c
WHERE Ranky = 1
--  AND adv_Ranky = 1
--  AND ag_Ranky = 1
--  AND op.Id IS NULL
  AND place_ranky = 1



CREATE PROCEDURE [dbo].[NordicsNew_Sell_Lines_sp] AS

IF OBJECT_ID('tempdb..#temp_Nordics_SL') IS NOT NULL DROP TABLE #temp_Nordics_SL;

CREATE TABLE #temp_Nordics_SL(
RecordTypeId [nvarchar](218) NULL,
--AccountId [nvarchar](218) NULL,
Gross_Cost__c [float] NULL,
Start_Date__c [datetime] NULL,
End_Date__c [datetime] NULL,
Buy_Name_txt__c [nvarchar](218) NULL,
Imputing_Margin_or_Net__c [nvarchar](218) NULL,
PackageType__c [nvarchar](218) NULL,
product_detail__c [nvarchar](218) NULL,
Media_Code__c [nvarchar](218) NULL,
--StageName [nvarchar](218) NULL,
External_Id__c [nvarchar](218) NULL,
Opp_Buy_Description__c [nvarchar](218) NULL,
Input_Mode__c [nvarchar](218) NULL,
[Opportunity__c] [nvarchar](218) NULL,
Id [nvarchar](218) NULL,
Media_Net_Cost__c [float] NULL,
Audience_Tier__c [nvarchar](100) NULL
) ON [PRIMARY];








INSERT INTO #temp_Nordics_SL
EXECUTE [dbo].[Nordics_Sell_Line_sp];

SELECT  RecordTypeId 
  --     ,AccountId 
       ,Gross_Cost__c 
       ,Start_Date__c 
       ,End_Date__c 
       ,Buy_Name_txt__c
       ,Imputing_Margin_or_Net__c
       ,PackageType__c
       ,product_detail__c 
       ,Media_Code__c 
 --      ,StageName 
       ,External_Id__c 
       ,Opp_Buy_Description__c
       ,Input_Mode__c 
	   ,Opportunity__c    
	   ,Media_Net_Cost__c
	   ,Audience_Tier__c
FROM #temp_Nordics_SL
WHERE Id IS NULL




CREATE PROCEDURE [dbo].[NordicsUpdate_Sell_Line_sp] AS


IF OBJECT_ID('tempdb..#sql_sell_line') IS NOT NULL DROP TABLE #sql_sell_line;
IF OBJECT_ID('tempdb..#sf_sell_line') IS NOT NULL DROP TABLE #sf_sell_line;

IF OBJECT_ID('tempdb..#hash_sf_temp') IS NOT NULL DROP TABLE #hash_sf_temp;
IF OBJECT_ID('tempdb..#hash_sql_temp') IS NOT NULL DROP TABLE #hash_sql_temp;
IF OBJECT_ID('tempdb..#hash_sql') IS NOT NULL DROP TABLE #hash_sql;
IF OBJECT_ID('tempdb..#hash_sf') IS NOT NULL DROP TABLE #hash_sf;

CREATE TABLE #hash_sql_temp(
[RecordTypeId] [nvarchar](218) NULL,
[Gross_Cost__c] [float] NULL,
[Start_Date__c] [datetime] NULL,
[End_Date__c] [datetime] NULL,
[Buy_Name_txt__c] [nvarchar](456) NULL,
[Imputing_Margin_or_Net__c] [nvarchar](233) NULL,
[PackageType__c] [nvarchar](250) NULL,
[Product_Detail__c] [nvarchar](242) NULL,
[Media_Code__c] [nvarchar](215) NULL,
[External_Id__c] [nvarchar](222) NULL,
[Opp_Buy_Description__c] [nvarchar](3924) NULL,
[Input_Mode__c] [nvarchar](224) NULL,
[Opportunity__c] [nvarchar](218) NULL,
[Id] [nvarchar](218) NULL,
[Media_Net_Cost__c] [float] NULL,
[Audience_Tier__c] [nvarchar](265) NULL
) ON [PRIMARY];

CREATE TABLE #hash_sql(
--[RecordTypeId] [nvarchar](218) NULL,
[Gross_Cost__c]				[float] NULL,
[Start_Date__c]				[datetime] NULL,
[End_Date__c]				[datetime] NULL,
[Buy_Name_txt__c]			[nvarchar](456) NULL,
[Imputing_Margin_or_Net__c] [nvarchar](233) NULL,
[PackageType__c]			[nvarchar](250) NULL,
[Product_Detail__c]			[nvarchar](242) NULL,
[Media_Code__c]				[nvarchar](215) NULL,
[External_Id__c]			[nvarchar](222) NULL,
[Opp_Buy_Description__c]	[nvarchar](3924) NULL,
[Input_Mode__c]				[nvarchar](224) NULL,
[Opportunity__c]			[nvarchar](218) NULL,
[Id]						[nvarchar](218) NULL,
[Media_Net_Cost__c]			[float] NULL,
[Audience_Tier__c]			[nvarchar](265) NULL
) ON [PRIMARY];


CREATE TABLE #hash_sf(
--[RecordTypeId] [nvarchar](218) NULL,
[Gross_Cost__c] [float] NULL,
[Start_Date__c] [datetime] NULL,
[End_Date__c] [datetime] NULL,
[Buy_Name_txt__c] [nvarchar](456) NULL,
[Imputing_Margin_or_Net__c] [nvarchar](233) NULL,
[PackageType__c] [nvarchar](250) NULL,
[Product_Detail__c] [nvarchar](242) NULL,
[Media_Code__c] [nvarchar](215) NULL,
[External_Id__c] [nvarchar](222) NULL,
[Opp_Buy_Description__c] [nvarchar](3924) NULL,
[Input_Mode__c] [nvarchar](224) NULL,
[Opportunity__c] [nvarchar](218) NULL,
[Id] [nvarchar](218) NULL,
[Media_Net_Cost__c] [float] NULL,
[Audience_Tier__c] [nvarchar](265) NULL
) ON [PRIMARY];

INSERT INTO #hash_sql_temp
EXECUTE [dbo].[Nordics_Sell_Line_sp];

INSERT INTO #hash_sql
SELECT [Gross_Cost__c]				
	  ,[Start_Date__c]				
	  ,[End_Date__c]				
	  ,[Buy_Name_txt__c]			
	  ,[Imputing_Margin_or_Net__c] 
	  ,[PackageType__c]			
	  ,[Product_Detail__c]			
	  ,[Media_Code__c]				
	  ,[External_Id__c]			
	  ,[Opp_Buy_Description__c]	
	  ,[Input_Mode__c]				
	  ,[Opportunity__c]			
	  ,[Id]						
	  ,[Media_Net_Cost__c]			
	  ,[Audience_Tier__c]			
FROM #hash_sql_temp;

INSERT INTO #hash_sf
SELECT [Gross_Cost__c]				
	  ,[Start_Date__c]				
	  ,[End_Date__c]				
	  ,[Buy_Name_txt__c]			
	  ,[Imputing_Margin_or_Net__c] 
	  ,[PackageType__c]			
	  ,[Product_Detail__c]			
	  ,[Media_Code__c]				
	  ,[External_Id__c]			
	  ,[Opp_Buy_Description__c]	
	  ,[Input_Mode__c]				
	  ,[Opportunity__c]			
	  ,[Id]						
	  ,[Media_Net_Cost__c]			
	  ,[Audience_Tier__c]			
FROM Opportunity_Buy__c;



SELECT sq.[Gross_Cost__c]				
	   ,sq.[Start_Date__c]				
	   ,sq.[End_Date__c]				
	   ,sq.[Buy_Name_txt__c]			
	   ,sq.[Imputing_Margin_or_Net__c] 
	   ,sq.[PackageType__c]			
	   ,sq.[Product_Detail__c]			
	   ,sq.[Media_Code__c]				
	   ,sq.[External_Id__c]			
	   ,sq.[Opp_Buy_Description__c]	
	   ,sq.[Input_Mode__c]				
	   ,sq.[Opportunity__c]			
	   ,sq.[Id]						
	   ,sq.[Media_Net_Cost__c]			
	   ,sq.[Audience_Tier__c]			
	  ,HASHBYTES('SHA1', (SELECT TOP 1 * FROM #h
ash_sql bb WHERE bb.External_ID__c = sq.External_ID__c FOR XML RAW)) AS sq_hash
INTO #sql_sell_line
-- FROM [Production].[dbo].[Turkey_Sell_Lines] sq
FROM #hash_sql sq
WHERE sq.Id IS NOT NULL;

SELECT [Gross_Cost__c]				
	  ,[Start_Date__c]				
	  ,[End_Date__c]				
	  ,[Buy_Name_txt__c]			
	  ,[Imputing_Margin_or_Net__c] 
	  ,[PackageType__c]			
	  ,[Product_Detail__c]			
	  ,[Media_Code__c]				
	  ,sf_in.[External_Id__c]			
	  ,[Opp_Buy_Description__c]	
	  ,[Input_Mode__c]				
	  ,[Opportunity__c]			
	  ,[Id]						
	  ,[Media_Net_Cost__c]			
	  ,[Audience_Tier__c]			
	  ,HASHBYTES('SHA1', (SELECT TOP 1 * FROM #hash_sf cc WHERE cc.External_ID__c = sf_in.External_ID__c FOR XML RAW)) AS sf_hash
INTO #sf_sell_line
FROM Opportunity_Buy__c sf_in
--	INNER JOIN (SELECT External_Id__c FROM Production.dbo.Turkey_Sell_Lines) sq
	INNER JOIN (SELECT External_Id__c FROM #hash_sql) sq
		ON sf_in.External_Id__c = sq.External_ID__c



SELECT   [Opportunity__c]
		,[External_ID__c]
		,[Product_Detail__c]
		,[Start_Date__c]
		,[End_Date__c]
		,[Buy_Name_txt__c]
		,[Imputing_Margin_or_Net__c]
		,[PackageType__c]
--		,[RecordTypeId]
		,[Gross_Cost__c]
		,[Media_Net_Cost__c]
		,[Audience_Tier__c]
		,[Opp_Buy_Description__c]
		,[Input_Mode__c]
		,[Media_Code__c]
		,[Id]
FROM #sql_sell_line sq
	LEFT JOIN ( SELECT sf_hash
				FROM #sf_sell_line
			   ) sf
		ON sq.sq_hash = sf.sf_hash
WHERE sf.sf_hash IS NULL



/*

SELECT *
FROM #hash_sql
UNION 
SELECT #hash_sf.*
FROM #hash_sf
	INNER JOIN (SELECT External_Id__c FROM #hash_sql) sq
		ON #hash_sf.External_Id__c = sq.External_ID__c

*/

 
-
Production
text
----


CREATE PROCEDURE [dbo].[TurkeyNew_Accounts_sp] AS
IF OBJECT_ID('tempdb..#temp_Turkey_Acct') IS NOT NULL DROP TABLE #temp_Turkey_Acct;

CREATE TABLE #temp_Turkey_Acct(
[Name] [nvarchar](218) NULL,
[Id] [nvarchar](218) NULL,
[Advertiser__c] [nvarchar](218),
[Agency__c] [nvarchar](218),
[Business_Unit__c] [nvarchar](218),
[Account_Opt_In_Status__c] [nvarchar](218),
[RecordTypeId] [nvarchar](218),
) ON [PRIMARY];


INSERT INTO #temp_Turkey_Acct
EXECUTE [dbo].[Turkey_Accounts_sp];

SELECT [Name]
      ,[Advertiser__c]
      ,[Agency__c]
      ,[Business_Unit__c]
      ,[Account_Opt_In_Status__c]
      ,[RecordTypeId]
FROM #temp_Turkey_Acct
WHERE Id IS NULL



CREATE PROCEDURE [dbo].[Spain_Accounts_New_sp] AS

IF OBJECT_ID('tempdb..#Temp_Spain_Accounts') IS NOT NULL DROP TABLE #Temp_Spain_Accounts;

CREATE TABLE #Temp_Spain_Accounts(
	[Name] [nvarchar](250) NULL,
	[Id] [nvarchar](218) NULL,
	[Advertiser__c] [nvarchar](218) NULL,
	[Agency__c] [nvarchar](218) NULL,
	[Business_Unit__c] [varchar](5) NOT NULL,
	[Account_Opt_In_Status__c] [varchar](6) NOT NULL,
	[RecordTypeId] [nvarchar](218) NULL
) ON [PRIMARY]
;

INSERT INTO #Temp_Spain_Accounts
EXEC Spain_Accounts_sp;


SELECT Name
	  ,Advertiser__c
	  ,Agency__c
	  ,Business_Unit__c
	  ,Account_Opt_In_Status__c
	  ,RecordTypeId
FROM #Temp_Spain_Accounts
WHERE Id IS NULL






CREATE PROCEDURE [dbo].[Spain_Opp_New_sp] AS

IF OBJECT_ID('tempdb..#Temp_Spain_Opp') IS NOT NULL DROP TABLE #Temp_Spain_Opp;

CREATE TABLE #Temp_Spain_Opp(
	[StageName] [varchar](23) NOT NULL,
	[CloseDate] [date] NULL,
	[Advertiser__c] [nvarchar](218) NULL,
	[Agency__c] [nvarchar](218) NULL,
	[RecordTypeId] [nvarchar](218) NULL,
	[AccountId] [nvarchar](218) NULL,
	[Name] [varchar](100) NULL,
	[Id] [nvarchar](218) NULL,
	[CurrencyIsoCode] [varchar](3) NOT NULL,
	[Description] [nvarchar](max) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY];

INSERT INTO #Temp_Spain_Opp
Exec Spain_Opp_sp;

SELECT nOpp.StageName
	  ,nOpp.CloseDate
	  ,nOpp.Advertiser__c
	  ,nOpp.Agency__c
	  ,nOpp.RecordTypeId
	  ,nOpp.AccountId
	  ,ISNULL('Spain:'+opp.Name, nOpp.Name) AS Name
	  ,nOpp.CurrencyIsoCode
	  ,nOpp.[Description]
FROM #Temp_Spain_Opp nOpp
	LEFT JOIN (SELECT DISTINCT Name FROM Opportunity) opp
		ON nOpp.Name = opp.Name
WHERE nOpp.Id IS NULL

CREATE PROCEDURE SpainNew_Buy_Placement_sp AS


IF OBJECT_ID('tempdb..#spainNew_temp') IS NOT NULL DROP TABLE #spainNew_temp;

CREATE TABLE #spainNew_temp(
Name VARCHAR(200) NULL,
Id VARCHAR(218) NULL,
Sell_Line__c VARCHAR(40) NULL,
Creative_Format__c VARCHAR(60) NULL,
Actual_Cost__c FLOAT NULL,
Start_Date__c DATETIME NULL,
End_Date__c DATETIME NULL,
CurrencyIsoCode VARCHAR(10) NULL
) ON [PRIMARY];

INSERT INTO #spainNew_temp
EXECUTE Spain_Buy_Placement_sp;

SELECT Name
		,Sell_Line__c
		,Creative_Format__c
		,Actual_Cost__c
		,Start_Date__c
		,End_Date__c
		,CurrencyIsoCode
FROM #spainNew_temp
WHERE Id IS NULL





CREATE PROCEDURE Turkey_Company_New_sp @FilterName VARCHAR(50) AS

IF OBJECT_ID('tempdb..#Turkey_Temp') IS NOT NULL DROP TABLE #Turkey_Temp;

CREATE TABLE #Turkey_Temp(
        Name varchar(200)
       ,Type__c varchar(10)
       ,Status__c varchar(6)
       ,Market__c varchar(6)
       ,Id nvarchar(218))
ON [PRIMARY];
INSERT INTO #Turkey_Temp
EXEC Turkey_Company_sp @FilterName=@FilterName;


SELECT Name
	  ,Type__c
	  ,Status__c
	  ,Market__c
FROM #Turkey_Temp
WHERE ID IS NULL








CREATE PROCEDURE [dbo].[Turkey_Company_sp] @FilterName VARCHAR(50) AS

SELECT DISTINCT ag.SF AS Name
	  ,'Agency' AS Type__c
	  ,'Active' AS Status__c
	  ,'Turkey' AS Market__c
	  ,com.Id
  FROM [XaxisETL].[dbo].[Extract_Turkey] et
	INNER JOIN (
				SELECT 'MX' AS [Turk], 'Maxus (Turkey)' AS [SF] UNION
				SELECT 'MEC' , 'MEC (Turkey)'		UNION
				SELECT 'MC' , 'Mediacom (Turkey)'	UNION
				SELECT 'MS' , 'Mindshare (Turkey)'
				) ag	
		ON et.[Field 9] = ag.Turk
	LEFT JOIN Company__c com
		ON com.Name = ag.SF
	   AND com.Market__c = 'Turkey'
	   AND com.Type__c = 'Agency'
WHERE [Field 9] IS NOT NULL
  AND [Field 9] <> 'Agency'
  AND [Field 9] <> 'Needs mapping to Turkish Agencies int the system'
  AND [Field 9] <> 'Campaign Details'
  AND Filter = @FilterName

UNION ALL

SELECT DISTINCT master.dbo.InitCap(LOWER(REPLACE(REPLACE(dbo.ReplaceExtraChars(et.[Field 10]), ' ', ''), '''', ''))) AS Name
      ,'Advertiser' AS Type__c
	  ,'Active' AS Status__c
	  ,NULL AS Market__c
	  ,com.Id
FROM [XaxisETL].[dbo].[Extract_Turkey] et
	LEFT JOIN (SELECT  REPLACE(REPLACE(Name, ' ', ''), '''', '') AS Name, Id 
			  FROM Company__c
			  WHERE LEFT(Name, 5) <> '[SFL]'
			    AND Type__c = 'Advertiser') com
		ON master.dbo.InitCap(LOWER(REPLACE(REPLACE(dbo.ReplaceExtraChars(et.[Field 10]), ' ', ''), '''', ''))) = com.Name
WHERE [Field 9] IS NOT NULL
  AND [Field 9] <> 'Agency'
  AND [Field 9] <> 'Needs mapping to Turkish Agencies int the system'
  AND [Field 9] <> 'Campaign Details'
  AND Filter = @FilterName





CREATE PROCEDURE [dbo].[Turkey_Accounts_sp]  @FilterName VARCHAR(50) AS
--DECLARE  @FilterName VARCHAR(50) = 'Turkey_LR';
IF OBJECT_ID('tempdb..#Agent_Map') IS NOT NULL DROP TABLE #Agent_Map;
IF OBJECT_ID('tempdb..#Turk_Account') IS NOT NULL DROP TABLE #Turk_Account;
IF OBJECT_ID('tempdb..#w_Account') IS NOT NULL DROP TABLE #w_Account;
IF OBJECT_ID('tempdb..#Adv_Count') IS NOT NULL DROP TABLE #Adv_Count;
IF OBJECT_ID('tempdb..#Adv_Created') IS NOT NULL DROP TABLE #Adv_Created;
IF OBJECT_ID('tempdb..#Ag_Created') IS NOT NULL DROP TABLE #Ag_Created;
IF OBJECT_ID('tempdb..#Acct_Count') IS NOT NULL DROP TABLE #Acct_Count;
IF OBJECT_ID('tempdb..#Adv_Created') IS NOT NULL DROP TABLE #Adv_Created;
IF OBJECT_ID('tempdb..#Turk_Company') IS NOT NULL DROP TABLE #Turk_Company;

CREATE TABLE #Turk_Company(
        Name varchar(200)
       ,Type__c varchar(10)
       ,Status__c varchar(6)
       ,Market__c varchar(6)
       ,Id nvarchar(218))
ON [PRIMARY];
INSERT INTO #Turk_Company
SELECT DISTINCT ag.SF AS Name
	  ,'Agency' AS Type__c
	  ,'Active' AS Status__c
	  ,'Turkey' AS Market__c
	  ,com.Id
  FROM [XaxisETL].[dbo].[Extract_Turkey] et
	INNER JOIN (
				SELECT 'MX' AS [Turk], 'Maxus (Turkey)' AS [SF] UNION
				SELECT 'MEC' , 'MEC (Turkey)'		UNION
				SELECT 'MC' , 'Mediacom (Turkey)'	UNION
				SELECT 'MS' , 'Mindshare (Turkey)'
				) ag	
		ON et.[Field 9] = ag.Turk
	LEFT JOIN Company__c com
		ON com.Name = ag.SF
	   AND com.Market__c = 'Turkey'
	   AND com.Type__c = 'Agency'
WHERE [Field 9] IS NOT NULL
  AND [Field 9] <> 'Agency'
  AND [Field 9] <> 'Needs mapping to Turkish Agencies int the system'
  AND [Field 9] <> 'Campaign Details'
  AND Filter = @FilterName

UNION ALL

SELECT DISTINCT master.dbo.InitCap(LOWER(REPLACE(REPLACE(dbo.ReplaceExtraChars(et.[Field 10]), ' ', ''), '''', ''))) AS Name
      ,'Advertiser' AS Type__c
	  ,'Active' AS Status__c
	  ,NULL AS Market__c
	  ,com.Id
FROM [XaxisETL].[dbo].[Extract_Turkey] et
	LEFT JOIN (SELECT  REPLACE(REPLACE(Name, ' ', ''), '''', '') AS Name, Id 
			  FROM Company__c
			  WHERE LEFT(Name, 5) <> '[SFL]'
			    AND Type__c = 'Advertiser') com
		ON master.dbo.InitCap(LOWER(REPLACE(REPLACE(dbo.ReplaceExtraChars(et.[Field 10]), ' ', ''), '''', ''))) = com.Name
WHERE [Field 9] IS NOT NULL
  AND [Field 9] <> 'Agency'
  AND [Field 9] <> 'Needs mapping to Turkish Agencies int the system'
  AND [Field 9] <> 'Campaign Details'
  AND Filter = @FilterName;


CREATE TABLE #Agent_Map(
[Turk] VARCHAR(4),
[SF] VARCHAR(30)
)
INSERT INTO #Agent_Map
SELECT 'MX' AS [Turk], 'Maxus (Turkey)' AS [SF] UNION
SELECT 'MEC' , 'MEC (Turkey)'		UNION
SELECT 'MC' , 'Mediacom (Turkey)'	UNION
SELECT 'MS' , 'Mindshare (Turkey)'
;
SELECT acct.Id
		,acct.Advertiser__c
		,acct.Agency__c
		,sf_ag.NAME AS Agency_Name
		,sf_ad.NAME AS Advertiser_Name
INTO #Turk_Account
FROM Account acct
	INNER JOIN (SELECT ag.NAME
						,ag.Id
				FROM #Turk_Company ag
				WHERE ag.type__c = 'Agency'
				AND Market__c = 'Turkey') sf_ag
		ON acct.Agency__c = sf_ag.Id
	INNER JOIN (SELECT ad.NAME
						,ad.Id
				FROM #Turk_Company ad
				WHERE ad.type__c = 'Advertiser') sf_ad
		ON acct.Advertiser__c = sf_ad.Id
WHERE acct.Advertiser__c IS NOT NULL
	AND acct.Agency__c IS NOT NULL
	AND sf_ag.NAME IS NOT NULL
	AND sf_ad.NAME IS NOT NULL
;

--WITH w_Account AS (
SELECT DISTINCT master.dbo.InitCap(LOWER(REPLACE(REPLACE(dbo.ReplaceExtraChars([Field 10]), ' ', ''), '''', ''))) +' - '+ ag.sf AS Name
	  ,acct.Id
	  ,acct.Agency__c AS Agency__c
	  ,ag.sf AS Agency_Name
	  ,acct.Advertiser__c AS Advertiser__c
	  ,master.dbo.InitCap(LOWER(REPLACE(REPLACE(dbo.ReplaceExtraChars([Field 10]), ' ', ''), '''', ''))) AS Advertiser_Name
INTO #w_Account
FROM XaxisETL.[dbo].[Extract_Turkey] eb
	LEFT JOIN #Agent_Map ag	
			ON eb.[Field 9] = ag.Turk
	LEFT JOIN #Turk_Account acct
	ON acct.Advertiser_Name = master.dbo.InitCap(LOWER(REPLACE(REPLACE(dbo.ReplaceExtr
aChars([Field 10]), ' ', ''), '''', '')))
   AND acct.Agency_Name = ag.SF
WHERE [Field 9] IS NOT NULL
  AND [Field 9] <> 'Agency'
  AND [Field 9] <> 'Needs mapping to Turkish Agencies int the system'
  AND [Field 9] <> 'Campaign Details'
  AND Filter = @FilterName
  
--)
;
--, Adv_Count AS (
	SELECT bc.id
		  ,ISNULL(COUNT(ba.Id), 0) AS a_count
	INTO #Adv_Count
	FROM #Turk_Company bc
		LEFT JOIN Account ba
			ON bc.Id = ba.Advertiser__c
	WHERE bc.Type__c = 'Advertiser'
	GROUP BY bc.Id
	;
--, Adv_Created AS (
	SELECT bc.id
		  ,bc.CreatedDate
	INTO #Adv_Created
	FROM Company__c bc
	WHERE bc.Type__c = 'Advertiser'
	;
--,  Ag_Created AS (
	SELECT bc.id
		  ,bc.CreatedDate
	INTO #Ag_Created
	FROM Company__c bc
	WHERE bc.Type__c = 'Agency'
	;
--, Acct_Count AS (
	SELECT AccountId
		  ,ISNULL(COUNT(Id), 0) AS account_count
	INTO #Acct_Count
	FROM Opportunity
	GROUP BY AccountId
	;

WITH with_rank AS (
	SELECT DISTINCT wa.Name
		  ,wa.Id
		  ,sf_ad.id AS Advertiser__c
		  ,sf_ag.id AS Agency__c
		  ,'Xaxis' AS Business_Unit__c
		  ,'Signed' AS Account_Opt_In_Status__c
		  ,(SELECT TOP 1 [Id] FROM RecordType WHERE SobjectType = 'Account' AND DeveloperName = 'Xaxis_Media_Buying_EMEA') AS RecordTypeId
		  ,DENSE_RANK() OVER (PARTITION BY wa.Name ORDER BY Adv_Count.a_count, Adv_Created.CreatedDate, Ag_Created.CreatedDate, ISNULL(Acct_Count.account_count, 0) DESC, wa.Id) AS Ranky
--		  ,Adv_Count.a_count
--		  ,Adv_Created.CreatedDate AS adv_crd
--		  ,Ag_Created.CreatedDate  
--		  ,ISNULL(Acct_Count.account_count, 0) AS account_count
	FROM #w_Account wa
		LEFT JOIN(
				   SELECT ag.NAME
						 ,ag.Id
				   FROM #Turk_Company ag
				   WHERE ag.type__c = 'Agency'
					 AND ag.Market__c = 'Turkey'
					 ) sf_ag
			ON wa.Agency_Name = sf_ag.Name
--		   AND wa.Agency__c = sf_ag.Id
		LEFT JOIN( 
				   SELECT ad.NAME
						,ad.Id
				   FROM #Turk_Company ad
				   WHERE ad.type__c = 'Advertiser') sf_ad
			ON wa.Advertiser_Name = sf_ad.Name
--		   AND wa.Advertiser__c = sf_ad.Id
		LEFT JOIN #Adv_Count Adv_Count
			ON Adv_Count.Id = sf_ad.id
		LEFT JOIN #Adv_Created Adv_Created
			ON Adv_Created.Id = sf_ad.id
		LEFT JOIN #Ag_Created Ag_Created
			ON Ag_Created.Id = sf_ag.id
		LEFT JOIN #Acct_Count Acct_Count
			ON wa.Id = Acct_Count.AccountId

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




CREATE PROCEDURE [dbo].[Turkey_Accounts_New_sp] @FilterName VARCHAR(50) AS
--DECLARE  @FilterName VARCHAR(50) = 'Turkey_LR';
IF OBJECT_ID('tempdb..#temp_Turkey_Acct') IS NOT NULL DROP TABLE #temp_Turkey_Acct;

CREATE TABLE #temp_Turkey_Acct(
[Name] [nvarchar](218) NULL,
[Id] [nvarchar](218) NULL,
[Advertiser__c] [nvarchar](218),
[Agency__c] [nvarchar](218),
[Business_Unit__c] [nvarchar](218),
[Account_Opt_In_Status__c] [nvarchar](218),
[RecordTypeId] [nvarchar](218)
) ON [PRIMARY];

INSERT INTO #temp_Turkey_Acct
EXECUTE [dbo].[Turkey_Accounts_sp] @FilterName=@FilterName;

SELECT [Name]
      ,[Advertiser__c]
      ,[Agency__c]
      ,[Business_Unit__c]
      ,[Account_Opt_In_Status__c]
      ,[RecordTypeId]
FROM #temp_Turkey_Acct
WHERE Id IS NULL



CREATE PROCEDURE [dbo].[Turkey_Opps_New_sp] @FilterName VARCHAR(50) AS
--DECLARE  @FilterName VARCHAR(50) = 'Turkey_LR';

IF OBJECT_ID('tempdb..#Turkey_Accounts') IS NOT NULL DROP TABLE #Turkey_Accounts;

CREATE TABLE #Turkey_Accounts(
Name VARCHAR(100),
Id VARCHAR(50),
Advertiser__c VARCHAR(50),
Agency__c  VARCHAR(50),
Business_Unit__c VARCHAR(50),
Account_Opt_In_Status__c VARCHAR(50),
RecordTypeId VARCHAR(50))
ON [PRIMARY]; 
INSERT INTO #Turkey_Accounts
EXEC Turkey_Accounts_sp @FilterName;


SELECT DISTINCT 'Closed Won' StageName
--	  ,CONVERT(VARCHAR(8), CONVERT(DATE,F8,3),3) AS CloseDate
	  ,dd.CloseDate
	  ,acct.Advertiser__c AS Advertiser__c
	  ,acct.Agency__c AS Agency__c
	  ,(SELECT TOP 1 [Id] FROM [RecordType] WHERE SobjectType = 'Opportunity' AND DeveloperName = 'Turkey') AS RecordTypeId
	  ,acct.Id AS AccountId
--	  ,'SpainOpp-'+[Field 4] AS External_Id__c
	  ,dbo.ReplaceExtraChars(LTRIM(RTRIM([Field 11]))) AS 'Name'
	  ,CASE WHEN sf_opp.Name IS NULL THEN 'Turkey:'+dbo.ReplaceExtraChars(LTRIM(RTRIM([Field 11])))
	        WHEN sf_opp_dup.Name IS NULL THEN 'Turkey:'+dbo.ReplaceExtraChars(LTRIM(RTRIM([Field 11])))
		ELSE dbo.ReplaceExtraChars(LTRIM(RTRIM([Field 11])))
		END AS Name
	  ,'TRY' AS CurrencyIsoCode
--	  ,sf_opp.Id
FROM XaxisETL.[dbo].[Extract_Turkey] eb
	INNER JOIN (
				SELECT 'MX' AS [Turk], 'Maxus (Turkey)' AS [SF] UNION
				SELECT 'MEC' , 'MEC (Turkey)'		UNION
				SELECT 'MC' , 'Mediacom (Turkey)'	UNION
				SELECT 'MS' , 'Mindshare (Turkey)'
				) ags	
		ON eb.[Field 9] = ags.Turk
	INNER JOIN (SELECT dbo.ReplaceExtraChars(LTRIM(RTRIM([Field 11]))) AS 'Name'
					  ,MAX(CONVERT(DATE,[Field 7], 101)) AS CloseDate
			    FROM XaxisETL.dbo.Extract_Turkey
				WHERE [Field 9] IS NOT NULL
				  AND [Field 9] <> 'Agency'
				  AND [Field 9] <> 'Needs mapping to Turkish Agencies int the system'
				  AND [Field 9] <> 'Campaign Details'
				GROUP BY dbo.ReplaceExtraChars(LTRIM(RTRIM([Field 11])))
				) dd
			ON dbo.ReplaceExtraChars(LTRIM(RTRIM(eb.[Field 11]))) = dd.Name
	LEFT JOIN #Turkey_Accounts acct
		LEFT JOIN Company__c adv
			ON acct.Advertiser__c = adv.Id
		LEFT JOIN Company__c ag
			ON acct.Agency__c = ag.Id
		ON ag.Name = ags.SF
	    AND master.dbo.InitCap(LOWER(REPLACE(REPLACE(dbo.ReplaceExtraChars(eb.[Field 10]), ' ', ''), '''', ''))) = REPLACE(REPLACE(adv.Name, ' ', ''), '''', '')
		AND ag.Market__c = 'Turkey'
	LEFT JOIN Opportunity sf_opp
		ON  dbo.ReplaceExtraChars(LTRIM(RTRIM(dbo.ReplaceExtraChars(eb.[Field 11])))) = REPLACE(sf_opp.Name,'Turkey:','')
	   AND sf_opp.AccountId = acct.Id
	LEFT JOIN Opportunity sf_opp_dup
		ON  dbo.ReplaceExtraChars(LTRIM(RTRIM(dbo.ReplaceExtraChars(eb.[Field 11])))) = sf_opp_dup.Name

WHERE [Field 9] IS NOT NULL
  AND [Field 9] <> 'Agency'
  AND [Field 9] <> 'Needs mapping to Turkish Agencies int the system'
  AND [Field 9] <> 'Campaign Details'
  AND sf_opp.Id IS NULL
  AND eb.Filter = @FilterName


CREATE PROCEDURE [dbo].[Turkey_Sell_Lines_New_sp] @FilterName VARCHAR(50) AS
--DECLARE  @FilterName VARCHAR(50) = 'Turkey_LR';

IF OBJECT_ID('tempdb..#temp_opp') IS NOT NULL DROP TABLE #temp_opp;

SELECT Opportunity.Name
	  ,Opportunity.Id
	  ,adv.Name AS adv_Name
	  ,Opportunity.Agency_Market__c
INTO #temp_opp
FROM Opportunity
	INNER JOIN Account
		ON Opportunity.AccountId = Account.Id
	INNER JOIN Company__c adv
		ON Account.Advertiser__c = adv.Id
WHERE Opportunity.Agency_Market__c = 'Turkey';


SELECT opp.Id AS Opportunity__c
--	  ,CONVERT(VARCHAR(1000), HashBytes('MD5', [Field 2] + [Field 13]), 2) AS External_ID__c
--      ,CAST(CHECKSUM(dbo.ReplaceExtraChars([Field 13]), dbo.ReplaceExtraChars([Field 16]), dbo.ReplaceExtraChars([Field 2])) AS VARCHAR(25)) AS External_ID__c
	  ,'TurkeyLR:' + CAST(et.External_pk AS VARCHAR) AS External_ID__c
--	  ,dbo.ReplaceExtraChars([Field 15]) AS Product_Detail__c
	  ,ISNULL(pro.sf_value, dbo.ReplaceExtraChars([Field 15]))  AS Product_Detail__c
      ,CASE WHEN CAST(CONVERT(DATE,[Field 6],101) AS DATE) > CAST(CONVERT(DATE,[Field 7],101) AS DATE) THEN CAST(CONVERT(DATE,[Field 7],101) AS DATE)
			ELSE CAST(CONVERT(DATE,[Field 6],101) AS DATE) 
		END AS Start_Date__c
      ,CASE WHEN CAST(CONVERT(DATE,[Field 7],101) AS DATE) < CAST(CONVERT(DATE,[Field 6],101) AS DATE) THEN CAST(CONVERT(DATE,[Field 6],101) AS DATE)
			ELSE CAST(CONVERT(DATE,[Field 7],101) AS DATE)
		END AS End_Date__c
      ,dbo.ReplaceExtraChars([Field 2]) AS Buy_Name_txt__c
      ,'Net Cost (Calc Margin)' AS Imputing_Margin_or_Net__c
      ,'MediaTrader' AS PackageType__c
      ,(SELECT TOP 1 Id FROM RecordType WHERE SobjectType = 'Opportunity_Buy__c' AND DeveloperName = 'Turkey') AS RecordTypeId
      ,dbo.ReplaceExtraChars([Field 13]) AS Supplier_Name__c
      ,CONVERT(MONEY, REPLACE(REPLACE(REPLACE(dbo.ReplaceExtraChars(ISNULL([Field 21], 0)), ',',''), 'TL', ''), 'click', '')) AS Buy_Volume__c
--      ,CONVERT(MONEY, REPLACE(REPLACE(dbo.ReplaceExtraChars(ISNULL([Field 31], 0)), ',',''), 'TL', '')) AS Gross_Cost__c
--	  ,CONVERT(MONEY, REPLACE(REPLACE(dbo.ReplaceExtraChars(ISNULL([Field 32], 0)), ',',''), 'TL', '')) AS Net_Cost__c
	  ,CONVERT(MONEY, REPLACE(REPLACE(dbo.ReplaceExtraChars(ISNULL([Field 31], 0)), ',',''), 'TL', '')) AS Original_Gross_Budget__c
--	  ,CONVERT(MONEY, REPLACE(REPLACE(dbo.ReplaceExtraChars(ISNULL([Field 29], 0)), ',',''), 'TL', '')) AS Gross_Cost__c
	  ,CONVERT(MONEY, REPLACE(REPLACE(dbo.ReplaceExtraChars(ISNULL([Field 32], 0)), ',',''), 'TL', '')) AS Gross_Cost__c
      ,CONVERT(MONEY, REPLACE(REPLACE(dbo.ReplaceExtraChars(ISNULL([Field 20], 0)), ',',''), 'TL', '')) AS Rate__c
--      ,CONVERT(MONEY, REPLACE(REPLACE(dbo.ReplaceExtraChars(ISNULL([Field 32], 0)), ',',''), 'TL', '')) AS Media_Net_Cost__c
	  ,CONVERT(MONEY, REPLACE(REPLACE(dbo.ReplaceExtraChars(ISNULL([Field 29], 0)), ',',''), 'TL', '')) AS Media_Net_Cost__c
      ,dbo.ReplaceExtraChars(ISNULL([Field 16], 'CPM')) AS Buy_Type__c
      ,'Triggers' AS Audience_Tier__c
      ,CONVERT(MONEY, REPLACE(REPLACE(dbo.ReplaceExtraChars(ISNULL([Field 37], 0)), ',',''), '%',''))/100 AS Current_Margin__c
      ,'From spreadsheet: ' + et.Title AS Current_Margin_Explanation__c
      ,dbo.ReplaceExtraChars([Field 17]) AS Opp_Buy_Description__c
      ,'Externally Managed' AS Input_Mode__c
--	  ,'Digital' AS Media_Code__c
	  ,ISNULL(ch.sf_value, 'Digital') AS Media_Code__c
	  ,dbo.ReplaceExtraChars([Field 18]) AS Formats__c
--	  ,sl.Id
--	  ,et.Filter
FROM XaxisETL.dbo.Extract_Turkey et
	LEFT JOIN #temp_opp opp
--		ON dbo.ReplaceExtraChars(LTRIM(RTRIM(et.[Field 11]))) = opp.Name
		ON dbo.ReplaceExtraChars(LTRIM(RTRIM(dbo.ReplaceExtraChars(et.[Field 11])))) = REPLACE(opp.Name,'Turkey:','')
	   AND master.dbo.InitCap(LOWER(REPLACE(REPLACE(dbo.ReplaceExtraChars(et.[Field 10]), ' ', ''), '''', ''))) = REPLACE(REPLACE(opp.adv_Name, ' ', ''), '''', '')
--	   AND CAST(External_pk AS VARCHAR) = REPL
ACE(et.External_ID__c, 'TurkeyOpp-', '')
	LEFT JOIN (SELECT Id	
				      ,External_Id__c
			   FROM dbo.Opportunity_Buy__c) sl
--		ON CAST(CHECKSUM(dbo.ReplaceExtraChars([Field 13]), dbo.ReplaceExtraChars([Field 16]), dbo.ReplaceExtraChars([Field 2])) AS VARCHAR(25)) = sl.External_Id__c
		ON 'TurkeyLR:' + CAST(et.External_pk AS VARCHAR) = sl.External_Id__c
	LEFT JOIN (SELECT m_value
					 ,sf_value
			   FROM XaxisETL.dbo.SF_Mapping
			   WHERE sf_market = 'Turkey'
			     AND sf_type = 'Products'
			   ) pro
		ON LTRIM(RTRIM([Field 15])) = pro.m_value
	LEFT JOIN (SELECT m_value
					 ,sf_value
			    FROM XaxisETL.dbo.SF_Mapping
				WHERE sf_market = 'Turkey'
				  AND sf_type = 'Channels'
				) ch
		ON LTRIM(RTRIM([Field 15])) = ch.m_value
WHERE [Field 9] IS NOT NULL
  AND [Field 9] <> 'Agency'
  AND [Field 9] <> 'Needs mapping to Turkish Agencies int the system'
  AND [Field 9] <> 'Campaign Details'
--  AND sl.External_Id__c IS NULL   
  AND Filter = @FilterName
  AND sl.Id IS NULL










--CREATE VIEW [dbo].[Turkey_Sell_Lines] AS

CREATE PROCEDURE [dbo].[Turkey_Sell_Lines_sp] @FilterName VARCHAR(50) AS
--DECLARE  @FilterName VARCHAR(50) = 'Turkey_LR';

IF OBJECT_ID('tempdb..#temp_opp') IS NOT NULL DROP TABLE #temp_opp;

SELECT Opportunity.Name
	  ,Opportunity.Id
	  ,adv.Name AS adv_Name
INTO #temp_opp
FROM Opportunity
	INNER JOIN Account
		ON Opportunity.AccountId = Account.Id
	INNER JOIN Company__c adv
		ON Account.Advertiser__c = adv.Id;


SELECT opp.Id AS Opportunity__c
--	  ,CONVERT(VARCHAR(1000), HashBytes('MD5', [Field 2] + [Field 13]), 2) AS External_ID__c
--      ,CAST(CHECKSUM(dbo.ReplaceExtraChars([Field 13]), dbo.ReplaceExtraChars([Field 16]), dbo.ReplaceExtraChars([Field 2])) AS VARCHAR(25)) AS External_ID__c
	  ,'TurkeyLR:' + CAST(et.External_pk AS VARCHAR) AS External_ID__c
--	  ,dbo.ReplaceExtraChars([Field 15]) AS Product_Detail__c
	  ,ISNULL(pro.sf_value, dbo.ReplaceExtraChars([Field 15]))  AS Product_Detail__c
      ,CAST(CONVERT(DATE,[Field 6],101) AS DATE) AS Start_Date__c
      ,CAST(CONVERT(DATE,[Field 7],101) AS DATE) AS End_Date__c
      ,dbo.ReplaceExtraChars([Field 2]) AS Buy_Name_txt__c
      ,'Net Cost (Calc Margin)' AS Imputing_Margin_or_Net__c
      ,'MediaTrader' AS PackageType__c
      ,(SELECT TOP 1 Id FROM RecordType WHERE SobjectType = 'Opportunity_Buy__c' AND DeveloperName = 'Turkey') AS RecordTypeId
      ,dbo.ReplaceExtraChars([Field 13]) AS Supplier_Name__c
      ,CONVERT(MONEY, REPLACE(REPLACE(REPLACE(dbo.ReplaceExtraChars(ISNULL([Field 21], 0)), ',',''), 'TL', ''), 'click', '')) AS Buy_Volume__c
--      ,CONVERT(MONEY, REPLACE(REPLACE(dbo.ReplaceExtraChars(ISNULL([Field 31], 0)), ',',''), 'TL', '')) AS Gross_Cost__c
--	  ,CONVERT(MONEY, REPLACE(REPLACE(dbo.ReplaceExtraChars(ISNULL([Field 32], 0)), ',',''), 'TL', '')) AS Net_Cost__c
	  ,CONVERT(MONEY, REPLACE(REPLACE(dbo.ReplaceExtraChars(ISNULL([Field 31], 0)), ',',''), 'TL', '')) AS Original_Gross_Budget__c
--	  ,CONVERT(MONEY, REPLACE(REPLACE(dbo.ReplaceExtraChars(ISNULL([Field 29], 0)), ',',''), 'TL', '')) AS Gross_Cost__c
	  ,CONVERT(MONEY, REPLACE(REPLACE(dbo.ReplaceExtraChars(ISNULL([Field 32], 0)), ',',''), 'TL', '')) AS Gross_Cost__c
      ,CONVERT(MONEY, REPLACE(REPLACE(dbo.ReplaceExtraChars(ISNULL([Field 20], 0)), ',',''), 'TL', '')) AS Rate__c
--      ,CONVERT(MONEY, REPLACE(REPLACE(dbo.ReplaceExtraChars(ISNULL([Field 32], 0)), ',',''), 'TL', '')) AS Media_Net_Cost__c
	  ,CONVERT(MONEY, REPLACE(REPLACE(dbo.ReplaceExtraChars(ISNULL([Field 29], 0)), ',',''), 'TL', '')) AS Media_Net_Cost__c
      ,dbo.ReplaceExtraChars(ISNULL([Field 16], 'CPM')) AS Buy_Type__c
      ,'Triggers' AS Audience_Tier__c
      ,CONVERT(MONEY, REPLACE(REPLACE(dbo.ReplaceExtraChars(ISNULL([Field 37], 0)), ',',''), '%',''))/100 AS Current_Margin__c
      ,'From spreadsheet: ' + et.Title AS Current_Margin_Explanation__c
      ,dbo.ReplaceExtraChars([Field 17]) AS Opp_Buy_Description__c
      ,'Externally Managed' AS Input_Mode__c
--	  ,'Digital' AS Media_Code__c
	  ,ISNULL(ch.sf_value, 'Digital') AS Media_Code__c
	  ,dbo.ReplaceExtraChars([Field 18]) AS Formats__c
	  ,sl.Id
--	  ,et.Filter
FROM XaxisETL.dbo.Extract_Turkey et
	LEFT JOIN #temp_opp opp
		ON dbo.ReplaceExtraChars(LTRIM(RTRIM(et.[Field 11]))) = opp.Name
	   AND master.dbo.InitCap(LOWER(REPLACE(REPLACE(dbo.ReplaceExtraChars(et.[Field 10]), ' ', ''), '''', ''))) = REPLACE(REPLACE(opp.adv_Name, ' ', ''), '''', '')
--	   AND CAST(External_pk AS VARCHAR) = REPLACE(et.External_ID__c, 'TurkeyOpp-', '')
	LEFT JOIN (SELECT Id	
				      ,External_Id__c
			   FROM dbo.Opportunity_Buy__c) sl
--		ON CAST(CHECKSUM(dbo.ReplaceExtraChars([Field 13]), dbo.ReplaceExtraChars([Field 16]), dbo.ReplaceExtraChars([Field 2])) AS VARCHAR(25)) = sl.External_Id__c
		ON 'TurkeyLR:' + CAST(et.External_pk AS VARCHAR) = sl.External_Id__c
	LEFT JOIN (SELECT m_value
					 ,sf_value
			   FROM XaxisETL.dbo.SF_Mapping
			   WHERE sf_market = 'Tur
key'
			     AND sf_type = 'Products'
			   ) pro
		ON LTRIM(RTRIM([Field 15])) = pro.m_value
	LEFT JOIN (SELECT m_value
					 ,sf_value
			    FROM XaxisETL.dbo.SF_Mapping
				WHERE sf_market = 'Turkey'
				  AND sf_type = 'Channels'
				) ch
		ON LTRIM(RTRIM([Field 15])) = ch.m_value
WHERE [Field 9] IS NOT NULL
  AND [Field 9] <> 'Agency'
  AND [Field 9] <> 'Needs mapping to Turkish Agencies int the system'
  AND [Field 9] <> 'Campaign Details'
  AND sl.External_Id__c IS NULL   
  AND Filter = @FilterName
  AND sl.Id IS NULL









/****** Object:  StoredProcedure [dbo].[TurkeyUpdate_Sell_Line_sp]    Script Date: 11/29/2016 1:21:30 PM ******/

CREATE PROCEDURE [dbo].[Turkey_Sell_Lines_Update_sp] @FilterName VARCHAR(50) AS
--DECLARE  @FilterName VARCHAR(50) = 'Turkey_LR';

IF OBJECT_ID('tempdb..#sql_sell_line') IS NOT NULL DROP TABLE #sql_sell_line;
IF OBJECT_ID('tempdb..#sf_sell_line') IS NOT NULL DROP TABLE #sf_sell_line;

IF OBJECT_ID('tempdb..#hash_sql') IS NOT NULL DROP TABLE #hash_sql;
IF OBJECT_ID('tempdb..#hash_sf') IS NOT NULL DROP TABLE #hash_sf;

CREATE TABLE #hash_sql(
[Opportunity__c] [nvarchar](218) NULL,
[External_Id__c] [nvarchar](222) NULL,
[Product_Detail__c] [nvarchar](242) NULL,
[Start_Date__c] [datetime] NULL,
[End_Date__c] [datetime] NULL,
[Buy_Name_txt__c] [nvarchar](456) NULL,
[Imputing_Margin_or_Net__c] [nvarchar](233) NULL,
[PackageType__c] [nvarchar](250) NULL,
[RecordTypeId] [nvarchar](218) NULL,
[Supplier_Name__c] [nvarchar](218) NULL,
[Buy_Volume__c] [float] NULL,
[Original_Gross_Budget__c] [float] NULL,
[Gross_Cost__c] [float] NULL,
[Rate__c] [float] NULL,
[Media_Net_Cost__c] [float] NULL,
[Buy_Type__c] [nvarchar](209) NULL,
[Audience_Tier__c] [nvarchar](265) NULL,
[Current_Margin__c] [float] NULL,
[Current_Margin_Explanation__c] [nvarchar](267) NULL,
[Opp_Buy_Description__c] [nvarchar](3924) NULL,
[Input_Mode__c] [nvarchar](224) NULL,
[Media_Code__c] [nvarchar](215) NULL,
[Formats__c] [nvarchar](264) NULL,
[Id] [nvarchar](218) NULL,
) ON [PRIMARY];

CREATE TABLE #hash_sf(
[Opportunity__c] [nvarchar](218) NULL,
[External_Id__c] [nvarchar](222) NULL,
[Product_Detail__c] [nvarchar](242) NULL,
[Start_Date__c] [datetime] NULL,
[End_Date__c] [datetime] NULL,
[Buy_Name_txt__c] [nvarchar](456) NULL,
[Imputing_Margin_or_Net__c] [nvarchar](233) NULL,
[PackageType__c] [nvarchar](250) NULL,
--[RecordTypeId] [nvarchar](218) NULL,
[Supplier_Name__c] [nvarchar](218) NULL,
[Buy_Volume__c] [float] NULL,
--[Original_Gross_Budget__c] [float] NULL,
[Gross_Cost__c] [float] NULL,
[Rate__c] [float] NULL,
[Media_Net_Cost__c] [float] NULL,
[Buy_Type__c] [nvarchar](209) NULL,
[Audience_Tier__c] [nvarchar](265) NULL,
[Current_Margin__c] [float] NULL,
[Current_Margin_Explanation__c] [nvarchar](267) NULL,
[Opp_Buy_Description__c] [nvarchar](3924) NULL,
[Input_Mode__c] [nvarchar](224) NULL,
[Media_Code__c] [nvarchar](215) NULL,
[Formats__c] [nvarchar](264) NULL,
[Id] [nvarchar](218) NULL,
) ON [PRIMARY];

INSERT INTO #hash_sql
EXEC Turkey_Sell_Lines_sp 'Turkey_LR';

INSERT INTO #hash_sf
SELECT [Opportunity__c]
		,[External_ID__c]
		,[Product_Detail__c]
		,[Start_Date__c]
		,[End_Date__c]
		,[Buy_Name_txt__c]
		,[Imputing_Margin_or_Net__c]
		,[PackageType__c]
--		,[RecordTypeId]
		,[Supplier_Name__c]
		,[Buy_Volume__c]
--		,[Original_Gross_Budget__c]
		,[Gross_Cost__c]
		,[Rate__c]
		,[Media_Net_Cost__c]
		,LOWER([Buy_Type__c]) AS Buy_Type__c
		,[Audience_Tier__c]
		,[Current_Margin__c]
		,[Current_Margin_Explanation__c]
		,[Opp_Buy_Description__c]
		,[Input_Mode__c]
		,[Media_Code__c]
		,[Formats__c]
		,[Id]
FROM Production.dbo.Opportunity_Buy__c;



SELECT sq.[Opportunity__c]
      ,sq.[External_ID__c]
      ,sq.[Product_Detail__c]
      ,sq.[Start_Date__c]
      ,sq.[End_Date__c]
      ,sq.[Buy_Name_txt__c]
      ,sq.[Imputing_Margin_or_Net__c]
      ,sq.[PackageType__c]
--      ,sq.[RecordTypeId]
      ,sq.[Supplier_Name__c]
      ,sq.[Buy_Volume__c]
--	  ,sq.[Original_Gross_Budget__c]
      ,sq.[Gross_Cost__c]
      ,sq.[Rate__c]
      ,sq.[Media_Net_Cost__c]
      ,sq.[Buy_Type__c]
      ,sq.[Audience_Tier__c]
      ,sq.[Current_Margin__c]
      ,sq.[Current_Margin_Explanation__c]
      ,sq.[Opp_Buy_Description__c]
      ,sq.[Input_Mode__c]
      ,sq.[Media_Code__c]
      ,sq.[Formats__c]
      ,sq.[Id]
	  ,HASHBYTES('SHA1', (SELECT TOP 1 * FROM #hash_sql bb WHERE bb.External_ID__c = sq.External_ID__c FOR XML RAW)) AS sq_hash
IN
TO #sql_sell_line
 FROM #hash_sql sq
WHERE sq.Id IS NOT NULL;

SELECT  [Opportunity__c]
		,sf_in.[External_ID__c]
		,[Product_Detail__c]
		,[Start_Date__c]
		,[End_Date__c]
		,[Buy_Name_txt__c]
		,[Imputing_Margin_or_Net__c]
		,[PackageType__c]
--		,[RecordTypeId]
		,[Supplier_Name__c]
		,[Buy_Volume__c]
--		,[Original_Gross_Budget__c]
		,[Gross_Cost__c]
		,[Rate__c]
		,[Media_Net_Cost__c]
		,[Buy_Type__c]
		,[Audience_Tier__c]
		,[Current_Margin__c]
		,[Current_Margin_Explanation__c]
		,[Opp_Buy_Description__c]
		,[Input_Mode__c]
		,[Media_Code__c]
		,[Formats__c]
		,[Id]
		,HASHBYTES('SHA1', (SELECT TOP 1 * FROM #hash_sf cc WHERE cc.External_ID__c = sf_in.External_ID__c FOR XML RAW)) AS sf_hash
INTO #sf_sell_line
FROM Opportunity_Buy__c sf_in
	INNER JOIN (SELECT External_Id__c FROM Production.dbo.Turkey_Sell_Lines) sq
		ON sf_in.External_Id__c = sq.External_ID__c



SELECT   [Opportunity__c]
		,[External_ID__c]
		,[Product_Detail__c]
		,[Start_Date__c]
		,[End_Date__c]
		,[Buy_Name_txt__c]
		,[Imputing_Margin_or_Net__c]
		,[PackageType__c]
--		,[RecordTypeId]
		,[Supplier_Name__c]
		,[Buy_Volume__c]
--		,[Original_Gross_Budget__c]
		,[Gross_Cost__c]
		,[Rate__c]
		,[Media_Net_Cost__c]
		,[Buy_Type__c]
		,[Audience_Tier__c]
		,[Current_Margin__c]
		,[Current_Margin_Explanation__c]
		,[Opp_Buy_Description__c]
		,[Input_Mode__c]
		,[Media_Code__c]
		,[Formats__c]
		,[Id]
FROM #sql_sell_line sq
	LEFT JOIN ( SELECT sf_hash
				FROM #sf_sell_line
			   ) sf
		ON sq.sq_hash = sf.sf_hash
WHERE sf.sf_hash IS NULL



/*
SELECT *
FROM #hash_sql
UNION 
SELECT #hash_sf.*
FROM #hash_sf
	INNER JOIN (SELECT External_Id__c FROM Production.dbo.Turkey_Sell_Lines) sq
		ON #hash_sf.External_Id__c = sq.External_ID__c

*/

/****** Object:  View [dbo].[Turkey_Buy_Placement]    Script Date: 11/29/2016 3:48:28 PM ******/
--DROP VIEW [dbo].[Turkey_Buy_Placement]

CREATE PROCEDURE [dbo].[Turkey_Buy_Placement_sp] @FilterName VARCHAR(50) AS
--DECLARE  @FilterName VARCHAR(50) = 'Turkey_LR';

IF OBJECT_ID('tempdb..#temp_opp') IS NOT NULL DROP TABLE #temp_opp;

SELECT Opportunity.Name
	  ,Opportunity.Id
	  ,adv.Name AS adv_Name
INTO #temp_opp
FROM Opportunity
	INNER JOIN Account
		ON Opportunity.AccountId = Account.Id
	INNER JOIN Company__c adv
		ON Account.Advertiser__c = adv.Id;


SELECT CASE WHEN LEN(dbo.ReplaceExtraChars([Field 2]) + ' - ' + sl.External_Id__c) > 80
			THEN LEFT(dbo.ReplaceExtraChars([Field 2]), 80-LEN(' - ' + sl.External_Id__c)) + ' - ' + sl.External_Id__c
			ELSE dbo.ReplaceExtraChars([Field 2]) + ' - ' + sl.External_Id__c
		END AS Name
	  ,CONVERT(MONEY, REPLACE(REPLACE(dbo.ReplaceExtraChars(ISNULL([Field 19], 0)), ',',''), 'TL', '')) AS Rate__c
	  ,CONVERT(MONEY, REPLACE(REPLACE(dbo.ReplaceExtraChars(ISNULL([Field 28], 0)), ',',''), 'TL', '')) AS Planned_Units__c
      ,CONVERT(MONEY, REPLACE(REPLACE(dbo.ReplaceExtraChars(ISNULL([Field 29], 0)), ',',''), 'TL', '')) AS Planned_Cost__c
	  
	  ,sl.Id AS Sell_Line__c
	  ,dbo.ReplaceExtraChars([Field 17]) AS Creative_Format__c
	  ,CASE WHEN CAST(CONVERT(DATE,[Field 6],101) AS DATE) > CAST(CONVERT(DATE,[Field 7],101) AS DATE) THEN CAST(CONVERT(DATE,[Field 7],101) AS DATE)
			ELSE CAST(CONVERT(DATE,[Field 6],101) AS DATE)
		END AS Start_Date__c
      ,CASE WHEN CAST(CONVERT(DATE,[Field 7],101) AS DATE) < CAST(CONVERT(DATE,[Field 6],101) AS DATE) THEN CAST(CONVERT(DATE,[Field 6],101) AS DATE)
			ELSE CAST(CONVERT(DATE,[Field 7],101) AS DATE) 
		END AS End_Date__c
	  ,bp.Id
FROM XaxisETL.dbo.Extract_Turkey et
	LEFT JOIN #temp_opp opp
		ON dbo.ReplaceExtraChars(LTRIM(RTRIM(et.[Field 11]))) = opp.Name
	   AND master.dbo.InitCap(LOWER(REPLACE(REPLACE(dbo.ReplaceExtraChars(et.[Field 10]), ' ', ''), '''', ''))) = REPLACE(REPLACE(opp.adv_Name, ' ', ''), '''', '')
	LEFT JOIN (SELECT Id	
				      ,External_Id__c
			   FROM dbo.Opportunity_Buy__c) sl
--		ON CAST(CHECKSUM(dbo.ReplaceExtraChars([Field 13]), dbo.ReplaceExtraChars([Field 16]), dbo.ReplaceExtraChars([Field 2])) AS VARCHAR(25)) = sl.External_Id__c
		ON 'TurkeyLR:' + CAST(et.External_pk AS VARCHAR) = sl.External_Id__c
	LEFT JOIN (SELECT Id	
					 ,Name
			   FROM dbo.Buy_Placement__c) bp
		ON CASE WHEN LEN(dbo.ReplaceExtraChars([Field 2]) + ' - ' + sl.External_Id__c) > 80
				THEN LEFT(dbo.ReplaceExtraChars([Field 2]), 80-LEN(' - ' + sl.External_Id__c)) + ' - ' + sl.External_Id__c
				ELSE dbo.ReplaceExtraChars([Field 2]) + ' - ' + sl.External_Id__c
		   END = bp.Name
WHERE [Field 9] IS NOT NULL
  AND [Field 9] <> 'Agency'
  AND [Field 9] <> 'Needs mapping to Turkish Agencies int the system'
  AND [Field 9] <> 'Campaign Details'
  AND et.Filter = @FilterName
--  AND sl.External_Id__c IS NULL   










/****** Object:  View [dbo].[Turkey_Buy_Placement]    Script Date: 11/29/2016 3:48:28 PM ******/
--DROP VIEW [dbo].[Turkey_Buy_Placement]

CREATE PROCEDURE [dbo].[Turkey_Buy_Placement_New_sp] @FilterName VARCHAR(50) AS
--DECLARE  @FilterName VARCHAR(50) = 'Turkey_LR';

IF OBJECT_ID('tempdb..#temp_opp') IS NOT NULL DROP TABLE #temp_opp;

SELECT Opportunity.Name
	  ,Opportunity.Id
	  ,adv.Name AS adv_Name
INTO #temp_opp
FROM Opportunity
	INNER JOIN Account
		ON Opportunity.AccountId = Account.Id
	INNER JOIN Company__c adv
		ON Account.Advertiser__c = adv.Id;


SELECT CASE WHEN LEN(dbo.ReplaceExtraChars([Field 2]) + ' - ' + sl.External_Id__c) > 80
			THEN LEFT(dbo.ReplaceExtraChars([Field 2]), 80-LEN(' - ' + sl.External_Id__c)) + ' - ' + sl.External_Id__c
			ELSE dbo.ReplaceExtraChars([Field 2]) + ' - ' + sl.External_Id__c
		END AS Name
	  ,CONVERT(MONEY, REPLACE(REPLACE(dbo.ReplaceExtraChars(ISNULL([Field 19], 0)), ',',''), 'TL', '')) AS Rate__c
	  ,CONVERT(MONEY, REPLACE(REPLACE(dbo.ReplaceExtraChars(ISNULL([Field 28], 0)), ',',''), 'TL', '')) AS Planned_Units__c
      ,CONVERT(MONEY, REPLACE(REPLACE(dbo.ReplaceExtraChars(ISNULL([Field 29], 0)), ',',''), 'TL', '')) AS Planned_Cost__c
	  
	  ,sl.Id AS Sell_Line__c
	  ,dbo.ReplaceExtraChars([Field 17]) AS Creative_Format__c
	  ,CASE WHEN CAST(CONVERT(DATE,[Field 6],101) AS DATE) > CAST(CONVERT(DATE,[Field 7],101) AS DATE) THEN CAST(CONVERT(DATE,[Field 7],101) AS DATE)
			ELSE CAST(CONVERT(DATE,[Field 6],101) AS DATE)
		END AS Start_Date__c
      ,CASE WHEN CAST(CONVERT(DATE,[Field 7],101) AS DATE) < CAST(CONVERT(DATE,[Field 6],101) AS DATE) THEN CAST(CONVERT(DATE,[Field 6],101) AS DATE)
			ELSE CAST(CONVERT(DATE,[Field 7],101) AS DATE) 
		END AS End_Date__c
--	  ,bp.Id
FROM XaxisETL.dbo.Extract_Turkey et
	LEFT JOIN #temp_opp opp
		ON dbo.ReplaceExtraChars(LTRIM(RTRIM(et.[Field 11]))) = opp.Name
	   AND master.dbo.InitCap(LOWER(REPLACE(REPLACE(dbo.ReplaceExtraChars(et.[Field 10]), ' ', ''), '''', ''))) = REPLACE(REPLACE(opp.adv_Name, ' ', ''), '''', '')
	LEFT JOIN (SELECT Id	
				      ,External_Id__c
			   FROM dbo.Opportunity_Buy__c) sl
--		ON CAST(CHECKSUM(dbo.ReplaceExtraChars([Field 13]), dbo.ReplaceExtraChars([Field 16]), dbo.ReplaceExtraChars([Field 2])) AS VARCHAR(25)) = sl.External_Id__c
		ON 'TurkeyLR:' + CAST(et.External_pk AS VARCHAR) = sl.External_Id__c
	LEFT JOIN (SELECT Id	
					 ,Name
			   FROM dbo.Buy_Placement__c) bp
		ON CASE WHEN LEN(dbo.ReplaceExtraChars([Field 2]) + ' - ' + sl.External_Id__c) > 80
				THEN LEFT(dbo.ReplaceExtraChars([Field 2]), 80-LEN(' - ' + sl.External_Id__c)) + ' - ' + sl.External_Id__c
				ELSE dbo.ReplaceExtraChars([Field 2]) + ' - ' + sl.External_Id__c
		   END = bp.Name
WHERE [Field 9] IS NOT NULL
  AND [Field 9] <> 'Agency'
  AND [Field 9] <> 'Needs mapping to Turkish Agencies int the system'
  AND [Field 9] <> 'Campaign Details'
  AND et.Filter = @FilterName
  AND bp.Id IS NULL
--  AND sl.External_Id__c IS NULL   









CREATE PROCEDURE [dbo].[Forecast_NTAM_sp] AS

IF OBJECT_ID('tempdb..#unP') IS NOT NULL DROP TABLE #unP;
IF OBJECT_ID('tempdb..#forecast_pivot') IS NOT NULL DROP TABLE #forecast_pivot;

WITH row_number_add AS (
	SELECT *
	      ,ROW_NUMBER() OVER (PARTITION BY [Source worksheet] ORDER BY  Row) AS row_sheet
	FROM XaxisETL.dbo.Forecast_NTAM
	)
,non_sort AS (
			SELECT [Source worksheet] AS 'Agency'
				  ,[Field 2] AS 'Advertiser'
				  ,[Field 3]
				  ,CASE WHEN LEFT([Field 4], 1) = '(' AND RIGHT([Field 4], 1) =')' THEN     '-' +REPLACE(REPLACE(REPLACE([Field 4]  , '(', ''), ')',''), ',','') ELSE [Field 4]  END AS 'Jan'
				  ,CASE WHEN LEFT([Field 15], 1) = '(' AND RIGHT([Field 15], 1) =')' THEN   '-' +REPLACE(REPLACE(REPLACE([Field 15] , '(', ''), ')',''), ',','') ELSE [Field 15] END AS 'Feb'
				  ,CASE WHEN LEFT([Field 26], 1) = '(' AND RIGHT([Field 26], 1) =')' THEN   '-' +REPLACE(REPLACE(REPLACE([Field 26] , '(', ''), ')',''), ',','') ELSE [Field 26] END AS 'Mar'
				  ,CASE WHEN LEFT([Field 37], 1) = '(' AND RIGHT([Field 37], 1) =')' THEN   '-' +REPLACE(REPLACE(REPLACE([Field 37] , '(', ''), ')',''), ',','') ELSE [Field 37] END AS 'Apr'
				  ,CASE WHEN LEFT([Field 48], 1) = '(' AND RIGHT([Field 48], 1) =')' THEN   '-' +REPLACE(REPLACE(REPLACE([Field 48] , '(', ''), ')',''), ',','') ELSE [Field 48] END AS 'May'
				  ,CASE WHEN LEFT([Field 59], 1) = '(' AND RIGHT([Field 59], 1) =')' THEN   '-' +REPLACE(REPLACE(REPLACE([Field 59] , '(', ''), ')',''), ',','') ELSE [Field 59] END AS 'Jun'
				  ,CASE WHEN LEFT([Field 70], 1) = '(' AND RIGHT([Field 70], 1) =')' THEN   '-' +REPLACE(REPLACE(REPLACE([Field 70] , '(', ''), ')',''), ',','') ELSE [Field 70] END AS 'Jul'
				  ,CASE WHEN LEFT([Field 81], 1) = '(' AND RIGHT([Field 81], 1) =')' THEN   '-' +REPLACE(REPLACE(REPLACE([Field 81] , '(', ''), ')',''), ',','') ELSE [Field 81] END AS 'Aug'
				  ,CASE WHEN LEFT([Field 92], 1) = '(' AND RIGHT([Field 92], 1) =')' THEN   '-' +REPLACE(REPLACE(REPLACE([Field 92] , '(', ''), ')',''), ',','') ELSE [Field 92] END AS 'Sep'
				  ,CASE WHEN LEFT([Field 103], 1) = '(' AND RIGHT([Field 103], 1) =')' THEN '-' +REPLACE(REPLACE(REPLACE([Field 103], '(', ''), ')',''), ',','') ELSE [Field 103] END AS 'Oct'
				  ,CASE WHEN LEFT([Field 114], 1) = '(' AND RIGHT([Field 114], 1) =')' THEN '-' +REPLACE(REPLACE(REPLACE([Field 114], '(', ''), ')',''), ',','') ELSE [Field 114] END AS 'Nov'
				  ,CASE WHEN LEFT([Field 125], 1) = '(' AND RIGHT([Field 125], 1) =')' THEN '-' +REPLACE(REPLACE(REPLACE([Field 125], '(', ''), ')',''), ',','') ELSE [Field 125] END AS 'Dec'
				  ,[Date modified]
				  ,Row AS row_cumulative
				  ,row_sheet
			FROM row_number_add
			WHERE ISNULL([Field 4], '') <> 'Est Jan Billings'
			  AND LOWER(ISNULL([Field 2], '')) NOT LIKE '%total%'
			  AND ISNULL([Field 2], '') <> 'Xaxis US'
			  AND ISNULL([Field 2], '') <> 'Action X US'
			  AND [Field 3] IS NOT NULL

)
, sorted AS (
			SELECT *
			FROM non_sort
			WHERE (Agency = 'Maxus' AND row_sheet > 32)
			   OR (Agency = 'MEC' AND row_sheet > 39)
			   OR (Agency = 'Mediacom' AND row_sheet > 47)
			   OR (Agency = 'Mindshare' AND row_sheet > 53)
			   OR (Agency = 'Metavision' AND row_sheet > 13)
			   OR (Agency = 'Team 5' AND row_sheet > 220)
			   OR (Agency = 'AX-LR' AND row_sheet > 76)
)
SELECT 'United States' AS Market
	  ,ag.Id AS Agency__c
	  ,SUM(ISNULL(CAST(REPLACE(REPLACE(REPLACE(Jan, ',', ''), ')',''),'(', '') AS REAL), 0)) AS Jan
	  ,SUM(ISNULL(CAST(REPLACE(REPLACE(REPLACE(Feb, ',', ''), ')',''),'(', '') AS REAL), 0)) AS Feb
	  ,SUM(ISNULL(CAST(REPLACE(REPLACE(REPLACE(Mar, ',', ''), ')',''),'(', '') AS REAL), 0)) AS Mar
	  ,SUM(ISNULL(CAST(REPLACE(REPLACE(REPLACE(Apr, ',', ''), ')',''),'(', '') AS REAL), 0)) AS Apr
	  ,SUM(ISNULL(CAST(REPLACE(REPLACE(REPLACE(May, ',', ''), ')',''),'(', '') AS REAL), 0)) AS May
	  ,SUM(ISNULL(CAST(REPLACE(REPLACE(REPLACE(Jun, ',', ''), ')',''),'(', '') AS REAL), 0)) AS Jun
	  
,SUM(ISNULL(CAST(REPLACE(REPLACE(REPLACE(Jul, ',', ''), ')',''),'(', '') AS REAL), 0)) AS Jul
	  ,SUM(ISNULL(CAST(REPLACE(REPLACE(REPLACE(Aug, ',', ''), ')',''),'(', '') AS REAL), 0)) AS Aug
	  ,SUM(ISNULL(CAST(REPLACE(REPLACE(REPLACE(Sep, ',', ''), ')',''),'(', '') AS REAL), 0)) AS Sep
	  ,SUM(ISNULL(CAST(REPLACE(REPLACE(REPLACE(Oct, ',', ''), ')',''),'(', '') AS REAL), 0)) AS Oct
	  ,SUM(ISNULL(CAST(REPLACE(REPLACE(REPLACE(Nov, ',', ''), ')',''),'(', '') AS REAL), 0)) AS Nov
	  ,SUM(ISNULL(CAST(REPLACE(REPLACE(REPLACE(Dec, ',', ''), ')',''),'(', '') AS REAL), 0)) AS Dec
	  ,'2016' AS Forecast_Year__c
	  ,'USD' AS CurrencyIsoCode
	  ,'Cartesis' AS Forecast_Type__c
	  ,'Q3RF 2016' AS Forecast_Quarter__c
	  ,CASE WHEN dbo.ReplaceExtraChars([Field 3]) = 'Demand' THEN 'Xaxis'
			WHEN Agency = 'Team 5' THEN 'Xaxis'
			ELSE dbo.ReplaceExtraChars([Field 3])
	   END AS Business_Unit__c
	   ,'Market' AS Forecast_Level__c
INTO #unP
FROM sorted
	INNER JOIN (
				SELECT DISTINCT Id
					  ,Name
				FROM Production.dbo.Company__c
				WHERE Market__c = 'United States'
				  AND Type__c = 'Agency'
				) ag
		ON REPLACE(Agency, 'Team 5', 'Xaxis Direct Agency') + ' (United States)' = ag.Name
WHERE (Jan IS NOT NULL
   OR Feb IS NOT NULL
   OR Mar IS NOT NULL
   OR Apr IS NOT NULL
   OR May IS NOT NULL
   OR Jun IS NOT NULL
   OR Jul IS NOT NULL
   OR Aug IS NOT NULL
   OR Sep IS NOT NULL
   OR Oct IS NOT NULL
   OR Nov IS NOT NULL
   OR Dec IS NOT NULL)
   AND  row_cumulative <> 1
   AND Advertiser IS NOT NULL
GROUP BY ag.Id
		,CASE WHEN dbo.ReplaceExtraChars([Field 3]) = 'Demand' THEN 'Xaxis'
			WHEN Agency = 'Team 5' THEN 'Xaxis'
			ELSE dbo.ReplaceExtraChars([Field 3])
	     END
;

SELECT Market
	  ,Agency__c
	  ,Forecast_Month__c
	  ,Forecast_Year__c
	  ,Forecast__c AS Forecast__c
	  ,CurrencyIsoCode
	  ,Forecast_Type__c
	  ,Forecast_Quarter__c
	  ,Business_Unit__c
	  ,Forecast_Level__c
INTO #forecast_pivot
FROM #unP
UNPIVOT
(
	Forecast__c
	FOR Forecast_Month__c IN (Jan, Feb, Mar, Apr, May, Jun, Jul, Aug, Sep, Oct, Nov, [Dec])
) u
WHERE Forecast__c <> 0

;

SELECT fc.Name
      ,foo.*
	  ,fc.Id
	  ,fc.Forecast__c
	  ,ABS(foo.Forecast__c/1.0 - fc.Forecast__c/1.0)

	  
FROM #forecast_pivot foo
	INNER JOIN Forecast__c fc
		ON foo.Agency__c = fc.Agency__c
	   AND foo.Forecast_Quarter__c = fc.Forecast_Quarter__c
	   AND foo.Forecast_Month__c = fc.Forecast_Month__c
	   AND foo.Business_Unit__c = fc.Business_Unit__c
--WHERE com.Name LIKE '%dummy%'
WHERE ABS(foo.Forecast__c/1.0 - fc.Forecast__c/1.0) > 0.5




CREATE PROCEDURE [dbo].[Spain_Accounts_sp] AS

WITH w_Account AS (
SELECT DISTINCT dbo.ReplaceExtraChars(LTRIM(RTRIM(eb.[Field 11]))) +' - '+ ag.SF AS Name
	  ,acct.Agency__c AS Agency__c
	  ,acct.Id
--	  ,[Field 9] AS Agency_Name
	  ,ag.SF AS Agency_Name
	  ,acct.Advertiser__c AS Advertiser__c
	  ,dbo.ReplaceExtraChars(LTRIM(RTRIM(eb.[Field 11]))) AS Advertiser_Name
FROM XaxisETL.[dbo].[Extract_Spain] eb
	LEFT JOIN XaxisETL.dbo.Spain_Agent_Mapping ag	
		ON dbo.ReplaceExtraChars(LTRIM(RTRIM(eb.[Field 9]))) = ag.Spain	
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
	ON acct.Advertiser_Name = dbo.ReplaceExtraChars(LTRIM(RTRIM(eb.[Field 11])))
   AND acct.Agency_Name = ag.SF
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
, Acct_Count AS (
	SELECT AccountId
		  ,ISNULL(COUNT(Id), 0) AS account_count
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
		LEFT JOIN Acct_Count
			ON Acct_Count.AccountId = wa.Advertiser__c

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





CREATE PROCEDURE [dbo].[SpainNew_Opp_sp] AS

IF OBJECT_ID('tempdb..#spain_acct') IS NOT NULL DROP TABLE #spain_acct;
IF OBJECT_ID('tempdb..#pos_dup') IS NOT NULL DROP TABLE #pos_dup;

CREATE TABLE #spain_acct(
Name VARCHAR(218),
ID VARCHAR(218),
Advertiser__c VARCHAR(100),
Agency__c VARCHAR(100),
Business_Unit__c VARCHAR(100),
Account_Opt_In_Status__c VARCHAR(100),
RecordTypeId VARCHAR(100))
ON [Primary];

INSERT INTO #spain_acct
EXEC [Turkey_Accounts_sp];


--WITH pos_dup AS (
				SELECT DISTINCT CASE WHEN [Field 5] IS NULL THEN 'Speculative'
									 WHEN [Field 5] LIKE 'Pendiente activación' THEN 'Closed Won'
									 WHEN [Field 5] LIKE 'Pendiente planificación' THEN 'Closed Won'
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
					   ,'EUR' AS CurrencyIsoCode
					   ,dbo.ReplaceExtraChars(LTRIM(RTRIM([Field 6]))) AS [Description]
				INTO #pos_dup
				FROM XaxisETL.[dbo].[Extract_Spain] eb
					LEFT JOIN (
							SELECT 'MX' AS [Spain], 'Maxus (Spain)' AS [SF] UNION
							SELECT 'MEC' , 'MEC (Spain)'		UNION
							SELECT 'MC BCN' , 'Mediacom (Spain)'	UNION
							SELECT 'MC MAD' , 'Mediacom (Spain)'	UNION
							SELECT 'MS' , 'Mindshare (Spain)'
							) ag_c
						ON dbo.ReplaceExtraChars(LTRIM(RTRIM(eb.[Field 9]))) = ag_c.Spain	
					LEFT JOIN #spain_acct acct
						LEFT JOIN Company__c adv
							ON acct.Advertiser__c = adv.Id
						LEFT JOIN Company__c ag
							ON acct.Agency__c = ag.Id
						ON ag.Name LIKE ag_c.SF
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
--		)
SELECT DISTINCT StageName
	  ,CloseDate
	  ,Advertiser__c
	  ,Agency__c
	  ,RecordTypeId
	  ,AccountId
	  ,Name
--	  ,Id
	  ,CurrencyIsoCode

	  ,STUFF((SELECT ','+ Description
			  FROM #pos_dup eb2
			  WHERE eb2.AccountId = eb1.AccountId
				AND eb2.Name = eb1.Name
			  FOR XML PATH('')),1,1,'') AS [Description]
FROM #pos_dup eb1
WHERE Id IS NULL







CREATE PROCEDURE [dbo].[Spain_Opp_sp] AS

IF OBJECT_ID('tempdb..#w_Account') IS NOT NULL DROP TABLE #w_Account;
IF OBJECT_ID('tempdb..#Adv_Created') IS NOT NULL DROP TABLE #Adv_Created;
IF OBJECT_ID('tempdb..#Ag_Created') IS NOT NULL DROP TABLE #Ag_Created;
IF OBJECT_ID('tempdb..#Acct_Count') IS NOT NULL DROP TABLE #Acct_Count;
IF OBJECT_ID('tempdb..#with_rank') IS NOT NULL DROP TABLE #with_rank;
IF OBJECT_ID('tempdb..#Adv_Count') IS NOT NULL DROP TABLE #Adv_Count;
IF OBJECT_ID('tempdb..#spain_acct') IS NOT NULL DROP TABLE #spain_acct;
IF OBJECT_ID('tempdb..#pos_dup') IS NOT NULL DROP TABLE #pos_dup;

--WITH w_Account AS (
SELECT DISTINCT dbo.ReplaceExtraChars(LTRIM(RTRIM(eb.[Field 11]))) +' - '+ ag.SF AS Name
	  ,acct.Agency__c AS Agency__c
	  ,acct.Id
--	  ,[Field 9] AS Agency_Name
	  ,ag.SF AS Agency_Name
	  ,acct.Advertiser__c AS Advertiser__c
	  ,dbo.ReplaceExtraChars(LTRIM(RTRIM(eb.[Field 11]))) AS Advertiser_Name
INTO #w_Account
FROM XaxisETL.[dbo].[Extract_Spain] eb
	LEFT JOIN XaxisETL.dbo.Spain_Agent_Mapping ag
		ON dbo.ReplaceExtraChars(LTRIM(RTRIM(eb.[Field 9]))) = ag.Spain	
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
	ON acct.Advertiser_Name = dbo.ReplaceExtraChars(LTRIM(RTRIM(eb.[Field 11])))
   AND acct.Agency_Name = ag.SF
WHERE eb.[Field 11] IS NOT NULL
  AND eb.[Field 11] <> 'Cliente'
 -- AND acct.id IS NULL
  AND [Field 9] IS NOT NULL
  AND [Field 11] IS NOT NULL
  AND LTRIM(RTRIM([Field 9])) <> ''
  AND LTRIM(RTRIM([Field 11])) <> ''
--)
;
--, Adv_Count AS (
	SELECT bc.id
		  ,ISNULL(COUNT(ba.Id), 0) AS a_count
	INTO #Adv_Count
	FROM Spain_Company bc
		LEFT JOIN Account ba
			ON bc.Id = ba.Advertiser__c
	WHERE bc.Type__c = 'Advertiser'
	GROUP BY bc.Id
	--)
	;
--, Adv_Created AS (
	SELECT bc.id
		  ,bc.CreatedDate
	INTO #Adv_Created
	FROM Company__c bc
	WHERE bc.Type__c = 'Advertiser'
	--)
	;
--,  Ag_Created AS (
	SELECT bc.id
		  ,bc.CreatedDate
	INTO #Ag_Created
	FROM Company__c bc
	WHERE bc.Type__c = 'Agency'
	--)
	;
--, Acct_Count AS (
	SELECT AccountId
		  ,ISNULL(COUNT(Id), 0) AS account_count
	INTO #Acct_Count
	FROM Opportunity
	GROUP BY AccountId
--	)
;
--, with_rank AS (
	SELECT DISTINCT wa.Name
		  ,wa.Id
		  ,sf_ad.id AS Advertiser__c
		  ,sf_ag.id AS Agency__c
		  ,'Xaxis' AS Business_Unit__c
		  ,'Signed' AS Account_Opt_In_Status__c
		  ,(SELECT TOP 1 [Id] FROM RecordType WHERE SobjectType = 'Account' AND DeveloperName = 'Xaxis_Media_Buying_EMEA') AS RecordTypeId
		  ,DENSE_RANK() OVER (PARTITION BY wa.Name ORDER BY #Adv_Count.a_count, #Adv_Created.CreatedDate, #Ag_Created.CreatedDate) AS Ranky
	INTO #with_rank
	FROM #w_Account wa
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
		INNER JOIN #Adv_Count
			ON #Adv_Count.Id = sf_ad.id
		INNER JOIN #Adv_Created
			ON #Adv_Created.Id = sf_ad.id
		INNER JOIN #Ag_Created
			ON #Ag_Created.Id = sf_ag.id
		LEFT JOIN #Acct_Count
			ON #Acct_Count.AccountId = wa.Advertiser__c
--	)
;
-
-, spain_acct AS (
		SELECT Name
			  ,Id
			  ,Advertiser__c
			  ,Agency__c
			  ,Business_Unit__c
			  ,Account_Opt_In_Status__c
			  ,RecordTypeId
		INTO #spain_acct
		FROM #with_rank
		WHERE Ranky = 1
--				)
;
--, pos_dup AS (
				SELECT DISTINCT CASE WHEN [Field 5] IS NULL THEN 'Speculative'
									 WHEN [Field 5] LIKE 'Pendiente activación' THEN 'Closed Won'
									 WHEN [Field 5] LIKE 'Pendiente planificación' THEN 'Closed Won'
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
					   ,'EUR' AS CurrencyIsoCode
					   ,dbo.ReplaceExtraChars(LTRIM(RTRIM([Field 6]))) AS [Description]
				INTO #pos_dup 
				FROM XaxisETL.[dbo].[Extract_Spain] eb
					LEFT JOIN XaxisETL.dbo.Spain_Agent_Mapping ag_c
						ON dbo.ReplaceExtraChars(LTRIM(RTRIM(eb.[Field 9]))) = ag_c.Spain	
					LEFT JOIN #spain_acct acct
						LEFT JOIN Company__c adv
							ON acct.Advertiser__c = adv.Id
						LEFT JOIN Company__c ag
							ON acct.Agency__c = ag.Id
						ON ag.Name LIKE ag_c.SF
						AND dbo.ReplaceExtraChars(LTRIM(RTRIM(eb.[Field 11]))) = adv.Name
						AND ag.Market__c = 'Spain'
					LEFT JOIN Opportunity sf_opp
						ON  dbo.ReplaceExtraChars(LTRIM(RTRIM(eb.[Field 12]))) = REPLACE(sf_opp.Name,'Spain:','')
--						  OR 'NewSpainOpp-' +dbo.ReplaceExtraChars(LTRIM(RTRIM(eb.[Field 12]))) = sf_opp.Name
					   AND sf_opp.AccountId = acct.Id

				WHERE eb.[Field 11] IS NOT NULL
				  AND eb.[Field 11] <> 'Cliente'
				  AND dbo.ReplaceExtraChars(LTRIM(RTRIM(eb.[Field 11]))) <> ''
				  AND eb.[Field 2] IS NOT NULL
--		)
;
WITH stage_o AS (
	SELECT DISTINCT StageName
		  ,CloseDate
		  ,Advertiser__c
		  ,Agency__c
		  ,RecordTypeId
		  ,AccountId
		  ,Name
		  ,Id
		  ,CurrencyIsoCode
		  ,DENSE_RANK() OVER (PARTITION BY eb1.Name ORDER BY eb1.StageName) As stageNameOrder
		  ,STUFF((SELECT ','+ Description
				  FROM #pos_dup eb2
				  WHERE eb2.AccountId = eb1.AccountId
					AND eb2.Name = eb1.Name
				  FOR XML PATH('')),1,1,'') AS [Description]
	FROM #pos_dup eb1
	)
SELECT StageName
	  ,CloseDate
	  ,Advertiser__c
	  ,Agency__c
	  ,RecordTypeId
	  ,AccountId
	  ,Name
	  ,Id
	  ,CurrencyIsoCode
	  ,[Description]
FROM stage_o
WHERE stageNameOrder = 1






CREATE PROCEDURE [dbo].[Spain_Sell_Lines_sp] AS



WITH w_Account AS (
SELECT DISTINCT [Field 11] +' - '+ ag.SF AS Name
	  ,acct.Agency__c AS Agency__c
	  ,acct.Id
--	  ,[Field 9] AS Agency_Name
	  ,ag.SF AS Agency_Name
	  ,acct.Advertiser__c AS Advertiser__c
	  ,[Field 11] AS Advertiser_Name
FROM XaxisETL.[dbo].[Extract_Spain] eb
	LEFT JOIN (
			SELECT 'MX' AS [Spain], 'Maxus (Spain)' AS [SF] UNION
			SELECT 'MEC' , 'MEC (Spain)'		UNION
			SELECT 'MC BCN' , 'Mediacom (Spain)'	UNION
			SELECT 'MC MAD' , 'Mediacom (Spain)'	UNION
			SELECT 'MS' , 'Mindshare (Spain)'
			) ag
		ON dbo.ReplaceExtraChars(LTRIM(RTRIM(eb.[Field 9]))) = ag.Spain	
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
   AND acct.Agency_Name = ag.SF
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
, Acct_Count AS (
	SELECT AccountId
		  ,ISNULL(COUNT(Id), 0) AS account_count
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
		LEFT JOIN Acct_Count
			ON Acct_Count.AccountId = wa.Advertiser__c

	)
, spain_acct AS (
		SELECT Name
			  ,Id
			  ,Advertiser__c
			  ,Agency__c
			  ,Business_Unit__c
			  ,Account_Opt_In_Status__c
			  ,RecordTypeId
		FROM with_rank
		WHERE Ranky = 1
				)
		
SELECT DISTINCT sf_opp.Id AS Opportunity__c
	    ,sl.Id
		,'SpainOpp-'+[Field 4] AS External_ID__c
--		,dbo.ReplaceExtraChars(LTRIM(RTRIM(eb.[Field 13]))) AS Product_Detail__c
	    ,ISNULL(pro.sf_value, dbo.ReplaceExtraChars(LTRIM(RTRIM(eb.[Field 13])))) AS Product_Detail__c
		,CAST(CONVERT(DATE,eb.[Field 7],101) AS DATE) AS Start_Date__c
		,CAST(CONVERT(DATE,eb.[Field 8],101) AS DATE) AS End_Date__c
--			  ,CONVERT(MONEY, REPLACE(e
b.F24, ',','')) AS Gross_Cost__c
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
		,ISNULL(ch.sf_value, 'Digital') AS Media_Code__c
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
		,dbo.ReplaceExtraChars(LTRIM(RTRIM(eb.[Field 14]))) AS Formats__c
--			  ,CASE WHEN [Field 5] IS NULL THEN 'Speculative'
--					 WHEN [Field 5] LIKE 'Pendiente activación' THEN 'Contract Pending'
--					 WHEN [Field 5] LIKE 'Pendiente planificación' THEN 'Proposal Sent'
--					 ELSE 'Contacted / Prospecting'
--				END AS Sell_Line_Status__c
FROM XaxisETL.[dbo].[Extract_Spain] eb
	LEFT JOIN (SELECT m_value
					 ,sf_value
			   FROM XaxisETL.dbo.SF_Mapping
			   WHERE sf_market = 'Spain'
			     AND sf_type = 'Products'
			   ) pro
		ON dbo.ReplaceExtraChars(LTRIM(RTRIM(eb.[Field 13]))) = pro.m_value
	LEFT JOIN (SELECT m_value
					 ,sf_value
			    FROM XaxisETL.dbo.SF_Mapping
				WHERE sf_market = 'Spain'
				  AND sf_type = 'Channels'
				) ch
		ON dbo.ReplaceExtraChars(LTRIM(RTRIM(eb.[Field 13]))) = ch.m_value
	LEFT JOIN (
			SELECT 'MX' AS [Spain], 'Maxus (Spain)' AS [SF] UNION
			SELECT 'MEC' , 'MEC (Spain)'		UNION
			SELECT 'MC BCN' , 'Mediacom (Spain)'	UNION
			SELECT 'MC MAD' , 'Mediacom (Spain)'	UNION
			SELECT 'MS' , 'Mindshare (Spain)'
			) ag_c
		ON dbo.ReplaceExtraChars(LTRIM(RTRIM(eb.[Field 9]))) = ag_c.Spain	
	LEFT JOIN spain_acct acct
		LEFT JOIN Company__c adv
			ON acct.Advertiser__c = adv.Id
		LEFT JOIN Company__c ag
			ON acct.Agency__c = ag.Id
		ON ag.Name = ag_c.SF 
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
						  AND LTRIM(RTRIM(REPLACE(dbo.ReplaceExtraChars([Field 21]), 
',',''))) <> ''
						GROUP BY [Field 4]
						) sum_media
				ON eb.[Field 4] = sum_media.id
	LEFT JOIN Opportunity_Buy__c sl
		ON 'SpainOpp-'+[Field 4]  = sl.External_Id__c
		  WHERE  eb.[Field 11] IS NOT NULL
			  AND eb.[Field 11] <> 'Cliente'
			  AND [Field 9] IS NOT NULL
			  AND [Field 11] IS NOT NULL
			  AND LTRIM(RTRIM([Field 9])) <> ''
			  AND LTRIM(RTRIM([Field 11])) <> ''
			  AND LTRIM(RTRIM([Field 1])) = 'Venta'
	--		  AND sl.Id IS NULL








CREATE PROCEDURE [dbo].[BelgiumSellLineUpdate_sp] AS


IF OBJECT_ID('tempdb..#sql_sell_line') IS NOT NULL DROP TABLE #sql_sell_line;
IF OBJECT_ID('tempdb..#sf_sell_line') IS NOT NULL DROP TABLE #sf_sell_line;

IF OBJECT_ID('tempdb..#hash_sf_temp') IS NOT NULL DROP TABLE #hash_sf;
IF OBJECT_ID('tempdb..#hash_sql_temp') IS NOT NULL DROP TABLE #hash_sql_temp;
IF OBJECT_ID('tempdb..#hash_sql') IS NOT NULL DROP TABLE #hash_sql;
IF OBJECT_ID('tempdb..#hash_sf') IS NOT NULL DROP TABLE #hash_sf;

CREATE TABLE #hash_sql_temp(
	[External_ID__c]	[nvarchar](222) NULL,
	[Opportunity__c]	[nvarchar](218) NULL,
	[Product_Detail__c]	[nvarchar](242) NULL,
	[Start_Date__c]	[datetime] NULL,
	[End_Date__c]	[datetime] NULL,
	[Buy_Name_txt__c]	[nvarchar](456) NULL,
	[Imputing_Margin_or_Net__c]	[nvarchar](233) NULL,
	[PackageType__c]	[nvarchar](250) NULL,
	[RecordTypeId]	[nvarchar](218) NULL,
	[Supplier_Name__c]	[nvarchar](218) NULL,
	[Buy_Volume__c]	[float] NULL,
	[Gross_Cost__c]	[float] NULL,
	[Rate__c]	[float] NULL,
	[Media_Net_Cost__c]	[float] NULL,
	[Target_Gender__c]	[nvarchar](50) NULL,
	[Target_Age_Freeform_Entry__c]	[nvarchar](50) NULL,
	[Opp_Buy_Description__c]	[nvarchar](3924) NULL,
	[Audience_Tier__c]	[nvarchar](265) NULL,
	[Media_Code__c]	[nvarchar](215) NULL,
	[Special_Product__c] [nvarchar](300) NULL,
	[Delivery_Market__c] [nvarchar](200) NULL,
	[Buy_Type__c]	[nvarchar](209) NULL,
	[Frequency_Cap__c] [nvarchar](20) NULL,
	[Frequency_Scope__c] [nvarchar](50) NULL,
	[Input_Mode__c]	[nvarchar](224) NULL,
	[Id] [nvarchar](218) NULL
) ON [PRIMARY];

CREATE TABLE #hash_sql(
	[External_ID__c]	[nvarchar](222) NULL,
	[Opportunity__c]	[nvarchar](218) NULL,
	[Product_Detail__c]	[nvarchar](242) NULL,
	[Start_Date__c]	[datetime] NULL,
	[End_Date__c]	[datetime] NULL,
	[Buy_Name_txt__c]	[nvarchar](456) NULL,
	[Imputing_Margin_or_Net__c]	[nvarchar](233) NULL,
	[PackageType__c]	[nvarchar](250) NULL,
--	[RecordTypeId]	[nvarchar](218) NULL,
	[Supplier_Name__c]	[nvarchar](218) NULL,
	[Buy_Volume__c]	[float] NULL,
	[Gross_Cost__c]	[float] NULL,
	[Rate__c]	[float] NULL,
	[Media_Net_Cost__c]	[float] NULL,
	[Target_Gender__c]	[nvarchar](50) NULL,
	[Target_Age_Freeform_Entry__c]	[nvarchar](50) NULL,
	[Opp_Buy_Description__c]	[nvarchar](3924) NULL,
	[Audience_Tier__c]	[nvarchar](265) NULL,
	[Media_Code__c]	[nvarchar](215) NULL,
	[Special_Product__c] [nvarchar](300) NULL,
	[Delivery_Market__c] [nvarchar](200) NULL,
	[Buy_Type__c]	[nvarchar](209) NULL,
	[Frequency_Cap__c] [nvarchar](20) NULL,
	[Frequency_Scope__c] [nvarchar](50) NULL,
	[Input_Mode__c]	[nvarchar](224) NULL,
	[Id] [nvarchar](218) NULL
) ON [PRIMARY];



INSERT INTO #hash_sql_temp
SELECT * FROM [Beligum_Sell_Lines];

INSERT INTO #hash_sql
SELECT [External_ID__c]
      ,[Opportunity__c]
      ,[Product_Detail__c]
      ,[Start_Date__c]
      ,[End_Date__c]
      ,[Buy_Name_txt__c]
      ,[Imputing_Margin_or_Net__c]
      ,[PackageType__c]
      ,[RecordTypeId]
      ,[Supplier_Name__c]
      ,[Buy_Volume__c]
      ,[Gross_Cost__c]
      ,[Rate__c]
      ,[Media_Net_Cost__c]
      ,[Target_Gender__c]
      ,[Target_Age_Freeform_Entry__c]
      ,[Opp_Buy_Description__c]
      ,[Audience_Tier__c]
      ,[Media_Code__c]
      ,[Special_Product__c]
      ,[Delivery_Market__c]
      ,LOWER([Buy_Type__c]) AS [Buy_Type__c]
      ,[Frequency_Cap__c]
      ,[Frequency_Scope__c]
      ,[Input_Mode__c]
	  ,[Id]
FROM #hash_sql_temp;

SELECT bsl.[External_ID__c]
      ,bsl.[Opportunity__c]
      ,bsl.[Product_Detail__c]
      ,bsl.[Start_Date__c]
      ,bsl.[End_Date__c]
      ,bsl.[Buy_Name_txt__c]
      ,bsl.[Imputing_Margin_or_Net__c]
      ,bsl.[PackageType__c]
      ,bsl.[RecordTypeId]
      ,bsl.[Supplier_Name__c]
      ,bsl.[Buy_Volume__c]
      ,bsl.[Gross_Cost__c]
      ,bsl.[Rate__c]
      ,bsl.[Media_Net_Cost__c]
      ,bsl.[Target_Gender__c]
      ,bsl.[Target_Age_Freeform_Entry__c]

      ,bsl.[Opp_Buy_Description__c]
      ,bsl.[Audience_Tier__c]
      ,bsl.[Media_Code__c]
      ,bsl.[Special_Product__c]
      ,bsl.[Delivery_Market__c]
      ,bsl.[Buy_Type__c]
      ,bsl.[Frequency_Cap__c]
      ,bsl.[Frequency_Scope__c]
      ,bsl.[Input_Mode__c]
  FROM #hash_sql_temp bsl
	LEFT JOIN dbo.Opportunity_Buy__c sl
		ON bsl.External_ID__c = CAST(sl.External_Id__c AS VARCHAR)
WHERE sl.External_Id__c IS NULL  


/*
SELECT *
FROM #hash_sql
UNION 
SELECT #hash_sf.*
FROM #hash_sf
	INNER JOIN (SELECT External_Id__c FROM Production.dbo.Turkey_Sell_Lines) sq
		ON #hash_sf.External_Id__c = sq.External_ID__c

*/



CREATE PROCEDURE [dbo].[Spain_Buy_Placement_sp] AS

IF OBJECT_ID('tempdb..#Spain_temp') IS NOT NULL DROP TABLE #Spain_temp;

SELECT DISTINCT ISNULL(dbo.ReplaceExtraChars(LTRIM(RTRIM(eb.[Field 16])))+'-' , '') 
			   + ISNULL(dbo.ReplaceExtraChars(LTRIM(RTRIM(eb.[Field 17])))+'-' , '') +
			   + sl.External_Id__c  AS Name
	 ,bu.Id
	 ,sl.Id AS Sell_Line__c
	 ,CASE WHEN dbo.ReplaceExtraChars(LTRIM(RTRIM(eb.[Field 16]))) = '' THEN NULL
		   ELSE dbo.ReplaceExtraChars(LTRIM(RTRIM(eb.[Field 16]))) 
	  END AS Creative_Format__c
	 ,ISNULL(CONVERT(MONEY, REPLACE(dbo.ReplaceExtraChars(eb.[Field 21]), ',','')), 0) AS Actual_Cost__c
	 ,CAST(CONVERT(DATE,eb.[Field 7],101) AS DATE) AS Start_Date__c
	 ,CAST(CONVERT(DATE,eb.[Field 8],101) AS DATE) AS End_Date__c
	 ,'EUR' AS [CurrencyIsoCode]
	 ,ROW_NUMBER() OVER(PARTITION BY  ISNULL(dbo.ReplaceExtraChars(LTRIM(RTRIM(eb.[Field 16])))+'-' , '') 
								    + ISNULL(dbo.ReplaceExtraChars(LTRIM(RTRIM(eb.[Field 17])))+'-' , '') +
								    + sl.External_Id__c
						ORDER BY bu.Id) AS row_n
INTO #Spain_temp
FROM XaxisETL.[dbo].[Extract_Spain] eb
	LEFT JOIN Opportunity_Buy__c sl
		ON CAST([Field 4] AS VARCHAR) = CAST(REPLACE(sl.External_Id__c, 'SpainOpp-', '') AS VARCHAR)
	LEFT JOIN Buy_Placement__c bu
		ON ISNULL(dbo.ReplaceExtraChars(LTRIM(RTRIM(eb.[Field 16])))+'-' , '') + sl.External_Id__c = bu.Name
WHERE  eb.[Field 11] IS NOT NULL
	AND eb.[Field 11] <> 'Cliente'
	-- AND acct.id IS NULL
	AND [Field 9] IS NOT NULL
	AND [Field 11] IS NOT NULL
	AND LTRIM(RTRIM([Field 9])) <> ''
	AND LTRIM(RTRIM([Field 11])) <> ''
	AND LTRIM(RTRIM([Field 1])) = 'Compra'
;

SELECT CASE WHEN row_n = 1 THEN Name
			ELSE Name + '-' + CAST(row_n AS VARCHAR) 
		END AS Name
		,Id
		,Sell_Line__c
		,Creative_Format__c
		,Actual_Cost__c
		,Start_Date__c
		,End_Date__c
		,CurrencyIsoCode
FROM #Spain_temp












CREATE PROCEDURE [dbo].[BelgiumOppUpdate_sp] AS

IF OBJECT_ID('tempdb..#sql_sell_line') IS NOT NULL DROP TABLE #sql_sell_line;
IF OBJECT_ID('tempdb..#sf_sell_line') IS NOT NULL DROP TABLE #sf_sell_line;

IF OBJECT_ID('tempdb..#hash_sf_temp') IS NOT NULL DROP TABLE #hash_sf_temp;
IF OBJECT_ID('tempdb..#hash_sql') IS NOT NULL DROP TABLE #hash_sql;
IF OBJECT_ID('tempdb..#hash_sf') IS NOT NULL DROP TABLE #hash_sf;

CREATE TABLE #hash_sql(
	StageName		varchar(45)
	,Id				nvarchar(218) NULL
	,CloseDate		date
	,External_Id__c	nvarchar(266)
	,Name			varchar(200)
	,Reason_Lost__c	varchar(60)
	,CurrencyIsoCode	varchar(3)
	,PO_Number__c	varchar(30));

CREATE TABLE #hash_sf(
	StageName		varchar(45)
	,Id				nvarchar(218) NULL
	,CloseDate		date
	,External_Id__c	nvarchar(266)
	,Name			varchar(200)
	,Reason_Lost__c	varchar(60)
	,CurrencyIsoCode	varchar(3)
	,PO_Number__c varchar(30));

INSERT INTO #hash_sql
SELECT StageName
	  ,Id
	  ,CloseDate
	  ,External_Id__c
	  ,Name
	  ,Reason_Lost__c
	  ,CurrencyIsoCode
	  ,PO_Number__c
FROM Beligum_Opp;

INSERT INTO #hash_sf
SELECT StageName
	  ,Id
	  ,CloseDate
	  ,External_Id__c
	  ,Name
	  ,Reason_Lost__c
	  ,CurrencyIsoCode
	  ,PO_Number__c
FROM Opportunity;

SELECT StageName
	  ,Id
	  ,CloseDate
	  ,External_Id__c
	  ,Name
	  ,Reason_Lost__c
	  ,CurrencyIsoCode
	  ,PO_Number__c
	  ,HASHBYTES('SHA1', (SELECT TOP 1 * FROM #hash_sql bb WHERE bb.External_ID__c = sq.External_ID__c FOR XML RAW)) AS sq_hash
INTO #sql_sell_line
FROM #hash_sql sq
WHERE sq.Id IS NOT NULL;

SELECT StageName
	  ,Id
	  ,CloseDate
	  ,sf_in.External_Id__c
	  ,Name
	  ,Reason_Lost__c
	  ,CurrencyIsoCode
	  ,PO_Number__c
		,HASHBYTES('SHA1', (SELECT TOP 1 * FROM #hash_sf cc WHERE cc.External_ID__c = sf_in.External_ID__c FOR XML RAW)) AS sf_hash
INTO #sf_sell_line
FROM Opportunity sf_in
	INNER JOIN (SELECT External_Id__c FROM #hash_sql) sq
		ON sf_in.External_Id__c = sq.External_ID__c;




SELECT StageName
	  ,Id
	  ,CloseDate
	  ,External_Id__c
	  ,Name
	  ,Reason_Lost__c
	  ,CurrencyIsoCode
	  ,PO_Number__c
FROM #sql_sell_line sq
	LEFT JOIN ( SELECT sf_hash
				FROM #sf_sell_line
			   ) sf
		ON sq.sq_hash = sf.sf_hash
WHERE sf.sf_hash IS NULL;


/*
SELECT *
FROM #hash_sql
UNION 
SELECT #hash_sf.*
FROM #hash_sf
	INNER JOIN (SELECT External_Id__c FROM Production.dbo.Beligum_Opp) sq
		ON #hash_sf.External_Id__c = sq.External_ID__c

*/


CREATE PROCEDURE [dbo].[TurkeyNew_Opps_sp] AS

IF OBJECT_ID('tempdb..#Turkey_Opps') IS NOT NULL DROP TABLE #Turkey_Opps;

CREATE TABLE #Turkey_Accounts(
Name VARCHAR(100),
Id VARCHAR(50),
Advertiser__c VARCHAR(50),
Agency__c  VARCHAR(50),
Business_Unit__c VARCHAR(50),
Account_Opt_In_Status__c VARCHAR(50),
RecordTypeId VARCHAR(50))
ON [PRIMARY]; 
INSERT INTO #Turkey_Accounts
EXEC Turkey_Accounts_sp;


SELECT DISTINCT 'Closed Won' StageName
--	  ,CONVERT(VARCHAR(8), CONVERT(DATE,F8,3),3) AS CloseDate
	  ,dd.CloseDate
	  ,acct.Advertiser__c AS Advertiser__c
	  ,acct.Agency__c AS Agency__c
	  ,(SELECT TOP 1 [Id] FROM [RecordType] WHERE SobjectType = 'Opportunity' AND DeveloperName = 'Turkey') AS RecordTypeId
	  ,acct.Id AS AccountId
--	  ,'SpainOpp-'+[Field 4] AS External_Id__c
	  ,dbo.ReplaceExtraChars(LTRIM(RTRIM([Field 11]))) AS 'Name'
	  ,'TRY' AS CurrencyIsoCode
--	  ,sf_opp.Id
FROM XaxisETL.[dbo].[Extract_Turkey] eb
	INNER JOIN (
				SELECT 'MX' AS [Turk], 'Maxus (Turkey)' AS [SF] UNION
				SELECT 'MEC' , 'MEC (Turkey)'		UNION
				SELECT 'MC' , 'Mediacom (Turkey)'	UNION
				SELECT 'MS' , 'Mindshare (Turkey)'
				) ags	
		ON eb.[Field 9] = ags.Turk
	INNER JOIN (SELECT dbo.ReplaceExtraChars(LTRIM(RTRIM([Field 11]))) AS 'Name'
					  ,MAX(CONVERT(DATE,[Field 7], 101)) AS CloseDate
			    FROM XaxisETL.dbo.Extract_Turkey
				WHERE [Field 9] IS NOT NULL
				  AND [Field 9] <> 'Agency'
				  AND [Field 9] <> 'Needs mapping to Turkish Agencies int the system'
				  AND [Field 9] <> 'Campaign Details'
				GROUP BY dbo.ReplaceExtraChars(LTRIM(RTRIM([Field 11])))
				) dd
			ON dbo.ReplaceExtraChars(LTRIM(RTRIM(eb.[Field 11]))) = dd.Name
	LEFT JOIN #Turkey_Accounts acct
		LEFT JOIN Company__c adv
			ON acct.Advertiser__c = adv.Id
		LEFT JOIN Company__c ag
			ON acct.Agency__c = ag.Id
		ON ag.Name = ags.SF
	    AND master.dbo.InitCap(LTRIM(RTRIM(LOWER([Field 10])))) = adv.Name
		AND ag.Market__c = 'Turkey'
	LEFT JOIN Opportunity sf_opp
		ON  dbo.ReplaceExtraChars(LTRIM(RTRIM(eb.[Field 11]))) = sf_opp.Name
	   AND sf_opp.AccountId = acct.Id
WHERE [Field 9] IS NOT NULL
  AND [Field 9] <> 'Agency'
  AND [Field 9] <> 'Needs mapping to Turkish Agencies int the system'
  AND [Field 9] <> 'Campaign Details'
  AND sf_opp.Id IS NULL


CREATE PROCEDURE [dbo].[Belgium_Sell_Line_New_sp] AS


IF OBJECT_ID('tempdb..#sql_sell_line') IS NOT NULL DROP TABLE #sql_sell_line;
IF OBJECT_ID('tempdb..#sf_sell_line') IS NOT NULL DROP TABLE #sf_sell_line;

IF OBJECT_ID('tempdb..#hash_sf_temp') IS NOT NULL DROP TABLE #hash_sf;
IF OBJECT_ID('tempdb..#hash_sql_temp') IS NOT NULL DROP TABLE #hash_sql_temp;
IF OBJECT_ID('tempdb..#hash_sql') IS NOT NULL DROP TABLE #hash_sql;
IF OBJECT_ID('tempdb..#hash_sf') IS NOT NULL DROP TABLE #hash_sf;

CREATE TABLE #hash_sql_temp(
	[External_ID__c]	[nvarchar](222) NULL,
	[Opportunity__c]	[nvarchar](218) NULL,
	[Product_Detail__c]	[nvarchar](242) NULL,
	[Start_Date__c]	[datetime] NULL,
	[End_Date__c]	[datetime] NULL,
	[Buy_Name_txt__c]	[nvarchar](456) NULL,
	[Imputing_Margin_or_Net__c]	[nvarchar](233) NULL,
	[PackageType__c]	[nvarchar](250) NULL,
	[RecordTypeId]	[nvarchar](218) NULL,
	[Supplier_Name__c]	[nvarchar](218) NULL,
	[Buy_Volume__c]	[float] NULL,
	[Gross_Cost__c]	[float] NULL,
	[Rate__c]	[float] NULL,
	[Media_Net_Cost__c]	[float] NULL,
	[Target_Gender__c]	[nvarchar](50) NULL,
	[Target_Age_Freeform_Entry__c]	[nvarchar](50) NULL,
	[Opp_Buy_Description__c]	[nvarchar](3924) NULL,
	[Audience_Tier__c]	[nvarchar](265) NULL,
	[Media_Code__c]	[nvarchar](215) NULL,
	[Special_Product__c] [nvarchar](300) NULL,
	[Delivery_Market__c] [nvarchar](200) NULL,
	[Buy_Type__c]	[nvarchar](209) NULL,
	[Frequency_Cap__c] [nvarchar](20) NULL,
	[Frequency_Scope__c] [nvarchar](50) NULL,
	[Input_Mode__c]	[nvarchar](224) NULL,
	[Id] [nvarchar](218) NULL
) ON [PRIMARY];

CREATE TABLE #hash_sql(
	[External_ID__c]	[nvarchar](222) NULL,
	[Opportunity__c]	[nvarchar](218) NULL,
	[Product_Detail__c]	[nvarchar](242) NULL,
	[Start_Date__c]	[datetime] NULL,
	[End_Date__c]	[datetime] NULL,
	[Buy_Name_txt__c]	[nvarchar](456) NULL,
	[Imputing_Margin_or_Net__c]	[nvarchar](233) NULL,
	[PackageType__c]	[nvarchar](250) NULL,
	[RecordTypeId]	[nvarchar](218) NULL,
	[Supplier_Name__c]	[nvarchar](218) NULL,
	[Buy_Volume__c]	[float] NULL,
	[Gross_Cost__c]	[float] NULL,
	[Rate__c]	[float] NULL,
	[Media_Net_Cost__c]	[float] NULL,
	[Target_Gender__c]	[nvarchar](50) NULL,
	[Target_Age_Freeform_Entry__c]	[nvarchar](50) NULL,
	[Opp_Buy_Description__c]	[nvarchar](3924) NULL,
	[Audience_Tier__c]	[nvarchar](265) NULL,
	[Media_Code__c]	[nvarchar](215) NULL,
	[Special_Product__c] [nvarchar](300) NULL,
	[Delivery_Market__c] [nvarchar](200) NULL,
	[Buy_Type__c]	[nvarchar](209) NULL,
	[Frequency_Cap__c] [nvarchar](20) NULL,
	[Frequency_Scope__c] [nvarchar](50) NULL,
	[Input_Mode__c]	[nvarchar](224) NULL,
	[Id] [nvarchar](218) NULL
) ON [PRIMARY];



INSERT INTO #hash_sql_temp
SELECT * FROM [Beligum_Sell_Lines];

INSERT INTO #hash_sql
SELECT [External_ID__c]
      ,[Opportunity__c]
      ,[Product_Detail__c]
      ,[Start_Date__c]
      ,[End_Date__c]
      ,[Buy_Name_txt__c]
      ,[Imputing_Margin_or_Net__c]
      ,[PackageType__c]
      ,[RecordTypeId]
      ,[Supplier_Name__c]
      ,[Buy_Volume__c]
      ,[Gross_Cost__c]
      ,[Rate__c]
      ,[Media_Net_Cost__c]
      ,[Target_Gender__c]
      ,[Target_Age_Freeform_Entry__c]
      ,[Opp_Buy_Description__c]
      ,[Audience_Tier__c]
      ,[Media_Code__c]
      ,[Special_Product__c]
      ,[Delivery_Market__c]
      ,[Buy_Type__c]
      ,[Frequency_Cap__c]
      ,[Frequency_Scope__c]
      ,[Input_Mode__c]
	  ,[Id]
FROM #hash_sql_temp;

SELECT bsl.[External_ID__c]
      ,bsl.[Opportunity__c]
      ,bsl.[Product_Detail__c]
      ,bsl.[Start_Date__c]
      ,bsl.[End_Date__c]
      ,bsl.[Buy_Name_txt__c]
      ,bsl.[Imputing_Margin_or_Net__c]
      ,bsl.[PackageType__c]
      ,bsl.[RecordTypeId]
      ,bsl.[Supplier_Name__c]
      ,bsl.[Buy_Volume__c]
      ,bsl.[Gross_Cost__c]
      ,bsl.[Rate__c]
      ,bsl.[Media_Net_Cost__c]
      ,bsl.[Target_Gender__c]
      ,bsl.[Target_Age_Freeform_Entry__c]
      ,bsl.[Opp_Buy_Descri
ption__c]
      ,bsl.[Audience_Tier__c]
      ,bsl.[Media_Code__c]
      ,bsl.[Special_Product__c]
      ,bsl.[Delivery_Market__c]
      ,bsl.[Buy_Type__c]
      ,bsl.[Frequency_Cap__c]
      ,bsl.[Frequency_Scope__c]
      ,bsl.[Input_Mode__c]
  FROM #hash_sql_temp bsl
	LEFT JOIN dbo.Opportunity_Buy__c sl
		ON bsl.External_ID__c = CAST(sl.External_Id__c AS VARCHAR)
WHERE sl.External_Id__c IS NULL  


/*
SELECT *
FROM #hash_sql
UNION 
SELECT #hash_sf.*
FROM #hash_sf
	INNER JOIN (SELECT External_Id__c FROM Production.dbo.Turkey_Sell_Lines) sq
		ON #hash_sf.External_Id__c = sq.External_ID__c

*/


CREATE PROCEDURE [dbo].[Forecast_Canada_sp] AS

IF OBJECT_ID('tempdb..#unP') IS NOT NULL DROP TABLE #unP;
IF OBJECT_ID('tempdb..#forecast_pivot') IS NOT NULL DROP TABLE #forecast_pivot;

WITH non_sort AS (
			SELECT [Field 1] AS 'Agency'
				  ,[Field 3] AS 'Advertiser'
				  ,[Field 3]
				  ,CASE WHEN LEFT([Field 5], 1) = '(' AND RIGHT( [Field 5], 1) =')' THEN     '-' +REPLACE(REPLACE(REPLACE([Field 5]  , '(', ''), ')',''), ',','') ELSE [Field 5]  END AS 'Jan'
				  ,CASE WHEN LEFT([Field 6], 1) = '(' AND RIGHT( [Field 6], 1) =')' THEN   '-' +REPLACE(REPLACE(REPLACE(  [Field 6] , '(', ''), ')',''), ',','') ELSE  [Field 6] END AS 'Feb'
				  ,CASE WHEN LEFT([Field 7], 1) = '(' AND RIGHT( [Field 7], 1) =')' THEN   '-' +REPLACE(REPLACE(REPLACE(  [Field 7] , '(', ''), ')',''), ',','') ELSE  [Field 7] END AS 'Mar'
				  ,CASE WHEN LEFT([Field 8], 1) = '(' AND RIGHT( [Field 8], 1) =')' THEN   '-' +REPLACE(REPLACE(REPLACE(  [Field 8] , '(', ''), ')',''), ',','') ELSE  [Field 8] END AS 'Apr'
				  ,CASE WHEN LEFT([Field 9], 1) = '(' AND RIGHT( [Field 9], 1) =')' THEN   '-' +REPLACE(REPLACE(REPLACE(  [Field 9] , '(', ''), ')',''), ',','') ELSE  [Field 9] END AS 'May'
				  ,CASE WHEN LEFT([Field 10], 1) = '(' AND RIGHT([Field 10], 1) =')' THEN   '-' +REPLACE(REPLACE(REPLACE( [Field 10] , '(', ''), ')',''), ',','') ELSE [Field 10] END AS 'Jun'
				  ,CASE WHEN LEFT([Field 11], 1) = '(' AND RIGHT([Field 11], 1) =')' THEN   '-' +REPLACE(REPLACE(REPLACE( [Field 11] , '(', ''), ')',''), ',','') ELSE [Field 11] END AS 'Jul'
				  ,CASE WHEN LEFT([Field 12], 1) = '(' AND RIGHT([Field 12], 1) =')' THEN   '-' +REPLACE(REPLACE(REPLACE( [Field 12] , '(', ''), ')',''), ',','') ELSE [Field 12] END AS 'Aug'
				  ,CASE WHEN LEFT([Field 13], 1) = '(' AND RIGHT([Field 13], 1) =')' THEN   '-' +REPLACE(REPLACE(REPLACE( [Field 13] , '(', ''), ')',''), ',','') ELSE [Field 13] END AS 'Sep'
				  ,CASE WHEN LEFT([Field 14], 1) = '(' AND RIGHT([Field 14], 1) =')' THEN '-' +REPLACE(REPLACE(REPLACE(   [Field 14], '(', ''), ')',''), ',','') ELSE  [Field 14] END AS 'Oct'
				  ,CASE WHEN LEFT([Field 15], 1) = '(' AND RIGHT([Field 15], 1) =')' THEN '-' +REPLACE(REPLACE(REPLACE(   [Field 15], '(', ''), ')',''), ',','') ELSE  [Field 15] END AS 'Nov'
				  ,CASE WHEN LEFT([Field 16], 1) = '(' AND RIGHT([Field 16], 1) =')' THEN '-' +REPLACE(REPLACE(REPLACE(   [Field 16], '(', ''), ')',''), ',','') ELSE  [Field 16] END AS 'Dec'
				  ,[Date modified]
				  ,[Row] AS row_cumulative
				  ,[Row] AS row_sheet
			FROM XaxisETL.dbo.Forecast_Canada
			WHERE ISNULL([Field 1], '') <> 'REVENUES'
			  AND LOWER(ISNULL([Field 1], '')) NOT LIKE '%total%'
			  AND [Field 1] IS NOT NULL

)
, sorted AS (
			SELECT *
			FROM non_sort
)
SELECT 'Canada' AS Market
	  ,ag.Id AS Agency__c
	  ,SUM(ISNULL(CAST(REPLACE(REPLACE(REPLACE(Jan, ',', ''), ')',''),'(', '') AS REAL), 0)) AS Jan
	  ,SUM(ISNULL(CAST(REPLACE(REPLACE(REPLACE(Feb, ',', ''), ')',''),'(', '') AS REAL), 0)) AS Feb
	  ,SUM(ISNULL(CAST(REPLACE(REPLACE(REPLACE(Mar, ',', ''), ')',''),'(', '') AS REAL), 0)) AS Mar
	  ,SUM(ISNULL(CAST(REPLACE(REPLACE(REPLACE(Apr, ',', ''), ')',''),'(', '') AS REAL), 0)) AS Apr
	  ,SUM(ISNULL(CAST(REPLACE(REPLACE(REPLACE(May, ',', ''), ')',''),'(', '') AS REAL), 0)) AS May
	  ,SUM(ISNULL(CAST(REPLACE(REPLACE(REPLACE(Jun, ',', ''), ')',''),'(', '') AS REAL), 0)) AS Jun
	  ,SUM(ISNULL(CAST(REPLACE(REPLACE(REPLACE(Jul, ',', ''), ')',''),'(', '') AS REAL), 0)) AS Jul
	  ,SUM(ISNULL(CAST(REPLACE(REPLACE(REPLACE(Aug, ',', ''), ')',''),'(', '') AS REAL), 0)) AS Aug
	  ,SUM(ISNULL(CAST(REPLACE(REPLACE(REPLACE(Sep, ',', ''), ')',''),'(', '') AS REAL), 0)) AS Sep
	  ,SUM(ISNULL(CAST(REPLACE(REPLACE(REPLACE(Oct, ',', ''), ')',''),'(', '') AS REAL), 0)) AS Oct
	  ,SUM(ISNULL(CAST(REPLACE(REPLACE(REPLACE(Nov, ',', ''), ')',''),'(', '') AS REAL), 0)) AS Nov
	  ,SUM(ISNULL(CAST(REPLACE(REPLACE(REPLACE(Dec, ',', ''), ')',''),'(', '') AS REAL), 0)) AS Dec
	  ,'20
16' AS Forecast_Year__c
	  ,'USD' AS CurrencyIsoCode
	  ,'Cartesis' AS Forecast_Type__c
	  ,'Q3RF 2016' AS Forecast_Quarter__c
	  ,CASE WHEN dbo.ReplaceExtraChars(Agency) = 'Direct Client' THEN 'Xaxis'
			WHEN Agency = 'Team 5' THEN 'Xaxis'
			ELSE dbo.ReplaceExtraChars(Agency)
	   END AS Business_Unit__c
	   ,'Market' AS Forecast_Level__c
INTO #unP
FROM sorted
	INNER JOIN (
				SELECT DISTINCT Id
					  ,Name
				FROM Production.dbo.Company__c
				WHERE Market__c = 'Canada'
				  AND Type__c = 'Agency'
				) ag
		ON REPLACE(Agency, 'Direct Client', 'Xaxis Direct Agency') + ' (Canada)' = ag.Name
--		ON Agency = ag.Name
WHERE (Jan IS NOT NULL
   OR Feb IS NOT NULL
   OR Mar IS NOT NULL
   OR Apr IS NOT NULL
   OR May IS NOT NULL
   OR Jun IS NOT NULL
   OR Jul IS NOT NULL
   OR Aug IS NOT NULL
   OR Sep IS NOT NULL
   OR Oct IS NOT NULL
   OR Nov IS NOT NULL
   OR Dec IS NOT NULL)
   AND  row_cumulative <> 1
   AND Advertiser IS NOT NULL
GROUP BY ag.Id
		,CASE WHEN dbo.ReplaceExtraChars(Agency) = 'Direct Client' THEN 'Xaxis'
			  WHEN Agency = 'Team 5' THEN 'Xaxis'
			  ELSE dbo.ReplaceExtraChars(Agency)
	     END
;

SELECT Market
	  ,Agency__c
	  ,Forecast_Month__c
	  ,Forecast_Year__c
	  ,Forecast__c AS Forecast__c
	  ,CurrencyIsoCode
	  ,Forecast_Type__c
	  ,Forecast_Quarter__c
	  ,Business_Unit__c
	  ,Forecast_Level__c
INTO #forecast_pivot
FROM #unP
UNPIVOT
(
	Forecast__c
	FOR Forecast_Month__c IN (Jan, Feb, Mar, Apr, May, Jun, Jul, Aug, Sep, Oct, Nov, [Dec])
) u
WHERE Forecast__c <> 0

;

SELECT foo.Market
	  ,foo.Agency__c
	  ,foo.Forecast_Month__c
	  ,foo.Forecast_Year__c
	  ,foo.Forecast__c *1000 AS Forecast__c
	  ,foo.CurrencyIsoCode
	  ,foo.Forecast_Type__c
	  ,foo.Forecast_Quarter__c
	  ,'Xaxis' AS Business_Unit__c
	  ,foo.Forecast_Level__c

--	  ,fc.Name
--	  ,fc.Id
--	  ,fc.Forecast__c
--	  ,ABS(foo.Forecast__c/1.0 - fc.Forecast__c/1.0)
FROM #forecast_pivot foo
WHERE foo.Business_Unit__c <> 'Xaxis'

/*
	LEFT JOIN Forecast__c fc
		ON foo.Agency__c = fc.Agency__c
	   AND foo.Forecast_Quarter__c = fc.Forecast_Quarter__c
	   AND foo.Forecast_Month__c = fc.Forecast_Month__c
	   AND foo.Business_Unit__c = fc.Business_Unit__c
--WHERE com.Name LIKE '%dummy%'
--WHERE ABS(foo.Forecast__c/1.0 - fc.Forecast__c/1.0) > 0.5
*/



CREATE PROCEDURE Forecast_sp AS

IF OBJECT_ID('tempdb..#unP') IS NOT NULL DROP TABLE #unP;
IF OBJECT_ID('tempdb..#f_market') IS NOT NULL DROP TABLE #f_market;

SELECT [Field 3] AS Market_Dirty
	  ,CASE WHEN [Field 3] = 'Xaxis EMEA 100%' THEN 'EMEA-Market'
			WHEN [Field 3] = 'Xaxis EMEA Client 100%' THEN 'EMEA-Market'
			WHEN [Field 3] = 'Xaxis Pan-Regional 100%' THEN 'EMEA-Market'
			ELSE dbo.CleanMarketName([Field 3])
		END AS Market_Clean
	  ,[Field 4] AS Jan
      ,[Field 5] AS Feb
      ,[Field 6] AS Mar
      ,[Field 7] AS Apr
      ,[Field 8] AS May
      ,[Field 9] AS Jun
      ,[Field 10] AS Jul
      ,[Field 11] AS Aug
      ,[Field 12] AS Sep
      ,[Field 13] AS Oct
      ,[Field 14] AS Nov
      ,[Field 15] AS [Dec]

	  ,CASE WHEN [Source worksheet] = 'LR Revenue by Month' THEN 'Light Reaction'
			WHEN [Source worksheet] = 'Pl Revenue by Month' THEN 'plista'
			WHEN [Source worksheet] = 'XA Revenue by Month' THEN 'Xaxis'
			ELSE NULL 
	   END AS [Business_Unit__c]
	  ,ROW_NUMBER() OVER(ORDER BY [Field 3]) AS pk
INTO  #f_market
FROM [XaxisETL].[dbo].[Forecast_EMEA]
WHERE	LOWER(ISNULL([Field 3], '')) NOT LIKE '%revenue%'
	AND LOWER(ISNULL([Field 3], '')) NOT LIKE '%total%'
	AND LOWER(ISNULL([Field 3], '')) NOT LIKE '%{ac%'
	AND LOWER(ISNULL([Field 3], '')) NOT LIKE '%AC=10900%'
	AND LOWER(ISNULL([Field 3], '')) NOT LIKE '%check%'
	AND LOWER([Source worksheet]) <> 'revenue by month'
	AND LOWER([Field 16]) IS NOT NULL
	AND ISNULL([Field 4], '') <> 'Jan'
	;

SELECT Market_Clean AS Market
	  ,Id AS Agency__c
	  ,'2016' AS Forecast_Year__c
	  ,'USD' AS CurrencyIsoCode
	  ,'Cartesis' AS Forecast_Type__c
	  ,'Q3RF 2016' AS Forecast_Quarter__c
	  ,Business_Unit__c
	  ,'Market' AS Forecast_Level__c
	  ,SUM(CAST(REPLACE(Jan, ',', '') AS REAL)) AS Jan
      ,SUM(CAST(REPLACE(Feb, ',', '') AS REAL)) AS Feb
      ,SUM(CAST(REPLACE(Mar, ',', '') AS REAL)) AS Mar
      ,SUM(CAST(REPLACE(Apr, ',', '') AS REAL)) AS Apr
      ,SUM(CAST(REPLACE(May, ',', '') AS REAL)) AS May
      ,SUM(CAST(REPLACE(Jun, ',', '') AS REAL)) AS Jun
      ,SUM(CAST(REPLACE(Jul, ',', '') AS REAL)) AS Jul
      ,SUM(CAST(REPLACE(Aug, ',', '') AS REAL)) AS Aug
      ,SUM(CAST(REPLACE(Sep, ',', '') AS REAL)) AS Sep
      ,SUM(CAST(REPLACE(Oct, ',', '') AS REAL)) AS Oct
      ,SUM(CAST(REPLACE(Nov, ',', '') AS REAL)) AS Nov
      ,SUM(CAST(REPLACE([Dec], ',', '') AS REAL)) AS [Dec]
INTO #unP
FROM #f_market mar
	LEFT JOIN (
				SELECT DISTINCT LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(Name, 'Mindshare', ''), '(', ''), ')', ''))) AS Market__c
					  ,Name
					  ,Id
				FROM Production.dbo.Company__c com
				WHERE LOWER(com.Type__c) = 'agency'
				  AND com.Market__c IS NOT NULL
				  AND com.Name <> 'Mindshare Netherlands'
				  ) com
		ON REPLACE(mar.Market_Clean, 'UK', '') = com.Market__c
GROUP BY Market_Clean 
	  ,Id 
	  ,Business_Unit__c
;

SELECT Market
	  ,Agency__c
	  ,Forecast_Month__c
	  ,Forecast_Year__c
	  ,Forecast__c * 1000 AS Forecast__c
	  ,CurrencyIsoCode
	  ,Forecast_Type__c
	  ,Forecast_Quarter__c
	  ,Business_Unit__c
	  ,Forecast_Level__c
FROM #unP
UNPIVOT
(
	Forecast__c
	FOR Forecast_Month__c IN (Jan, Feb, Mar, Apr, May, Jun, Jul, Aug, Sep, Oct, Nov, [Dec])
) u
WHERE Forecast__c <> 0

CREATE PROCEDURE [dbo].[TurkeyUpdate_Sell_Line_sp] AS


IF OBJECT_ID('tempdb..#sql_sell_line') IS NOT NULL DROP TABLE #sql_sell_line;
IF OBJECT_ID('tempdb..#sf_sell_line') IS NOT NULL DROP TABLE #sf_sell_line;

IF OBJECT_ID('tempdb..#hash_sql') IS NOT NULL DROP TABLE #hash_sql;
IF OBJECT_ID('tempdb..#hash_sf') IS NOT NULL DROP TABLE #hash_sf;

CREATE TABLE #hash_sql(
[Opportunity__c] [nvarchar](218) NULL,
[External_Id__c] [nvarchar](222) NULL,
[Product_Detail__c] [nvarchar](242) NULL,
[Start_Date__c] [datetime] NULL,
[End_Date__c] [datetime] NULL,
[Buy_Name_txt__c] [nvarchar](456) NULL,
[Imputing_Margin_or_Net__c] [nvarchar](233) NULL,
[PackageType__c] [nvarchar](250) NULL,
--[RecordTypeId] [nvarchar](218) NULL,
[Supplier_Name__c] [nvarchar](218) NULL,
[Buy_Volume__c] [float] NULL,
--[Original_Gross_Budget__c] [float] NULL,
[Gross_Cost__c] [float] NULL,
[Rate__c] [float] NULL,
[Media_Net_Cost__c] [float] NULL,
[Buy_Type__c] [nvarchar](209) NULL,
[Audience_Tier__c] [nvarchar](265) NULL,
[Current_Margin__c] [float] NULL,
[Current_Margin_Explanation__c] [nvarchar](267) NULL,
[Opp_Buy_Description__c] [nvarchar](3924) NULL,
[Input_Mode__c] [nvarchar](224) NULL,
[Media_Code__c] [nvarchar](215) NULL,
[Formats__c] [nvarchar](264) NULL,
[Id] [nvarchar](218) NULL,
) ON [PRIMARY];

CREATE TABLE #hash_sf(
[Opportunity__c] [nvarchar](218) NULL,
[External_Id__c] [nvarchar](222) NULL,
[Product_Detail__c] [nvarchar](242) NULL,
[Start_Date__c] [datetime] NULL,
[End_Date__c] [datetime] NULL,
[Buy_Name_txt__c] [nvarchar](456) NULL,
[Imputing_Margin_or_Net__c] [nvarchar](233) NULL,
[PackageType__c] [nvarchar](250) NULL,
--[RecordTypeId] [nvarchar](218) NULL,
[Supplier_Name__c] [nvarchar](218) NULL,
[Buy_Volume__c] [float] NULL,
--[Original_Gross_Budget__c] [float] NULL,
[Gross_Cost__c] [float] NULL,
[Rate__c] [float] NULL,
[Media_Net_Cost__c] [float] NULL,
[Buy_Type__c] [nvarchar](209) NULL,
[Audience_Tier__c] [nvarchar](265) NULL,
[Current_Margin__c] [float] NULL,
[Current_Margin_Explanation__c] [nvarchar](267) NULL,
[Opp_Buy_Description__c] [nvarchar](3924) NULL,
[Input_Mode__c] [nvarchar](224) NULL,
[Media_Code__c] [nvarchar](215) NULL,
[Formats__c] [nvarchar](264) NULL,
[Id] [nvarchar](218) NULL,
) ON [PRIMARY];

INSERT INTO #hash_sql
SELECT [Opportunity__c]
		,[External_ID__c]
		,[Product_Detail__c]
		,[Start_Date__c]
		,[End_Date__c]
		,[Buy_Name_txt__c]
		,[Imputing_Margin_or_Net__c]
		,[PackageType__c]
--		,[RecordTypeId]
		,[Supplier_Name__c]
		,[Buy_Volume__c]
--		,[Original_Gross_Budget__c]
		,[Gross_Cost__c]
		,[Rate__c]
		,[Media_Net_Cost__c]
		,LOWER([Buy_Type__c]) AS Buy_Type__c
		,[Audience_Tier__c]
		,[Current_Margin__c]
		,[Current_Margin_Explanation__c]
		,[Opp_Buy_Description__c]
		,[Input_Mode__c]
		,[Media_Code__c]
		,[Formats__c]
		,[Id]
FROM Production.dbo.Turkey_Sell_Lines;

INSERT INTO #hash_sf
SELECT [Opportunity__c]
		,[External_ID__c]
		,[Product_Detail__c]
		,[Start_Date__c]
		,[End_Date__c]
		,[Buy_Name_txt__c]
		,[Imputing_Margin_or_Net__c]
		,[PackageType__c]
--		,[RecordTypeId]
		,[Supplier_Name__c]
		,[Buy_Volume__c]
--		,[Original_Gross_Budget__c]
		,[Gross_Cost__c]
		,[Rate__c]
		,[Media_Net_Cost__c]
		,LOWER([Buy_Type__c]) AS Buy_Type__c
		,[Audience_Tier__c]
		,[Current_Margin__c]
		,[Current_Margin_Explanation__c]
		,[Opp_Buy_Description__c]
		,[Input_Mode__c]
		,[Media_Code__c]
		,[Formats__c]
		,[Id]
FROM Production.dbo.Opportunity_Buy__c;



SELECT sq.[Opportunity__c]
      ,sq.[External_ID__c]
      ,sq.[Product_Detail__c]
      ,sq.[Start_Date__c]
      ,sq.[End_Date__c]
      ,sq.[Buy_Name_txt__c]
      ,sq.[Imputing_Margin_or_Net__c]
      ,sq.[PackageType__c]
--      ,sq.[RecordTypeId]
      ,sq.[Supplier_Name__c]
      ,sq.[Buy_Volume__c]
--	  ,sq.[Original_Gross_Budget__c]
      ,sq.[Gross_Cost__c]
      ,sq.[Rate__c]
      ,sq.[Media_Net_Cost__c]
      ,sq.[Buy_Typ
e__c]
      ,sq.[Audience_Tier__c]
      ,sq.[Current_Margin__c]
      ,sq.[Current_Margin_Explanation__c]
      ,sq.[Opp_Buy_Description__c]
      ,sq.[Input_Mode__c]
      ,sq.[Media_Code__c]
      ,sq.[Formats__c]
      ,sq.[Id]
	  ,HASHBYTES('SHA1', (SELECT TOP 1 * FROM #hash_sql bb WHERE bb.External_ID__c = sq.External_ID__c FOR XML RAW)) AS sq_hash
INTO #sql_sell_line
 FROM [Production].[dbo].[Turkey_Sell_Lines] sq
WHERE sq.Id IS NOT NULL;

SELECT  [Opportunity__c]
		,sf_in.[External_ID__c]
		,[Product_Detail__c]
		,[Start_Date__c]
		,[End_Date__c]
		,[Buy_Name_txt__c]
		,[Imputing_Margin_or_Net__c]
		,[PackageType__c]
--		,[RecordTypeId]
		,[Supplier_Name__c]
		,[Buy_Volume__c]
--		,[Original_Gross_Budget__c]
		,[Gross_Cost__c]
		,[Rate__c]
		,[Media_Net_Cost__c]
		,[Buy_Type__c]
		,[Audience_Tier__c]
		,[Current_Margin__c]
		,[Current_Margin_Explanation__c]
		,[Opp_Buy_Description__c]
		,[Input_Mode__c]
		,[Media_Code__c]
		,[Formats__c]
		,[Id]
		,HASHBYTES('SHA1', (SELECT TOP 1 * FROM #hash_sf cc WHERE cc.External_ID__c = sf_in.External_ID__c FOR XML RAW)) AS sf_hash
INTO #sf_sell_line
FROM [Production].[dbo].Opportunity_Buy__c sf_in
	INNER JOIN (SELECT External_Id__c FROM Production.dbo.Turkey_Sell_Lines) sq
		ON sf_in.External_Id__c = sq.External_ID__c



SELECT   [Opportunity__c]
		,[External_ID__c]
		,[Product_Detail__c]
		,[Start_Date__c]
		,[End_Date__c]
		,[Buy_Name_txt__c]
		,[Imputing_Margin_or_Net__c]
		,[PackageType__c]
--		,[RecordTypeId]
		,[Supplier_Name__c]
		,[Buy_Volume__c]
--		,[Original_Gross_Budget__c]
		,[Gross_Cost__c]
		,[Rate__c]
		,[Media_Net_Cost__c]
		,[Buy_Type__c]
		,[Audience_Tier__c]
		,[Current_Margin__c]
		,[Current_Margin_Explanation__c]
		,[Opp_Buy_Description__c]
		,[Input_Mode__c]
		,[Media_Code__c]
		,[Formats__c]
		,[Id]
FROM #sql_sell_line sq
	LEFT JOIN ( SELECT sf_hash
				FROM #sf_sell_line
			   ) sf
		ON sq.sq_hash = sf.sf_hash
WHERE sf.sf_hash IS NULL



/*
SELECT *
FROM #hash_sql
UNION 
SELECT #hash_sf.*
FROM #hash_sf
	INNER JOIN (SELECT External_Id__c FROM Production.dbo.Turkey_Sell_Lines) sq
		ON #hash_sf.External_Id__c = sq.External_ID__c

*/

CREATE PROCEDURE [dbo].[SpainNew_Sell_Lines_sp] AS

IF OBJECT_ID('tempdb..#temp_Spain_SL') IS NOT NULL DROP TABLE #temp_Spain_SL;

CREATE TABLE #temp_Spain_SL(
[Opportunity__c] [nvarchar](218) NULL,
[Id] [nvarchar](218) NULL,
[External_Id__c] [nvarchar](222) NULL,
[Product_Detail__c] [nvarchar](242) NULL,
[Start_Date__c] [datetime] NULL,
[End_Date__c] [datetime] NULL,
[Buy_Name_txt__c] [nvarchar](456) NULL,
[Imputing_Margin_or_Net__c] [nvarchar](233) NULL,
[PackageType__c] [nvarchar](250) NULL,
[RecordTypeId] [nvarchar](218) NULL,
[Supplier_Name__c] [nvarchar](218) NULL,
[Buy_Volume__c] [float] NULL,
[Gross_Cost__c] [float] NULL,
[Rate__c] [float] NULL,
[Media_Net_Cost__c] [float] NULL,
[Buy_Type__c] [nvarchar](209) NULL,
[Media_Code__c] [nvarchar](215) NULL,
[Audience_Tier__c] [nvarchar](265) NULL,
[Current_Margin__c] [float] NULL,
[Current_Margin_Explanation__c] [nvarchar](267) NULL,
[Opp_Buy_Description__c] [nvarchar](3924) NULL,
[Input_Mode__c] [nvarchar](224) NULL,
[Formats__c] [nvarchar](264) NULL,
) ON [PRIMARY];


INSERT INTO #temp_Spain_SL
EXECUTE [dbo].[Spain_Sell_Lines_sp];

SELECT Opportunity__c
	  ,External_ID__c
	  ,Product_Detail__c
	  ,Start_Date__c
	  ,End_Date__c
	  ,Buy_Name_txt__c
	  ,Imputing_Margin_or_Net__c
	  ,PackageType__c
	  ,RecordTypeId
	  ,Supplier_Name__c
	  ,Buy_Volume__c
	  ,Gross_Cost__c
	  ,Rate__c
	  ,Media_Net_Cost__c
	  ,Buy_Type__c
	  ,Media_Code__c
	  ,Audience_Tier__c
	  ,Current_Margin__c
	  ,Current_Margin_Explanation__c
	  ,Opp_Buy_Description__c
	  ,Input_Mode__c
	  ,Formats__c
FROM #temp_Spain_SL
WHERE Id IS NULL




CREATE PROCEDURE [dbo].[SpainUpdate_Sell_Line_sp] AS


IF OBJECT_ID('tempdb..#sql_sell_line') IS NOT NULL DROP TABLE #sql_sell_line;
IF OBJECT_ID('tempdb..#sf_sell_line') IS NOT NULL DROP TABLE #sf_sell_line;

IF OBJECT_ID('tempdb..#hash_sf_temp') IS NOT NULL DROP TABLE #hash_sf;
IF OBJECT_ID('tempdb..#hash_sql') IS NOT NULL DROP TABLE #hash_sql;
IF OBJECT_ID('tempdb..#hash_sf') IS NOT NULL DROP TABLE #hash_sf;

CREATE TABLE #hash_sql_temp(
	[Opportunity__c] [nvarchar](218) NULL,
	[Id] [nvarchar](218) NULL,
	[External_Id__c] [nvarchar](222) NULL,
	[Product_Detail__c] [nvarchar](242) NULL,
	[Start_Date__c] [datetime] NULL,
	[End_Date__c] [datetime] NULL,
	[Buy_Name_txt__c] [nvarchar](456) NULL,
	[Imputing_Margin_or_Net__c] [nvarchar](233) NULL,
	[PackageType__c] [nvarchar](250) NULL,
	[RecordTypeId] [nvarchar](218) NULL,
	[Supplier_Name__c] [nvarchar](218) NULL,
	[Buy_Volume__c] [float] NULL,
	[Gross_Cost__c] [float] NULL,
	[Rate__c] [float] NULL,
	[Media_Net_Cost__c] [float] NULL,
	[Buy_Type__c] [nvarchar](209) NULL,
	[Media_Code__c] [nvarchar](215) NULL,
	[Audience_Tier__c] [nvarchar](265) NULL,
	[Current_Margin__c] [float] NULL,
	[Current_Margin_Explanation__c] [nvarchar](267) NULL,
	[Opp_Buy_Description__c] [nvarchar](3924) NULL,
	[Input_Mode__c] [nvarchar](224) NULL,
	[Formats__c] [nvarchar](264) NULL,
) ON [PRIMARY];

CREATE TABLE #hash_sql(
	[Opportunity__c] [nvarchar](218) NULL,
	[Id] [nvarchar](218) NULL,
	[External_Id__c] [nvarchar](222) NULL,
	[Product_Detail__c] [nvarchar](242) NULL,
	[Start_Date__c] [datetime] NULL,
	[End_Date__c] [datetime] NULL,
	[Buy_Name_txt__c] [nvarchar](456) NULL,
	[Imputing_Margin_or_Net__c] [nvarchar](233) NULL,
	[PackageType__c] [nvarchar](250) NULL,
--	[RecordTypeId] [nvarchar](218) NULL,
	[Supplier_Name__c] [nvarchar](218) NULL,
	[Buy_Volume__c] [float] NULL,
	[Gross_Cost__c] [float] NULL,
	[Rate__c] [float] NULL,
	[Media_Net_Cost__c] [float] NULL,
	[Buy_Type__c] [nvarchar](209) NULL,
	[Media_Code__c] [nvarchar](215) NULL,
	[Audience_Tier__c] [nvarchar](265) NULL,
	[Current_Margin__c] [float] NULL,
	[Current_Margin_Explanation__c] [nvarchar](267) NULL,
	[Opp_Buy_Description__c] [nvarchar](3924) NULL,
	[Input_Mode__c] [nvarchar](224) NULL,
	[Formats__c] [nvarchar](264) NULL,
) ON [PRIMARY];


CREATE TABLE #hash_sf(
	[Opportunity__c]			[nvarchar](218) NULL,
	[Id]						[nvarchar](218) NULL,
	[External_Id__c]			[nvarchar](222) NULL,
	[Product_Detail__c]			[nvarchar](242) NULL,
	[Start_Date__c]				[datetime] NULL,
	[End_Date__c]				[datetime] NULL,
	[Buy_Name_txt__c]			[nvarchar](456) NULL,
	[Imputing_Margin_or_Net__c] [nvarchar](233) NULL,
	[PackageType__c]			[nvarchar](250) NULL,
	--[RecordTypeId]			[nvarchar](218) NULL,
	[Supplier_Name__c]			[nvarchar](218) NULL,
	[Buy_Volume__c]				[float] NULL,
	[Gross_Cost__c]				[float] NULL,
	[Rate__c]					[float] NULL,
	[Media_Net_Cost__c]			[float] NULL,
	[Buy_Type__c]				[nvarchar](209) NULL,
	[Media_Code__c]				[nvarchar](215) NULL,
	[Audience_Tier__c]			[nvarchar](265) NULL,
	[Current_Margin__c]			[float] NULL,
	[Current_Margin_Explanation__c] [nvarchar](267) NULL,
	[Opp_Buy_Description__c]	[nvarchar](3924) NULL,
	[Input_Mode__c]				[nvarchar](224) NULL,
	[Formats__c]				[nvarchar](264) NULL,
) ON [PRIMARY];

INSERT INTO #hash_sql_temp
EXECUTE [dbo].[Spain_Sell_Lines_sp];

INSERT INTO #hash_sql
SELECT [Opportunity__c]
		,[Id]
		,[External_Id__c]
		,[Product_Detail__c]
		,[Start_Date__c]				
		,[End_Date__c]		
		,[Buy_Name_txt__c]
		,[Imputing_Margin_or_Net__c]
		,[PackageType__c]		
		--,[RecordTypeId],			
		,[Supplier_Name__c]			
		,[Buy_Volume__c]				
		,[Gross_Cost__c]
		,[Rate__c]		
		,[Media_Net_Cost__c]
		,LOWER([Buy_Type__c]) AS Buy_Type__c
		,[Media_Code__c]			
		,[Audience_Tier__c]			
		,[Current_Margin__c]
		,[Current_Margin_Explanation__c]
		,[Opp_Buy_Description__c]
		,[Input_Mode__c]
		,[Formats__c]
FROM #hash_sql_tem
p;

INSERT INTO #hash_sf
SELECT [Opportunity__c]
		,[Id]
		,[External_Id__c]
		,[Product_Detail__c]
		,[Start_Date__c]				
		,[End_Date__c]		
		,[Buy_Name_txt__c]
		,[Imputing_Margin_or_Net__c]
		,[PackageType__c]		
		--,[RecordTypeId],			
		,[Supplier_Name__c]			
		,[Buy_Volume__c]				
		,[Gross_Cost__c]
		,[Rate__c]		
		,[Media_Net_Cost__c]
		,LOWER([Buy_Type__c]) AS Buy_Type__c
		,[Media_Code__c]			
		,[Audience_Tier__c]			
		,[Current_Margin__c]
		,[Current_Margin_Explanation__c]
		,[Opp_Buy_Description__c]
		,[Input_Mode__c]
		,[Formats__c]
FROM Production.dbo.Opportunity_Buy__c;



SELECT sq.[Opportunity__c]
      ,sq.[External_ID__c]
      ,sq.[Product_Detail__c]
      ,sq.[Start_Date__c]
      ,sq.[End_Date__c]
      ,sq.[Buy_Name_txt__c]
      ,sq.[Imputing_Margin_or_Net__c]
      ,sq.[PackageType__c]
--      ,sq.[RecordTypeId]
      ,sq.[Supplier_Name__c]
      ,sq.[Buy_Volume__c]
      ,sq.[Gross_Cost__c]
      ,sq.[Rate__c]
      ,sq.[Media_Net_Cost__c]
      ,sq.[Buy_Type__c]
      ,sq.[Audience_Tier__c]
      ,sq.[Current_Margin__c]
      ,sq.[Current_Margin_Explanation__c]
      ,sq.[Opp_Buy_Description__c]
      ,sq.[Input_Mode__c]
      ,sq.[Media_Code__c]
      ,sq.[Formats__c]
      ,sq.[Id]
	  ,HASHBYTES('SHA1', (SELECT TOP 1 * FROM #hash_sql bb WHERE bb.External_ID__c = sq.External_ID__c FOR XML RAW)) AS sq_hash
INTO #sql_sell_line
-- FROM [Production].[dbo].[Turkey_Sell_Lines] sq
FROM #hash_sql sq
WHERE sq.Id IS NOT NULL;

SELECT  [Opportunity__c]
		,sf_in.[External_ID__c]
		,[Product_Detail__c]
		,[Start_Date__c]
		,[End_Date__c]
		,[Buy_Name_txt__c]
		,[Imputing_Margin_or_Net__c]
		,[PackageType__c]
--		,[RecordTypeId]
		,[Supplier_Name__c]
		,[Buy_Volume__c]
		,[Gross_Cost__c]
		,[Rate__c]
		,[Media_Net_Cost__c]
		,[Buy_Type__c]
		,[Audience_Tier__c]
		,[Current_Margin__c]
		,[Current_Margin_Explanation__c]
		,[Opp_Buy_Description__c]
		,[Input_Mode__c]
		,[Media_Code__c]
		,[Formats__c]
		,[Id]
		,HASHBYTES('SHA1', (SELECT TOP 1 * FROM #hash_sf cc WHERE cc.External_ID__c = sf_in.External_ID__c FOR XML RAW)) AS sf_hash
INTO #sf_sell_line
FROM [Production].[dbo].Opportunity_Buy__c sf_in
--	INNER JOIN (SELECT External_Id__c FROM Production.dbo.Turkey_Sell_Lines) sq
	INNER JOIN (SELECT External_Id__c FROM #hash_sql) sq
		ON sf_in.External_Id__c = sq.External_ID__c



SELECT   [Opportunity__c]
		,[External_ID__c]
		,[Product_Detail__c]
		,[Start_Date__c]
		,[End_Date__c]
		,[Buy_Name_txt__c]
		,[Imputing_Margin_or_Net__c]
		,[PackageType__c]
--		,[RecordTypeId]
		,[Supplier_Name__c]
		,[Buy_Volume__c]
		,[Gross_Cost__c]
		,[Rate__c]
		,[Media_Net_Cost__c]
		,[Buy_Type__c]
		,[Audience_Tier__c]
		,[Current_Margin__c]
		,[Current_Margin_Explanation__c]
		,[Opp_Buy_Description__c]
		,[Input_Mode__c]
		,[Media_Code__c]
		,[Formats__c]
		,[Id]
FROM #sql_sell_line sq
	LEFT JOIN ( SELECT sf_hash
				FROM #sf_sell_line
			   ) sf
		ON sq.sq_hash = sf.sf_hash
WHERE sf.sf_hash IS NULL



/*
SELECT *
FROM #hash_sql
UNION 
SELECT #hash_sf.*
FROM #hash_sf
	INNER JOIN (SELECT External_Id__c FROM Production.dbo.Turkey_Sell_Lines) sq
		ON #hash_sf.External_Id__c = sq.External_ID__c

*/

