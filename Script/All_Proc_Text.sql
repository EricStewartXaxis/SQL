 
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
						   END LIKE '%' + sf.Name + '%'
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
		  ,(SELECT TOP 1 [Id] FROM RecordType WHERE SobjectType = 'Account' AND DeveloperName = 'Xaxis_Media_Buying_EMEA') AS RecordTypeId
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
						  ,Advertiser__c
						  ,Agency__c
						  ,SUM(ISNULL(BudgetNet, 0)) AS sum_BudgetNet
						  ,MAX(ISNULL(EndDate, '1900-01-01 00:00:00.0000000')) AS EndDate					  
				    FROM w_Account
					GROUP BY Id
							,CampaignName
							,Advertiser__c
							,Agency__c
					) sum_net
			ON wa.Id = sum_net.Id
		   AND wa.CampaignName = sum_net.CampaignName
		   And wa.Advertiser__c = sum_net.Advertiser__c
		   AND wa.Agency__c = sum_net.Agency__c					


--	WHERE wa.Agency__c     IS NULL
--	OR    wa.Advertiser__c IS NULL
	)
SELECT DISTINCT wr.Business_Unit__c
	   ,wr.Advertiser__c
	   ,wr.Agency__c
	   ,wr.RecordTypeId
	   ,wr.Id AS AccountId
	   ,wr.Advertiser_Name + ' - ' + dbo.ReplaceExtraChars(CampaignName) AS Name
	   ,CONVERT(DATE,ISNULL(EndDate, '1900-01-01 00:00:00.0000000'),102) AS CloseDate
	   ,'Opportunity Closed Won' AS StageName	   
FROM with_rank wr
	LEFT JOIN Opportunity op
		ON wr.Advertiser_Name + ' - ' + dbo.ReplaceExtraChars(CampaignName) = op.Name
	   AND wr.Id = op.AccountId
WHERE Ranky = 1
  AND adv_Ranky = 1
  AND ag_Ranky = 1
  AND op.Id IS NULL








CREATE PROCEDURE [dbo].[NordicsNew_Opp_sp] AS

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
						   END LIKE '%' + sf.Name + '%'
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

--					  ,eb.OrderID
					  ,eb.CampaignName
					  ,eb.PlacementName
					  ,eb.Unit
					  ,eb.MediaName
					  ,eb.BudgetNet 
					  ,eb.[Client Name]
					  ,eb.EndDate
					  ,eb.OrderID
	--				  ,eb.BookingID
				INTO #w_Account
				FROM XaxisETL.[dbo].[Extract_Nordics] eb
					LEFT JOIN (SELECT acct.Id
									  ,acct.Advertiser__c
									  ,acct.Agency__c
									  ,sf_ag.NAME AS Agency_Name
									  ,sf_adv.NAME AS Adverti
ser_Name
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

						  ,SUM(ISNULL(BudgetNet, 0)) AS sum_BudgetNet
						  ,MAX(ISNULL(EndDate, '1900-01-01 00:00:00.0000000')) AS EndDate					  
				    FROM #w_Account w_Account
					GROUP BY Id
							,CampaignName
							,Advertiser__c
							,Agency__c
							,OrderID

					) sum_net
			ON wa.Id = sum_net.Id
		   AND wa.CampaignName = sum_net.CampaignName
		   And wa.Advertiser__c = sum_net.Advertiser__c
		   AND wa.Agency__c = sum_net.Agency__c		
		   AND wa.OrderID = sum_net.OrderID		


--	WHERE wa.Agency__c     IS NULL
--	OR    wa.Advertiser__c IS NULL
	)
SELECT DISTINCT wr.Business_Unit__c
	   ,wr.Advertiser__c
	   ,wr.Agency__c
	   ,wr.RecordTypeId
	   ,wr.Id AS AccountId
	   ,CASE WHEN LEN(wr.Advertiser_Name + ' - ' + dbo.ReplaceExtraChars(CampaignName) + ' OrderID:' +Cast(OrderID AS VARCHAR)) > 120
				  AND LEN(dbo.ReplaceExtraChars(CampaignName) + ' OrderID:' +Cast(OrderID AS VARCHAR)) > 120
			 THEN  ' OrderID:' +Cast(OrderID AS VARCHAR)
			 WHEN LEN( wr.Advertiser_Name + ' - ' + dbo.ReplaceExtraChars(CampaignName) + ' OrderID:' +Cast(OrderID AS VARCHAR)) >120
			 THEN  dbo.ReplaceExtraChars(CampaignName) + ' 
OrderID:' +Cast(OrderID AS VARCHAR)
			 ELSE  wr.Advertiser_Name + ' - ' + dbo.ReplaceExtraChars(CampaignName) + ' OrderID:' +Cast(OrderID AS VARCHAR) 
		END AS Name
	   ,CONVERT(DATE,ISNULL(EndDate, '1900-01-01 00:00:00.0000000'),102) AS CloseDate
	   ,'Closed Won' AS StageName	   
	   ,' OrderID:' +Cast(OrderID AS VARCHAR) AS External_Id__c
--	   ,op.Id
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


 
-
Production
text
----

CREATE PROCEDURE [dbo].[SpainNew_Accounts_sp] AS

WITH w_Account AS (
SELECT DISTINCT [Field 11] +' - '+ ag.SF AS Name
	  ,acct.Agency__c AS Agency__c
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





CREATE PROCEDURE [dbo].[Spain_Accounts_sp] AS

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

, pos_dup AS (
				SELECT DISTINCT CASE WHEN [Field 5] IS NULL THEN 'Speculative'
									 WHEN [Field 5] LIKE 'Pendiente activación' THEN 'Closed Won'
									 WHEN [Field 5] LIKE 'Pendiente planificación' THEN 'Closed Won'
									 ELSE 'Contacted / Prospecting'
								END AS StageName
				--	  ,CONVERT(VARCHAR(8), CONVERT(DATE,F8,3),3) AS CloseDate
					  ,CONVERT(DATE,[Field 8],101) AS CloseDate
					  ,acct.Advertiser__c AS Advert
iser__c
					  ,acct.Agency__c AS Agency__c
					  ,(SELECT TOP 1 [Id] FROM [RecordType] WHERE SobjectType = 'Opportunity' AND DeveloperName = 'Spain') AS RecordTypeId
					  ,acct.Id AS AccountId
				--	  ,'SpainOpp-'+[Field 4] AS External_Id__c
					  ,dbo.ReplaceExtraChars(LTRIM(RTRIM([Field 12]))) AS 'Name'
					  ,sf_opp.Id
					   ,'EUR' AS CurrencyIsoCode
					   ,dbo.ReplaceExtraChars(LTRIM(RTRIM([Field 6]))) AS [Description]

				FROM XaxisETL.[dbo].[Extract_Spain] eb
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
		)
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
			  FROM pos_dup eb2
			  WHERE eb2.AccountId = eb1.AccountId
				AND eb2.Name = eb1.Name
			  FOR XML PATH('')),1,1,'') AS [Description]
FROM pos_dup eb1
WHERE Id IS NULL









CREATE PROCEDURE [dbo].[Spain_Opp_sp] AS


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

, pos_dup AS (
				SELECT DISTINCT CASE WHEN [Field 5] IS NULL THEN 'Speculative'
									 WHEN [Field 5] LIKE 'Pendiente activación' THEN 'Closed Won'
									 WHEN [Field 5] LIKE 'Pendiente planificación' THEN 'Closed Won'
									 ELSE 'Contacted / Prospecting'
								END AS StageName
				--	  ,CONVERT(VARCHAR(8), CONVERT(DATE,F8,3),3) AS CloseDate
					  ,CONVERT(DATE,[Field 8],101) AS CloseDate
					  ,acct.Advertiser__c AS Advertise
r__c
					  ,acct.Agency__c AS Agency__c
					  ,(SELECT TOP 1 [Id] FROM [RecordType] WHERE SobjectType = 'Opportunity' AND DeveloperName = 'Spain') AS RecordTypeId
					  ,acct.Id AS AccountId
				--	  ,'SpainOpp-'+[Field 4] AS External_Id__c
					  ,dbo.ReplaceExtraChars(LTRIM(RTRIM([Field 12]))) AS 'Name'
					  ,sf_opp.Id
					   ,'EUR' AS CurrencyIsoCode
					   ,dbo.ReplaceExtraChars(LTRIM(RTRIM([Field 6]))) AS [Description]

				FROM XaxisETL.[dbo].[Extract_Spain] eb
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
		)
SELECT DISTINCT StageName
	  ,CloseDate
	  ,Advertiser__c
	  ,Agency__c
	  ,RecordTypeId
	  ,AccountId
	  ,Name
	  ,Id
	  ,CurrencyIsoCode

	  ,STUFF((SELECT ','+ Description
			  FROM pos_dup eb2
			  WHERE eb2.AccountId = eb1.AccountId
				AND eb2.Name = eb1.Name
			  FOR XML PATH('')),1,1,'') AS [Description]
FROM pos_dup eb1






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








CREATE PROCEDURE [dbo].[Spain_Buy_Placement_sp] AS

SELECT DISTINCT ISNULL(dbo.ReplaceExtraChars(LTRIM(RTRIM(eb.[Field 16])))+'-' , '') + sl.External_Id__c  AS Name
	 ,bu.Id
	 ,sl.Id AS Sell_Line__c
	 ,CASE WHEN dbo.ReplaceExtraChars(LTRIM(RTRIM(eb.[Field 16]))) = '' THEN NULL
		   ELSE dbo.ReplaceExtraChars(LTRIM(RTRIM(eb.[Field 16]))) 
	  END AS Creative_Format__c
	 ,SUM(ISNULL(CONVERT(MONEY, REPLACE(dbo.ReplaceExtraChars(eb.[Field 21]), ',','')), 0)) AS Actual_Cost__c
	 ,CAST(CONVERT(DATE,eb.[Field 7],101) AS DATE) AS Start_Date__c
	 ,CAST(CONVERT(DATE,eb.[Field 8],101) AS DATE) AS End_Date__c
	 ,'EUR' AS [CurrencyIsoCode]
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
GROUP BY ISNULL(dbo.ReplaceExtraChars(LTRIM(RTRIM(eb.[Field 16])))+'-' , '') + sl.External_Id__c
	   ,bu.Id
	   ,sl.Id
	   ,CASE WHEN dbo.ReplaceExtraChars(LTRIM(RTRIM(eb.[Field 16]))) = '' THEN NULL
			   ELSE dbo.ReplaceExtraChars(LTRIM(RTRIM(eb.[Field 16]))) 
		  END
	   ,CAST(CONVERT(DATE,eb.[Field 7],101) AS DATE)
	   ,CAST(CONVERT(DATE,eb.[Field 8],101) AS DATE)









CREATE PROCEDURE [dbo].[SpainNew_Buy_Placement_sp] AS

SELECT DISTINCT ISNULL(dbo.ReplaceExtraChars(LTRIM(RTRIM(eb.[Field 16])))+'-' , '') + sl.External_Id__c  AS Name
--	 ,bu.Id
	 ,sl.Id AS Sell_Line__c
	 ,CASE WHEN dbo.ReplaceExtraChars(LTRIM(RTRIM(eb.[Field 16]))) = '' THEN NULL
		   ELSE dbo.ReplaceExtraChars(LTRIM(RTRIM(eb.[Field 16]))) 
	  END AS Creative_Format__c
	 ,SUM(ISNULL(CONVERT(MONEY, REPLACE(dbo.ReplaceExtraChars(eb.[Field 21]), ',','')), 0)) AS Actual_Cost__c
	 ,CAST(CONVERT(DATE,eb.[Field 7],101) AS DATE) AS Start_Date__c
	 ,CAST(CONVERT(DATE,eb.[Field 8],101) AS DATE) AS End_Date__c
	 ,'EUR' AS [CurrencyIsoCode]
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
	AND bu.Id IS NULL
GROUP BY ISNULL(dbo.ReplaceExtraChars(LTRIM(RTRIM(eb.[Field 16])))+'-' , '') + sl.External_Id__c
	   ,bu.Id
	   ,sl.Id
	   ,CASE WHEN dbo.ReplaceExtraChars(LTRIM(RTRIM(eb.[Field 16]))) = '' THEN NULL
			   ELSE dbo.ReplaceExtraChars(LTRIM(RTRIM(eb.[Field 16]))) 
		  END
	   ,CAST(CONVERT(DATE,eb.[Field 7],101) AS DATE)
	   ,CAST(CONVERT(DATE,eb.[Field 8],101) AS DATE)









CREATE PROCEDURE [dbo].[Turkey_Opps_sp] AS
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



CREATE PROCEDURE TurkeyUpdate_Sell_Line_sp AS


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

