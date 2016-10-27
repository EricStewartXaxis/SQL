 
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


CREATE VIEW [dbo].[NordicsNew_Accounts] AS

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
--				SELECT DISTINCT dbo.ReplaceExtraChars(eb.[client name]) +' - '+ dbo.ReplaceExtraChars(eb.brand) +' (Nordics)' AS Name
				SELECT DISTINCT eb.client_name_clean +' - '+ dbo.ReplaceExtraChars(eb.brand) AS Name
					  ,acct.Id
					  ,acct.Agency__c AS Agency__c
					  ,dbo.ReplaceExtraChars(eb.brand) AS Agency_Name
					  ,acct.Advertiser__c AS Advertiser__c
					  ,client_name_clean AS Advertiser_Name
--					  ,acct.ag_Ranky
--					  ,acct.adv_Ranky
				FROM XaxisETL.[dbo].[Extract_Nordics] eb
					LEFT JOIN (SELECT acct.Id
									  ,acct.Advertiser__c
									  ,acct.Agency__c
									  ,sf_ag.NAME AS Agency_Name
									  ,sf_adv.NAME AS Advertiser_Name
									  ,sf_ag.Market__c
									  ,sf_ag.Ranky AS ag_Ranky
									  ,sf_adv.Ranky AS adv_Ranky
								FROM Acc
ount acct
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





CREATE VIEW [dbo].[Nordics_Accounts] AS

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
								WHERE acct
.Advertiser__c IS NOT NULL
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
	SELECT CASE WHEN sf_ag.Market__c IS NULL THEN wa.Name + ' (Nordic)'
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

--	WHERE wa.Agency__c     IS NULL
--	OR    wa.Advertiser__c IS NULL
	)
SELECT Name
	  ,Id AS AccountId
	  ,Advertiser__c
	  ,Agency__c
	  ,Business_Unit__c
	  ,Account_Opt_In_Status__c
	  ,RecordTypeId
FROM with_rank
WHERE Ranky = 1
  AND adv_Ranky = 1
  AND ag_Ranky = 1
  








CREATE VIEW [dbo].[NordicsNew_Opp] AS

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








CREATE VIEW [dbo].[Nordics_Opp] AS

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
SELECT DISTINCT op.Id
	   ,wr.Business_Unit__c
	   ,wr.Advertiser__c
	   ,wr.Agency__c
	   ,wr.RecordTypeId
	   ,wr.Id AS AccountId
	   ,wr.Advertiser_Name + ' - ' + dbo.ReplaceExtraChars(CampaignName) AS Name
	   ,CONVERT(DATE,ISNULL(EndDate, '1900-01-01 00:00:00.0000000'),102) AS CloseDate
	   ,'Opportunity Closed Won' AS StageName	   
FROM with_rank wr
	INNER JOIN Opportunity op
		ON wr.Advertiser_Name + ' - ' + dbo.ReplaceExtraChars(CampaignName) = op.Name
	   AND wr.Id = op.AccountId
WHERE Ranky = 1
  AND adv_Ranky = 1
  AND ag_Ranky = 1
  AND op.Id IS NULL






CREATE VIEW [dbo].[NordicsNew_Sell_Line_sp] AS

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
					  ,eb.StartDate
					  ,eb.OrderID
					  ,eb.Volume

				FROM XaxisETL.[dbo].[Extract_Nordics] eb
					LEFT JOIN (SELECT acct.Id
									  ,acct.Advertiser__c
									  ,acct.Agency__c
									  ,sf_ag.NAME AS Agency_Name
									  ,sf_adv.NAME AS Advertiser_Name
									  ,sf_ag.Market__c
									  ,s
f_ag.Ranky AS ag_Ranky
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
			,wa.OrderID
			,wa.PlacementName
			,sum_net.StartDate AS Start_Date__c
			,sum_net.EndDate AS End_Date__c
			,sum_net.sum_BudgetNet AS Gross_Cost__c
			,wa.MediaName AS Buy_Name_txt__c
			,'Net Cost (Calc Margin)' AS Imputing_Margin_or_Net__c
			,'MediaTrader' AS PackageType__c
			,'Externally Managed' AS Input_Mode__c
			,sum_net.sum_Volume AS Buy_Volume__c
			,'Triggers' AS Audience_Tier__c

			,wa.CampaignName

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
						  ,OrderID
						  ,PlacementName
						  ,MediaName
						  ,SUM(ISNULL(BudgetNet, 0)) AS sum_BudgetNet
						  ,MAX(ISNULL(EndDate, '1900-01-01 00:00:00.0000000')) AS EndDate
						  ,MIN(ISNULL(StartDate,'1900-01-01 00:00:00.0000000')) AS StartDate	
						  ,SUM(ISNULL(Volume, 0)) AS sum_Volume				  
				    FROM w_Account
					GROUP BY Id
							,CampaignName
							,Advertiser__c
							,Agency__c
							,OrderID
							,PlacementName
							,MediaName
					) sum_net
			ON wa.Id = sum_net.Id
		   AND wa.CampaignName = sum_net.CampaignName
		   And wa.Advertiser__c = sum_net.Advertiser__c
		   AND wa.Agency__c = sum_net.Agency__c		
		   AND wa.OrderID = sum_net.OrderID
		   AND wa.PlacementName = sum_net.PlacementName
		   AND wa.MediaName = sum_net.MediaName			



CREATE VIEW [dbo].[Turkey_Company] AS

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
	LEFT JOIN GLStaging.dbo.Company__c com
		ON com.Name = ag.SF
	   AND com.Market__c = 'Turkey'
	   AND com.Type__c = 'Agency'
WHERE [Field 9] IS NOT NULL
  AND [Field 9] <> 'Agency'
  AND [Field 9] <> 'Needs mapping to Turkish Agencies int the system'
  AND [Field 9] <> 'Campaign Details'

UNION ALL

SELECT DISTINCT master.dbo.InitCap(LTRIM(RTRIM(LOWER([Field 10])))) AS Name
      ,'Advertiser' AS Type__c
	  ,'Active' AS Status__c
	  ,NULL AS Market__c
	  ,com.Id
FROM [XaxisETL].[dbo].[Extract_Turkey] et
	LEFT JOIN (SELECT Name, Id 
			  FROM GLStaging.dbo.Company__c
			  WHERE LEFT(Name, 5) <> '[SFL]'
			    AND Type__c = 'Advertiser') com
		ON master.dbo.InitCap(LTRIM(RTRIM(LOWER(et.[Field 10])))) = com.Name
WHERE [Field 9] IS NOT NULL
  AND [Field 9] <> 'Agency'
  AND [Field 9] <> 'Needs mapping to Turkish Agencies int the system'
  AND [Field 9] <> 'Campaign Details'




CREATE VIEW [dbo].[Turkey_Accounts] AS


WITH w_Account AS (
SELECT DISTINCT master.dbo.InitCap(LTRIM(RTRIM(LOWER([Field 10])))) +' - '+ ag.sf AS Name
	  ,acct.Id
	  ,acct.Agency__c AS Agency__c
	  ,ag.sf AS Agency_Name
	  ,acct.Advertiser__c AS Advertiser__c
	  ,master.dbo.InitCap(LTRIM(RTRIM(LOWER([Field 10])))) AS Advertiser_Name
FROM XaxisETL.[dbo].[Extract_Turkey] eb
	LEFT JOIN (
					SELECT 'MX' AS [Turk], 'Maxus (Turkey)' AS [SF] UNION
					SELECT 'MEC' , 'MEC (Turkey)'		UNION
					SELECT 'MC' , 'Mediacom (Turkey)'	UNION
					SELECT 'MS' , 'Mindshare (Turkey)'
					) ag	
			ON eb.[Field 9] = ag.Turk
	LEFT JOIN (SELECT acct.Id
					  ,acct.Advertiser__c
					  ,acct.Agency__c
					  ,sf_ag.NAME AS Agency_Name
					  ,sf_ad.NAME AS Advertiser_Name
				FROM Account acct
					INNER JOIN (SELECT ag.NAME
									 ,ag.Id
							   FROM Turkey_Company ag
							   WHERE ag.type__c = 'Agency'
							   AND Market__c = 'Turkey') sf_ag
						ON acct.Agency__c = sf_ag.Id
					INNER JOIN (SELECT ad.NAME
									  ,ad.Id
								FROM Turkey_Company ad
								WHERE ad.type__c = 'Advertiser') sf_ad
						ON acct.Advertiser__c = sf_ad.Id
				WHERE acct.Advertiser__c IS NOT NULL
				  AND acct.Agency__c IS NOT NULL
				  AND sf_ag.NAME IS NOT NULL
				  AND sf_ad.NAME IS NOT NULL
				  ) acct
	ON acct.Advertiser_Name = master.dbo.InitCap(LTRIM(RTRIM(LOWER(eb.[Field 10]))))
   AND acct.Agency_Name = ag.SF
WHERE [Field 9] IS NOT NULL
  AND [Field 9] <> 'Agency'
  AND [Field 9] <> 'Needs mapping to Turkish Agencies int the system'
  AND [Field 9] <> 'Campaign Details'
  
)
, Adv_Count AS (
	SELECT bc.id
		  ,ISNULL(COUNT(ba.Id), 0) AS a_count
	FROM Turkey_Company bc
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
		  ,DENSE_RANK() OVER (PARTITION BY wa.Name ORDER BY Adv_Count.a_count, Adv_Created.CreatedDate, Ag_Created.CreatedDate, ISNULL(Acct_Count.account_count, 0) DESC, wa.Id) AS Ranky
--		  ,Adv_Count.a_count
--		  ,Adv_Created.CreatedDate AS adv_crd
--		  ,Ag_Created.CreatedDate  
--		  ,ISNULL(Acct_Count.account_count, 0) AS account_count
	FROM w_Account wa
		LEFT JOIN(
				   SELECT ag.NAME
						 ,ag.Id
				   FROM Turkey_Company ag
				   WHERE ag.type__c = 'Agency'
					 AND ag.Market__c = 'Turkey'
					 ) sf_ag
			ON wa.Agency_Name = sf_ag.Name
--		   AND wa.Agency__c = sf_ag.Id
		LEFT JOIN( 
				   SELECT ad.NAME
						,ad.Id
				   FROM Turkey_Company ad
				   WHERE ad.type__c = 'Advertiser') sf_ad
			ON wa.Advertiser_Name = sf_ad.Name
--		   AND wa.Advertiser__c = sf_ad.Id
		LEFT JOIN Adv_Count
			ON Adv_Count.Id = sf_ad.id
		LEFT JOIN Adv_Created
			ON Adv_Created.Id = sf_ad.id
		LEFT JOIN Ag_Created
			ON Ag_Created.Id = sf_ag.id
		LEFT JOIN Acct_Count
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







CREATE VIEW [dbo].[Turkey_Sell_Lines] AS

SELECT opp.Id AS Opportunity__c
--	  ,CONVERT(VARCHAR(1000), HashBytes('MD5', [Field 2] + [Field 13]), 2) AS External_ID__c
      ,CAST(CHECKSUM(dbo.ReplaceExtraChars([Field 13]), dbo.ReplaceExtraChars([Field 16]), dbo.ReplaceExtraChars([Field 2])) AS VARCHAR(25)) AS External_ID__c
	  ,dbo.ReplaceExtraChars([Field 15]) AS Product_Detail__c
      ,CAST(CONVERT(DATE,[Field 6],101) AS DATE) AS Start_Date__c
      ,CAST(CONVERT(DATE,[Field 7],101) AS DATE) AS End_Date__c
      ,dbo.ReplaceExtraChars([Field 2]) AS Buy_Name_txt__c
      ,'Net Cost (Calc Margin)' AS Imputing_Margin_or_Net__c
      ,'MediaTrader' AS PackageType__c
      ,(SELECT TOP 1 Id FROM RecordType WHERE SobjectType = 'Opportunity_Buy__c' AND DeveloperName = 'Turkey') AS RecordTypeId
      ,dbo.ReplaceExtraChars([Field 13]) AS Supplier_Name__c
      ,CONVERT(MONEY, REPLACE(REPLACE(REPLACE(dbo.ReplaceExtraChars(ISNULL([Field 21], 0)), ',',''), 'TL', ''), 'click', '')) AS Buy_Volume__c
      ,CONVERT(MONEY, REPLACE(REPLACE(dbo.ReplaceExtraChars(ISNULL([Field 31], 0)), ',',''), 'TL', '')) AS Gross_Cost__c
      ,CONVERT(MONEY, REPLACE(REPLACE(dbo.ReplaceExtraChars(ISNULL([Field 20], 0)), ',',''), 'TL', '')) AS Rate__c
      ,CONVERT(MONEY, REPLACE(REPLACE(dbo.ReplaceExtraChars(ISNULL([Field 32], 0)), ',',''), 'TL', '')) AS Media_Net_Cost__c
      ,dbo.ReplaceExtraChars([Field 16]) AS Buy_Type__c
      ,'Triggers' AS Audience_Tier__c
      ,CONVERT(MONEY, REPLACE(REPLACE(dbo.ReplaceExtraChars(ISNULL([Field 36], 0)), ',',''), '%',''))/100 AS Current_Margin__c
      ,'From spreadsheet: ' + [Tag: Filename] AS Current_Margin_Explanation__c
      ,dbo.ReplaceExtraChars([Field 17]) AS Opp_Buy_Description__c
      ,'Externally Managed' AS Input_Mode__c
	  ,'Digital' AS Media_Code__c
	  ,dbo.ReplaceExtraChars([Field 18]) AS Formats__c
	  ,sl.Id
FROM XaxisETL.dbo.Extract_Turkey et
	LEFT JOIN (SELECT Name, Id
			   FROM GLStaging.dbo.Opportunity
			   ) opp
		ON dbo.ReplaceExtraChars(LTRIM(RTRIM(et.[Field 11]))) = opp.Name
	LEFT JOIN dbo.Opportunity_Buy__c sl
		ON CAST(CHECKSUM(dbo.ReplaceExtraChars([Field 13]), dbo.ReplaceExtraChars([Field 16]), dbo.ReplaceExtraChars([Field 2])) AS VARCHAR(25)) = sl.External_Id__c
WHERE [Field 9] IS NOT NULL
  AND [Field 9] <> 'Agency'
  AND [Field 9] <> 'Needs mapping to Turkish Agencies int the system'
  AND [Field 9] <> 'Campaign Details'
--  AND sl.External_Id__c IS NULL   



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



CREATE VIEW [dbo].[Turkey_Opps] AS
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
					 WHEN [Field 5] LIKE 'Pendiente activaci�n' THEN 'Contract Pending'
					 WHEN [Field 5] LIKE 'Pendiente planificaci�n' THEN 'Contract Pending'
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
--					 WHEN [Field 5] LIKE 'Pendiente activaci�n' THEN 'Contract Pending'
--					 WHEN [Field 5] LIKE 'Pendiente planificaci�n' THEN 'Proposal Sent'
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








CREATE VIEW [dbo].[Nordics_Company_Locic] AS
WITH lev_Agency AS (
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
,lev_Adv AS (
				SELECT DISTINCT dbo.ReplaceExtraChars([client name]) AS Name
					 ,'Advertiser' AS Type__c
					,'Active' AS Status__c
					, NULL AS Market__c
					,sf.Id
					,sf.Name_Full

					,master.dbo.Levenshtein(dbo.ReplaceExtraChars([client name]),Name_full, LEN(dbo.ReplaceExtraChars([client name]))-1) AS dis

					,DENSE_RANK() OVER(PARTITION BY dbo.ReplaceExtraChars([client name]) 
							   ORDER BY ISNULL(master.dbo.Levenshtein(dbo.ReplaceExtraChars([client name]),Name_full, LEN(dbo.ReplaceExtraChars([client name]))-1)
							                  ,LEN(dbo.ReplaceExtraChars([client name]))), sf.Id) AS Ranky

				FROM XaxisETL.dbo.Extract_Nordics en
					LEFT JOIN (SELECT DISTINCT CASE WHEN CHARINDEX(' ', Name) < 8 THEN Name
											ELSE LEFT(Name,CHARINDEX(' ', NAME) - 1) 
									  END AS Name
									 , Id
									 ,Name AS Name_Full
							  FROM GLStaging.dbo.Company__c
							  WHERE type__c = 'Advertiser') sf
					ON CASE WHEN CHARINDEX(' ', [client name]) < 8 THEN [client name]
									ELSE LEFT([client name],CHARINDEX(' ', [client name]) - 1)
								   END LIKE '%' + REPLACE(REPLACE(sf.Name,'[',''), ']', '') + '%'
				WHERE [client name] IS NOT NULL
		)

SELECT Name
	  ,Type__c
	  ,Status__c
	  ,Market__c
	  ,Id
FROM lev_Agency
WHERE Ranky = 1

UNION ALL 

SELECT Name
	  ,Type__c
	  ,Status__c
	  ,Market__c
	  ,Id
FROM lev_Adv
WHERE Ranky = 1




CREATE VIEW [dbo].[Nordics_Company_OLD] AS

SELECT DISTINCT dbo.ReplaceExtraChars(brand) AS Name
	  ,'Agency' AS Type__c
	  ,'Active' AS Status__c
	  ,'Nordic' AS Market__c
	  ,sf.Id
FROM XaxisETL.dbo.Extract_Nordics en
	LEFT JOIN (SELECT DISTINCT NAME, Id
			  FROM GLStaging.dbo.Company__c 
			  WHERE type__c = 'Agency'
			  AND Market__c LIKE '%Spain%') sf
		ON dbo.ReplaceExtraChars(en.brand) = sf.Name
WHERE brand IS NOT NULL

UNION ALL

SELECT DISTINCT dbo.ReplaceExtraChars([client name]) AS Name
	 ,'Advertiser' AS Type__c
	,'Active' AS Status__c
	, NULL AS Market__c
	,sf.Id
FROM XaxisETL.dbo.Extract_Nordics en
	LEFT JOIN (SELECT DISTINCT NAME, Id
			  FROM GLStaging.dbo.Company__c
			  WHERE type__c = 'Advertiser') sf
	ON  dbo.ReplaceExtraChars(en.[client name]) = sf.NAME
WHERE [client name] IS NOT NULL









CREATE VIEW [dbo].[Nordics_Company_Agency] AS

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



CREATE VIEW [dbo].[Nordics_Company] AS

SELECT Name
	  ,Type__c
	  ,Status__c
	  ,Market__c
	  ,Id
FROM Nordics_Company_Agency
WHERE Ranky = 1

UNION ALL 

SELECT Name
	  ,Type__c
	  ,Status__c
	  ,NULL AS Market__c
	  ,Id
FROM Nordics_Company_Advertiser
WHERE Ranky = 1


CREATE VIEW [dbo].[Nordics_Company_Advertiser] AS

SELECT DISTINCT [client name] AS Name
				,'Advertiser' AS Type__c
			,'Active' AS Status__c
--					, NULL AS Market__c
			,sf.Id
			,sf.Name_Full

--					,master.dbo.Levenshtein(dbo.ReplaceExtraChars([client name]),Name_full, DEFAULT) AS dis

			,DENSE_RANK() OVER(PARTITION BY [client name]
						ORDER BY ISNULL(master.dbo.Levenshtein([client name],Name_full, DEFAULT)
							            ,LEN([client name])), sf.Id) AS Ranky

FROM (SELECT DISTINCT dbo.ReplaceExtraChars([client name]) AS [client name] From XaxisETL.dbo.Extract_Nordics) en
	LEFT JOIN (SELECT DISTINCT CASE WHEN CHARINDEX(' ', Name) < 8 THEN Name
							ELSE LEFT(Name,CHARINDEX(' ', NAME) - 1) 
						END AS Name
						, Id
						,Name AS Name_Full
				FROM GLStaging.dbo.Company__c
				WHERE type__c = 'Advertiser') sf
	ON CASE WHEN CHARINDEX(' ', [client name]) < 8 THEN [client name]
					ELSE LEFT([client name],CHARINDEX(' ', [client name]) - 1)
--								   END LIKE '%' + REPLACE(REPLACE(sf.Name,'[',''), ']', '') + '%'
					END LIKE REPLACE(REPLACE(sf.Name,'[',''), ']', '') + '%'
	AND ISNULL( 1- (CAST(master.dbo.Levenshtein([client name]
			                                    ,Name_full
												,DEFAULT) AS DECIMAL)
				/LEN([client name])), 1) >= .2
WHERE [client name] IS NOT NULL






 
-
Production
text
----



CREATE VIEW [dbo].[BeligumNew_Accounts] AS

WITH w_Account AS (
SELECT DISTINCT F5 +' - '+ F3 +' (Belgium)' AS Name
	  ,acct.Agency__c AS Agency__c
	  ,F3 AS Agency_Name
	  ,acct.Advertiser__c AS Advertiser__c
	  ,F5 AS Advertiser_Name
	  ,acct.Id AS accountID
FROM XaxisETL.[dbo].[Extract_Belgium] eb
	LEFT JOIN (SELECT acct.Id
					  ,acct.Advertiser__c
					  ,acct.Agency__c
					  ,sf_ag.NAME AS Agency_Name
					  ,sf_ad.NAME AS Advertiser_Name
				FROM Account acct
					INNER JOIN (SELECT ag.NAME
									 ,ag.Id
							   FROM Company__c ag
							   WHERE ag.type__c = 'Agency'
							   AND Market__c LIKE '%Belgium%') sf_ag
						ON acct.Agency__c = sf_ag.Id
					INNER JOIN (SELECT ad.NAME
									  ,ad.Id
								FROM Company__c ad
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
		  ,DENSE_RANK() OVER (PARTITION BY wa.Name ORDER BY ISNULL(Adv_Count.a_count, 0), ISNULL(Adv_Created.CreatedDate, 0), Ag_Created.CreatedDate) AS Ranky
		  ,wa.accountID AS Id
	FROM w_Account wa
		LEFT JOIN(
				   SELECT ag.NAME
						 ,ag.Id
				   FROM Company__c ag
				   WHERE ag.type__c = 'Agency'
					 AND ag.Market__c LIKE '%Belgium%'
					 ) sf_ag
			ON wa.Agency_Name = LEFT(sf_ag.Name, LEN(wa.Agency_Name))
		LEFT JOIN( 
				   SELECT ad.NAME
						,ad.Id
				   FROM Company__c ad
				   WHERE ad.type__c = 'Advertiser') sf_ad
			ON wa.Advertiser_Name = sf_ad.Name
		LEFT JOIN Adv_Count
			ON sf_ad.id = Adv_Count.Id
		LEFT JOIN Adv_Created
			ON sf_ad.id = Adv_Created.Id
		LEFT JOIN Ag_Created
			ON Ag_Created.Id = sf_ag.id

--	WHERE wa.Agency__c IS NULL
--	OR wa.Advertiser__c IS NULL
	)
SELECT Name
	  ,Advertiser__c
	  ,Agency__c
	  ,Business_Unit__c
	  ,Account_Opt_In_Status__c
	  ,RecordTypeId
	  ,Id
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
--		   AND wa.Agency__c = sf_ag.Id
		INNER JOIN( 
				   SELECT ad.NAME
						,ad.Id
				   FROM Beligum_Company ad
				   WHERE ad.type__c = 'Advertiser') sf_ad
			ON wa.Advertiser_Name = sf_ad.Name
--		   AND wa.Advertiser__c = sf_ad.Id
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








CREATE VIEW [dbo].[Beligum_Company] AS
SELECT DISTINCT dbo.ReplaceExtraChars(LTRIM(RTRIM(eb.[F3]))) AS 'Name'
	  ,'Agency' AS Type__c
	  ,'Active' AS Status__c
	  ,'Belgium' AS Market__c
	  ,sf.Id
FROM XaxisETL.[dbo].[Extract_Belgium] eb
	LEFT JOIN (SELECT DISTINCT NAME, Id
			  FROM Company__c 
			  WHERE type__c = 'Agency'
			  AND Market__c LIKE '%Belgium%') sf
	ON  sf.NAME LIKE '%'+dbo.ReplaceExtraChars(LTRIM(RTRIM(eb.[F3])))+'%'
WHERE eb.[F3] IS NOT NULL
	AND eb.[F5] IS NOT NULL
	AND eb.[F8] IS NOT NULL
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
			  FROM Company__c
			  WHERE type__c = 'Advertiser') sf
	ON  dbo.ReplaceExtraChars(LTRIM(RTRIM(eb.[F5]))) = sf.NAME
WHERE eb.[F3] IS NOT NULL
	AND eb.[F5] IS NOT NULL
	AND eb.[F8] IS NOT NULL
	AND dbo.ReplaceExtraChars(LTRIM(RTRIM(eb.[F5]))) <> ''
	AND eb.id > 8
--	AND sf.NAME IS NULL










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
	  ,CASE WHEN sf_opp_dup.Name IS NULL OR sf_opp.Id IS NOT NULL THEN dbo.ReplaceExtraChars(LTRIM(RTRIM(eb.F6)))
			ELSE dbo.ReplaceExtraChars(LTRIM(RTRIM(eb.F5 + ' - ' +eb.F6)))
		END AS 'Name'
	  ,sf_opp.Id
	  ,CASE WHEN StageName = 'Closed Lost' THEN'Campaign not running' ELSE NULL END AS Reason_Lost__c   
	  ,'EUR' AS CurrencyIsoCode

FROM XaxisETL.[dbo].[Extract_Belgium] eb
	LEFT JOIN Beligum_Accounts acct
		LEFT JOIN Company__c adv
			ON acct.Advertiser__c = adv.Id
		LEFT JOIN Company__c ag
			ON acct.Agency__c = ag.Id
		ON ag.Name LIKE dbo.ReplaceExtraChars(eb.F3) + '%(Belgium)%'
	    AND dbo.ReplaceExtraChars(eb.F5) = adv.Name
	LEFT JOIN Opportunity sf_opp
		ON  (dbo.ReplaceExtraChars(LTRIM(RTRIM(eb.[F6]))) = sf_opp.Name
		  OR 'NewBelgiumOpp-' +dbo.ReplaceExtraChars(LTRIM(RTRIM(eb.[F6]))) = sf_opp.Name
		  OR 'BelgiumOpp-'+eb.F2 = sf_opp.External_Id__c
		  )
	   AND sf_opp.AccountId = acct.Id
	LEFT JOIN (SELECT DISTINCT Name, External_Id__c
			   FROM Opportunity) sf_opp_dup
		ON dbo.ReplaceExtraChars(LTRIM(RTRIM([F6]))) = sf_opp_dup.Name
	   AND 'BelgiumOpp-'+F2 <> ISNULL(sf_opp_dup.External_Id__c, 0)
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
--			  ,0 AS Media_Net_Cost__c
			  ,CASE WHEN LTRIM(RTRIM(eb.F28)) = '' THEN NULL 
					WHEN LTRIM(RTRIM(eb.F28)) = 'None' THEN NULL 
					ELSE LTRIM(RTRIM(eb.F28)) END AS Target_Gender__c
			  ,CASE WHEN LTRIM(RTRIM(eb.F29)) = '' THEN NULL
				    WHEN LTRIM(RTRIM(eb.F29)) = 'None' THEN NULL
					ELSE LTRIM(RTRIM(eb.F29)) END AS Target_Age_Freeform_Entry__c
			  ,CASE WHEN dbo.ReplaceExtraChars(LTRIM(RTRIM(CONCAT(ISNULL(eb.F34, ''), ' ', ISNULL(eb.F35, ''), ' ', ISNULL(eb.F36, ''), ' ', ISNULL(eb.F37, ''), ' ', ISNULL(eb.F43, ''))))) = ''
					THEN NULL
					ELSE dbo.ReplaceExtraChars(LTRIM(RTRIM(CONCAT(ISNULL(eb.F34, ''), ' ', ISNULL(eb.F35, ''), ' ', ISNULL(eb.F36, ''), ' ', ISNULL(eb.F37, ''), ' ', ISNULL(eb.F43, '')))))
			   END AS Opp_Buy_Description__c
			  ,'Externally Managed' AS Input_Mode__c
			  ,dbo.ReplaceExtraChars(LTRIM(RTRIM(eb.F12))) AS Audience_Tier__c
			  ,dbo.ReplaceExtraChars(LTRIM(RTRIM(eb.F11))) AS Media_Code__c
			  ,dbo.ReplaceExtraChars(LTRIM(RTRIM(eb.F10))) AS Special_Product__c
			  ,CASE WHEN dbo.ReplaceExtraChars(LTRIM(RTRIM(EB.F18))) = '' THEN NULL ELSE dbo.ReplaceExtraChars(LTRIM(RTRIM(EB.F18))) END AS FR_line
			  ,CASE WHEN dbo.ReplaceExtraChars(LTRIM(RTRIM(EB.F19))) = '' THEN NULL ELSE dbo.ReplaceExtraChars(LTRIM(RTRIM(EB.F19))) END AS NL_line
			  ,'Belgium' AS Delivery_Market__c
			  ,'CPM' AS Buy_Type__c
			  ,dbo.ReplaceExtraChars(LTRIM(RTRIM(F30))) AS Frequency_Cap__c
			  ,CASE WHEN dbo.ReplaceExtraChars(LTRIM(RTRIM(F30))) IS NULL THEN NULL	
					ELSE 'Per day' 
				END AS Frequency_Scope__c
		  FROM Beligum_Opp bo
			INNER JOIN XaxisETL.dbo.Extract_Belgium eb
				ON REPLACE( External_Id__c, 'BelgiumOpp-', '') = eb.F2
		  WHERE eb.id > 8
		  AND [F3] IS NOT NULL
		  AND [F5] IS NOT NULL
		  AND [F8] IS NOT NULL
		  AND dbo.ReplaceExtraChars(LTRIM(RTRIM([F8]))) <> ''





CREATE VIEW [dbo].[Beligum_Sell_Lines] AS

SELECT External_ID__c + '-FR' AS External_ID__c
	  ,[Opportunity__c]
      ,[Product_Detail__c]
      ,[Start_Date__c]
      ,[End_Date__c]
      ,[Buy_Name_txt__c]
      ,[Imputing_Margin_or_Net__c]
      ,[PackageType__c]
      ,[RecordTypeId]
      ,[Supplier_Name__c]
	  ,Buy_Volume__c_FR AS Buy_Volume__c
      ,Gross_Cost__c_FR AS Gross_Cost__c
      ,[Rate__c]
	  ,0 AS Media_Net_Cost__c
      ,[Target_Gender__c]
      ,[Target_Age_Freeform_Entry__c]
      ,[Opp_Buy_Description__c]
 --     ,[Input_Mode__c]
      ,[Audience_Tier__c]
      ,[Media_Code__c]
      ,[Special_Product__c]
      ,[Delivery_Market__c]
      ,[Buy_Type__c]
      ,[Frequency_Cap__c]
      ,[Frequency_Scope__c]
	  ,'Externally Managed' AS Input_Mode__c

FROM Beligum_Sell_Lines_PreSplit
WHERE FR_line IS NOT NULL
   OR LTRIM(RTRIM(FR_line)) <> ''
UNION ALL
SELECT External_ID__c + '-NL' AS External_ID__c
       ,[Opportunity__c]
      ,[Product_Detail__c]
      ,[Start_Date__c]
      ,[End_Date__c]
      ,[Buy_Name_txt__c]
      ,[Imputing_Margin_or_Net__c]
      ,[PackageType__c]
      ,[RecordTypeId]
      ,[Supplier_Name__c]
	  ,Buy_Volume__c_NL AS Buy_Volume__c
      ,Gross_Cost__c_NL AS Gross_Cost__c
	  ,[Rate__c]
      ,0 AS Media_Net_Cost__c
      ,[Target_Gender__c]
      ,[Target_Age_Freeform_Entry__c]
      ,[Opp_Buy_Description__c]
--      ,[Input_Mode__c]
      ,[Audience_Tier__c]
      ,[Media_Code__c]
      ,[Special_Product__c]
      ,[Delivery_Market__c]
      ,[Buy_Type__c]
      ,[Frequency_Cap__c]
      ,[Frequency_Scope__c]
	  ,'Externally Managed' AS Input_Mode__c
	  
FROM Beligum_Sell_Lines_PreSplit
WHERE NL_line IS NOT NULL
   OR LTRIM(RTRIM(NL_line)) <> ''







CREATE VIEW [dbo].[Spain_Company] AS
SELECT DISTINCT ag.SF AS 'Name'
--	  , dbo.ReplaceExtraChars(LTRIM(RTRIM(eb.[Field 9]))) AS 'Name'
	  ,'Agency' AS Type__c
	  ,'Active' AS Status__c
	  ,'Spain' AS Market__c
	  ,sf.Id
FROM XaxisETL.[dbo].[Extract_Spain] eb
	LEFT JOIN (
				SELECT 'MX' AS [Spain], 'Maxus (Spain)' AS [SF] UNION
				SELECT 'MEC' , 'MEC (Spain)'		UNION
				SELECT 'MC BCN' , 'Mediacom (Spain)'	UNION
				SELECT 'MC MAD' , 'Mediacom (Spain)'	UNION
				SELECT 'MS' , 'Mindshare (Spain)'
				) ag	
		ON dbo.ReplaceExtraChars(LTRIM(RTRIM(eb.[Field 9]))) = ag.Spain
	LEFT JOIN (SELECT DISTINCT NAME, Id
			  FROM Company__c 
			  WHERE type__c = 'Agency'
			  AND Market__c LIKE '%Spain%') sf
		ON  sf.NAME LIKE ag.SF
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
			  FROM Company__c
			  WHERE type__c = 'Advertiser') sf
	ON  dbo.ReplaceExtraChars(LTRIM(RTRIM(eb.[Field 11]))) = sf.NAME
WHERE eb.[Field 11] IS NOT NULL
  AND eb.[Field 11] <> 'Cliente'
  AND dbo.ReplaceExtraChars(LTRIM(RTRIM(eb.[Field 11]))) <> ''
  AND eb.[Field 2] IS NOT NULL





CREATE VIEW [dbo].[SpainNew_Accounts] AS

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





CREATE VIEW [dbo].[Spain_Accounts] AS

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





CREATE VIEW [dbo].[Spain_Buy_Placement] AS

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
		ON ISNULL(dbo.ReplaceExtraChars(LTRIM(RTRIM(eb.[Field 16])))+'-' , '') + sl.External_Id__c = bu.External_Id__c
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









CREATE VIEW [dbo].[Turkey_Company] AS

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

UNION ALL

SELECT DISTINCT master.dbo.InitCap(LTRIM(RTRIM(LOWER([Field 10])))) AS Name
      ,'Advertiser' AS Type__c
	  ,'Active' AS Status__c
	  ,NULL AS Market__c
	  ,com.Id
FROM [XaxisETL].[dbo].[Extract_Turkey] et
	LEFT JOIN (SELECT Name, Id 
			  FROM Company__c
			  WHERE LEFT(Name, 5) <> '[SFL]'
			    AND Type__c = 'Advertiser') com
		ON master.dbo.InitCap(LTRIM(RTRIM(LOWER(et.[Field 10])))) = com.Name
WHERE [Field 9] IS NOT NULL
  AND [Field 9] <> 'Agency'
  AND [Field 9] <> 'Needs mapping to Turkish Agencies int the system'
  AND [Field 9] <> 'Campaign Details'




CREATE VIEW [dbo].[Turkey_Accounts] AS

WITH w_Account AS (
SELECT DISTINCT master.dbo.InitCap(LTRIM(RTRIM(LOWER([Field 10])))) +' - '+ ag.sf AS Name
	  ,acct.Id
	  ,acct.Agency__c AS Agency__c
	  ,ag.sf AS Agency_Name
	  ,acct.Advertiser__c AS Advertiser__c
	  ,master.dbo.InitCap(LTRIM(RTRIM(LOWER([Field 10])))) AS Advertiser_Name
FROM XaxisETL.[dbo].[Extract_Turkey] eb
	LEFT JOIN (
					SELECT 'MX' AS [Turk], 'Maxus (Turkey)' AS [SF] UNION
					SELECT 'MEC' , 'MEC (Turkey)'		UNION
					SELECT 'MC' , 'Mediacom (Turkey)'	UNION
					SELECT 'MS' , 'Mindshare (Turkey)'
					) ag	
			ON eb.[Field 9] = ag.Turk
	LEFT JOIN (SELECT acct.Id
					  ,acct.Advertiser__c
					  ,acct.Agency__c
					  ,sf_ag.NAME AS Agency_Name
					  ,sf_ad.NAME AS Advertiser_Name
				FROM Account acct
					INNER JOIN (SELECT ag.NAME
									 ,ag.Id
							   FROM Turkey_Company ag
							   WHERE ag.type__c = 'Agency'
							   AND Market__c = 'Turkey') sf_ag
						ON acct.Agency__c = sf_ag.Id
					INNER JOIN (SELECT ad.NAME
									  ,ad.Id
								FROM Turkey_Company ad
								WHERE ad.type__c = 'Advertiser') sf_ad
						ON acct.Advertiser__c = sf_ad.Id
				WHERE acct.Advertiser__c IS NOT NULL
				  AND acct.Agency__c IS NOT NULL
				  AND sf_ag.NAME IS NOT NULL
				  AND sf_ad.NAME IS NOT NULL
				  ) acct
	ON acct.Advertiser_Name = master.dbo.InitCap(LTRIM(RTRIM(LOWER(eb.[Field 10]))))
   AND acct.Agency_Name = ag.SF
WHERE [Field 9] IS NOT NULL
  AND [Field 9] <> 'Agency'
  AND [Field 9] <> 'Needs mapping to Turkish Agencies int the system'
  AND [Field 9] <> 'Campaign Details'
  
)
, Adv_Count AS (
	SELECT bc.id
		  ,ISNULL(COUNT(ba.Id), 0) AS a_count
	FROM Turkey_Company bc
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
		  ,DENSE_RANK() OVER (PARTITION BY wa.Name ORDER BY Adv_Count.a_count, Adv_Created.CreatedDate, Ag_Created.CreatedDate, ISNULL(Acct_Count.account_count, 0) DESC, wa.Id) AS Ranky
--		  ,Adv_Count.a_count
--		  ,Adv_Created.CreatedDate AS adv_crd
--		  ,Ag_Created.CreatedDate  
--		  ,ISNULL(Acct_Count.account_count, 0) AS account_count
	FROM w_Account wa
		LEFT JOIN(
				   SELECT ag.NAME
						 ,ag.Id
				   FROM Turkey_Company ag
				   WHERE ag.type__c = 'Agency'
					 AND ag.Market__c = 'Turkey'
					 ) sf_ag
			ON wa.Agency_Name = sf_ag.Name
--		   AND wa.Agency__c = sf_ag.Id
		LEFT JOIN( 
				   SELECT ad.NAME
						,ad.Id
				   FROM Turkey_Company ad
				   WHERE ad.type__c = 'Advertiser') sf_ad
			ON wa.Advertiser_Name = sf_ad.Name
--		   AND wa.Advertiser__c = sf_ad.Id
		LEFT JOIN Adv_Count
			ON Adv_Count.Id = sf_ad.id
		LEFT JOIN Adv_Created
			ON Adv_Created.Id = sf_ad.id
		LEFT JOIN Ag_Created
			ON Ag_Created.Id = sf_ag.id
		LEFT JOIN Acct_Count
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


CREATE VIEW [dbo].[Turkey_Sell_Lines] AS

SELECT opp.Id AS Opportunity__c
--	  ,CONVERT(VARCHAR(1000), HashBytes('MD5', [Field 2] + [Field 13]), 2) AS External_ID__c
--      ,CAST(CHECKSUM(dbo.ReplaceExtraChars([Field 13]), dbo.ReplaceExtraChars([Field 16]), dbo.ReplaceExtraChars([Field 2])) AS VARCHAR(25)) AS External_ID__c
	  ,'TurkeyOpp-' + CAST(et.External_pk AS VARCHAR) AS External_ID__c
	  ,dbo.ReplaceExtraChars([Field 15]) AS Product_Detail__c
      ,CAST(CONVERT(DATE,[Field 6],101) AS DATE) AS Start_Date__c
      ,CAST(CONVERT(DATE,[Field 7],101) AS DATE) AS End_Date__c
      ,dbo.ReplaceExtraChars([Field 2]) AS Buy_Name_txt__c
      ,'Net Cost (Calc Margin)' AS Imputing_Margin_or_Net__c
      ,'MediaTrader' AS PackageType__c
      ,(SELECT TOP 1 Id FROM RecordType WHERE SobjectType = 'Opportunity_Buy__c' AND DeveloperName = 'Turkey') AS RecordTypeId
      ,dbo.ReplaceExtraChars([Field 13]) AS Supplier_Name__c
      ,CONVERT(MONEY, REPLACE(REPLACE(REPLACE(dbo.ReplaceExtraChars(ISNULL([Field 21], 0)), ',',''), 'TL', ''), 'click', '')) AS Buy_Volume__c
      ,CONVERT(MONEY, REPLACE(REPLACE(dbo.ReplaceExtraChars(ISNULL([Field 31], 0)), ',',''), 'TL', '')) AS Gross_Cost__c
      ,CONVERT(MONEY, REPLACE(REPLACE(dbo.ReplaceExtraChars(ISNULL([Field 20], 0)), ',',''), 'TL', '')) AS Rate__c
      ,CONVERT(MONEY, REPLACE(REPLACE(dbo.ReplaceExtraChars(ISNULL([Field 32], 0)), ',',''), 'TL', '')) AS Media_Net_Cost__c
      ,dbo.ReplaceExtraChars([Field 16]) AS Buy_Type__c
      ,'Triggers' AS Audience_Tier__c
      ,CONVERT(MONEY, REPLACE(REPLACE(dbo.ReplaceExtraChars(ISNULL([Field 36], 0)), ',',''), '%',''))/100 AS Current_Margin__c
      ,'From spreadsheet: ' + [Tag: Filename] AS Current_Margin_Explanation__c
      ,dbo.ReplaceExtraChars([Field 17]) AS Opp_Buy_Description__c
      ,'Externally Managed' AS Input_Mode__c
	  ,'Digital' AS Media_Code__c
	  ,dbo.ReplaceExtraChars([Field 18]) AS Formats__c
	  ,sl.Id
FROM XaxisETL.dbo.Extract_Turkey et
	LEFT JOIN (SELECT Name, Id
			   FROM Opportunity
			   ) opp
		ON dbo.ReplaceExtraChars(LTRIM(RTRIM(et.[Field 11]))) = opp.Name
	   AND HashBytes('MD5',  ISNULL(dbo.ReplaceExtraChars([Field 2]), '') + ISNULL(dbo.ReplaceExtraChars([Field 13]), '') + ISNULL(dbo.ReplaceExtraChars([Field 16]), ''))
		 = et.External_ID__c
	LEFT JOIN dbo.Opportunity_Buy__c sl
--		ON CAST(CHECKSUM(dbo.ReplaceExtraChars([Field 13]), dbo.ReplaceExtraChars([Field 16]), dbo.ReplaceExtraChars([Field 2])) AS VARCHAR(25)) = sl.External_Id__c
		ON 'TurkeyOpp-' + CAST(et.External_pk AS VARCHAR) = sl.External_Id__c
WHERE [Field 9] IS NOT NULL
  AND [Field 9] <> 'Agency'
  AND [Field 9] <> 'Needs mapping to Turkish Agencies int the system'
  AND [Field 9] <> 'Campaign Details'
--  AND sl.External_Id__c IS NULL   


