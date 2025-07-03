
## OpenBB Models Supported

Please refer to the following table. AKShare only supports part of the Hong Kong stock data.

| akshare                        | A股  | 港股  |
| ------------------------------ | --- | --- |
| AKShareAvailableIndicesData    | x   |     |
| AKShareBalanceSheetData        | x   |     |
| AKShareCompanyNewsData         | x   |     |
| AKShareEquityHistoricalData    | x   | x   |
| AKShareEquityQuoteData         | x   | x   |
| AKShareHistoricalDividendsData | x   | x   |

## BalanceSheetData

### FMP

**URL**: https://site.financialmodelingprep.com/developer/docs#Balance-Sheet

[Balance Sheet Statements API](https://site.financialmodelingprep.com/developer/docs/balance-sheet-statements-financial-statements)

The balance sheet is a financial statement that displays a company’s total assets, liabilities, and shareholder equity over a specific timeframe (quarterly or yearly). Investors can use this statement to determine if the company can fund its operations, meet its debt obligations, and pay a dividend.

```
https://financialmodelingprep.com/api/v3/balance-sheet-statement/AAPL?period=annual


https://financialmodelingprep.com/api/v3/balance-sheet-statement/0000320193?period=annual
```

```json
[
	{
		"date": "2022-09-24",
		"symbol": "AAPL",
		"reportedCurrency": "USD",
		"cik": "0000320193",
		"fillingDate": "2022-10-28",
		"acceptedDate": "2022-10-27 18:01:14",
		"calendarYear": "2022",
		"period": "FY",
		"cashAndCashEquivalents": 23646000000,
		"shortTermInvestments": 24658000000,
		"cashAndShortTermInvestments": 48304000000,
		"netReceivables": 60932000000,
		"inventory": 4946000000,
		"otherCurrentAssets": 21223000000,
		"totalCurrentAssets": 135405000000,
		"propertyPlantEquipmentNet": 42117000000,
		"goodwill": 0,
		"intangibleAssets": 0,
		"goodwillAndIntangibleAssets": 0,
		"longTermInvestments": 120805000000,
		"taxAssets": 0,
		"otherNonCurrentAssets": 54428000000,
		"totalNonCurrentAssets": 217350000000,
		"otherAssets": 0,
		"totalAssets": 352755000000,
		"accountPayables": 64115000000,
		"shortTermDebt": 21110000000,
		"taxPayables": 0,
		"deferredRevenue": 7912000000,
		"otherCurrentLiabilities": 60845000000,
		"totalCurrentLiabilities": 153982000000,
		"longTermDebt": 98959000000,
		"deferredRevenueNonCurrent": 0,
		"deferredTaxLiabilitiesNonCurrent": 0,
		"otherNonCurrentLiabilities": 49142000000,
		"totalNonCurrentLiabilities": 148101000000,
		"otherLiabilities": 0,
		"capitalLeaseObligations": 0,
		"totalLiabilities": 302083000000,
		"preferredStock": 0,
		"commonStock": 64849000000,
		"retainedEarnings": -3068000000,
		"accumulatedOtherComprehensiveIncomeLoss": -11109000000,
		"othertotalStockholdersEquity": 0,
		"totalStockholdersEquity": 50672000000,
		"totalEquity": 50672000000,
		"totalLiabilitiesAndStockholdersEquity": 352755000000,
		"minorityInterest": 0,
		"totalLiabilitiesAndTotalEquity": 352755000000,
		"totalInvestments": 145463000000,
		"totalDebt": 120069000000,
		"netDebt": 96423000000,
		"link": "https://www.sec.gov/Archives/edgar/data/320193/000032019322000108/0000320193-22-000108-index.htm",
		"finalLink": "https://www.sec.gov/Archives/edgar/data/320193/000032019322000108/aapl-20220924.htm"
	}
]
```

中文：

```json
[
	{
		"日期": "2022-09-24",
		"股票代码": "AAPL",
		"报告货币": "USD",
		"CIK编码": "0000320193",
		"填报日期": "2022-10-28",
		"接受日期": "2022-10-27 18:01:14",
		"财年": "2022",
		"期间": "FY",
		"现金及现金等价物": 23646000000,
		"短期投资": 24658000000,
		"现金及短期投资总额": 48304000000,
		"应收账款净额": 60932000000,
		"存货": 4946000000,
		"其他流动资产": 21223000000,
		"流动资产总额": 135405000000,
		"固定资产净值": 42117000000,
		"商誉": 0,
		"无形资产": 0,
		"商誉和无形资产总额": 0,
		"长期投资": 120805000000,
		"递延所得税资产": 0,
		"其他非流动资产": 54428000000,
		"非流动资产总额": 217350000000,
		"其他资产": 0,
		"总资产": 352755000000,
		"应付账款": 64115000000,
		"短期债务": 21110000000,
		"应交税费": 0,
		"递延收入": 7912000000,
		"其他流动负债": 60845000000,
		"流动负债总额": 153982000000,
		"长期债务": 98959000000,
		"非流动递延收入": 0,
		"非流动递延税负债": 0,
		"其他非流动负债": 49142000000,
		"非流动负债总额": 148101000000,
		"其他负债": 0,
		"资本租赁义务": 0,
		"负债总额": 302083000000,
		"优先股": 0,
		"普通股": 64849000000,
		"留存收益": -3068000000,
		"累计其他综合收益/损失": -11109000000,
		"其他股东权益": 0,
		"股东权益总额": 50672000000,
		"总权益": 50672000000,
		"负债和股东权益总额": 352755000000,
		"少数股东权益": 0,
		"负债和总权益总额": 352755000000,
		"投资总额": 145463000000,
		"债务总额": 120069000000,
		"净债务": 96423000000,
		"链接": "https://www.sec.gov/Archives/edgar/data/320193/000032019322000108/0000320193-22-000108-index.htm",
		"最终链接": "https://www.sec.gov/Archives/edgar/data/320193/000032019322000108/aapl-20220924.htm"
	}
]
```

