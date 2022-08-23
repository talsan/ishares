# A Quantsplainer
`README_Quant.md` provides quantitative context behind the code. If you want to learn more about the technical elements and usage patterns, visit `README.md`.

## Background
At my old quant shop, every financial data-item you could reasonably want was sitting neatly for you in a SQL table. In a few joins, you usually had what you wanted.

Fast forward to now: *I got nothin*. Most research projects require a few fundamental data-items just to get started: date-stamped identifiers, asset returns (prices/dividends), industry constituents, and broad market indices. Traditional Index vendors like MSCI and Russell charge exhorbitant amounts of money for a history of index constituents and other basic data items. 

**The core idea behind this project to use ETFs as proxies for broad market/sector indices.**

By design, each ETF tracks its index very closely, and they're also freely available through most all company websites (iShares, SPDRs, Invesco, Vangaurd, etc.). In this project, I extract data from iShares because of its comprehensiveness and ease-of-access.

## Data Snippets
The list of all 300+ available iShares ETFs [can be found here.](https://github.com/talsan/ishares/blob/master/ishares/data/ishares-etf-index.csv)

Here are the top 10 by AUM:
<to do>
  
A sample file looks like this.
<to do>
  
## Use-Cases for Quantitative Analysis
1. **"Universe" histories** - to the extent that your model is cross-sectional (comparing stocks across a distribution, at a given point in time) it's important to know what companies existed when. 
2. **Benchmarks** - comparing (and optimizing) your strategy against a low-cost ETF is as good a benchmark as any
3. **Stock-Level Exposures to Sectors** - meta-data within each holding file contains sector assignments, which are critical inputs to model construciton and performance attribution (industries explain roughly %X percent of cross-sectional variance)
4. **Understand Market movements** - with over 300 ETFs, iShares has nearly every corner of the market covered; tracking returns of different representative categories helps describe micro market movements.
5. **Id-mapping**: point-in-time SEDOLs, CUSIPs, ISINs, Tickers, etc.
