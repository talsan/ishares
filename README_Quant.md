# Quantsplainer
#### The purpose of the `README_Quant.md` file is to answer the question: why would a quant care about this? This document provides context behind the code.
At my old quant shop, every financial data-item you could reasonably want would be sitting neatly for you in a SQL table. 
In a few joins, you usually had what you wanted. Fast forward to now: I got nothin. Most research projects or even basic analysis, requires a few fundamental data-items just to get started: Identifiers, asset returns (prices/dividends), industry constituents, and broad market indices.

Traditional Index vendors like MSCI and Russell charge exhorbitant amounts of money for a history of index constituents*

The core idea behind this project to use ETFs as a proxies for broad market/sector indices. By design, each ETF tracks its index very closely, and they're also freely available through most all company websites (iShares, SPDRs, Invesco, Vangaurd, etc.). 

The list of all 300+ available iShares ETFs [can be found here.](https://github.com/talsan/ishares/blob/master/ishares/data/ishares-etf-index.csv)

Here are the top 10 by AUM:

A sample file looks like this.

#### Here are some initial use-cases for the data:
1. "universe" histories - to the extent that your model is cross-sectional (comparing stocks across a distribution, at a given point in time) it's important to know what companies existed when. 
2. benchmark - comparing (and optimizing) your strategy against a low-cost ETF is as good a benchmark as any
3. Stock-Level Exposures to Industries - 
4. Stock-Level Exposures to Styles
5. Understand Market movements
6. Id-mapping: point-in-time SEDOLS, CUSIPS
7. Other Metadata 

** Most index data is incredibly basic and easy to construct. MSCI and Russell weren't first to do it and did not perfect it (if something like that is even possible), but since everyone in the community aggreed to go with them (it's one less moving part when comparing the deluge of active vehiclces), they reap massive licensing rewards over a methodology that isn't remotely unique. 

It's like if the driving public got together and agreed that stop lights, stop signs, and brake lights should all be the same hue of red. An opportunistic team opened up MS Paint, picked the color "red," and sold it as a subscription with their name on it in really big letters. It somehow made sense at the time, and now, when someone wants to put up a stop sign (using the color everyone widely recognizes), they need to pay dearly for it.
