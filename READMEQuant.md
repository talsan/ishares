# Quantsplainer
#### The purpose of READMEQuant.md file is to provide context behind all the code.
At my old quantitative investment firm, every financial data-item you could reasonably want would be sitting neatly for you in a SQL table. 
In a few joins, you usually had what you wanted. Fast forward to now: I got nothin. I can't even start thinking about projects until I have basic, foundational data. 
Examples: univeses, indicies (benchmarks), industry constituents, point-in-time IDs like SEDOLS & CUSIPS. 

Getting these things from traditional vendors (MSCI and Russell, mainly) is insanely expensive for what it is. The core idea behind this project to just use ETFs for all that. After all, an ETF's core mandate is to track its stated index closely. And, importantly, their holdings are freely available through most all manager websites (iShares, SPDRs, Invesco, Vangaurd, etc.).

The list of all 300+ available ETFs [can be found here.](https://github.com/talsan/ishares/blob/master/ishares/data/ishares-etf-index.csv) 

#### Here are some initial use-cases for the data:
1. a "universe" of names, with a history. 
2. a benchmark
3. Stock-Level Exposures to Industries
4. Stock-Level Exposures to Styles
5. Understand Market movements
6. Id-mapping: point-in-time SEDOLS, CUSIPS
7. Other Metadata 
