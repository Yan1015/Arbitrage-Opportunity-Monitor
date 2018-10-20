# Arbitrage-Opportunity-Monitor
## Introduction
This project is to monitor the arbitrage opportunity of stocks, options and futures every second based on Put-Call parity in Chinese stock market. Here, we focus on ETFs whose index are SSE 50, CSI 300 and CIC 500. Whenever it finds a trading signal, it would record it, do the trading operations and make profit. However, there are small profit and small chance to get a trading signal.
## Main
The time interval is 20 seconds which means every 20 seconds we download all the etf futures, options and the stocks index data and extract them into a csv file.

And all the details for calculation, you can see in CalStat.py
