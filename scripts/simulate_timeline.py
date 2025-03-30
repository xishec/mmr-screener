import os
import datetime
import sys
from dateutil.relativedelta import relativedelta
import yfinance as yf
import csv
import time
import pandas as pd
import json

DIR = os.path.dirname(os.path.realpath(__file__))


def simulate():
    timeline = []

    output_dir = os.path.join(os.path.dirname(DIR), 'screen_results')
    file_path = os.path.join(output_dir, 'screen_results copy.csv')
    dates = {}
    with open(file_path, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            date_key = row.get("Date")
            if date_key:
                dates.setdefault(date_key, []).append(row)

    initial_cash = 100
    current_cash = initial_cash
    current_holding = 0
    current_nb_holding = 0
    holdings = {}
    zero_budget_counter = 0
    trading_counter = 0

    for current_date, rows in dates.items():
        # sell
        for sell_date in list(holdings.keys()):
            if current_date > sell_date:
                list_to_sell = holdings.get(sell_date)
                for holding in list_to_sell:
                    current_nb_holding -= 1
                    money_out = holding["money_out"]
                    money_in = holding["money_in"]
                    profit_percentage = holding["profit_percentage"]
                    current_cash += money_in
                    current_holding -= money_out
                    timeline.append({
                        "Date": current_date,
                        "Action": "Sell",
                        "Ticker": holding["ticker"],
                        "Profit": f"{profit_percentage}%",
                        "Cash": f"{current_cash:.2f}",
                        "Cash Change": f"+{money_in:.2f}",
                        "Holding": f"{current_holding:.2f} ({current_nb_holding})",
                        "Holding Change": f"-{money_in:.2f}",
                        "Total": f"{current_cash + current_holding:.2f}"
                    })
                holdings.pop(sell_date)

        # buy attempt
        # money_out = current_cash / 1 / len(rows)
        money_out = current_cash / 1 / len(rows)

        trading_counter += 1
        for row in rows:
            ticker = row.get("Ticker")

            # no cash
            if money_out < 1 or (current_cash - money_out) < 0:
                zero_budget_counter += 1
                timeline.append({
                    "Date": current_date,
                    "Action": "No money",
                    "Ticker": ticker,
                    "Profit": "",
                    "Cash": f"{current_cash:.2f}",
                    "Cash Change": "",
                    "Holding": f"{current_holding:.2f} ({current_nb_holding})",
                    "Holding Change": "",
                    "Profit": "",
                    "Total": f"{current_cash + current_holding:.2f}"
                })
                continue

            # yes cash
            current_nb_holding += 1
            current_cash -= money_out
            current_holding += money_out
            profit_percentage = float(row["Profit"].replace("%", ""))
            money_in = money_out * (1 + profit_percentage / 100)
            sell_date = row.get("Sell Date")
            holdings.setdefault(sell_date, []).append({"ticker": ticker,
                                                       "money_out": money_out,
                                                       "money_in": money_in,
                                                       "profit_percentage": profit_percentage})
            timeline.append({
                "Date": current_date,
                "Action": "Buy",
                "Ticker": ticker,
                "Profit": "",
                "Cash": f"{current_cash:.2f}",
                "Cash Change": f"-{money_out:.2f}",
                "Holding": f"{current_holding:.2f} ({current_nb_holding})",
                "Holding Change": f"+{money_out:.2f}",
                "Total": f"{current_cash + current_holding:.2f}"
            })

    # Add remaining holdings to cash
    for sell_date in list(holdings.keys()):
            list_to_sell = holdings.get(sell_date)
            for holding in list_to_sell:
                current_nb_holding -= 1
                money_out = holding["money_out"]
                money_in = holding["money_in"]
                profit_percentage = holding["profit_percentage"]
                current_cash += money_in
                current_holding -= money_out
                timeline.append({
                    "Date": "End",
                    "Action": "Sell",
                    "Ticker": holding["ticker"],
                    "Profit": f"{profit_percentage}%",
                    "Cash": f"{current_cash:.2f}",
                    "Cash Change": f"+{money_in:.2f}",
                    "Holding": f"{current_holding:.2f} ({current_nb_holding})",
                    "Holding Change": f"-{money_in:.2f}",
                    "Total": f"{current_cash + current_holding:.2f}"
                })
            holdings.pop(sell_date)
    percentage = (current_cash - initial_cash) / initial_cash * 100
    print(f"{percentage:.2f}%, {zero_budget_counter} zero budget days / {trading_counter} trading days")

    # New: write simulation timeline to CSV file with header and rows
    timeline_file = os.path.join(output_dir, 'screen_results_timeline.csv')
    with open(timeline_file, mode='w', newline='') as csvfile:
        fieldnames = ["Date", "Action", "Ticker", "Profit", "Cash", "Cash Change", "Holding", "Holding Change", "Total"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for row in timeline:
            writer.writerow(row)


def main():
    simulate()


if __name__ == "__main__":
    main()
