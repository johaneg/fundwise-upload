import datetime
import os
from tkinter import N
import xml.etree.ElementTree as ET

from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv()

DATA_DIR = "data"
CONNECTION_STRING = f"mongodb+srv://gugge:{os.getenv('mongo_pass')}@cluster0.fxfss.mongodb.net/fundwise?retryWrites=true&w=majority"
COLLECTION = "funds"
DATABASE = "myFirstDatabase"

fund_name = "fundName"
holdings_date = "holdingsDate"
fund_company_name = "fundCompanyName"
fund_holdings = "fundHoldings"
company_name = "companyName"
share_of_fund = "shareOfFund"


def get_period(root: ET.Element, ns: dict) -> datetime.datetime:
    report_info = root.findall("fund:Rapportinformation", ns)[0]
    return datetime.datetime.strptime(
        report_info.find("fund:Kvartalsslut", ns).text, "%Y-%m-%d"
    )


def get_fund_company_name(root: ET.Element, ns: dict):
    report_info = root.findall("fund:Bolagsinformation", ns)[0]
    return report_info.find("fund:Fondbolag_namn", ns).text


def get_fund_name(root: ET.Element, ns: dict):
    report_info = root.findall("fund:Fondinformation", ns)[0]
    return report_info.find("fund:Fond_namn", ns).text


def get_holdings(root: ET.Element, ns: dict):
    fund_info = root.findall("fund:Fondinformation", ns)[0]
    for child in fund_info:
        if "FinansiellaInstrument" in child.tag:
            break
    holdings = []
    for financial_instrument in child:
        holdings.append(
            {
                company_name: financial_instrument.find("fund:Instrumentnamn", ns).text,
                share_of_fund: financial_instrument.find(
                    "fund:Andel_av_fondförmögenhet_instrument", ns
                ).text,
            }
        )
    return holdings


def get_files() -> list[str]:
    files = []
    for quarter_dir in os.listdir(DATA_DIR):
        quarter_fund_data = os.path.join(DATA_DIR, quarter_dir)
        if not os.path.isdir(quarter_fund_data):
            continue
        for quarter in os.listdir(quarter_fund_data):
            fund_quarter_data = os.path.join(quarter_fund_data, quarter)
            if not os.path.isdir(fund_quarter_data):
                continue
            for fund in os.listdir(fund_quarter_data):
                if not ".xml" in fund:
                    continue
                files.append(os.path.join(fund_quarter_data, fund))
    return files


def get_root(filepath):
    tree = ET.parse(filepath)
    root = tree.getroot()
    ns = {"fund": root.tag.split("}")[0].replace("{", "")}
    return root, ns


def insert_data(root, ns):
    date = get_period(root, ns)
    company_name = get_fund_company_name(root, ns)
    name = get_fund_name(root, ns)
    holdings = get_holdings(root, ns)

    fund_data_record = {
        "$set": {
            fund_name: name,
            holdings_date: date,
            fund_company_name: company_name,
            fund_holdings: holdings,
        }
    }

    client = MongoClient(CONNECTION_STRING)
    db = client[DATABASE]
    collection = db[COLLECTION]
    collection.update_one({fund_name: name, holdings_date: date}, fund_data_record, upsert=True)


if __name__ == "__main__":
    files = get_files()
    for file in files:
        print(file)
        root, ns = get_root(file)
        insert_data(root, ns)
