// SPDX-FileCopyrightText: 2022-2025 RustInFinance
// SPDX-License-Identifier: BSD-3-Clause

#![debugger_visualizer(natvis_file = "../rust_decimal.natvis")]

mod csvparser;
mod ecb;
mod logging;
pub mod pdfparser;
mod transactions;
mod xlsxparser;

use rust_decimal::Decimal;
use std::str::FromStr;

type ReqwestClient = reqwest::blocking::Client;

pub use logging::ResultExt;
use transactions::{
    create_detailed_div_transactions, create_detailed_interests_transactions,
    create_detailed_revolut_sold_transactions, create_detailed_revolut_transactions,
    create_detailed_sold_transactions, create_per_company_report, reconstruct_sold_transactions,
    verify_dividends_transactions, verify_interests_transactions, verify_transactions,
};

#[derive(Debug, PartialEq, PartialOrd, Copy, Clone)]
pub enum Currency {
    PLN(Decimal),
    EUR(Decimal),
    USD(Decimal),
}

impl Currency {
    pub fn value(&self) -> Decimal {
        match self {
            Currency::EUR(val) => *val,
            Currency::PLN(val) => *val,
            Currency::USD(val) => *val,
        }
    }
    pub fn derive(&self, val: Decimal) -> Currency {
        match self {
            Currency::EUR(_) => Currency::EUR(val),
            Currency::PLN(_) => Currency::PLN(val),
            Currency::USD(_) => Currency::USD(val),
        }
    }

    pub fn derive_exchange(&self, date: String) -> Exchange {
        match self {
            Currency::EUR(_) => Exchange::EUR(date),
            Currency::PLN(_) => Exchange::PLN(date),
            Currency::USD(_) => Exchange::USD(date),
        }
    }
}

///
#[derive(Debug, PartialEq, Eq, Hash)]
pub enum Exchange {
    EUR(String),
    PLN(String),
    USD(String),
}

#[derive(Debug, PartialEq, PartialOrd)]
pub struct Transaction {
    pub transaction_date: String,
    pub gross: Currency,
    pub tax_paid: Currency,
    pub exchange_rate_date: String,
    pub exchange_rate: Decimal,
    pub company: Option<String>,
}

impl Transaction {
    pub fn format_to_print(&self, prefix: &str) -> Result<String, &'static str> {
        let msg = match (&self.gross,&self.tax_paid) {
            (Currency::PLN(gross),Currency::PLN(tax_paid)) => {

                format!("{prefix} TRANSACTION date: {}, gross: {:.2} PLN, tax paid: {:.2} PLN",
                chrono::NaiveDate::parse_from_str(&self.transaction_date, "%m/%d/%y").map_err(|_| "Error: unable to format date")?.format("%Y-%m-%d"),
                gross, tax_paid
            )
            .to_owned()
            },
            (Currency::USD(gross),Currency::USD(tax_paid)) => {

                format!("{prefix} TRANSACTION date: {}, gross: ${:.2}, tax paid: ${:.2}, exchange_rate: {} , exchange_rate_date: {}",
                chrono::NaiveDate::parse_from_str(&self.transaction_date, "%m/%d/%y").map_err(|_| "Error: unable to format date")?.format("%Y-%m-%d"), gross, tax_paid, &self.exchange_rate,&self.exchange_rate_date
            )
            .to_owned()
            },

            (Currency::EUR(gross),Currency::EUR(tax_paid)) => {

                format!("{prefix} TRANSACTION date: {}, gross: €{:.2}, tax paid: €{:.2}, exchange_rate: {} , exchange_rate_date: {}",
                chrono::NaiveDate::parse_from_str(&self.transaction_date, "%m/%d/%y").map_err(|_| "Error: unable to format date")?.format("%Y-%m-%d"), gross, tax_paid, &self.exchange_rate,&self.exchange_rate_date
            )
            .to_owned()
            },
            (_,_) => return Err("Error: Gross and Tax paid currency does not match!"),
        };

        Ok(msg)
    }
}

// 1. trade date (a.k.a. transaction date, T)
// 2. settlement date (display only, no tax implications)
// 3. date of purchase (a.k.a. acquisition date, A)
// 4. net income
// 5. cost basis
//
#[derive(Debug, PartialEq, PartialOrd)]
pub struct SoldTransaction {
    pub trade_date: String,
    pub settlement_date: String,
    pub acquisition_date: String,
    pub income_us: Decimal,  // net proceeds (what seller receives, excluding fees)
    pub cost_basis: Decimal,
    pub fees: Decimal,       // fees amount (0 if no trade confirmations)
    pub exchange_rate_trade_date: String, // T-1 (working day preceding trade date)
    pub exchange_rate_trade: Decimal,
    pub exchange_rate_acquisition_date: String, // A-1 (working day preceding acquisition date)
    pub exchange_rate_acquisition: Decimal,
    pub company: Option<String>,
    // TODO
    //pub country : Option<String>,
}

impl SoldTransaction {
    pub fn format_to_print(&self, prefix: &str) -> String {
        let (income_label, fees_str) = if self.fees > Decimal::ZERO {
            // When fees are present, we have trade confirmations: show net + fees
            ("net_proceeds", format!(" + ${:.2} fees", self.fees))
        } else {
            // When no fees, we have no trade confirmations: income_us is net from Account Statements
            ("net_proceeds", String::new())
        };
        format!(
                "{prefix} SOLD TRANSACTION trade_date: {}, settlement_date: {}, acquisition_date: {}, {}: ${}{},  cost_basis: {}, exchange_rate_trade: {} , exchange_rate_trade_date: {}, exchange_rate_acquisition: {} , exchange_rate_acquisition_date: {}",
                chrono::NaiveDate::parse_from_str(&self.trade_date, "%m/%d/%y").unwrap().format("%Y-%m-%d"), 
                chrono::NaiveDate::parse_from_str(&self.settlement_date, "%m/%d/%y").unwrap().format("%Y-%m-%d"), 
                chrono::NaiveDate::parse_from_str(&self.acquisition_date, "%m/%d/%y").unwrap().format("%Y-%m-%d"), 
                income_label, &self.income_us, &fees_str, &self.cost_basis, &self.exchange_rate_trade, &self.exchange_rate_trade_date, &self.exchange_rate_acquisition, &self.exchange_rate_acquisition_date,
            )
            .to_owned()
    }
}

pub trait Residency {
    fn present_result(
        &self,
        gross_div: Decimal,
        tax_div: Decimal,
        gross_sold: Decimal,
        cost_sold: Decimal,
    ) -> (Vec<String>, Option<String>);
    fn get_exchange_rates(
        &self,
        dates: &mut std::collections::HashMap<Exchange, Option<(String, Decimal)>>,
    ) -> Result<(), String>;

    // Default parser (not to be used)
    fn parse_exchange_rates(&self, _body: &str) -> Result<(Decimal, String), String> {
        panic!("This method should not be used. Implement your own if needed!");
    }

    fn get_currency_exchange_rates(
        &self,
        dates: &mut std::collections::HashMap<Exchange, Option<(String, Decimal)>>,
        to: &str,
    ) -> Result<(), String> {
        if to == "EUR" {
            self.get_currency_exchange_rates_ecb(dates, to)
        } else {
            self.get_currency_exchange_rates_legacy(dates, to)
        }
    }

    fn get_currency_exchange_rates_ecb(
        &self,
        dates: &mut std::collections::HashMap<Exchange, Option<(String, Decimal)>>,
        _to: &str,
    ) -> Result<(), String> {
        dates.iter_mut().try_for_each(|(exchange, val)| {
            let (_from, date) = match exchange {
                Exchange::USD(date) => ("usd", date),
                Exchange::EUR(date) => ("eur", date),
                Exchange::PLN(date) => ("pln", date),
            };

            let converted_date = chrono::NaiveDate::parse_from_str(&date, "%m/%d/%y")
                .map_err(|x| format!("Unable to convert date {x}"))?;

            let day_before = converted_date
                .checked_sub_signed(chrono::Duration::days(1))
                .ok_or("Error traversing date")?;

            let day_before_str = day_before.format("%Y-%m-%d").to_string();

            let exchange_rate = ecb::get_eur_to_usd_exchange_rate(day_before, day_before)
                .map_err(|x| format!("Error getting exchange rate from ECB: {x}"))?;

            *val = Some((day_before_str, exchange_rate));
            Ok::<(), String>(())
        })?;

        Ok(())
    }

    fn get_currency_exchange_rates_legacy(
        &self,
        dates: &mut std::collections::HashMap<Exchange, Option<(String, Decimal)>>,
        to: &str,
    ) -> Result<(), String> {
        let client = create_client();

        // Example URL: https://www.exchange-rates.org/Rate/USD/EUR/2-27-2021

        let base_exchange_rate_url = "https://www.exchange-rates.org/Rate/";

        dates.iter_mut().try_for_each(|(exchange, val)| {
            let (from, date) = match exchange {
                Exchange::USD(date) => ("usd", date),
                Exchange::EUR(date) => ("eur", date),
                Exchange::PLN(date) => ("pln", date),
            };

            let mut converted_date = chrono::NaiveDate::parse_from_str(&date, "%m/%d/%y")
                .map_err(|x| format!("Unable to convert date {x}"))?;

            converted_date = converted_date
                .checked_sub_signed(chrono::Duration::days(1))
                .ok_or("Error traversing date")?;

            let fms =
                format!("{}/{}/{}", from, to, converted_date.format("%m-%d-%Y")) + "/?format=json";
            let exchange_rate_url: String = base_exchange_rate_url.to_string() + fms.as_str();

            let body = client.get(&(exchange_rate_url)).send();
            let actual_body = body.map_err(|_| {
                format!(
                    "Getting Exchange Rate from Exchange-Rates.org ({}) failed",
                    exchange_rate_url
                )
            })?;
            if actual_body.status().is_success() {
                log::info!("RESPONSE {:#?}", actual_body);

                let exchange_rates_response = actual_body
                    .text()
                    .map_err(|_| "Error converting response to Text")?;
                log::info!("body of exchange_rate = {:#?}", &exchange_rates_response);
                // parsing text response
                if let Ok((exchange_rate, exchange_rate_date)) =
                    self.parse_exchange_rates(&exchange_rates_response)
                {
                    *val = Some((exchange_rate_date, exchange_rate));
                }
                Ok(())
            } else {
                return Err("Error getting exchange rate".to_string());
            }
        })?;

        Ok(())
    }
}

pub struct TaxCalculationResult {
    pub gross_income: Decimal,
    pub tax: Decimal,
    pub gross_sold: Decimal,
    pub cost_sold: Decimal,
    pub interests: Vec<Transaction>,
    pub transactions: Vec<Transaction>,
    pub revolut_dividends_transactions: Vec<Transaction>,
    pub sold_transactions: Vec<SoldTransaction>,
    pub revolut_sold_transactions: Vec<SoldTransaction>,
    pub missing_trade_confirmations_warning: Option<String>,
}

fn create_client() -> reqwest::blocking::Client {
    // proxies are taken from env vars: http_proxy and https_proxy
    let http_proxy = std::env::var("http_proxy");
    let https_proxy = std::env::var("https_proxy");

    // If there is proxy then pick first URL
    let base_client = ReqwestClient::builder();
    let client = match &http_proxy {
        Ok(proxy) => base_client
            .proxy(reqwest::Proxy::http(proxy).expect_and_log("Error setting HTTP proxy")),
        Err(_) => base_client,
    };
    let client = match &https_proxy {
        Ok(proxy) => {
            client.proxy(reqwest::Proxy::https(proxy).expect_and_log("Error setting HTTP proxy"))
        }
        Err(_) => client,
    };
    let client = client.build().expect_and_log("Could not create client");
    client
}

fn compute_div_taxation(transactions: &Vec<Transaction>) -> (Decimal, Decimal) {
    // Gross income from dividends in target currency (PLN, EUR etc.)
    let gross_us_pl: Decimal = transactions
        .iter()
        .map(|x| x.exchange_rate * x.gross.value())
        .sum();
    // Tax paid in US in PLN
    let tax_us_pl: Decimal = transactions
        .iter()
        .map(|x| x.exchange_rate * x.tax_paid.value())
        .sum();
    (gross_us_pl, tax_us_pl)
}

fn compute_sold_taxation(transactions: &Vec<SoldTransaction>) -> (Decimal, Decimal) {
    // Net income from sold stock in target currency (PLN, EUR etc.)
    let gross_us_pl: Decimal = transactions
        .iter()
        .map(|x| (x.exchange_rate_trade * x.income_us).round_dp(2))
        .sum();
    // Cost of income e.g. cost_basis[target currency]
    let cost_us_pl: Decimal = transactions
        .iter()
        .map(|x| (x.exchange_rate_acquisition * x.cost_basis).round_dp(2))
        .sum();
    (gross_us_pl, cost_us_pl)
}

pub fn format_sold_transactions_to_string() {}

use std::collections::HashSet;
use std::ffi::OsStr;
use std::path::Path;

/* Check:
if file names have no duplicates
if there is only one xlsx spreadsheet
if extensions are only pdf, csv, xlsx
*/
pub fn validate_file_names(files: &Vec<String>) -> Result<(), String> {
    let mut names_set = HashSet::new();
    let mut spreadsheet_count = 0;
    let mut errors = Vec::<String>::new();

    for file_str in files {
        let path = Path::new(&file_str);
        if !path.is_file() {
            errors.push(format!("Not a file or path doesn't exist: {}", file_str));
            continue;
        }

        if let Some(file_stem) = path.file_stem().and_then(OsStr::to_str) {
            if !names_set.insert(file_stem.to_owned()) {
                let file_name = path.file_name().and_then(OsStr::to_str).unwrap();
                errors.push(format!("Duplicate file name found: {}", file_name));
            }
        } else {
            // Couldn't test it on windows.
            errors.push(format!("File has no name: {}", file_str));
        }

        match path.extension().and_then(OsStr::to_str) {
            Some("xlsx") => spreadsheet_count += 1,
            Some("csv") | Some("pdf") => {},
            Some(other_ext) => errors.push(format!("Unexpected extension {other_ext} for file: {file_str}. Only pdf, csv and xlsx are expected.")),
            None => errors.push(format!("File has no extension: {}", file_str))
        }
    }

    if spreadsheet_count > 1 {
        errors.push(format!(
            "Expected a single xlsx spreadsheet, found: {}",
            spreadsheet_count
        ));
    }

    if errors.len() > 0 {
        return Err(errors.join("\n"));
    }
    Ok(())
}

pub fn run_taxation(
    rd: &Box<dyn Residency>,
    names: Vec<String>,
    per_company: bool,
    multiyear: bool,
) -> Result<TaxCalculationResult, String> {
    validate_file_names(&names)?;

    let mut parsed_interests_transactions: Vec<(String, Decimal, Decimal)> = vec![];
    let mut parsed_div_transactions: Vec<(String, Decimal, Decimal, Option<String>)> = vec![];
    let mut parsed_sold_transactions: Vec<(String, String, i32, Decimal, Decimal, Option<String>)> = vec![];
    let mut parsed_gain_and_losses: Vec<(String, String, Decimal, Decimal, Decimal, i32)> = vec![];
    let mut parsed_sell_trade_confirmations: Vec<(String, String, i32, Decimal, Decimal, Decimal, Decimal, Decimal)> = vec![];
    let mut parsed_revolut_dividends_transactions: Vec<(
        String,
        Currency,
        Currency,
        Option<String>,
    )> = vec![];
    let mut parsed_revolut_sold_transactions: Vec<(
        String,
        String,
        Currency,
        Currency,
        Option<String>,
    )> = vec![];
    let mut seen_multi_transaction_trade_confirmation_pages: HashSet<u64> = HashSet::new();

    // 1. Parse PDF,XLSX and CSV documents to get list of transactions
    names.iter().try_for_each(|x| {
        // If name contains .pdf then parse as pdf
        // if name contains .xlsx then parse as spreadsheet
        if x.contains(".pdf") {
            let (mut int_t, mut div_t, mut sold_t, mut trades_t) =
                pdfparser::parse_statement_with_seen_pages(
                    x,
                    &mut seen_multi_transaction_trade_confirmation_pages,
                )?;
            parsed_interests_transactions.append(&mut int_t);
            parsed_div_transactions.append(&mut div_t);
            parsed_sold_transactions.append(&mut sold_t);
            parsed_sell_trade_confirmations.append(&mut trades_t);
        } else if x.contains(".xlsx") {
            parsed_gain_and_losses.append(&mut xlsxparser::parse_gains_and_losses(x)?);
        } else if x.contains(".csv") {
            let csvparser::RevolutTransactions {
                mut dividend_transactions,
                mut sold_transactions,
                ..
            } = csvparser::parse_revolut_transactions(x)?;
            parsed_revolut_dividends_transactions.append(&mut dividend_transactions);
            parsed_revolut_sold_transactions.append(&mut sold_transactions);
        } else {
            return Err(format!("Error: Unable to open a file: {x}"));
        }
        Ok::<(), String>(())
    })?;
    // 2. Verify Transactions (if they all come from same year unless multiyear is enabled)
    if multiyear == false {
        verify_interests_transactions(&parsed_interests_transactions)?;
        log::info!("Interests transactions are consistent");
        verify_dividends_transactions(&parsed_div_transactions)?;
        log::info!("Dividends transactions are consistent");
        verify_dividends_transactions(&parsed_revolut_dividends_transactions)?;
        log::info!("Revolut Dividends transactions are consistent");
        verify_transactions(&parsed_revolut_sold_transactions)?;
        log::info!("Revolut Sold transactions are consistent");
    } else {
        log::info!("Multi-year mode enabled, skipping verification of transaction years");
    }

    // 3. Verify and create full sold transactions info needed for TAX purposes
    let (detailed_sold_transactions, missing_tc_warning) =
        reconstruct_sold_transactions(&parsed_sold_transactions, &parsed_gain_and_losses, &parsed_sell_trade_confirmations)?;

    // 4. Get Exchange rates
    // Gather all trade , settlement and transaction dates into hash map to be passed to
    // get_exchange_rate
    // Hash map : Key(event date) -> (preceeding date, exchange_rate)
    let mut dates: std::collections::HashMap<Exchange, Option<(String, Decimal)>> =
        std::collections::HashMap::new();
    parsed_interests_transactions
        .iter()
        .for_each(|(trade_date, _, _)| {
            let ex = Exchange::USD(trade_date.clone());
            if dates.contains_key(&ex) == false {
                dates.insert(ex, None);
            }
        });
    parsed_div_transactions
        .iter()
        .for_each(|(trade_date, _, _, _)| {
            let ex = Exchange::USD(trade_date.clone());
            if dates.contains_key(&ex) == false {
                dates.insert(ex, None);
            }
        });
    detailed_sold_transactions.iter().for_each(
        |(trade_date, _settlement_date, acquisition_date, _, _, _, _)| {
            // No need to get exchange rate for settlement date as it has no tax implications
            let ex = Exchange::USD(trade_date.clone());
            if dates.contains_key(&ex) == false {
                dates.insert(ex, None);
            }
            let ex = Exchange::USD(acquisition_date.clone());
            if dates.contains_key(&ex) == false {
                dates.insert(ex, None);
            }
        },
    );
    parsed_revolut_dividends_transactions
        .iter()
        .for_each(|(trade_date, gross, _, _)| {
            let ex = gross.derive_exchange(trade_date.clone());
            if dates.contains_key(&ex) == false {
                dates.insert(ex, None);
            }
        });
    parsed_revolut_sold_transactions.iter().for_each(
        |(acquired_date, sold_date, cost, gross, _)| {
            let ex = cost.derive_exchange(acquired_date.clone());
            if dates.contains_key(&ex) == false {
                dates.insert(ex, None);
            }
            let ex = gross.derive_exchange(sold_date.clone());
            if dates.contains_key(&ex) == false {
                dates.insert(ex, None);
            }
        },
    );

    rd.get_exchange_rates(&mut dates).map_err(|x| "Error: unable to get exchange rates.  Please check your internet connection or proxy settings\n\nDetails:".to_string()+x.as_str())?;

    // Make a detailed_div_transactions
    let interests = create_detailed_interests_transactions(parsed_interests_transactions, &dates)?;
    let transactions = create_detailed_div_transactions(parsed_div_transactions, &dates)?;
    let sold_transactions = create_detailed_sold_transactions(detailed_sold_transactions, &dates)?;
    let revolut_dividends_transactions =
        create_detailed_revolut_transactions(parsed_revolut_dividends_transactions, &dates)?;
    let revolut_sold_transactions =
        create_detailed_revolut_sold_transactions(parsed_revolut_sold_transactions, &dates)?;

    if per_company {
        let per_company_report = create_per_company_report(
            &interests,
            &transactions,
            &sold_transactions,
            &revolut_dividends_transactions,
            &revolut_sold_transactions,
        )?;

        println!("{}", per_company_report);
    }

    let (gross_interests, _) = compute_div_taxation(&interests);
    let (gross_div, tax_div) = compute_div_taxation(&transactions);
    let (gross_sold, cost_sold) = compute_sold_taxation(&sold_transactions);
    let (gross_revolut, tax_revolut) = compute_div_taxation(&revolut_dividends_transactions);
    let (gross_revolut_sold, cost_revolut_sold) = compute_sold_taxation(&revolut_sold_transactions);
    Ok(TaxCalculationResult {
        gross_income: gross_interests + gross_div + gross_revolut,
        tax: tax_div + tax_revolut,
        gross_sold: gross_sold + gross_revolut_sold,
        cost_sold: cost_sold + cost_revolut_sold,
        interests,
        transactions: transactions,
        revolut_dividends_transactions: revolut_dividends_transactions,
        sold_transactions: sold_transactions,
        revolut_sold_transactions: revolut_sold_transactions,
        missing_trade_confirmations_warning: missing_tc_warning,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal::dec;

    #[test]
    fn test_validate_file_names_invalid_path() {
        let files = vec![
            String::from("file1.csv"),
            String::from("data/G&L_Expanded.xlsx"),
            String::from("data"),
        ];

        let result = validate_file_names(&files);
        assert_eq!(result.err(), Some(String::from("Not a file or path doesn't exist: file1.csv\nNot a file or path doesn't exist: data")));
    }

    #[test]
    fn test_validate_file_names_two_spreadsheets() {
        let files = vec![
            String::from("data/G&L_Expanded.xlsx"),
            String::from("data/G&L_Collapsed.xlsx"),
            String::from("revolut_data/revolut-savings-eng.csv"),
            String::from("revolut_data/Revolut_21sie2023_27lis2023.csv"),
        ];

        let result = validate_file_names(&files);
        assert_eq!(
            result.err(),
            Some(String::from("Expected a single xlsx spreadsheet, found: 2"))
        );
    }

    #[test]
    fn test_validate_file_names_duplicate_file() {
        let files = vec![
            String::from("data/G&L_Expanded.xlsx"),
            String::from("data/G&L_Expanded.xlsx"),
        ];

        let result = validate_file_names(&files);
        assert_eq!(result.err(), Some(String::from("Duplicate file name found: G&L_Expanded.xlsx\nExpected a single xlsx spreadsheet, found: 2")));
    }

    #[test]
    fn test_validate_file_names_unexpected_extension() {
        let files = vec![
            String::from("Cargo.toml"),
            String::from("revolut_data/revolut-savings-eng.csv"),
            String::from("revolut_data/Revolut_21sie2023_27lis2023.csv"),
        ];

        let result = validate_file_names(&files);
        assert_eq!(result.err(), Some(String::from("Unexpected extension toml for file: Cargo.toml. Only pdf, csv and xlsx are expected.")));
    }

    #[test]
    fn test_validate_file_names_no_extension() {
        let fpath = ".git/description";
        let files = vec![String::from(fpath)];

        let err = validate_file_names(&files).unwrap_err();
        assert_eq!(err, format!("File has no extension: {}", fpath));
    }

    #[test]
    fn test_simple_div_taxation() -> Result<(), String> {
        // Init Transactions
        let transactions: Vec<Transaction> = vec![Transaction {
            transaction_date: "N/A".to_string(),
            gross: crate::Currency::USD(dec!(100.0)),
            tax_paid: crate::Currency::USD(dec!(25.0)),
            exchange_rate_date: "N/A".to_string(),
            exchange_rate: dec!(4.0),
            company: Some("INTEL CORP".to_owned()),
        }];
        assert_eq!(compute_div_taxation(&transactions), (dec!(400.0), dec!(100.0)));
        Ok(())
    }

    #[test]
    fn test_div_taxation() -> Result<(), String> {
        // Init Transactions
        let transactions: Vec<Transaction> = vec![
            Transaction {
                transaction_date: "N/A".to_string(),
                gross: crate::Currency::USD(dec!(100.0)),
                tax_paid: crate::Currency::USD(dec!(25.0)),
                exchange_rate_date: "N/A".to_string(),
                exchange_rate: dec!(4.0),
                company: Some("INTEL CORP".to_owned()),
            },
            Transaction {
                transaction_date: "N/A".to_string(),
                gross: crate::Currency::USD(dec!(126.0)),
                tax_paid: crate::Currency::USD(dec!(10.0)),
                exchange_rate_date: "N/A".to_string(),
                exchange_rate: dec!(3.5),
                company: Some("INTEL CORP".to_owned()),
            },
        ];
        assert_eq!(
            compute_div_taxation(&transactions),
            (dec!(400.0) + dec!(126.0) * dec!(3.5), dec!(100.0) + dec!(10.0) * dec!(3.5))
        );
        Ok(())
    }
    #[test]
    fn test_revolut_savings_taxation_pln() -> Result<(), String> {
        let transactions: Vec<Transaction> = vec![
            Transaction {
                transaction_date: "03/01/21".to_string(),
                gross: crate::Currency::PLN(dec!(0.44)),
                tax_paid: crate::Currency::PLN(dec!(0.0)),
                exchange_rate_date: "N/A".to_string(),
                exchange_rate: dec!(1.0),
                company: None,
            },
            Transaction {
                transaction_date: "04/11/21".to_string(),
                gross: crate::Currency::PLN(dec!(0.45)),
                tax_paid: crate::Currency::PLN(dec!(0.0)),
                exchange_rate_date: "N/A".to_string(),
                exchange_rate: dec!(1.0),
                company: None,
            },
        ];
        assert_eq!(
            compute_div_taxation(&transactions),
            (dec!(0.44) * dec!(1.0) + dec!(0.45) * dec!(1.0), dec!(0.0))
        );
        Ok(())
    }

    #[test]
    fn test_revolut_savings_taxation_eur() -> Result<(), String> {
        let transactions: Vec<Transaction> = vec![
            Transaction {
                transaction_date: "03/01/21".to_string(),
                gross: crate::Currency::EUR(dec!(0.44)),
                tax_paid: crate::Currency::EUR(dec!(0.0)),
                exchange_rate_date: "02/28/21".to_string(),
                exchange_rate: dec!(2.0),
                company: None,
            },
            Transaction {
                transaction_date: "04/11/21".to_string(),
                gross: crate::Currency::EUR(dec!(0.45)),
                tax_paid: crate::Currency::EUR(dec!(0.0)),
                exchange_rate_date: "04/10/21".to_string(),
                exchange_rate: dec!(3.0),
                company: None,
            },
        ];
        assert_eq!(
            compute_div_taxation(&transactions),
            (dec!(0.44) * dec!(2.0) + dec!(0.45) * dec!(3.0), dec!(0.0))
        );
        Ok(())
    }

    #[test]
    fn test_simple_sold_taxation() -> Result<(), String> {
        // Init Transactions
        let transactions: Vec<SoldTransaction> = vec![SoldTransaction {
            trade_date: "N/A".to_string(),
            settlement_date: "N/A".to_string(),
            acquisition_date: "N/A".to_string(),
            income_us: dec!(100.0),
            cost_basis: dec!(70.0),
            fees: dec!(0.0),
            exchange_rate_trade_date: "N/A".to_string(),
            exchange_rate_trade: dec!(5.0),
            exchange_rate_acquisition_date: "N/A".to_string(),
            exchange_rate_acquisition: dec!(6.0),
            company: Some("TFC".to_owned()),
        }];
        assert_eq!(
            compute_sold_taxation(&transactions),
            (dec!(100.0) * dec!(5.0), dec!(70.0) * dec!(6.0))
        );
        Ok(())
    }

    #[test]
    fn test_sold_taxation() -> Result<(), String> {
        // Init Transactions
        let transactions: Vec<SoldTransaction> = vec![
            SoldTransaction {
                trade_date: "N/A".to_string(),
                settlement_date: "N/A".to_string(),
                acquisition_date: "N/A".to_string(),
                income_us: dec!(100.0),
                cost_basis: dec!(70.0),
                fees: dec!(0.0),
                exchange_rate_trade_date: "N/A".to_string(),
                exchange_rate_trade: dec!(5.0),
                exchange_rate_acquisition_date: "N/A".to_string(),
                exchange_rate_acquisition: dec!(6.0),
                company: Some("PXD".to_owned()),
            },
            SoldTransaction {
                trade_date: "N/A".to_string(),
                settlement_date: "N/A".to_string(),
                acquisition_date: "N/A".to_string(),
                income_us: dec!(10.0),
                cost_basis: dec!(4.0),
                fees: dec!(0.0),
                exchange_rate_trade_date: "N/A".to_string(),
                exchange_rate_trade: dec!(2.0),
                exchange_rate_acquisition_date: "N/A".to_string(),
                exchange_rate_acquisition: dec!(3.0),
                company: Some("TFC".to_owned()),
            },
        ];
        assert_eq!(
            compute_sold_taxation(&transactions),
            (dec!(100.0) * dec!(5.0) + dec!(10.0) * dec!(2.0), dec!(70.0) * dec!(6.0) + dec!(4.0) * dec!(3.0))
        );
        Ok(())
    }
}
