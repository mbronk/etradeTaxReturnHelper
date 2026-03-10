// SPDX-FileCopyrightText: 2022-2025 RustInFinance
// SPDX-License-Identifier: BSD-3-Clause

use chrono;
use chrono::Datelike;
use polars::prelude::*;
use rust_decimal::Decimal;
use rust_decimal::dec;
use rust_decimal::prelude::{FromPrimitive, ToPrimitive};
use std::collections::HashMap;

pub use crate::logging::ResultExt;
use crate::{SoldTransaction, Transaction};

/// Check if all interests rate transactions come from the same year
pub fn verify_interests_transactions<T>(transactions: &Vec<(String, T, T)>) -> Result<(), String> {
    let mut trans = transactions.iter();
    let transaction_date = match trans.next() {
        Some((x, _, _)) => x,
        None => {
            log::info!("No interests transactions");
            return Ok(());
        }
    };

    let transaction_year = chrono::NaiveDate::parse_from_str(transaction_date, "%m/%d/%y")
        .map_err(|_| format!("Unable to parse transaction date: \"{transaction_date}\""))?
        .year();
    let mut verification: Result<(), String> = Ok(());
    trans.try_for_each(|(tr_date, _, _)| {
        let tr_year = chrono::NaiveDate::parse_from_str(tr_date, "%m/%d/%y")
            .map_err(|_| format!("Unable to parse transaction date: \"{tr_date}\""))?
            .year();
        if tr_year != transaction_year {
            let msg: &str = "Error:  Statements are related to different years!";
            verification = Err(msg.to_owned());
        }
        Ok::<(), String>(())
    })?;
    verification
}

/// Check if all dividends transaction come from the same year
pub fn verify_dividends_transactions<T>(
    div_transactions: &Vec<(String, T, T, Option<String>)>,
) -> Result<(), String> {
    let mut trans = div_transactions.iter();
    let transaction_date = match trans.next() {
        Some((x, _, _, _)) => x,
        None => {
            log::info!("No Dividends transactions");
            return Ok(());
        }
    };

    let transaction_year = chrono::NaiveDate::parse_from_str(transaction_date, "%m/%d/%y")
        .map_err(|_| format!("Unable to parse transaction date: \"{transaction_date}\""))?
        .year();
    let mut verification: Result<(), String> = Ok(());
    trans.try_for_each(|(tr_date, _, _, _)| {
        let tr_year = chrono::NaiveDate::parse_from_str(tr_date, "%m/%d/%y")
            .map_err(|_| format!("Unable to parse transaction date: \"{tr_date}\""))?
            .year();
        if tr_year != transaction_year {
            let msg: &str = "Error:  Statements are related to different years!";
            verification = Err(msg.to_owned());
        }
        Ok::<(), String>(())
    })?;
    verification
}

pub fn verify_transactions<T>(
    transactions: &Vec<(String, String, T, T, Option<String>)>,
) -> Result<(), String> {
    let mut trans = transactions.iter();
    let transaction_date = match trans.next() {
        Some((_, x, _, _, _)) => x,
        None => {
            log::info!("No revolut sold transactions");
            return Ok(());
        }
    };

    let transaction_year = chrono::NaiveDate::parse_from_str(transaction_date, "%m/%d/%y")
        .map_err(|_| format!("Unable to parse transaction date: \"{transaction_date}\""))?
        .year();
    let mut verification: Result<(), String> = Ok(());
    trans.try_for_each(|(_, tr_date, _, _, _)| {
        let tr_year = chrono::NaiveDate::parse_from_str(tr_date, "%m/%d/%y")
            .map_err(|_| format!("Unable to parse transaction date: \"{tr_date}\""))?
            .year();
        if tr_year != transaction_year {
            let msg: &str = "Error: Statements are related to different years!";
            verification = Err(msg.to_owned());
        }
        Ok::<(), String>(())
    })?;
    verification
}

/// Trade date(T) is when transaction has executed (triggered).
/// fees and commission are applied at the moment of settlement date so
/// we ignore those and use net income rather than principal.
/// Actual Tax is to be paid from trade_date (tax is due at the moment of trade,
/// not settlement - ref: 'Art. 17 ust 1ab pkt 1' of the Polish PIT Act).
/// Note: "trade date" is when the transaction is executed, and the *rights*
///       are reassigned to the buyer. Settlement date (T+2) is when the money
///       is transferred to the seller, but it bears no tax implications.
///       TL;DR; For currency exchange for tax, we always take *T-1* NBP rate
pub fn reconstruct_sold_transactions(
    sold_transactions: &Vec<(String, String, Decimal, Decimal, Decimal, Option<String>)>,
    gains_and_losses: &Vec<(String, String, Decimal, Decimal, Decimal, Decimal)>,
    trade_confirmations: &Vec<(String, String, i32, Decimal, Decimal, Decimal, Decimal, Decimal)>,
) -> Result<(Vec<(String, String, String, Decimal, Decimal, Decimal, Option<String>)>, Option<String>), String> {
    #[derive(Clone, Debug)]
    struct SoldTransactionEx {
        trade_date: String,
        settlement_date: String,
        quantity: Decimal,
        price: Decimal,
        symbol: Option<String>,
        net_amount: Decimal,
        commission: Decimal,
        fee: Decimal,
        has_trade_confirmation: bool,
    }

    fn build_sold_transactions_ex(
        sold_transactions: &Vec<(String, String, Decimal, Decimal, Decimal, Option<String>)>,
        trade_confirmations: &Vec<(String, String, i32, Decimal, Decimal, Decimal, Decimal, Decimal)>,
    ) -> Result<Vec<SoldTransactionEx>, String> {
        fn normalize_short_us_date_key(date: &str) -> String {
            // Only normalize zero-padding for month/day. Keep original month/day field order.
            let raw = date.trim();
            let parts: Vec<&str> = raw.split('/').collect();
            if parts.len() != 3 {
                return raw.to_string();
            }

            let month = parts[0].trim();
            let day = parts[1].trim();
            let year = parts[2].trim();

            let month_norm = if month.len() == 1 {
                format!("0{month}")
            } else {
                month.to_string()
            };
            let day_norm = if day.len() == 1 {
                format!("0{day}")
            } else {
                day.to_string()
            };

            format!("{month_norm}/{day_norm}/{year}")
        }

        let mut confirmations_by_key: HashMap<(String, String, i32), Vec<(Decimal, Decimal, Decimal)>> =
            HashMap::new();

        for (trade_date, settlement_date, qty, _price, _principal, commission, fee, net_amount) in
            trade_confirmations
        {
            let commission_val = *commission;
            let fee_val = *fee;
            confirmations_by_key
                .entry((
                    normalize_short_us_date_key(trade_date),
                    normalize_short_us_date_key(settlement_date),
                    *qty,
                ))
                .or_insert(vec![])
                .push((*net_amount, commission_val, fee_val));
        }

        let mut sold_transactions_ex: Vec<SoldTransactionEx> = vec![];
        for (trade_date, settlement_date, qty, price, _income, symbol) in sold_transactions {
            let qty_key = qty.round().to_i32().unwrap_or(0);
            let confirmation = confirmations_by_key
                .get_mut(&(
                    normalize_short_us_date_key(trade_date),
                    normalize_short_us_date_key(settlement_date),
                    qty_key,
                ))
                .and_then(|entries| entries.pop());

            let (net_amount, commission, fee, has_trade_confirmation) = match confirmation {
                Some((net_amount, commission, fee)) => (net_amount, commission, fee, true),
                None => (Decimal::ZERO, Decimal::ZERO, Decimal::ZERO, false),
            };

            sold_transactions_ex.push(SoldTransactionEx {
                trade_date: trade_date.clone(),
                settlement_date: settlement_date.clone(),
                quantity: *qty,
                price: *price,
                symbol: symbol.clone(),
                net_amount,
                commission,
                fee,
                has_trade_confirmation,
            });
        }

        for ((trade_date, settlement_date, qty), entries) in confirmations_by_key {
            if !entries.is_empty() {
                return Err(format!(
                    "\n\nERROR: Not all Trade Confirmations could be matched by trade date + settlement date + quantity.\n\
Unmatched confirmation: trade_date={}, settlement_date={}, quantity={}\n",
                    trade_date, settlement_date, qty
                ));
            }
        }

        Ok(sold_transactions_ex)
    }

    fn sanity_check_pdf_vs_gl_totals(
        sold_transactions: &Vec<(String, String, Decimal, Decimal, Decimal, Option<String>)>,
        gains_and_losses: &Vec<(String, String, Decimal, Decimal, Decimal, Decimal)>,
    ) -> Result<(), String> {
        let pdf_total: Decimal = sold_transactions
            .iter()
            .map(|(_, _, _, _, income, _)| *income)
            .sum();
        let gl_total: Decimal = gains_and_losses
            .iter()
            .map(|(_, _, _, _, total_proceeds, _)| *total_proceeds)
            .sum();

        let diff = (pdf_total - gl_total).abs();
        let accepted_delta = dec!(0.004999);

        if diff > accepted_delta {
            return Err(format!(
                "\n\nERROR: Sold transactions mismatch between ClientStatement's PDFs and Gain&Losses XLSX.\n\
PDF total proceeds: {pdf_total:.2}\n\
G&L total proceeds: {gl_total:.2}\n\
Difference: {diff:.2}\n\n\
Please verify that all matching PDF account statements and exactly one Gain&Losses XLSX for the same period are selected.\n"
            ));
        }

        Ok(())
    }

    fn sanity_check_trade_confirmations(
        trade_confirmations: &Vec<(String, String, i32, Decimal, Decimal, Decimal, Decimal, Decimal)>,
        gains_and_losses: &Vec<(String, String, Decimal, Decimal, Decimal, Decimal)>,
    ) -> Result<(), String> {
        if trade_confirmations.is_empty() {
            return Ok(());
        }

        let tc_total_net_amount: Decimal = trade_confirmations
            .iter()
            .map(|(_, _, _, _, _, _, _, net_amount)| *net_amount)
            .sum();
        let gl_total: Decimal = gains_and_losses
            .iter()
            .map(|(_, _, _, _, total_proceeds, _)| *total_proceeds)
            .sum();

        let diff = (tc_total_net_amount - gl_total).abs();
        let accepted_delta = dec!(0.004999);

        if diff > accepted_delta {
            return Err(format!(
                "\n\nERROR: Trade Confirmation totals mismatch with Gain&Losses XLSX.\n\
Trade Confirmation total net amount: {tc_total_net_amount:.2}\n\
G&L total proceeds: {gl_total:.2}\n\
Difference: {diff:.2}\n\n\
Please verify that all Trade Confirmation PDFs and the Gain&Losses XLSX match the same period.\n"
            ));
        }

        log::info!("Trade Confirmation validation passed: net amount total matches G&L proceeds (diff: {diff:.4})");
        Ok(())
    }

    // Ok What do I need.
    // 1. trade date
    // 2. settlement date (display only, no tax implications)
    // 3. date of purchase
    // 4. gross income (or net if no confirmations)
    // 5. cost basis
    // 6. fees (when confirmations exist)
    // 7. company symbol (ticker)
    let mut detailed_sold_transactions: Vec<(String, String, String, Decimal, Decimal, Decimal, Option<String>)> =
        vec![];

    if sold_transactions.len() > 0 && gains_and_losses.is_empty() {
        return Err("\n\nERROR: Sold transaction detected, but corressponding Gain&Losses document is missing. Please download Gain&Losses  XLSX document at:\n
            https://us.etrade.com/etx/sp/stockplan#/myAccount/gainsLosses\n\n".to_string());
    }

    let missing_tc_warning = if trade_confirmations.is_empty() && !gains_and_losses.is_empty() {
        let warning = "⚠ NOTE: No Trade Confirmation PDFs provided.\n   SELL transactions are based on NET amount from Account Statements.\n   Fees and commissions are not separately extracted.\n   For detailed fee breakdown, include Trade Confirmation PDFs.\n  Obtainable at: https://us.etrade.com/etx/pxy/accountdocs#/documents (select 'Trade Confirmation' type)".to_string();
        println!("\n{warning}\n");
        log::info!("No Trade Confirmation PDFs provided; using Account Statement net amounts for sold transactions");
        Some(warning.to_string())
    } else {
        None
    };

    let sold_transactions_ex = build_sold_transactions_ex(sold_transactions, trade_confirmations)?;

    let mut gain_to_sold_matches: Vec<(usize, usize)> = vec![];
    let qty_delta = dec!(0.001);

    let mut gains_by_day: HashMap<chrono::NaiveDate, Vec<usize>> = HashMap::new();
    for (gain_idx, (_acquisition_date, tr_date, _cost_basis, _adjusted_cost_basis, _inc, quantity)) in
        gains_and_losses.iter().enumerate()
    {
        let trade_date = chrono::NaiveDate::parse_from_str(tr_date, "%m/%d/%Y")
            .expect_and_log(&format!("Unable to parse trade date: {tr_date}"));
        if *quantity <= Decimal::ZERO {
            return Err(format!(
                "\n\nERROR: Gain&Losses quantity must be positive for trade date {tr_date}.\n"
            ));
        }
        gains_by_day.entry(trade_date).or_insert(vec![]).push(gain_idx);
    }

    let mut sold_by_day: HashMap<chrono::NaiveDate, Vec<usize>> = HashMap::new();
    for (sold_idx, sold_ex) in sold_transactions_ex.iter().enumerate() {
        let trade_date_pdf = chrono::NaiveDate::parse_from_str(&sold_ex.trade_date, "%m/%d/%y")
            .expect_and_log(&format!("Unable to parse trade date: {}", sold_ex.trade_date));
        sold_by_day.entry(trade_date_pdf).or_insert(vec![]).push(sold_idx);
    }

    fn solve_day_knapsack(
        gain_indices: &Vec<usize>,
        sold_indices: &Vec<usize>,
        gains_and_losses: &Vec<(String, String, Decimal, Decimal, Decimal, Decimal)>,
        sold_transactions_ex: &Vec<SoldTransactionEx>,
        qty_delta: Decimal,
    ) -> Option<Vec<(usize, usize)>> {
        fn dfs(
            pos: usize,
            ordered_gain_indices: &Vec<usize>,
            sold_indices: &Vec<usize>,
            gain_qty: &HashMap<usize, Decimal>,
            remaining_per_sold: &mut HashMap<usize, Decimal>,
            assignments: &mut Vec<(usize, usize)>,
            qty_delta: Decimal,
        ) -> bool {
            if pos >= ordered_gain_indices.len() {
                return sold_indices.iter().all(|s| {
                    remaining_per_sold
                        .get(s)
                        .map(|r| r.abs() <= qty_delta)
                        .unwrap_or(false)
                });
            }

            let gain_idx = ordered_gain_indices[pos];
            let qty = *gain_qty.get(&gain_idx).unwrap_or(&Decimal::ZERO);

            let mut candidate_sold_indices: Vec<usize> = sold_indices
                .iter()
                .copied()
                .filter(|sold_idx| {
                    let rem = *remaining_per_sold.get(sold_idx).unwrap_or(&Decimal::ZERO);
                    rem + qty_delta >= qty
                })
                .collect();

            candidate_sold_indices.sort_by(|a, b| {
                let ra = *remaining_per_sold.get(a).unwrap_or(&Decimal::ZERO);
                let rb = *remaining_per_sold.get(b).unwrap_or(&Decimal::ZERO);
                rb.cmp(&ra)
            });

            for sold_idx in candidate_sold_indices {
                let rem = *remaining_per_sold.get(&sold_idx).unwrap_or(&Decimal::ZERO);
                remaining_per_sold.insert(sold_idx, rem - qty);
                assignments.push((gain_idx, sold_idx));

                if dfs(
                    pos + 1,
                    ordered_gain_indices,
                    sold_indices,
                    gain_qty,
                    remaining_per_sold,
                    assignments,
                    qty_delta,
                ) {
                    return true;
                }

                assignments.pop();
                remaining_per_sold.insert(sold_idx, rem);
            }

            false
        }

        let mut gain_qty: HashMap<usize, Decimal> = HashMap::new();
        for gain_idx in gain_indices {
            gain_qty.insert(
                *gain_idx,
                gains_and_losses[*gain_idx].5,
            );
        }

        let mut remaining_per_sold: HashMap<usize, Decimal> = HashMap::new();
        for sold_idx in sold_indices {
            remaining_per_sold.insert(
                *sold_idx,
                sold_transactions_ex[*sold_idx].quantity,
            );
        }

        let mut ordered_gain_indices = gain_indices.clone();
        ordered_gain_indices.sort_by(|a, b| {
            let qa = *gain_qty.get(a).unwrap_or(&Decimal::ZERO);
            let qb = *gain_qty.get(b).unwrap_or(&Decimal::ZERO);
            qb.cmp(&qa)
        });

        let mut assignments: Vec<(usize, usize)> = vec![];
        if dfs(
            0,
            &ordered_gain_indices,
            sold_indices,
            &gain_qty,
            &mut remaining_per_sold,
            &mut assignments,
            qty_delta,
        ) {
            Some(assignments)
        } else {
            None
        }
    }

    let mut all_days: Vec<chrono::NaiveDate> = gains_by_day
        .keys()
        .chain(sold_by_day.keys())
        .cloned()
        .collect();
    all_days.sort();
    all_days.dedup();

    for day in all_days {
        let day_gains = gains_by_day.get(&day).cloned().unwrap_or(vec![]);
        let day_sold = sold_by_day.get(&day).cloned().unwrap_or(vec![]);

        let total_gain_qty = day_gains.iter().fold(Decimal::ZERO, |acc, gain_idx| {
            acc + gains_and_losses[*gain_idx].5
        });
        let total_sold_qty = day_sold.iter().fold(Decimal::ZERO, |acc, sold_idx| {
            acc + sold_transactions_ex[*sold_idx].quantity
        });

        if (total_gain_qty - total_sold_qty).abs() > qty_delta {
            return Err(format!(
                "\n\nERROR: Same-day quantity mismatch between Gain&Losses XLSX and PDF sold transactions.\n\
trade_date: {}\n\
G&L total quantity: {}\n\
PDF total quantity: {}\n",
                day.format("%m/%d/%Y"),
                total_gain_qty,
                total_sold_qty
            ));
        }

        let mut day_matches = solve_day_knapsack(
            &day_gains,
            &day_sold,
            gains_and_losses,
            &sold_transactions_ex,
            qty_delta,
        )
        .ok_or(format!(
            "\n\nERROR: Unable to allocate Gain&Losses rows into same-day sold transactions by quantity.\n\
trade_date: {}\n",
            day.format("%m/%d/%Y")
        ))?;

        gain_to_sold_matches.append(&mut day_matches);
    }

    gain_to_sold_matches.sort_by_key(|(gain_idx, _)| *gain_idx);

    let mut total_gl_per_sold_index: HashMap<usize, Decimal> = HashMap::new();
    for (gain_idx, sold_index) in &gain_to_sold_matches {
        let gl_inc = gains_and_losses[*gain_idx].4;
        *total_gl_per_sold_index
            .entry(*sold_index)
            .or_insert(Decimal::ZERO) += gl_inc;
    }

    // iterate through all sold transactions and update it with needed info
    for (gain_idx, sold_index) in gain_to_sold_matches {
        let (acquisition_date, tr_date, cost_basis, _, inc, _quantity) = &gains_and_losses[gain_idx];
        // match trade date and gross with principal and trade date of  trade confirmation

        log::info!("Reconstructing G&L sold transaction: trade date: {tr_date}, acquisition date: {acquisition_date}, cost basis: {cost_basis}, income: {inc}");
        let sold_ex = &sold_transactions_ex[sold_index];
        let settlement_date = &sold_ex.settlement_date;
        let symbol = &sold_ex.symbol;

        let (mut adjusted_income, mut adjusted_cost_basis, mut adjusted_fees) = (*inc, *cost_basis, Decimal::ZERO);
        if sold_ex.has_trade_confirmation {
            let gl_total_for_sold = *total_gl_per_sold_index
                .get(&sold_index)
                .unwrap_or(&Decimal::ZERO);
            if gl_total_for_sold > Decimal::ZERO {
                let ratio = *inc / gl_total_for_sold;
                let proportional_net = sold_ex.net_amount * ratio;
                let proportional_fee = (sold_ex.commission + sold_ex.fee) * ratio;

                // adjusted_income is the net proceeds (what the seller receives)
                adjusted_income = proportional_net;
                // fees separately for display
                adjusted_fees = proportional_fee;
                // cost_basis still includes fees for tax purposes
                adjusted_cost_basis = *cost_basis + proportional_fee;

                log::info!(
                    "Applied Trade Confirmation enrichment to sold trade: qty={}, price={:.4}, ratio={}, gross={}, fee={}",
                    sold_ex.quantity,
                    sold_ex.price,
                    ratio,
                    proportional_net + proportional_fee,
                    proportional_fee
                );
            }
        }

        detailed_sold_transactions.push((
            chrono::NaiveDate::parse_from_str(&tr_date, "%m/%d/%Y")
                .expect(&format!("Unable to parse trade date: {tr_date}"))
                .format("%m/%d/%y")
                .to_string(),
            settlement_date.clone(),
            chrono::NaiveDate::parse_from_str(&acquisition_date, "%m/%d/%Y")
                .expect(&format!(
                    "Unable to parse acquisition_date: {acquisition_date}"
                ))
                .format("%m/%d/%y")
                .to_string(),
            adjusted_income,
            adjusted_cost_basis,
            adjusted_fees,
            symbol.clone(),
        ));
    }

    // Seems matching was OK. Double check we have the same totals in all flavors of PDFs and XLSX
    // Doing it only now, not at the beginning, to have a better contextual help on missed match in the logic above.
    sanity_check_pdf_vs_gl_totals(sold_transactions, gains_and_losses)?;
    sanity_check_trade_confirmations(trade_confirmations, gains_and_losses)?;


    Ok((detailed_sold_transactions, missing_tc_warning))
}

pub fn create_detailed_revolut_transactions(
    transactions: Vec<(String, crate::Currency, crate::Currency, Option<String>)>,
    dates: &std::collections::HashMap<crate::Exchange, Option<(String, Decimal)>>,
) -> Result<Vec<Transaction>, &str> {
    let mut detailed_transactions: Vec<Transaction> = Vec::new();

    transactions
        .iter()
        .try_for_each(|(transaction_date, gross, tax, company)| {
            let (exchange_rate_date, exchange_rate) = dates
                [&gross.derive_exchange(transaction_date.clone())]
                .clone()
                .unwrap();

            let transaction = Transaction {
                transaction_date: transaction_date.clone(),
                gross: *gross,
                tax_paid: *tax,
                exchange_rate_date,
                exchange_rate,
                company: company.clone(),
            };

            let msg = transaction.format_to_print("REVOLUT")?;

            println!("{}", msg);
            log::info!("{}", msg);
            detailed_transactions.push(transaction);
            Ok::<(), &str>(())
        })?;
    Ok(detailed_transactions)
}

pub fn create_detailed_interests_transactions(
    transactions: Vec<(String, Decimal, Decimal)>,
    dates: &std::collections::HashMap<crate::Exchange, Option<(String, Decimal)>>,
) -> Result<Vec<Transaction>, &str> {
    let mut detailed_transactions: Vec<Transaction> = Vec::new();
    transactions
        .iter()
        .try_for_each(|(transaction_date, gross_us, tax_us)| {
            let (exchange_rate_date, exchange_rate) = dates
                [&crate::Exchange::USD(transaction_date.clone())]
                .clone()
                .unwrap();

            let transaction = Transaction {
                transaction_date: transaction_date.clone(),
                gross: crate::Currency::USD(*gross_us),
                tax_paid: crate::Currency::USD(*tax_us),
                exchange_rate_date,
                exchange_rate,
                company: None, // No company info when interests are paid on money
            };

            let msg = transaction.format_to_print("INTERESTS")?;

            println!("{}", msg);
            log::info!("{}", msg);
            detailed_transactions.push(transaction);
            Ok::<(), &str>(())
        })?;
    Ok(detailed_transactions)
}

pub fn create_detailed_div_transactions(
    transactions: Vec<(String, Decimal, Decimal, Option<String>)>,
    dates: &std::collections::HashMap<crate::Exchange, Option<(String, Decimal)>>,
) -> Result<Vec<Transaction>, &str> {
    let mut detailed_transactions: Vec<Transaction> = Vec::new();
    transactions
        .iter()
        .try_for_each(|(transaction_date, gross_us, tax_us, company)| {
            let (exchange_rate_date, exchange_rate) = dates
                [&crate::Exchange::USD(transaction_date.clone())]
                .clone()
                .unwrap();

            let transaction = Transaction {
                transaction_date: transaction_date.clone(),
                gross: crate::Currency::USD(*gross_us),
                tax_paid: crate::Currency::USD(*tax_us),
                exchange_rate_date,
                exchange_rate,
                company: company.clone(),
            };

            let msg = transaction.format_to_print("DIV")?;

            println!("{}", msg);
            log::info!("{}", msg);
            detailed_transactions.push(transaction);
            Ok::<(), &str>(())
        })?;
    Ok(detailed_transactions)
}

//    pub trade_date: String,
//    pub settlement_date: String,
//    pub acquisition_date: String,
//    pub income_us: f32,
//    pub cost_basis: f32,
//    pub exchange_rate_trade_date: String,
//    pub exchange_rate_trade: f32,
//    pub exchange_rate_acquisition_date: String,
//    pub exchange_rate_acquisition: f32,
pub fn create_detailed_sold_transactions(
    transactions: Vec<(String, String, String, Decimal, Decimal, Decimal, Option<String>)>,
    dates: &std::collections::HashMap<crate::Exchange, Option<(String, Decimal)>>,
) -> Result<Vec<SoldTransaction>, &str> {
    let mut detailed_transactions: Vec<SoldTransaction> = Vec::new();
    transactions.iter().for_each(
        |(trade_date, settlement_date, acquisition_date, income, cost_basis, fees, symbol)| {
            let (exchange_rate_trade_date, exchange_rate_trade) = dates
                [&crate::Exchange::USD(trade_date.clone())]
                .clone()
                .unwrap();
            let (exchange_rate_acquisition_date, exchange_rate_acquisition) = dates
                [&crate::Exchange::USD(acquisition_date.clone())]
                .clone()
                .unwrap();

            let transaction = SoldTransaction {
                trade_date: trade_date.clone(),
                settlement_date: settlement_date.clone(), // No tax implications (for reference only)
                acquisition_date: acquisition_date.clone(),
                income_us: *income,
                cost_basis: *cost_basis,
                fees: *fees,
                exchange_rate_trade_date,
                exchange_rate_trade,
                exchange_rate_acquisition_date,
                exchange_rate_acquisition,
                company: symbol.clone(),
            };

            let msg = transaction.format_to_print("");

            println!("{}", msg);
            log::info!("{}", msg);

            detailed_transactions.push(transaction);
        },
    );
    Ok(detailed_transactions)
}

pub fn create_detailed_revolut_sold_transactions(
    transactions: Vec<(
        String,
        String,
        crate::Currency,
        crate::Currency,
        Option<String>,
    )>,
    dates: &std::collections::HashMap<crate::Exchange, Option<(String, Decimal)>>,
) -> Result<Vec<SoldTransaction>, &str> {
    let mut detailed_transactions: Vec<SoldTransaction> = Vec::new();
    transactions.iter().for_each(
        |(acquired_date, sold_date, cost_basis, gross_income, symbol)| {
            // For Revolut transactions sold_date is the transaction date.
            let (exchange_rate_trade_date, exchange_rate_trade) = dates
                [&gross_income.derive_exchange(sold_date.clone())]
                .clone()
                .unwrap();
            let (exchange_rate_acquisition_date, exchange_rate_acquisition) = dates
                [&cost_basis.derive_exchange(acquired_date.clone())]
                .clone()
                .unwrap();

            let transaction = SoldTransaction {
                trade_date: sold_date.clone(),
                settlement_date: sold_date.clone(), // No tax implications (for reference only)
                acquisition_date: acquired_date.clone(),
                income_us: gross_income.value(),
                cost_basis: cost_basis.value(),
                fees: Decimal::ZERO,
                exchange_rate_trade_date,
                exchange_rate_trade,
                exchange_rate_acquisition_date,
                exchange_rate_acquisition,
                company: symbol.clone(),
            };

            let msg = transaction.format_to_print("REVOLUT ");

            println!("{}", msg);
            log::info!("{}", msg);

            detailed_transactions.push(transaction);
        },
    );
    Ok(detailed_transactions)
}

// Make a dataframe with
pub(crate) fn create_per_company_report(
    interests: &[Transaction],
    dividends: &[Transaction],
    sold_transactions: &[SoldTransaction],
    revolut_dividends_transactions: &[Transaction],
    revolut_sold_transactions: &[SoldTransaction],
) -> Result<DataFrame, &'static str> {
    // Key: Company Name , Value : (gross_pl, tax_paid_in_us_pl, cost_pl)
    let mut per_company_data: HashMap<Option<String>, (Decimal, Decimal, Decimal)> = HashMap::new();

    let interests_or_dividends = interests
        .iter()
        .chain(dividends.iter())
        .chain(revolut_dividends_transactions.iter());

    interests_or_dividends.for_each(|x| {
        let entry = per_company_data
            .entry(x.company.clone())
            .or_insert((Decimal::ZERO, Decimal::ZERO, Decimal::ZERO));
        entry.0 += x.exchange_rate * x.gross.value();
        entry.1 += x.exchange_rate * x.tax_paid.value();
        // No cost for dividends being paid
    });

    let sells = sold_transactions
        .iter()
        .chain(revolut_sold_transactions.iter());
    sells.for_each(|x| {
        let entry = per_company_data
            .entry(x.company.clone())
            .or_insert((Decimal::ZERO, Decimal::ZERO, Decimal::ZERO));
        entry.0 += x.income_us * x.exchange_rate_trade;
        // No tax from sold transactions
        entry.2 += x.cost_basis * x.exchange_rate_acquisition;
    });

    // Convert my HashMap into DataFrame
    let mut companies: Vec<Option<String>> = Vec::new();
    let mut gross: Vec<f64> = Vec::new();
    let mut tax: Vec<f64> = Vec::new();
    let mut cost: Vec<f64> = Vec::new();
    per_company_data
        .iter()
        .try_for_each(|(company, (gross_pl, tax_paid_in_us_pl, cost_pl))| {
            log::info!(
                "Company: {:?}, Gross PLN: {:.2}, Tax Paid in USD PLN: {:.2}, Cost PLN: {:.2}",
                company,
                gross_pl,
                tax_paid_in_us_pl,
                cost_pl
            );
            companies.push(company.clone());
            gross.push(gross_pl.to_f64().unwrap_or(0.0));
            tax.push(tax_paid_in_us_pl.to_f64().unwrap_or(0.0));
            cost.push(cost_pl.to_f64().unwrap_or(0.0));

            Ok::<(), &str>(())
        })?;
    let series = vec![
        Series::new("Company", companies),
        Series::new("Gross[PLN]", gross),
        Series::new("Cost[PLN]", cost),
        Series::new("Tax Paid in USD[PLN]", tax),
    ];
    DataFrame::new(series)
        .map_err(|_| "Unable to create per company report dataframe")?
        .sort(["Company"], false, true)
        .map_err(|_| "Unable to sort per company report dataframe")
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::Currency;
    use rust_decimal::dec;

    fn round4(val: f64) -> f64 {
        (val * 10_000.0).round() / 10_000.0
    }

    fn add_missing_gl_quantity(
        gains: &Vec<(String, String, Decimal, Decimal, Decimal)>,
        sold: &Vec<(String, String, Decimal, Decimal, Decimal, Option<String>)>,
    ) -> Vec<(String, String, Decimal, Decimal, Decimal, Decimal)> {
        let mut sold_qty_by_day: HashMap<chrono::NaiveDate, Decimal> = HashMap::new();
        for (trade_date, _settlement_date, qty, _price, _income, _symbol) in sold {
            let day = chrono::NaiveDate::parse_from_str(trade_date, "%m/%d/%y")
                .expect_and_log(&format!("Unable to parse trade date: {trade_date}"));
            *sold_qty_by_day.entry(day).or_insert(Decimal::ZERO) += *qty;
        }

        let mut gains_by_day: HashMap<chrono::NaiveDate, Vec<usize>> = HashMap::new();
        for (idx, (_acq_date, sold_date, _acq_cost, _cost_basis, _proceeds)) in gains.iter().enumerate() {
            let day = chrono::NaiveDate::parse_from_str(sold_date, "%m/%d/%Y")
                .expect_and_log(&format!("Unable to parse sold date: {sold_date}"));
            gains_by_day.entry(day).or_insert(vec![]).push(idx);
        }

        let mut out: Vec<(String, String, Decimal, Decimal, Decimal, Decimal)> = gains
            .iter()
            .map(|(a, b, c, d, e)| (a.clone(), b.clone(), *c, *d, *e, Decimal::ZERO))
            .collect();

        for (day, gain_indices) in gains_by_day {
            let day_sold_qty = *sold_qty_by_day.get(&day).unwrap_or(&Decimal::ZERO);
            if day_sold_qty <= Decimal::ZERO {
                continue;
            }

            let day_total_proceeds: Decimal = gain_indices.iter().map(|idx| gains[*idx].4).sum();
            if day_total_proceeds <= Decimal::ZERO {
                continue;
            }

            let mut assigned_sum = Decimal::ZERO;
            for (i, gain_idx) in gain_indices.iter().enumerate() {
                if i + 1 == gain_indices.len() {
                    out[*gain_idx].5 = (day_sold_qty - assigned_sum).max(Decimal::ZERO);
                } else {
                    let qty = day_sold_qty * (gains[*gain_idx].4 / day_total_proceeds);
                    out[*gain_idx].5 = qty;
                    assigned_sum += qty;
                }
            }
        }

        out
    }

    #[test]
    fn test_create_per_company_report_interests() -> Result<(), String> {
        let input = vec![
            Transaction {
                transaction_date: "03/01/21".to_string(),
                gross: crate::Currency::EUR(dec!(0.05)),
                tax_paid: crate::Currency::EUR(dec!(0.0)),
                exchange_rate_date: "02/28/21".to_string(),
                exchange_rate: dec!(2.0),
                company: None,
            },
            Transaction {
                transaction_date: "04/11/21".to_string(),
                gross: crate::Currency::EUR(dec!(0.07)),
                tax_paid: crate::Currency::EUR(dec!(0.0)),
                exchange_rate_date: "04/10/21".to_string(),
                exchange_rate: dec!(3.0),
                company: None,
            },
        ];
        let df = create_per_company_report(&input, &[], &[], &[], &[])
            .map_err(|e| format!("Error creating per company report: {}", e))?;

        // Interests are having company == None, and data should be folded to one row
        assert_eq!(df.height(), 1);
        assert_eq!(df.width(), 4);

        let company_col = df.column("Company").unwrap();
        assert_eq!(company_col.get(0).is_err(), false); // None company
        let gross_col = df.column("Gross[PLN]").unwrap();
        assert_eq!(
            round4(gross_col.get(0).unwrap().extract::<f64>().unwrap()),
            round4(0.05 * 2.0 + 0.07 * 3.0)
        );
        let cost_col = df.column("Cost[PLN]").unwrap();
        assert_eq!(cost_col.get(0).unwrap().extract::<f64>().unwrap(), 0.00);
        let tax_col = df.column("Tax Paid in USD[PLN]").unwrap();
        assert_eq!(tax_col.get(0).unwrap().extract::<f64>().unwrap(), 0.00);

        Ok(())
    }

    #[test]
    fn test_create_per_company_report_dividends() -> Result<(), String> {
        let input = vec![
            Transaction {
                transaction_date: "04/11/21".to_string(),
                gross: crate::Currency::USD(dec!(100.0)),
                tax_paid: crate::Currency::USD(dec!(25.0)),
                exchange_rate_date: "04/10/21".to_string(),
                exchange_rate: dec!(3.0),
                company: Some("INTEL CORP".to_owned()),
            },
            Transaction {
                transaction_date: "03/01/21".to_string(),
                gross: crate::Currency::USD(dec!(126.0)),
                tax_paid: crate::Currency::USD(dec!(10.0)),
                exchange_rate_date: "02/28/21".to_string(),
                exchange_rate: dec!(2.0),
                company: Some("INTEL CORP".to_owned()),
            },
            Transaction {
                transaction_date: "03/11/21".to_string(),
                gross: crate::Currency::USD(dec!(100.0)),
                tax_paid: crate::Currency::USD(dec!(0.0)),
                exchange_rate_date: "02/28/21".to_string(),
                exchange_rate: dec!(10.0),
                company: Some("ABEV".to_owned()),
            },
        ];
        let df = create_per_company_report(&[], &input, &[], &[], &[])
            .map_err(|e| format!("Error creating per company report: {}", e))?;

        // Interests are having company == None, and data should be folded to one row
        assert_eq!(df.height(), 2);
        assert_eq!(df.width(), 4);

        let company_col = df.column("Company").unwrap().utf8().unwrap();
        let gross_col = df.column("Gross[PLN]").unwrap();
        let tax_col = df.column("Tax Paid in USD[PLN]").unwrap();
        let (abev_index, intc_index) = match company_col.get(0) {
            Some("INTEL CORP") => (1, 0),
            Some("ABEV") => (0, 1),
            _ => return Err("Unexpected company name in first row".to_owned()),
        };
        assert_eq!(
            round4(gross_col.get(intc_index).unwrap().extract::<f64>().unwrap()),
            round4(100.0 * 3.0 + 126.0 * 2.0)
        );
        assert_eq!(
            round4(gross_col.get(abev_index).unwrap().extract::<f64>().unwrap()),
            round4(100.0 * 10.0)
        );
        assert_eq!(
            tax_col.get(intc_index).unwrap().extract::<f64>().unwrap(),
            round4(25.0 * 3.0 + 10.0 * 2.0)
        );
        assert_eq!(
            tax_col.get(abev_index).unwrap().extract::<f64>().unwrap(),
            round4(0.0)
        );

        let cost_col = df.column("Cost[PLN]").unwrap();
        assert_eq!(cost_col.get(0).unwrap().extract::<f64>().unwrap(), 0.00);
        assert_eq!(cost_col.get(1).unwrap().extract::<f64>().unwrap(), 0.00);

        Ok(())
    }

    #[test]
    fn test_create_per_company_report_sells() -> Result<(), String> {
        let input = vec![
            SoldTransaction {
                trade_date: "03/01/21".to_string(),
                settlement_date: "03/03/21".to_string(),
                acquisition_date: "01/01/21".to_string(),
                income_us: dec!(20.0),
                cost_basis: dec!(20.0),
                fees: dec!(0.0),
                exchange_rate_trade_date: "02/28/21".to_string(),
                exchange_rate_trade: dec!(2.5),
                exchange_rate_acquisition_date: "02/28/21".to_string(),
                exchange_rate_acquisition: dec!(5.0),
                company: Some("INTEL CORP".to_owned()),
            },
            SoldTransaction {
                trade_date: "06/01/21".to_string(),
                settlement_date: "06/03/21".to_string(),
                acquisition_date: "01/01/19".to_string(),
                income_us: dec!(25.0),
                cost_basis: dec!(10.0),
                fees: dec!(0.0),
                exchange_rate_trade_date: "05/31/21".to_string(),
                exchange_rate_trade: dec!(4.0),
                exchange_rate_acquisition_date: "12/30/18".to_string(),
                exchange_rate_acquisition: dec!(6.0),
                company: Some("INTEL CORP".to_owned()),
            },
            SoldTransaction {
                trade_date: "06/01/21".to_string(),
                settlement_date: "06/03/21".to_string(),
                acquisition_date: "01/01/19".to_string(),
                income_us: dec!(20.0),
                cost_basis: dec!(0.0),
                fees: dec!(0.0),
                exchange_rate_trade_date: "05/31/21".to_string(),
                exchange_rate_trade: dec!(4.0),
                exchange_rate_acquisition_date: "12/30/18".to_string(),
                exchange_rate_acquisition: dec!(6.0),
                company: Some("PXD".to_owned()),
            },
        ];
        let df = create_per_company_report(&[], &[], &input, &[], &[])
            .map_err(|e| format!("Error creating per company report: {}", e))?;

        // Solds are having company
        assert_eq!(df.height(), 2);
        assert_eq!(df.width(), 4);

        let company_col = df.column("Company").unwrap().utf8().unwrap();
        let gross_col = df.column("Gross[PLN]").unwrap();
        let cost_col = df.column("Cost[PLN]").unwrap();
        let (abev_index, intc_index) = match company_col.get(0) {
            Some("INTEL CORP") => (1, 0),
            Some("PXD") => (0, 1),
            _ => return Err("Unexpected company name in first row".to_owned()),
        };
        assert_eq!(
            round4(gross_col.get(intc_index).unwrap().extract::<f64>().unwrap()),
            round4(20.0 * 2.5 + 25.0 * 4.0)
        );
        assert_eq!(
            round4(gross_col.get(abev_index).unwrap().extract::<f64>().unwrap()),
            round4(20.0 * 4.0)
        );
        assert_eq!(
            cost_col.get(intc_index).unwrap().extract::<f64>().unwrap(),
            round4(20.0 * 5.0 + 10.0 * 6.0)
        );
        assert_eq!(
            cost_col.get(abev_index).unwrap().extract::<f64>().unwrap(),
            round4(0.0)
        );

        let tax_col = df.column("Tax Paid in USD[PLN]").unwrap();
        assert_eq!(tax_col.get(0).unwrap().extract::<f64>().unwrap(), 0.00);
        assert_eq!(tax_col.get(1).unwrap().extract::<f64>().unwrap(), 0.00);

        Ok(())
    }

    #[test]
    fn test_interests_verification_ok() -> Result<(), String> {
        let transactions: Vec<(String, Decimal, Decimal)> = vec![
            ("06/01/21".to_string(), dec!(100.0), dec!(0.00)),
            ("03/01/21".to_string(), dec!(126.0), dec!(0.00)),
        ];
        verify_interests_transactions(&transactions)
    }

    #[test]
    fn test_revolut_sold_verification_false() -> Result<(), String> {
        let transactions: Vec<(String, String, Currency, Currency, Option<String>)> = vec![
            (
                "06/01/21".to_string(),
                "06/01/22".to_string(),
                Currency::PLN(dec!(10.0)),
                Currency::PLN(dec!(2.0)),
                Some("INTEL CORP".to_owned()),
            ),
            (
                "06/01/21".to_string(),
                "07/04/23".to_string(),
                Currency::PLN(dec!(10.0)),
                Currency::PLN(dec!(2.0)),
                Some("INTEL CORP".to_owned()),
            ),
        ];
        assert_eq!(
            verify_transactions(&transactions),
            Err("Error: Statements are related to different years!".to_owned())
        );
        Ok(())
    }

    #[test]
    fn test_dividends_verification_ok() -> Result<(), String> {
        let transactions: Vec<(String, Decimal, Decimal, Option<String>)> = vec![
            (
                "06/01/21".to_string(),
                dec!(100.0),
                dec!(25.0),
                Some("INTEL CORP".to_owned()),
            ),
            (
                "03/01/21".to_string(),
                dec!(126.0),
                dec!(10.0),
                Some("INTEL CORP".to_owned()),
            ),
        ];
        verify_dividends_transactions(&transactions)
    }

    #[test]
    fn test_dividends_verification_false() -> Result<(), String> {
        let transactions: Vec<(String, Currency, Currency, Option<String>)> = vec![
            (
                "06/01/21".to_string(),
                Currency::PLN(dec!(10.0)),
                Currency::PLN(dec!(2.0)),
                Some("INTEL CORP".to_owned()),
            ),
            (
                "03/01/22".to_string(),
                Currency::PLN(dec!(126.0)),
                Currency::PLN(dec!(10.0)),
                Some("INTEL CORP".to_owned()),
            ),
        ];
        assert_eq!(
            verify_dividends_transactions(&transactions),
            Err("Error:  Statements are related to different years!".to_owned())
        );
        Ok(())
    }

    #[test]
    fn test_create_detailed_revolut_transactions_eur() -> Result<(), String> {
        let parsed_transactions = vec![
            (
                "03/01/21".to_owned(),
                crate::Currency::EUR(dec!(0.05)),
                crate::Currency::EUR(dec!(0.00)),
                None,
            ),
            (
                "04/11/21".to_owned(),
                crate::Currency::EUR(dec!(0.07)),
                crate::Currency::EUR(dec!(0.00)),
                None,
            ),
        ];

        let mut dates: std::collections::HashMap<crate::Exchange, Option<(String, Decimal)>> =
            std::collections::HashMap::new();

        dates.insert(
            crate::Exchange::EUR("03/01/21".to_owned()),
            Some(("02/28/21".to_owned(), dec!(2.0))),
        );
        dates.insert(
            crate::Exchange::EUR("04/11/21".to_owned()),
            Some(("04/10/21".to_owned(), dec!(3.0))),
        );

        let transactions = create_detailed_revolut_transactions(parsed_transactions, &dates);

        assert_eq!(
            transactions,
            Ok(vec![
                Transaction {
                    transaction_date: "03/01/21".to_string(),
                    gross: crate::Currency::EUR(dec!(0.05)),
                    tax_paid: crate::Currency::EUR(dec!(0.0)),
                    exchange_rate_date: "02/28/21".to_string(),
                    exchange_rate: dec!(2.0),
                    company: None,
                },
                Transaction {
                    transaction_date: "04/11/21".to_string(),
                    gross: crate::Currency::EUR(dec!(0.07)),
                    tax_paid: crate::Currency::EUR(dec!(0.0)),
                    exchange_rate_date: "04/10/21".to_string(),
                    exchange_rate: dec!(3.0),
                    company: None,
                },
            ])
        );
        Ok(())
    }

    #[test]
    fn test_create_detailed_revolut_transactions_pln() -> Result<(), String> {
        let parsed_transactions = vec![
            (
                "03/01/21".to_owned(),
                crate::Currency::PLN(dec!(0.44)),
                crate::Currency::PLN(dec!(0.00)),
                None,
            ),
            (
                "04/11/21".to_owned(),
                crate::Currency::PLN(dec!(0.45)),
                crate::Currency::PLN(dec!(0.00)),
                None,
            ),
        ];

        let mut dates: std::collections::HashMap<crate::Exchange, Option<(String, Decimal)>> =
            std::collections::HashMap::new();

        dates.insert(
            crate::Exchange::PLN("03/01/21".to_owned()),
            Some(("N/A".to_owned(), dec!(1.0))),
        );
        dates.insert(
            crate::Exchange::PLN("04/11/21".to_owned()),
            Some(("N/A".to_owned(), dec!(1.0))),
        );

        let transactions = create_detailed_revolut_transactions(parsed_transactions, &dates);

        assert_eq!(
            transactions,
            Ok(vec![
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
            ])
        );
        Ok(())
    }

    #[test]
    fn test_create_detailed_interests_transactions() -> Result<(), String> {
        let parsed_transactions: Vec<(String, Decimal, Decimal)> = vec![
            ("04/11/21".to_string(), dec!(100.0), dec!(0.00)),
            ("03/01/21".to_string(), dec!(126.0), dec!(0.00)),
        ];

        let mut dates: std::collections::HashMap<crate::Exchange, Option<(String, Decimal)>> =
            std::collections::HashMap::new();

        dates.insert(
            crate::Exchange::USD("03/01/21".to_owned()),
            Some(("02/28/21".to_owned(), dec!(2.0))),
        );
        dates.insert(
            crate::Exchange::USD("04/11/21".to_owned()),
            Some(("04/10/21".to_owned(), dec!(3.0))),
        );

        let transactions = create_detailed_interests_transactions(parsed_transactions, &dates);

        assert_eq!(
            transactions,
            Ok(vec![
                Transaction {
                    transaction_date: "04/11/21".to_string(),
                    gross: crate::Currency::USD(dec!(100.0)),
                    tax_paid: crate::Currency::USD(dec!(0.0)),
                    exchange_rate_date: "04/10/21".to_string(),
                    exchange_rate: dec!(3.0),
                    company: None,
                },
                Transaction {
                    transaction_date: "03/01/21".to_string(),
                    gross: crate::Currency::USD(dec!(126.0)),
                    tax_paid: crate::Currency::USD(dec!(0.0)),
                    exchange_rate_date: "02/28/21".to_string(),
                    exchange_rate: dec!(2.0),
                    company: None,
                },
            ])
        );
        Ok(())
    }

    #[test]
    fn test_create_detailed_div_transactions() -> Result<(), String> {
        let parsed_transactions: Vec<(String, Decimal, Decimal, Option<String>)> = vec![
            (
                "04/11/21".to_string(),
                dec!(100.0),
                dec!(25.0),
                Some("INTEL CORP".to_owned()),
            ),
            (
                "03/01/21".to_string(),
                dec!(126.0),
                dec!(10.0),
                Some("INTEL CORP".to_owned()),
            ),
        ];

        let mut dates: std::collections::HashMap<crate::Exchange, Option<(String, Decimal)>> =
            std::collections::HashMap::new();

        dates.insert(
            crate::Exchange::USD("03/01/21".to_owned()),
            Some(("02/28/21".to_owned(), dec!(2.0))),
        );
        dates.insert(
            crate::Exchange::USD("04/11/21".to_owned()),
            Some(("04/10/21".to_owned(), dec!(3.0))),
        );

        let transactions = create_detailed_div_transactions(parsed_transactions, &dates);

        assert_eq!(
            transactions,
            Ok(vec![
                Transaction {
                    transaction_date: "04/11/21".to_string(),
                    gross: crate::Currency::USD(dec!(100.0)),
                    tax_paid: crate::Currency::USD(dec!(25.0)),
                    exchange_rate_date: "04/10/21".to_string(),
                    exchange_rate: dec!(3.0),
                    company: Some("INTEL CORP".to_owned())
                },
                Transaction {
                    transaction_date: "03/01/21".to_string(),
                    gross: crate::Currency::USD(dec!(126.0)),
                    tax_paid: crate::Currency::USD(dec!(10.0)),
                    exchange_rate_date: "02/28/21".to_string(),
                    exchange_rate: dec!(2.0),
                    company: Some("INTEL CORP".to_owned())
                },
            ])
        );
        Ok(())
    }

    #[test]
    fn test_create_detailed_revolut_sold_transactions() -> Result<(), String> {
        let parsed_transactions: Vec<(String, String, Currency, Currency, Option<String>)> =
            vec![(
                "11/20/23".to_string(),
                "12/08/24".to_string(),
                Currency::USD(dec!(5000.0)),
                Currency::USD(dec!(5804.62)),
                Some("INTEL CORP".to_owned()),
            )];

        let mut dates: std::collections::HashMap<crate::Exchange, Option<(String, Decimal)>> =
            std::collections::HashMap::new();

        dates.insert(
            crate::Exchange::USD("11/20/23".to_owned()),
            Some(("11/19/23".to_owned(), dec!(2.0))),
        );
        dates.insert(
            crate::Exchange::USD("12/08/24".to_owned()),
            Some(("12/06/24".to_owned(), dec!(3.0))),
        );

        let transactions = create_detailed_revolut_sold_transactions(parsed_transactions, &dates);

        assert_eq!(
            transactions,
            Ok(vec![SoldTransaction {
                trade_date: "12/08/24".to_string(),
                settlement_date: "12/08/24".to_string(),
                acquisition_date: "11/20/23".to_string(),
                income_us: dec!(5804.62),
                cost_basis: dec!(5000.0),
                fees: dec!(0.0),
                exchange_rate_trade_date: "12/06/24".to_string(),
                exchange_rate_trade: dec!(3.0),
                exchange_rate_acquisition_date: "11/19/23".to_string(),
                exchange_rate_acquisition: dec!(2.0),
                company: Some("INTEL CORP".to_owned()),
            },])
        );
        Ok(())
    }

    #[test]
    fn test_create_detailed_sold_transactions() -> Result<(), String> {
        let parsed_transactions: Vec<(String, String, String, Decimal, Decimal, Decimal, Option<String>)> = vec![
            (
                "03/01/21".to_string(),
                "03/03/21".to_string(),
                "01/01/21".to_string(),
                dec!(20.0),
                dec!(20.0),
                dec!(0.0),  // fees
                Some("INTEL CORP".to_owned()),
            ),
            (
                "06/01/21".to_string(),
                "06/03/21".to_string(),
                "01/01/19".to_string(),
                dec!(25.0),
                dec!(10.0),
                dec!(0.0),  // fees
                Some("INTEL CORP".to_owned()),
            ),
        ];

        let mut dates: std::collections::HashMap<crate::Exchange, Option<(String, Decimal)>> =
            std::collections::HashMap::new();

        dates.insert(
            crate::Exchange::USD("01/01/21".to_owned()),
            Some(("12/30/20".to_owned(), dec!(1.0))),
        );
        dates.insert(
            crate::Exchange::USD("03/01/21".to_owned()),
            Some(("02/28/21".to_owned(), dec!(2.0))),
        );
        dates.insert(
            crate::Exchange::USD("03/03/21".to_owned()),
            Some(("03/02/21".to_owned(), dec!(2.5))),
        );
        dates.insert(
            crate::Exchange::USD("06/01/21".to_owned()),
            Some(("06/03/21".to_owned(), dec!(3.0))),
        );
        dates.insert(
            crate::Exchange::USD("06/03/21".to_owned()),
            Some(("06/05/21".to_owned(), dec!(4.0))),
        );
        dates.insert(
            crate::Exchange::USD("01/01/21".to_owned()),
            Some(("02/28/21".to_owned(), dec!(5.0))),
        );
        dates.insert(
            crate::Exchange::USD("01/01/19".to_owned()),
            Some(("12/30/18".to_owned(), dec!(6.0))),
        );
        dates.insert(
            crate::Exchange::USD("04/11/21".to_owned()),
            Some(("04/10/21".to_owned(), dec!(7.0))),
        );

        let transactions = create_detailed_sold_transactions(parsed_transactions, &dates);

        assert_eq!(
            transactions,
            Ok(vec![
                SoldTransaction {
                    trade_date: "03/01/21".to_string(),
                    settlement_date: "03/03/21".to_string(),
                    acquisition_date: "01/01/21".to_string(),
                    income_us: dec!(20.0),
                    cost_basis: dec!(20.0),
                    fees: dec!(0.0),
                    exchange_rate_trade_date: "02/28/21".to_string(),
                    exchange_rate_trade: dec!(2.0),
                    exchange_rate_acquisition_date: "02/28/21".to_string(),
                    exchange_rate_acquisition: dec!(5.0),
                    company: Some("INTEL CORP".to_owned()),
                },
                SoldTransaction {
                    trade_date: "06/01/21".to_string(),
                    settlement_date: "06/03/21".to_string(),
                    acquisition_date: "01/01/19".to_string(),
                    income_us: dec!(25.0),
                    cost_basis: dec!(10.0),
                    fees: dec!(0.0),
                    exchange_rate_trade_date: "06/03/21".to_string(),
                    exchange_rate_trade: dec!(3.0),
                    exchange_rate_acquisition_date: "12/30/18".to_string(),
                    exchange_rate_acquisition: dec!(6.0),
                    company: Some("INTEL CORP".to_owned()),
                },
            ])
        );
        Ok(())
    }

    #[test]
    fn test_dividends_verification_empty_ok() -> Result<(), String> {
        let transactions: Vec<(String, Decimal, Decimal, Option<String>)> = vec![];
        verify_dividends_transactions(&transactions)
    }

    #[test]
    fn test_dividends_verification_fail() -> Result<(), String> {
        let transactions: Vec<(String, Decimal, Decimal, Option<String>)> = vec![
            (
                "04/11/22".to_string(),
                dec!(100.0),
                dec!(25.0),
                Some("INTEL CORP".to_owned()),
            ),
            (
                "03/01/21".to_string(),
                dec!(126.0),
                dec!(10.0),
                Some("INTEL CORP".to_owned()),
            ),
        ];
        assert!(verify_dividends_transactions(&transactions).is_err());
        Ok(())
    }

    #[test]
    fn test_sold_transaction_reconstruction_dividiends_only() -> Result<(), String> {
        let parsed_sold_transactions: Vec<(String, String, Decimal, Decimal, Decimal, Option<String>)> = vec![];

        let parsed_gains_and_losses: Vec<(String, String, Decimal, Decimal, Decimal)> = vec![];
        let empty_trade_confirmations = vec![];

        let (detailed_sold_transactions, _) =
            reconstruct_sold_transactions(&parsed_sold_transactions, &add_missing_gl_quantity(&parsed_gains_and_losses, &parsed_sold_transactions), &empty_trade_confirmations)?;
        // 1. trade date
        // 2. settlement date
        // 3. date of purchase
        // 4. net income
        // 5. cost cost basis
        assert_eq!(detailed_sold_transactions, vec![]);
        Ok(())
    }

    #[test]
    fn test_sold_transaction_reconstruction_ok() -> Result<(), String> {
        let parsed_sold_transactions: Vec<(String, String, Decimal, Decimal, Decimal, Option<String>)> = vec![
            (
                "06/01/21".to_string(),
                "06/03/21".to_string(),
                dec!(1.0),
                dec!(25.0),
                dec!(24.8),
                Some("INTEL CORP".to_owned()),
            ),
            (
                "03/01/21".to_string(),
                "03/03/21".to_string(),
                dec!(2.0),
                dec!(10.0),
                dec!(19.8),
                Some("INTEL CORP".to_owned()),
            ),
        ];

        let parsed_gains_and_losses: Vec<(String, String, Decimal, Decimal, Decimal)> = vec![
            (
                "01/01/2019".to_string(),
                "06/01/2021".to_string(),
                dec!(10.0),
                dec!(10.0),
                dec!(24.8),
            ),
            (
                "01/01/2021".to_string(),
                "03/01/2021".to_string(),
                dec!(20.0),
                dec!(20.0),
                dec!(19.8),
            ),
        ];

        let empty_trade_confirmations = vec![];
        let (detailed_sold_transactions, _) =
            reconstruct_sold_transactions(&parsed_sold_transactions, &add_missing_gl_quantity(&parsed_gains_and_losses, &parsed_sold_transactions), &empty_trade_confirmations)?;

        // 1. trade date
        // 2. settlement date
        // 3. date of purchase
        // 4. gross income (or net if no confirmations)
        // 5. cost cost basis
        // 6. fees
        assert_eq!(
            detailed_sold_transactions,
            vec![
                (
                    "06/01/21".to_string(),
                    "06/03/21".to_string(),
                    "01/01/19".to_string(),
                    dec!(24.8),
                    dec!(10.0),
                    dec!(0.0),
                    Some("INTEL CORP".to_owned())
                ),
                (
                    "03/01/21".to_string(),
                    "03/03/21".to_string(),
                    "01/01/21".to_string(),
                    dec!(19.8),
                    dec!(20.0),
                    dec!(0.0),
                    Some("INTEL CORP".to_owned())
                ),
            ]
        );
        Ok(())
    }

    #[test]
    fn test_sold_transaction_reconstruction_single_digits_ok() -> Result<(), String> {
        let parsed_sold_transactions: Vec<(String, String, Decimal, Decimal, Decimal, Option<String>)> = vec![
            (
                "6/1/21".to_string(),
                "6/3/21".to_string(),
                dec!(1.0),
                dec!(25.0),
                dec!(24.8),
                Some("INTEL CORP".to_owned()),
            ),
            (
                "3/1/21".to_string(),
                "3/3/21".to_string(),
                dec!(2.0),
                dec!(10.0),
                dec!(19.8),
                Some("INTEL CORP".to_owned()),
            ),
        ];

        let parsed_gains_and_losses: Vec<(String, String, Decimal, Decimal, Decimal)> = vec![
            (
                "01/01/2019".to_string(),
                "06/01/2021".to_string(),
                dec!(10.0),
                dec!(10.0),
                dec!(24.8),
            ),
            (
                "01/01/2021".to_string(),
                "03/01/2021".to_string(),
                dec!(20.0),
                dec!(20.0),
                dec!(19.8),
            ),
        ];

        let empty_trade_confirmations = vec![];
        let (detailed_sold_transactions, _) =
            reconstruct_sold_transactions(&parsed_sold_transactions, &add_missing_gl_quantity(&parsed_gains_and_losses, &parsed_sold_transactions), &empty_trade_confirmations)?;

        // 1. trade date
        // 2. settlement date
        // 3. date of purchase
        // 4. gross income (or net if no confirmations)
        // 5. cost cost basis
        // 6. fees
        assert_eq!(
            detailed_sold_transactions,
            vec![
                (
                    "06/01/21".to_string(),
                    "6/3/21".to_string(),
                    "01/01/19".to_string(),
                    dec!(24.8),
                    dec!(10.0),
                    dec!(0.0),
                    Some("INTEL CORP".to_owned())
                ),
                (
                    "03/01/21".to_string(),
                    "3/3/21".to_string(),
                    "01/01/21".to_string(),
                    dec!(19.8),
                    dec!(20.0),
                    dec!(0.0),
                    Some("INTEL CORP".to_owned())
                ),
            ]
        );
        Ok(())
    }

    #[test]
    fn test_sold_transaction_reconstruction_second_fail() {
        let parsed_sold_transactions: Vec<(String, String, Decimal, Decimal, Decimal, Option<String>)> =
            vec![(
                "11/07/22".to_string(),        // trade date
                "11/09/22".to_string(),        // settlement date
                dec!(173.0),                         // quantity
                dec!(28.2035),                       // price
                dec!(4877.36),                       // amount sold
                Some("INTEL CORP".to_owned()), // company symbol (ticker)
            )];

        let parsed_gains_and_losses: Vec<(String, String, Decimal, Decimal, Decimal)> = vec![
            (
                "05/02/22".to_string(), // date when sold stock was acquired (date_acquired)
                "07/19/22".to_string(), // date when stock was sold (date_sold)
                dec!(0.0),                    // aqusition cost of sold stock (aquisition_cost)
                dec!(1593.0),                 // adjusted aquisition cost of sold stock (cost_basis)
                dec!(1415.480004),            // income from sold stock (total_proceeds)
            ),
            (
                "02/18/22".to_string(),
                "07/19/22".to_string(),
                dec!(4241.16),
                dec!(4989.6),
                dec!(4325.10001),
            ),
            (
                "08/19/22".to_string(),
                "11/07/22".to_string(),
                dec!(5236.0872),
                dec!(6160.0975),
                dec!(4877.355438),
            ),
        ];

        let empty_trade_confirmations = vec![];
        assert_eq!(
            reconstruct_sold_transactions(&parsed_sold_transactions, &add_missing_gl_quantity(&parsed_gains_and_losses, &parsed_sold_transactions), &empty_trade_confirmations)
                .is_ok(),
            false
        );
    }

    #[test]
    fn test_sold_transaction_reconstruction_multistock() -> Result<(), String> {
        let parsed_sold_transactions: Vec<(String, String, Decimal, Decimal, Decimal, Option<String>)> = vec![
            (
                "12/21/22".to_string(),
                "12/23/22".to_string(),
                dec!(163.0),
                dec!(26.5900),
                dec!(4332.44),
                Some("INTEL CORP".to_owned()),
            ),
            (
                "12/19/22".to_string(),
                "12/21/22".to_string(),
                dec!(252.0),
                dec!(26.5900),
                dec!(6698.00),
                Some("INTEL CORP".to_owned()),
            ),
        ];

        let parsed_gains_and_losses: Vec<(String, String, Decimal, Decimal, Decimal)> = vec![
            (
                "08/19/2021".to_string(),
                "12/19/2022".to_string(),
                dec!(4336.4874),
                dec!(4758.6971),
                dec!(2711.0954),
            ),
            (
                "05/03/2021".to_string(),
                "12/21/2022".to_string(),
                dec!(0.0),
                dec!(3876.918),
                dec!(2046.61285),
            ),
            (
                "08/19/2022".to_string(),
                "12/19/2022".to_string(),
                dec!(5045.6257),
                dec!(5936.0274),
                dec!(3986.9048),
            ),
            (
                "05/02/2022".to_string(),
                "12/21/2022".to_string(),
                dec!(0.0),
                dec!(4013.65),
                dec!(2285.82733),
            ),
        ];

        let empty_trade_confirmations = vec![];
        let (detailed_sold_transactions, _) =
            reconstruct_sold_transactions(&parsed_sold_transactions, &add_missing_gl_quantity(&parsed_gains_and_losses, &parsed_sold_transactions), &empty_trade_confirmations)?;

        assert_eq!(
            detailed_sold_transactions,
            vec![
                (
                    "12/19/22".to_string(),
                    "12/21/22".to_string(),
                    "08/19/21".to_string(),
                    dec!(2711.0954),
                    dec!(4336.4874),
                    dec!(0.0),
                    Some("INTEL CORP".to_owned())
                ),
                (
                    "12/21/22".to_string(),
                    "12/23/22".to_string(),
                    "05/03/21".to_string(),
                    dec!(2046.61285),
                    dec!(0.0),
                    dec!(0.0),
                    Some("INTEL CORP".to_owned())
                ),
                (
                    "12/19/22".to_string(),
                    "12/21/22".to_string(),
                    "08/19/22".to_string(),
                    dec!(3986.9048),
                    dec!(5045.6257),
                    dec!(0.0),
                    Some("INTEL CORP".to_owned())
                ),
                (
                    "12/21/22".to_string(),
                    "12/23/22".to_string(),
                    "05/02/22".to_string(),
                    dec!(2285.82733),
                    dec!(0.0),
                    dec!(0.0),
                    Some("INTEL CORP".to_owned())
                ),
            ]
        );
        Ok(())
    }

    #[test]
    fn test_sold_transaction_reconstruction_no_gains_fail() {
        let parsed_sold_transactions: Vec<(String, String, Decimal, Decimal, Decimal, Option<String>)> = vec![
            (
                "06/01/21".to_string(),
                "06/03/21".to_string(),
                dec!(1.0),
                dec!(25.0),
                dec!(24.8),
                Some("INTEL CORP".to_owned()),
            ),
            (
                "03/01/21".to_string(),
                "03/03/21".to_string(),
                dec!(2.0),
                dec!(10.0),
                dec!(19.8),
                Some("INTEL CORP".to_owned()),
            ),
        ];

        let parsed_gains_and_losses: Vec<(String, String, Decimal, Decimal, Decimal)> = vec![];

        let empty_trade_confirmations = vec![];
        let result =
            reconstruct_sold_transactions(&parsed_sold_transactions, &add_missing_gl_quantity(&parsed_gains_and_losses, &parsed_sold_transactions), &empty_trade_confirmations);
        assert_eq!( result , Err("\n\nERROR: Sold transaction detected, but corressponding Gain&Losses document is missing. Please download Gain&Losses  XLSX document at:\n
            https://us.etrade.com/etx/sp/stockplan#/myAccount/gainsLosses\n\n".to_string()));
    }

    #[test]
    fn test_trade_confirmation_fees_increase_cost_basis_in_detailed_sold() -> Result<(), String> {
        let parsed_sold_transactions: Vec<(String, String, Decimal, Decimal, Decimal, Option<String>)> = vec![
            (
                "06/01/21".to_string(),
                "06/03/21".to_string(),
                dec!(1.0),
                dec!(25.0),
                dec!(24.8),
                Some("INTEL CORP".to_owned()),
            ),
        ];

        let parsed_gains_and_losses: Vec<(String, String, Decimal, Decimal, Decimal)> = vec![
            (
                "01/01/2019".to_string(),
                "06/01/2021".to_string(),
                dec!(10.0),
                dec!(10.0),
                dec!(24.8),
            ),
        ];

        // net amount should replace income, while commission+fee should be added to cost basis
        let trade_confirmations = vec![(
            "06/01/21".to_string(),
            "06/03/21".to_string(),
            1,
            Decimal::new(2510, 2),
            Decimal::new(2510, 2),
            Decimal::new(20, 2),
            Decimal::new(10, 2),
            Decimal::new(2480, 2),
        )];

        let (reconstructed, _warning) = reconstruct_sold_transactions(
            &parsed_sold_transactions,
            &add_missing_gl_quantity(&parsed_gains_and_losses, &parsed_sold_transactions),
            &trade_confirmations,
        )?;

        let mut dates: std::collections::HashMap<crate::Exchange, Option<(String, Decimal)>> =
            std::collections::HashMap::new();
        dates.insert(
            crate::Exchange::USD("06/01/21".to_owned()),
            Some(("05/31/21".to_owned(), dec!(4.0))),
        );
        dates.insert(
            crate::Exchange::USD("01/01/19".to_owned()),
            Some(("12/30/18".to_owned(), dec!(3.0))),
        );

        let detailed = create_detailed_sold_transactions(reconstructed, &dates)
            .map_err(|e| format!("Unable to create detailed sold transactions: {e}"))?;

        assert_eq!(detailed.len(), 1);
        assert!((detailed[0].income_us - dec!(24.8)).abs() < dec!(0.0001));
        assert!((detailed[0].cost_basis - dec!(10.3)).abs() < dec!(0.0001));

        Ok(())
    }

    #[test]
    fn test_trade_confirmation_matches_when_date_padding_differs() -> Result<(), String> {
        let parsed_sold_transactions: Vec<(String, String, Decimal, Decimal, Decimal, Option<String>)> = vec![
            (
                "2/12/25".to_string(),
                "2/13/25".to_string(),
                dec!(89.0),
                dec!(10.0),
                dec!(889.0),
                Some("INTEL CORP".to_owned()),
            ),
        ];

        let parsed_gains_and_losses: Vec<(String, String, Decimal, Decimal, Decimal)> = vec![
            (
                "01/01/2020".to_string(),
                "02/12/2025".to_string(),
                dec!(500.0),
                dec!(500.0),
                dec!(889.0),
            ),
        ];

        let trade_confirmations = vec![(
            "02/12/25".to_string(),
            "02/13/25".to_string(),
            89,
            Decimal::new(8920, 2),
            Decimal::new(8920, 2),
            Decimal::new(20, 2),
            Decimal::new(10, 2),
            Decimal::new(88900, 2),
        )];

        let result = reconstruct_sold_transactions(
            &parsed_sold_transactions,
            &add_missing_gl_quantity(&parsed_gains_and_losses, &parsed_sold_transactions),
            &trade_confirmations,
        )?;

        assert_eq!(result.0.len(), 1);
        assert!((result.0[0].3 - dec!(889.0)).abs() < dec!(0.0001));
        assert!((result.0[0].4 - dec!(500.3)).abs() < dec!(0.0001));
        Ok(())
    }

    #[test]
    fn test_reconstruction_empty_sold_transactions() -> Result<(), String> {
        let parsed_sold_transactions: Vec<(String, String, Decimal, Decimal, Decimal, Option<String>)> = vec![];
        let parsed_gains_and_losses: Vec<(String, String, Decimal, Decimal, Decimal)> = vec![];
        let trade_confirmations = vec![];

        let result = reconstruct_sold_transactions(
            &parsed_sold_transactions,
            &add_missing_gl_quantity(&parsed_gains_and_losses, &parsed_sold_transactions),
            &trade_confirmations,
        )?;

        assert_eq!(result.0.len(), 0);
        assert_eq!(result.1, None);
        Ok(())
    }

    #[test]
    fn test_reconstruction_quantity_zero_error() -> Result<(), String> {
        let parsed_sold_transactions: Vec<(String, String, Decimal, Decimal, Decimal, Option<String>)> = vec![
            (
                "1/2/25".to_string(),
                "1/3/25".to_string(),
                dec!(100.0),
                dec!(10.0),
                dec!(1000.0),
                Some("AAPL".to_owned()),
            ),
        ];
        let parsed_gains_and_losses: Vec<(String, String, Decimal, Decimal, Decimal, Decimal)> = vec![
            (
                "1/1/2020".to_string(),
                "1/2/2025".to_string(),
                dec!(500.0),
                dec!(500.0),
                dec!(1000.0),
                dec!(0.0), // zero quantity should fail
            ),
        ];
        let trade_confirmations = vec![];

        let result = reconstruct_sold_transactions(
            &parsed_sold_transactions,
            &parsed_gains_and_losses,
            &trade_confirmations,
        );

        assert!(result.is_err());
        assert!(result.unwrap_err().contains("quantity must be positive"));
        Ok(())
    }

    #[test]
    fn test_reconstruction_quantity_mismatch_error() -> Result<(), String> {
        let parsed_sold_transactions: Vec<(String, String, Decimal, Decimal, Decimal, Option<String>)> = vec![
            (
                "1/2/25".to_string(),
                "1/3/25".to_string(),
                dec!(100.0),  // sell 100 shares
                dec!(10.0),
                dec!(1000.0),
                Some("AAPL".to_owned()),
            ),
        ];
        let parsed_gains_and_losses: Vec<(String, String, Decimal, Decimal, Decimal, Decimal)> = vec![
            (
                "1/1/2020".to_string(),
                "1/2/2025".to_string(),
                dec!(500.0),
                dec!(500.0),
                dec!(1000.0),
                dec!(50.0),  // G&L only shows 50 shares - mismatch!
            ),
        ];
        let trade_confirmations = vec![];

        let result = reconstruct_sold_transactions(
            &parsed_sold_transactions,
            &parsed_gains_and_losses,
            &trade_confirmations,
        );

        assert!(result.is_err());
        let err_msg = result.unwrap_err();
        assert!(err_msg.contains("Same-day quantity mismatch") || err_msg.contains("Unable to allocate"));
        assert!(err_msg.contains("1/2/2025") || err_msg.contains("01/02/2025"));
        Ok(())
    }

    #[test]
    fn test_reconstruction_very_small_quantities() -> Result<(), String> {
        let parsed_sold_transactions: Vec<(String, String, Decimal, Decimal, Decimal, Option<String>)> = vec![
            (
                "3/15/25".to_string(),
                "3/17/25".to_string(),
                dec!(0.001),  // fractional shares
                dec!(50000.0),
                dec!(50.0),
                Some("AMZN".to_owned()),
            ),
        ];
        let parsed_gains_and_losses: Vec<(String, String, Decimal, Decimal, Decimal, Decimal)> = vec![
            (
                "6/1/2024".to_string(),
                "3/15/2025".to_string(),
                dec!(45.0),
                dec!(45.0),
                dec!(50.0),
                dec!(0.001),  // matching fractional quantity
            ),
        ];
        let trade_confirmations = vec![];

        let result = reconstruct_sold_transactions(
            &parsed_sold_transactions,
            &parsed_gains_and_losses,
            &trade_confirmations,
        )?;

        assert_eq!(result.0.len(), 1);
        assert!((result.0[0].3 - dec!(50.0)).abs() < dec!(0.01));
        Ok(())
    }

    #[test]
    fn test_reconstruction_decimal_precision_allocation() -> Result<(), String> {
        // Test that proportional allocation maintains precision with Decimal math
        // Total proceeds: 6172.80 + 6172.80 = 12345.60
        let parsed_sold_transactions: Vec<(String, String, Decimal, Decimal, Decimal, Option<String>)> = vec![
            (
                "5/10/25".to_string(),
                "5/12/25".to_string(),
                dec!(100.0),
                dec!(123.456),
                dec!(12345.60),  // This matches total G&L proceeds
                Some("GOOGL".to_owned()),
            ),
        ];
        let parsed_gains_and_losses: Vec<(String, String, Decimal, Decimal, Decimal, Decimal)> = vec![
            (
                "3/1/2024".to_string(),
                "5/10/2025".to_string(),
                dec!(6000.0),
                dec!(6000.0),
                dec!(6172.80),  // 50% of total
                dec!(50.0),
            ),
            (
                "4/1/2024".to_string(),
                "5/10/2025".to_string(),
                dec!(6173.0),
                dec!(6173.0),
                dec!(6172.80),  // 50% of total
                dec!(50.0),
            ),
        ];
        
        let trade_confirmations = vec![(
            "05/10/25".to_string(),
            "05/12/25".to_string(),
            100,
            Decimal::new(123456, 2),  // $123.456 per share
            Decimal::new(1234560, 2), // principal
            Decimal::new(2000, 2),    // $20 commission
            Decimal::new(1000, 2),    // $10 fee
            Decimal::new(1234560, 2), // net amount = total proceeds (simplified)
        )];

        let result = reconstruct_sold_transactions(
            &parsed_sold_transactions,
            &parsed_gains_and_losses,
            &trade_confirmations,
        )?;

        assert_eq!(result.0.len(), 2);
        // Each G&L row should get proportional allocation
        // Income should be 6172.80 (proportional net) for each
        assert!((result.0[0].3 - dec!(6172.80)).abs() < dec!(0.01));
        assert!((result.0[1].3 - dec!(6172.80)).abs() < dec!(0.01));
        Ok(())
    }

    #[test]
    fn test_reconstruction_multiple_symbols_same_day() -> Result<(), String> {
        // Note: Current design is day-scoped, not symbol-scoped
        // This tests that same-day matching works across different symbols
        // Important: PDF total proceeds MUST equal G&L total proceeds
        let parsed_sold_transactions: Vec<(String, String, Decimal, Decimal, Decimal, Option<String>)> = vec![
            (
                "6/1/25".to_string(),
                "6/3/25".to_string(),
                dec!(50.0),
                dec!(100.0),
                dec!(5000.0),  // AAPL: 50 shares * $100
                Some("AAPL".to_owned()),
            ),
            (
                "6/1/25".to_string(),
                "6/3/25".to_string(),
                dec!(50.0),
                dec!(100.0),  // same price
                dec!(5000.0),  // MSFT: 50 shares * $100
                Some("MSFT".to_owned()),
            ),
        ];
        // Total PDF proceeds: 5000 + 5000 = 10000
        let parsed_gains_and_losses: Vec<(String, String, Decimal, Decimal, Decimal, Decimal)> = vec![
            (
                "1/1/2024".to_string(),
                "6/1/2025".to_string(),
                dec!(1000.0),
                dec!(1000.0),
                dec!(3000.0),  // AAPL first lot
                dec!(25.0),
            ),
            (
                "2/1/2024".to_string(),
                "6/1/2025".to_string(),
                dec!(2000.0),
                dec!(2000.0),
                dec!(2000.0),  // AAPL second lot
                dec!(25.0),
            ),
            (
                "3/1/2024".to_string(),
                "6/1/2025".to_string(),
                dec!(5000.0),
                dec!(5000.0),
                dec!(5000.0),  // MSFT
                dec!(50.0),
            ),
        ];
        // Total G&L proceeds: 3000 + 2000 + 5000 = 10000 ✓ matches PDF

        let trade_confirmations = vec![];

        let result = reconstruct_sold_transactions(
            &parsed_sold_transactions,
            &parsed_gains_and_losses,
            &trade_confirmations,
        )?;

        assert_eq!(result.0.len(), 3);
        // Verify all 3 G&L rows are matched
        assert!(result.0.iter().all(|r| r.6.is_some()));
        // Check that proceeds were preserved
        assert!((result.0[0].3 - dec!(3000.0)).abs() < dec!(0.0001));
        assert!((result.0[1].3 - dec!(2000.0)).abs() < dec!(0.0001));
        assert!((result.0[2].3 - dec!(5000.0)).abs() < dec!(0.0001));
        Ok(())
    }

    #[test]
    fn test_reconstruction_allocation_cannot_be_satisfied() -> Result<(), String> {
        // Test infeasible knapsack: bought 10, trying to sell 100
        let parsed_sold_transactions: Vec<(String, String, Decimal, Decimal, Decimal, Option<String>)> = vec![
            (
                "7/5/25".to_string(),
                "7/7/25".to_string(),
                dec!(100.0),  // trying to sell 100
                dec!(50.0),
                dec!(5000.0),
                Some("TSLA".to_owned()),
            ),
        ];
        let parsed_gains_and_losses: Vec<(String, String, Decimal, Decimal, Decimal, Decimal)> = vec![
            (
                "1/15/2024".to_string(),
                "7/5/2025".to_string(),
                dec!(10.0),
                dec!(10.0),
                dec!(5000.0),
                dec!(10.0),  // only 10 shares bought - impossible to sell 100
            ),
        ];
        let trade_confirmations = vec![];

        let result = reconstruct_sold_transactions(
            &parsed_sold_transactions,
            &parsed_gains_and_losses,
            &trade_confirmations,
        );

        assert!(result.is_err());
        let err_msg = result.unwrap_err();
        // Either quantity mismatch or unable to allocate
        assert!(
            err_msg.contains("mismatch") || err_msg.contains("Unable to allocate"),
            "Expected error message to mention mismatch or allocation failure, got: {err_msg}"
        );
        Ok(())
    }

    #[test]
    fn test_reconstruction_date_normalization_consistency() -> Result<(), String> {
        // Ensure consistent date handling across different padding styles
        let parsed_sold_transactions: Vec<(String, String, Decimal, Decimal, Decimal, Option<String>)> = vec![
            ("8/9/25".to_string(), "8/10/25".to_string(), dec!(30.0), dec!(75.0), dec!(2250.0), Some("IBM".to_owned())),
            ("08/09/25".to_string(), "8/11/25".to_string(), dec!(20.0), dec!(75.0), dec!(1500.0), Some("IBM".to_owned())),
        ];
        let parsed_gains_and_losses: Vec<(String, String, Decimal, Decimal, Decimal, Decimal)> = vec![
            ("10/1/2023".to_string(), "8/9/2025".to_string(), dec!(100.0), dec!(100.0), dec!(2250.0), dec!(30.0)),
            ("11/1/2023".to_string(), "8/9/2025".to_string(), dec!(200.0), dec!(200.0), dec!(1500.0), dec!(20.0)),
        ];
        let trade_confirmations = vec![];

        let result = reconstruct_sold_transactions(
            &parsed_sold_transactions,
            &parsed_gains_and_losses,
            &trade_confirmations,
        )?;

        assert_eq!(result.0.len(), 2);
        // Both should match despite different padding in inputs
        assert!((result.0[0].3 - dec!(2250.0)).abs() < dec!(0.0001));
        assert!((result.0[1].3 - dec!(1500.0)).abs() < dec!(0.0001));
        Ok(())
    }

    #[test]
    fn test_reconstruction_missing_trade_confirmation_warning() -> Result<(), String> {
        // Verify warning is returned when no trade confirmations provided
        let parsed_sold_transactions: Vec<(String, String, Decimal, Decimal, Decimal, Option<String>)> = vec![
            (
                "9/1/25".to_string(),
                "9/3/25".to_string(),
                dec!(25.0),
                dec!(150.0),
                dec!(3750.0),
                Some("META".to_owned()),
            ),
        ];
        let parsed_gains_and_losses: Vec<(String, String, Decimal, Decimal, Decimal, Decimal)> = vec![
            (
                "4/1/2024".to_string(),
                "9/1/2025".to_string(),
                dec!(800.0),
                dec!(800.0),
                dec!(3750.0),
                dec!(25.0),
            ),
        ];
        let trade_confirmations = vec![];

        let result = reconstruct_sold_transactions(
            &parsed_sold_transactions,
            &parsed_gains_and_losses,
            &trade_confirmations,
        )?;

        assert_eq!(result.0.len(), 1);
        assert!(result.1.is_some());
        let warning_msg = result.1.unwrap();
        assert!(warning_msg.contains("Trade Confirmation"));
        assert!(warning_msg.contains("Fees"));
        Ok(())
    }
}


