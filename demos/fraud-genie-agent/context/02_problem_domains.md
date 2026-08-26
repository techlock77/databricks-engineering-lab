# Financial Fraud Problem Domains

The agents should consider these domains, but may introduce better ones.

## 1. Money Mule Networks

Individuals or accounts receive and rapidly forward funds on behalf of fraud networks.

Interesting analytical characteristics:

- funnel accounts;
- many-to-one transfers;
- rapid onward movement;
- newly created accounts;
- repeated destination accounts;
- small transactions that individually look harmless.

## 2. Authorized Push Payment Scams

A victim is manipulated into authorizing a transfer.

Examples:

- impersonation;
- investment scams;
- romance scams;
- invoice redirection;
- urgent family / emergency scams.

Challenge:

The payment itself may appear authorized and technically legitimate.

## 3. Elder Financial Exploitation

Suspicious changes in transaction behavior affecting older or vulnerable customers.

Prototype focus should be on behavioral anomalies and account evidence rather than demographic stereotyping.

## 4. Account Takeover

Fraudsters gain access to a legitimate account.

Potential evidence:

- new device;
- new location;
- credential-reset event;
- unusual payee;
- rapid transfer sequence;
- changed authentication behavior.

## 5. Synthetic Identity Fraud

Fraudsters create identities using combinations of real and fabricated information.

Potential signals:

- shared devices;
- shared addresses;
- shared phone numbers;
- repeated identity attributes;
- coordinated account creation;
- credit-building behavior.

## 6. Card / Payment Fraud

Examples:

- card-not-present fraud;
- unusual merchant behavior;
- coordinated purchase patterns;
- device-level anomalies.

Avoid building only a generic transaction risk-score dashboard.

## 7. Fraud Rings

Multiple accounts, devices, merchants, or identities act together.

This is particularly suitable for Genie because the investigation can move from one event to a larger network.

## 8. Merchant Fraud

Potential issues:

- abnormal refund patterns;
- excessive chargebacks;
- card testing;
- transaction laundering;
- suspicious merchant-account relationships.

## 9. Cross-Border Transaction Abuse

Potential evidence:

- rapid movement across jurisdictions;
- unusual counterparties;
- account chains;
- inconsistent historical behavior.

## 10. Payment Operations Abuse

Possible application:

"Why are payments suddenly failing or being abused?"

This can blend fraud signals with operational anomalies.

## High-Priority Idea Characteristics

Prefer ideas where:

- individual transactions appear normal;
- relationships reveal the risk;
- time sequence matters;
- multiple datasets must be correlated;
- traditional threshold rules can miss the pattern;
- investigators benefit from conversational exploration.
