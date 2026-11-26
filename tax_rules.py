"""
Vantage · Fluid Tax — hardcoded base ruleset (India, direct tax).

Tax Year 2026-27 (1 Apr 2026 – 31 Mar 2027) under the Income-tax Act, 2025,
as amended by the Finance Act, 2026 and the Taxation and Other Laws (Amendment) Act, 2026.

This file is the BASE layer. Approved updates from the tax agent are stored in Firestore
(`tax_rules/live`) as an overlay and merged on top at runtime, so a rule change does not
need a code deploy. Every block carries:
  verified : True  = checked against a cited source when this file was written
             False = best current understanding, flagged for the agent to confirm
  src      : ids into SOURCES
  ref      : section references {"ita1961": "...", "ita2025": "..."}; ita2025 numbers are
             only filled where confirmed, otherwise left for the agent.

Amounts in INR. Rates in percent.
"""

BASE_VERSION = "2026.11.26d-base"

META = {
    "jurisdiction": "India",
    "tax_year": "2026-27",
    "period": {"from": "2026-04-01", "to": "2027-03-31"},
    "act": "Income-tax Act, 2025 (in force from 1 Apr 2026, replaces Income-tax Act, 1961)",
    "amended_by": ["Finance Act, 2026", "Taxation and Other Laws (Amendment) Act, 2026 (assent 17 Aug 2026)"],
    "transition_note": "Income earned up to 31 Mar 2026 (FY 2025-26 / AY 2026-27) is assessed under the 1961 Act; "
                       "'Tax Year' replaces previous year / assessment year from TY 2026-27.",
    "cess_pct": 4.0,
    "cess_note": "Health & Education Cess at 4% on income-tax plus surcharge, for all assessees.",
    "disclaimer": "Reference tool for advisor conversations. Not tax advice; confirm complex cases with a CA.",
    "verified": True,
    "src": ["itd_act2025", "bt_act2025", "taxguru_fb2026_rates"],
}

SOURCES = {
    "itd_act2025": {"title": "Income-tax Act, 2025 comes into force from 1 April 2026 (Income Tax Department)",
                    "url": "https://www.incometaxindia.gov.in/w/income-tax-act-2025-comes-into-force-from-1st-april-2026"},
    "bt_act2025": {"title": "Income-tax Act 2025 comes into force from April 1 (Business Today)",
                   "url": "https://www.businesstoday.in/personal-finance/tax/story/income-tax-act-2025-comes-into-force-from-april-1-marks-major-tax-reform-cbdt-523616-2026-04-01"},
    "taxguru_fb2026_rates": {"title": "Finance Bill 2026 retains rate structure for TY 2026-27 (TaxGuru)",
                             "url": "https://taxguru.in/?p=1022835"},
    "bt_slabs_2627": {"title": "Income tax slabs FY 2026-27 unchanged (Business Today)",
                      "url": "https://www.businesstoday.in/personal-finance/tax/story/income-tax-slabs-fy-2026-27-will-rates-change-from-april-1-under-new-tax-rules-details-523303-2026-04-01"},
    "tatacap_slabs": {"title": "Income tax slabs FY 2026-27 (Tata Capital)",
                      "url": "https://www.tatacapital.com/blog/wealth-services/personal-income-tax-slabs/"},
    "bankbazaar_slabs": {"title": "Income tax slabs and rates FY 2026-27 (BankBazaar)",
                         "url": "https://www.bankbazaar.com/tax/income-tax-slabs.html"},
    "itd_rates_huf": {"title": "Tax rates — HUF/AOP/BOI (Income Tax Department)",
                      "url": "https://incometaxindia.gov.in/Documents/Left%20Menu/TAX%20RATES_HUF.htm"},
    "itd_rates_domestic": {"title": "Tax rates — domestic company (Income Tax Department)",
                           "url": "https://incometaxindia.gov.in/Documents/Left%20Menu/TAX%20RATES-domestic.htm"},
    "aaptax_firm": {"title": "Income tax rates AY 2026-27 — firms/LLP (AAP Tax Law)",
                    "url": "https://aaptaxlaw.com/Income-tax/income-tax-rate-for-assessment-year-2026-27.html"},
    "inkl_mmr": {"title": "Maximum 39% marginal rate for TY 2026-27 (MMR cases)",
                 "url": "https://www.inkl.com/news/maximum-39-marginal-rate-of-income-tax-applicable-for-these-individuals-in-tax-year-2026-2027"},
    "bs_mat": {"title": "Budget 2026: MAT a final tax at 14% (Business Standard)",
               "url": "https://www.business-standard.com/budget/news/budget-2026-mat-final-tax-credit-setoff-new-regime-126020101235_1.html"},
    "ey_mat": {"title": "EY Global Tax News — India Union Budget 2026",
               "url": "https://globaltaxnews.ey.com/news/2026-0351"},
    "bs_ltcg_2026": {"title": "Budget 2026: LTCG rates unchanged (Business Standard)",
                     "url": "https://www.business-standard.com/budget/news/budget-2026-ltcg-tax-rates-unchanged-for-fy27-fm-sitharaman-126020100424_1.html"},
    "icici_reckoner_2627": {"title": "ICICI Prudential MF Tax Reckoner 2026-27",
                            "url": "https://www.icicipruamc.com/blob/knowledgecentre/tax_reckoner/Tax%20Reckoner%202026-27.pdf"},
    "edelweiss_reckoner": {"title": "Edelweiss MF Tax Reckoner (Jun 2026)",
                           "url": "https://www.edelweissmf.com/Files/MF/Downloads/TAXRECKONER/TAXRECKONER/EMF_Tax_Reckoner_25062026154659.pdf"},
    "finnovate_sif": {"title": "SIF taxation in India (Finnovate)",
                      "url": "https://www.finnovate.in/learn/blog/sif-taxation-india"},
    "bs_buyback": {"title": "Budget 2026: buyback proceeds taxed as capital gains (Business Standard)",
                   "url": "https://www.business-standard.com/budget/news/budget-2026-buyback-proceeds-to-be-treated-as-capital-gains-126020101050_1.html"},
    "dh_buyback": {"title": "Promoters pay additional buyback tax: 22% / 30% (Deccan Herald)",
                   "url": "https://www.deccanherald.com/business/union-budget/union-budget-2026-now-promoters-need-to-pay-additional-buyback-tax-3882125"},
    "fpj_stt": {"title": "Budget 2026 STT hike on F&O effective 1 Apr 2026 (Free Press Journal)",
                "url": "https://www.freepressjournal.in/analysis/union-budget-2026-triggers-sharp-market-sell-off-amid-derivatives-tax-hike"},
    "nism_sgb": {"title": "How Budget 2026 changes SGB taxation (NISM)",
                 "url": "https://www.nism.ac.in/how-budget-2026-changes-sovereign-gold-bond-sgb-taxation/"},
    "cleartax_sgb": {"title": "Capital gains tax on SGB from 1 April 2026 (ClearTax)",
                     "url": "https://cleartax.in/s/capital-gain-tax-on-sovereign-gold-bond-sgb"},
    "upstox_interest": {"title": "Budget 2026: interest deduction against dividend/MF income removed (Upstox)",
                        "url": "https://upstox.com/news/personal-finance/tax/how-will-budget-2026-announcements-impact-individual-taxpayers-from-now-on/article-188775"},
    "taxguru_budget2026": {"title": "Direct tax proposals Union Budget 2026 (TaxGuru)",
                           "url": "https://taxguru.in/income-tax/direct-tax-proposals-union-budget-2026-comprehensive-analysis.html"},
    "mondaq_reit": {"title": "REIT/InvIT tax reforms — TOLA 2026 (Shardul Amarchand Mangaldas via Mondaq)",
                    "url": "https://www.mondaq.com/taxing-the-trust-what-indias-reit-and-invit-tax-reforms-mean-for-investors/1839624"},
    "itd_aif": {"title": "Pass-through income — AIF (Income Tax Department)",
                "url": "https://www.incometaxindia.gov.in/e/reckoner-detail/29501/8298207"},
    "bs_nps": {"title": "NPS withdrawal norms: 80% lump sum, 60% tax-exempt (Business Standard)",
               "url": "https://www.business-standard.com/finance/personal-finance/nps-withdrawal-norms-fund-access-comes-with-onus-to-manage-longevity-risk-125122300130_1.html"},
    "itd_tds_2627": {"title": "TDS rates for tax year 2026-27 (Income Tax Department)",
                     "url": "https://www.incometaxindia.gov.in/w/tds-rates"},
    "taxguru_tds_2025act": {"title": "TDS rates & thresholds under Income-tax Act 2025, TY 2026-27 (TaxGuru)",
                            "url": "https://taxguru.in/income-tax/income-tax-act-2025-tds-rates-thresholds-compliance-ty-2026-27.html"},
    "bankbazaar_tds": {"title": "TDS rate chart FY 2026-27 (BankBazaar)",
                       "url": "https://www.bankbazaar.com/tax/tds-rates-chart.html"},
    "taxconcept_map": {"title": "Section code mapper ITA 1961 vs 2025 (TaxConcept)",
                       "url": "https://taxconcept.net/?p=141992"},
}

# --------------------------------------------------------------------------- slabs
SLABS = {
    "new_regime": {
        "label": "New regime (default)",
        "applies_to": ["individual", "huf", "aop", "boi", "ajp"],
        "ref": {"ita1961": "115BAC", "ita2025": "202"},
        "slabs": [[0, 400000, 0], [400000, 800000, 5], [800000, 1200000, 10], [1200000, 1600000, 15],
                  [1600000, 2000000, 20], [2000000, 2400000, 25], [2400000, None, 30]],
        "rebate": {"who": "resident_individual", "income_limit": 1200000, "max_rebate": 60000,
                   "not_on": ["special_rate_income (equity STCG/LTCG, other LTCG, VDA)"],
                   "marginal_relief": True},
        "standard_deduction": {"salary": 75000, "family_pension": 25000},
        "senior_citizen_slabs": False,
        "deductions_allowed": ["employer NPS 80CCD(2) up to 14% of salary", "standard deduction",
                               "Agniveer 80CCH", "family pension deduction"],
        "verified": True, "src": ["bt_slabs_2627", "tatacap_slabs", "taxconcept_map"],
    },
    "old_regime": {
        "label": "Old regime (opt-in)",
        "applies_to": ["individual", "huf", "aop", "boi", "ajp"],
        "slabs": {
            "below_60": [[0, 250000, 0], [250000, 500000, 5], [500000, 1000000, 20], [1000000, None, 30]],
            "senior_60_79": [[0, 300000, 0], [300000, 500000, 5], [500000, 1000000, 20], [1000000, None, 30]],
            "super_senior_80": [[0, 500000, 0], [500000, 1000000, 20], [1000000, None, 30]],
        },
        "senior_slabs_only_for": "resident individuals",
        "rebate": {"who": "resident_individual", "income_limit": 500000, "max_rebate": 12500},
        "standard_deduction": {"salary": 50000},
        "key_deductions": {"80C": 150000, "80CCD(1B)_nps": 50000, "80D_self_family": "25,000 (50,000 senior)",
                           "80TTA_savings_interest": 10000, "80TTB_senior_interest": 50000},
        "verified": True, "src": ["tatacap_slabs", "bankbazaar_slabs"],
    },
}

# --------------------------------------------------------------------------- surcharge
SURCHARGE = {
    "individual_like": {
        "applies_to": ["individual", "huf", "aop", "boi", "ajp"],
        "bands": [[5000000, 10], [10000000, 15], [20000000, 25], [50000000, 37]],
        "new_regime_cap": 25,
        "special_income_cap": 15,
        "special_income": ["equity STCG (111A)", "equity LTCG (112A)", "other LTCG (112)", "dividend income"],
        "aop_all_company_members_cap": 15,
        "marginal_relief": True,
        "verified": True, "src": ["bankbazaar_slabs", "itd_rates_huf"],
    },
    "firm_llp": {"bands": [[10000000, 12]], "marginal_relief": True, "verified": True, "src": ["aaptax_firm"]},
    "local_authority": {"bands": [[10000000, 12]], "marginal_relief": True, "verified": True, "src": ["aaptax_firm"]},
    "domestic_company_normal": {"bands": [[10000000, 7], [100000000, 12]], "marginal_relief": True,
                                "verified": True, "src": ["bankbazaar_slabs"]},
    "domestic_company_115BAA_BAB": {"flat": 10, "verified": True, "src": ["itd_rates_domestic", "bankbazaar_slabs"]},
    "foreign_company": {"bands": [[10000000, 2], [100000000, 5]], "verified": False, "src": []},
    "cooperative_normal": {"bands": [[10000000, 7], [100000000, 12]], "verified": False, "src": []},
    "cooperative_115BAD_BAE": {"flat": 10, "verified": False, "src": []},
}

# --------------------------------------------------------------------------- entities
ENTITIES = {
    "individual_resident": {
        "label": "Individual (resident)",
        "base": "slabs", "regimes": ["new_regime", "old_regime"], "default_regime": "new_regime",
        "surcharge": "individual_like", "rebate": True, "basic_exemption_vs_special_income": True,
        "ltcg_112A_exemption": 125000,
        "notes": ["Unutilised basic exemption can absorb equity STCG/LTCG (residents only).",
                  "Clubbing: income from assets gifted to spouse / minor child is taxed in the giver's hands."],
        "verified": True, "src": ["bt_slabs_2627"],
    },
    "individual_nri": {
        "label": "Individual (non-resident / NRI)",
        "base": "slabs", "regimes": ["new_regime", "old_regime"], "default_regime": "new_regime",
        "surcharge": "individual_like", "rebate": False, "basic_exemption_vs_special_income": False,
        "ltcg_112A_exemption": 125000,
        "notes": ["No 87A rebate; senior-citizen slabs (old regime) not available.",
                  "Basic exemption cannot be set against equity STCG/LTCG.",
                  "TDS on redemptions/sales at source — see TDS table; DTAA may lower rate (needs TRC + Form 10F).",
                  "NRE / FCNR interest exempt while non-resident; NRO interest taxable (TDS 30%+)."],
        "verified": True, "src": ["bankbazaar_slabs"],
    },
    "huf": {
        "label": "HUF",
        "base": "slabs", "regimes": ["new_regime", "old_regime"], "default_regime": "new_regime",
        "surcharge": "individual_like", "rebate": False, "basic_exemption_vs_special_income": True,
        "ltcg_112A_exemption": 125000,
        "notes": ["Separate PAN: its own slabs and its own ₹1.25 lakh equity LTCG exemption.",
                  "Old regime: 80C available; senior-citizen slabs not applicable.",
                  "Gifts from members/relatives to HUF are exempt; income from assets a member converts into "
                  "HUF property is clubbed with that member."],
        "verified": True, "src": ["itd_rates_huf"],
    },
    "aop_boi": {
        "label": "AOP / BOI",
        "base": "slabs_or_mmr", "regimes": ["new_regime", "old_regime"], "default_regime": "new_regime",
        "surcharge": "individual_like", "rebate": False,
        "rules": ["Members' shares determinate and no member above basic exemption → slab rates.",
                  "Shares indeterminate / unknown → maximum marginal rate (MMR) on total income.",
                  "If any member is taxable at a higher rate, that member's share can be taxed at the higher rate.",
                  "Surcharge capped at 15% where all members are companies."],
        "ref": {"ita1961": "167B"},
        "verified": True, "src": ["itd_rates_huf", "inkl_mmr"],
    },
    "trust_private_specific": {
        "label": "Private trust — specific (determinate beneficiaries)",
        "base": "beneficiary_rates",
        "rules": ["Trustee is a representative assessee; tax as it would be in each beneficiary's hands.",
                  "If the trust has business income → MMR (except a will-trust solely for a dependent relative)."],
        "ref": {"ita1961": "161"},
        "verified": True, "src": ["inkl_mmr"],
    },
    "trust_private_discretionary": {
        "label": "Private trust — discretionary (indeterminate shares)",
        "base": "mmr",
        "rules": ["Taxed at MMR on total income.",
                  "Exceptions taxed at normal rates: will-trusts where it is the only trust by the settlor, "
                  "trusts for dependent relatives, certain pension/provident trusts.",
                  "Oral trusts → MMR."],
        "ref": {"ita1961": "164 / 164A", "ita2025": "307 / 308"},
        "open_question": "Whether equity STCG/LTCG inside a discretionary trust is taxed at MMR or at the special "
                         "CG rates is contested — confirm with a CA before advising.",
        "verified": True, "src": ["inkl_mmr"],
    },
    "trust_charitable": {
        "label": "Charitable / religious trust (registered)",
        "base": "exempt_if_applied",
        "rules": ["Income exempt if registered and at least 85% applied to objects (accumulation allowed within limits).",
                  "Anonymous donations above limits taxed at 30%.",
                  "Violations / investments outside specified modes → income taxed at MMR.",
                  "Investments must be in specified modes (e.g. bank deposits, govt securities, certain MF units)."],
        "ref": {"ita1961": "11-13, 115BBC, 115BBI"},
        "verified": False, "src": [],
    },
    "firm_llp": {
        "label": "Firm / LLP — general rules",
        "base": "flat", "rate": 30, "surcharge": "firm_llp",
        "rules": ["Flat 30% on normal income; capital gains at their special rates.",
                  "Surcharge 12% above ₹1 crore (no 15% cap on capital gains).",
                  "Partner remuneration/interest deductible within limits; partner's share of profit exempt in partner's hands.",
                  "AMT 18.5% applies if claiming specified deductions."],
        "ltcg_112A_exemption": 125000,
        "verified": True, "src": ["aaptax_firm"],
    },
    "company_domestic": {
        "label": "Domestic company — rate options",
        "options": [
            {"id": "115BAA", "rate": 22, "surcharge": "domestic_company_115BAA_BAB", "mat": False,
             "effective_max": 25.168, "note": "No specified deductions/incentives."},
            {"id": "115BAB", "rate": 15, "surcharge": "domestic_company_115BAA_BAB", "mat": False,
             "effective_max": 17.16, "note": "New manufacturing companies (production commenced by 31 Mar 2024)."},
            {"id": "normal_small", "rate": 25, "surcharge": "domestic_company_normal", "mat": True,
             "note": "Turnover / gross receipts ≤ ₹400 crore in the specified base year (verify base year)."},
            {"id": "normal", "rate": 30, "surcharge": "domestic_company_normal", "mat": True},
        ],
        "mat": {"rate": 14, "final_tax": True, "credit": "No new MAT credit from TY 2026-27; companies moving to "
                "115BAA can set off MAT credit up to 31 Mar 2026, capped at 25% of tax liability."},
        "investment_notes": ["₹1.25 lakh 112A exemption applies to companies too.",
                             "Debt MF / bond gains taxed at the company rate (22% / 25% / 30%).",
                             "Dividends received are taxable; deduction for dividends further distributed (old 80M)."],
        "verified": True, "src": ["itd_rates_domestic", "bs_mat", "ey_mat"],
    },
    "company_foreign": {
        "label": "Foreign company",
        "base": "flat", "rate": 35, "surcharge": "foreign_company",
        "mat": {"rate": 14},
        "verified": False, "src": ["ey_mat"],
    },
    "cooperative_society": {
        "label": "Co-operative society",
        "options": [
            {"id": "normal", "slabs": [[0, 10000, 10], [10000, 20000, 20], [20000, None, 30]], "surcharge": "cooperative_normal"},
            {"id": "115BAD", "rate": 22, "surcharge": "cooperative_115BAD_BAE"},
            {"id": "115BAE", "rate": 15, "surcharge": "cooperative_115BAD_BAE", "note": "New manufacturing co-ops."},
        ],
        "verified": False, "src": [],
    },
}

MMR = {
    "description": "Maximum marginal rate = 30% plus highest applicable surcharge plus 4% cess.",
    "tax_year_2026_27_max_effective_pct": 39.0,
    "old_regime_max_effective_pct": 42.744,
    "verified": True, "src": ["inkl_mmr"],
}

# --------------------------------------------------------------------------- capital gains
# Rate 'slab' = taxed at the assessee's normal rate (slab for individuals, flat rate for firms/companies).
CAPITAL_GAINS = {
    "common": {
        "indexation": "Not available on any asset for transfers on/after 23 Jul 2024, except the option below.",
        "land_building_option": "Resident individual/HUF, land or building acquired before 23 Jul 2024: "
                                "LTCG = lower of 12.5% without indexation or 20% with indexation.",
        "ltcg_112A_exemption": 125000,
        "grandfathering_112A": "Equity/equity-MF bought before 1 Feb 2018: cost = higher of actual cost and "
                               "lower of (FMV on 31 Jan 2018, sale price).",
        "verified": True, "src": ["bs_ltcg_2026", "icici_reckoner_2627"],
    },
    "assets": [
        {"id": "listed_equity", "label": "Listed equity shares (STT paid)",
         "lt_months": 12, "stcg": 20, "ltcg": 12.5, "exemption_112A": True,
         "ref": {"ita1961": "111A / 112A", "ita2025": "196 / 198"},
         "verified": True, "src": ["bs_ltcg_2026", "taxconcept_map"]},
        {"id": "equity_mf", "label": "Equity-oriented MF (≥65% domestic equity) — incl. ELSS, index funds, equity ETFs, arbitrage, equity savings",
         "lt_months": 12, "stcg": 20, "ltcg": 12.5, "exemption_112A": True,
         "notes": ["ELSS has a 3-year lock-in.", "FoF investing ≥90% in domestic equity ETFs is equity-oriented."],
         "ref": {"ita1961": "111A / 112A", "ita2025": "196 / 198"},
         "verified": True, "src": ["icici_reckoner_2627", "edelweiss_reckoner"]},
        {"id": "debt_mf_post_apr2023", "label": "Specified MF (>65% debt & money market) bought on/after 1 Apr 2023 — debt, liquid, debt index, debt ETFs, debt FoFs",
         "lt_months": None, "stcg": "slab", "ltcg": None, "deemed_stcg": True,
         "notes": ["Always taxed at slab/normal rate, whatever the holding period."],
         "ref": {"ita1961": "50AA", "ita2025": "76"},
         "verified": True, "src": ["icici_reckoner_2627", "edelweiss_reckoner"]},
        {"id": "debt_mf_pre_apr2023", "label": "Specified MF (>65% debt) bought before 1 Apr 2023",
         "lt_months": 24, "lt_months_listed": 12, "stcg": "slab", "ltcg": 12.5,
         "notes": ["No indexation for transfers on/after 23 Jul 2024."],
         "verified": True, "src": ["icici_reckoner_2627", "edelweiss_reckoner"]},
        {"id": "other_mf", "label": "Other MFs — hybrid (35–65% equity), multi-asset, gold/silver FoF, international funds/FoF, FoF with <65% debt underlying",
         "lt_months": 24, "stcg": "slab", "ltcg": 12.5,
         "verified": True, "src": ["icici_reckoner_2627", "edelweiss_reckoner"]},
        {"id": "commodity_intl_etf", "label": "Gold / silver / international ETFs (listed, not debt)",
         "lt_months": 12, "stcg": "slab", "ltcg": 12.5,
         "verified": True, "src": ["icici_reckoner_2627", "edelweiss_reckoner"]},
        {"id": "sif", "label": "Specialised Investment Fund (SIF) units",
         "follows": "MF classification by portfolio: ≥65% domestic equity → equity_mf; >65% debt → specified MF; else other_mf",
         "verified": True, "src": ["finnovate_sif"]},
        {"id": "listed_bond", "label": "Listed bonds / debentures (not market-linked), listed zero-coupon bonds",
         "lt_months": 12, "stcg": "slab", "ltcg": 12.5,
         "verified": False, "src": []},
        {"id": "unlisted_bond", "label": "Unlisted bonds / debentures",
         "lt_months": None, "stcg": "slab", "ltcg": None, "deemed_stcg": True,
         "notes": ["Taxed at slab/normal rate regardless of holding for transfers on/after 23 Jul 2024."],
         "verified": False, "src": []},
        {"id": "mld", "label": "Market-linked debentures",
         "lt_months": None, "stcg": "slab", "ltcg": None, "deemed_stcg": True,
         "ref": {"ita1961": "50AA", "ita2025": "76"}, "verified": True, "src": ["icici_reckoner_2627"]},
        {"id": "sgb", "label": "Sovereign Gold Bonds",
         "lt_months": 12, "stcg": "slab", "ltcg": 12.5,
         "maturity_exemption": "Exempt only if subscribed by an individual at original issue and held continuously till maturity.",
         "notes": ["Bought on the exchange (secondary market): no exemption even at maturity — 12.5% LTCG if held >12 months.",
                   "Interest 2.5% p.a. taxable at slab.",
                   "Premature redemption with RBI: exemption now tied to holding till maturity — treat as taxable (verify)."],
         "ref": {"ita2025": "70(1)(x)"},
         "verified": True, "src": ["nism_sgb", "cleartax_sgb"]},
        {"id": "physical_gold", "label": "Physical gold / jewellery / silver",
         "lt_months": 24, "stcg": "slab", "ltcg": 12.5, "verified": True, "src": ["bs_ltcg_2026"]},
        {"id": "unlisted_equity", "label": "Unlisted shares (incl. foreign shares not listed in India, startup/pre-IPO)",
         "lt_months": 24, "stcg": "slab", "ltcg": 12.5, "verified": True, "src": ["bs_ltcg_2026"]},
        {"id": "real_estate", "label": "Land / building",
         "lt_months": 24, "stcg": "slab", "ltcg": 12.5,
         "notes": ["Residents (individual/HUF), acquired before 23 Jul 2024: option of 20% with indexation.",
                   "Reinvestment exemptions: house (54/54F, cap ₹10 crore), 54EC bonds (₹50 lakh, 5-year lock-in).",
                   "Budget 2026: resident buyer deducts TDS when buying from an NRI."],
         "verified": True, "src": ["bs_ltcg_2026", "bankbazaar_tds"]},
        {"id": "reit_invit_units", "label": "Listed REIT / InvIT units",
         "lt_months": 12, "stcg": 20, "ltcg": 12.5, "exemption_112A": True,
         "verified": True, "src": ["bs_ltcg_2026"]},
        {"id": "ulip_high_premium", "label": "ULIP with annual premium > ₹2.5 lakh (issued on/after 1 Feb 2021)",
         "follows": "Equity-oriented fund rules if ≥65% equity (12 months, 20% / 12.5%), else other assets",
         "notes": ["Death benefit always exempt.", "ULIPs within the ₹2.5 lakh limit: maturity exempt under 10(10D) conditions."],
         "verified": False, "src": []},
        {"id": "aif_units", "label": "AIF units (sale / redemption)",
         "lt_months": 24, "stcg": "slab", "ltcg": 12.5,
         "notes": ["Cat I/II: pass-through — income other than business income taxed in investor's hands by its nature; business income taxed at fund level.",
                   "Cat III: no pass-through — taxed at fund (trust) level, typically MMR on non-CG income."],
         "verified": True, "src": ["itd_aif"]},
        {"id": "pms", "label": "PMS (investor holds securities directly)",
         "follows": "Each underlying transaction taxed as if held directly (listed_equity, listed_bond, etc.)",
         "notes": ["Frequent churn creates STCG at 20%.", "Deductibility of PMS fees against capital gains is contested — treat as not deductible."],
         "verified": False, "src": []},
        {"id": "vda", "label": "Crypto / virtual digital assets",
         "flat": 30, "notes": ["Flat 30% + surcharge + cess regardless of holding period.", "Only cost deductible; losses cannot be set off or carried forward.",
                               "1% TDS on transfer."],
         "verified": False, "src": []},
        {"id": "buyback", "label": "Share buyback proceeds (from 1 Apr 2026)",
         "follows": "Capital gains (consideration − cost) by holding period and listing",
         "promoter_effective": {"corporate_promoter": 22, "non_corporate_promoter": 30},
         "notes": ["Replaces the 2024 rule that treated buyback proceeds as dividend."],
         "verified": True, "src": ["bs_buyback", "dh_buyback"]},
    ],
}

# --------------------------------------------------------------------------- income items
INCOME = [
    {"id": "dividend", "label": "Dividends (shares) / IDCW (mutual funds)",
     "tax": "slab", "notes": ["No deduction for interest on borrowings used to invest (from 1 Apr 2026; earlier capped at 20%).",
                              "Surcharge on dividends capped at 15% for individual-type assessees."],
     "verified": True, "src": ["upstox_interest", "taxguru_budget2026"]},
    {"id": "fd_interest", "label": "Bank / post office FD interest",
     "tax": "slab", "notes": ["Old regime: 80TTB up to ₹50,000 for senior citizens.", "Form 15G/15H to avoid TDS when no tax payable."],
     "verified": True, "src": ["taxguru_tds_2025act"]},
    {"id": "savings_interest", "label": "Savings account interest",
     "tax": "slab", "notes": ["Old regime: 80TTA up to ₹10,000 (non-seniors)."], "verified": True, "src": ["tatacap_slabs"]},
    {"id": "bond_interest", "label": "Taxable bond / debenture / NCD interest", "tax": "slab", "verified": True, "src": []},
    {"id": "tax_free_bond_interest", "label": "Tax-free bond interest (PSU tax-free bonds)", "tax": "exempt",
     "notes": ["Sale of the bond itself attracts capital gains."], "verified": True, "src": []},
    {"id": "sgb_interest", "label": "SGB interest (2.5% p.a.)", "tax": "slab", "verified": True, "src": ["cleartax_sgb"]},
    {"id": "ppf", "label": "PPF", "tax": "exempt", "notes": ["EEE. Old regime 80C up to ₹1.5 lakh."], "verified": True, "src": []},
    {"id": "epf", "label": "EPF / VPF", "tax": "exempt_with_limits",
     "notes": ["Interest on employee contributions above ₹2.5 lakh a year (₹5 lakh if no employer contribution) is taxable.",
               "Withdrawal before 5 years of continuous service is taxable."], "verified": False, "src": []},
    {"id": "nps", "label": "NPS (Tier I)", "tax": "partly_exempt",
     "notes": ["Exit: 60% lump sum exempt. PFRDA now allows up to 80% lump sum for non-government subscribers; the extra 20% may be taxable at slab unless the law changes.",
               "Annuity income taxable at slab.",
               "Employer contribution deductible: 14% of salary (new regime); old regime 10% (private) / 14% (govt).",
               "Own contribution 80CCD(1B) ₹50,000 — old regime only.",
               "Partial withdrawal up to 25% of own contributions exempt."],
     "verified": True, "src": ["bs_nps"]},
    {"id": "scss", "label": "Senior Citizens' Savings Scheme", "tax": "slab", "notes": ["Old regime: deposit qualifies for 80C."], "verified": True, "src": []},
    {"id": "nsc", "label": "NSC", "tax": "slab", "notes": ["Old regime: deposit and reinvested interest qualify for 80C."], "verified": True, "src": []},
    {"id": "life_insurance", "label": "Traditional life insurance maturity", "tax": "conditional",
     "notes": ["Exempt under 10(10D) if premium ≤10% of sum assured.",
               "Non-ULIP policies issued on/after 1 Apr 2023 with aggregate annual premium > ₹5 lakh: maturity proceeds minus premiums taxable.",
               "Death benefit always exempt."], "verified": False, "src": []},
    {"id": "reit_invit_distribution", "label": "REIT / InvIT distributions",
     "tax": "by_component",
     "components": {"dividend": "Exempt for unit holders, whether or not the SPV opted for the concessional regime (TOLA 2026, effective 1 Apr 2026).",
                    "interest": "Taxable at slab (TDS applies).",
                    "rent": "Taxable (REIT direct rental).",
                    "repayment_of_debt": "Taxable as residual income from other sources beyond cost reduction rules (since 2023 amendment) — verify specifics."},
     "verified": True, "src": ["mondaq_reit"]},
    {"id": "fno", "label": "Equity futures & options", "tax": "business_income",
     "notes": ["Non-speculative business income at slab/normal rate; expenses deductible; audit thresholds apply.",
               "STT from 1 Apr 2026: futures 0.05% (sell), options premium 0.15% (sell), options exercise 0.15%."],
     "verified": True, "src": ["fpj_stt"]},
    {"id": "intraday", "label": "Intraday equity", "tax": "speculative_business",
     "notes": ["Speculative business income; losses set off only against speculative income, carried forward 4 years."],
     "verified": True, "src": []},
    {"id": "gift", "label": "Gifts received", "tax": "conditional",
     "notes": ["From relatives (and to HUF from members), on marriage, by will/inheritance: exempt.",
               "From non-relatives above ₹50,000 in a year: taxable in full."], "verified": True, "src": []},
]

STT = {
    "equity_delivery": {"buy": 0.1, "sell": 0.1},
    "equity_intraday": {"sell": 0.025},
    "equity_mf_redemption": {"sell": 0.001},
    "futures": {"sell": 0.05, "effective": "2026-04-01", "previous": 0.02},
    "options_premium": {"sell": 0.15, "effective": "2026-04-01", "previous": 0.10},
    "options_exercise": {"rate": 0.15, "effective": "2026-04-01", "previous": 0.125},
    "verified": True, "src": ["fpj_stt"],
    "note": "Delivery, intraday and MF STT unchanged in Budget 2026 (verify MF redemption rate).",
}

# --------------------------------------------------------------------------- set-off
SET_OFF = {
    "rules": [
        "Short-term capital loss: set off against STCG and LTCG.",
        "Long-term capital loss: set off only against LTCG.",
        "Capital losses carried forward 8 years — only if the return is filed by the due date.",
        "The ₹1.25 lakh 112A exemption applies after set-off of losses.",
        "Residents (individual/HUF): unused basic exemption absorbs equity STCG/LTCG before tax.",
        "VDA (crypto) losses: no set-off, no carry forward.",
        "Speculative (intraday) loss: only against speculative income, 4 years.",
        "Non-speculative business loss (F&O): against any income except salary in the same year; carry forward 8 years against business income.",
        "House property loss: up to ₹2 lakh against other income (old regime); new regime disallows set-off against other heads.",
    ],
    "harvesting": "Booking up to ₹1.25 lakh of equity LTCG each year and reinvesting resets cost tax-free; "
                  "booking losses offsets gains in the same year. No wash-sale rule in India (GAAR caveat).",
    "verified": True, "src": ["bs_ltcg_2026"],
}

# --------------------------------------------------------------------------- TDS (TY 2026-27)
TDS = {
    "resident": [
        {"item": "Dividend (company)", "ita1961": "194", "ita2025": "393(1) T7", "rate": 10, "threshold": 10000,
         "verified": False, "note": "Threshold under the 2025 Act table to be confirmed (sources differ: ₹10,000 vs no limit).",
         "src": ["bankbazaar_tds", "taxguru_tds_2025act"]},
        {"item": "MF income / IDCW", "ita1961": "194K", "rate": 10, "threshold": 10000, "verified": False, "src": ["bankbazaar_tds"]},
        {"item": "Bank / PO interest", "ita1961": "194A", "ita2025": "393(1) T5(ii)", "rate": 10,
         "threshold": 50000, "threshold_senior": 100000, "verified": True, "src": ["taxguru_tds_2025act"]},
        {"item": "Other interest", "ita1961": "194A", "ita2025": "393(1) T5(iii)", "rate": 10, "threshold": 10000,
         "verified": True, "src": ["taxguru_tds_2025act"]},
        {"item": "Interest on securities", "ita1961": "193", "ita2025": "393(1) T5(i)", "rate": 10, "threshold": 10000,
         "verified": True, "src": ["taxguru_tds_2025act"]},
        {"item": "Equity / MF capital gains (resident)", "rate": 0, "note": "No TDS for residents; tax via advance tax / return.",
         "verified": True, "src": ["edelweiss_reckoner"]},
        {"item": "VDA transfer", "ita1961": "194S", "rate": 1, "verified": False, "src": []},
        {"item": "No PAN / inoperative PAN", "rate": 20, "note": "Higher of 20% or the section rate.", "verified": True, "src": []},
    ],
    "nri": [
        {"item": "Equity / equity MF STCG", "rate": 20, "plus": "surcharge + cess", "verified": False, "src": []},
        {"item": "Equity / equity MF LTCG (above ₹1.25 lakh)", "rate": 12.5, "plus": "surcharge + cess", "verified": False, "src": []},
        {"item": "Debt MF (specified) gains", "rate": 30, "plus": "surcharge + cess", "verified": False, "src": []},
        {"item": "Other LTCG (unlisted units, gold FoF, international FoF)", "rate": 12.5, "plus": "surcharge + cess", "verified": False, "src": []},
        {"item": "Dividend / MF income", "rate": 20, "note": "Or lower DTAA rate with TRC + Form 10F.",
         "verified": True, "src": ["taxguru_tds_2025act"]},
        {"item": "NRO interest", "rate": 30, "plus": "surcharge + cess", "verified": False, "src": []},
    ],
}

# --------------------------------------------------------------------------- recent changes (for talking points)
CHANGES_2026 = [
    {"what": "Income-tax Act, 2025 in force; 'Tax Year' replaces FY/AY; all section numbers renumbered.", "effective": "2026-04-01", "src": ["itd_act2025"]},
    {"what": "Slabs, LTCG 12.5% / STCG 20% and ₹1.25 lakh exemption unchanged.", "effective": "2026-04-01", "src": ["bs_ltcg_2026", "bt_slabs_2627"]},
    {"what": "Buybacks taxed as capital gains for all shareholders; promoters effective 22% (corporate) / 30% (non-corporate).", "effective": "2026-04-01", "src": ["bs_buyback", "dh_buyback"]},
    {"what": "SGB maturity exemption only for original individual subscribers holding till maturity; secondary-market SGBs taxable.", "effective": "2026-04-01", "src": ["nism_sgb", "cleartax_sgb"]},
    {"what": "No interest deduction against dividend / MF income (earlier up to 20%).", "effective": "2026-04-01", "src": ["upstox_interest"]},
    {"what": "STT up: futures 0.05%, options premium & exercise 0.15%. Arbitrage fund returns modestly lower.", "effective": "2026-04-01", "src": ["fpj_stt"]},
    {"what": "MAT cut to 14% and made a final tax; limited MAT-credit use on moving to the new corporate regime.", "effective": "2026-04-01", "src": ["bs_mat", "ey_mat"]},
    {"what": "REIT/InvIT dividends exempt for unit holders regardless of SPV's tax regime (TOLA 2026).", "effective": "2026-04-01", "src": ["mondaq_reit"]},
    {"what": "NPS: PFRDA allows up to 80% lump sum for non-govt subscribers; tax exemption still 60%.", "effective": "2025-12", "src": ["bs_nps"]},
]


ENTITY_GROUP = {
    "individual_resident": "Individuals", "individual_nri": "Individuals", "huf": "Family structures",
    "aop_boi": "AOP / BOI / others", "trust_private_specific": "Trusts", "trust_private_discretionary": "Trusts",
    "trust_charitable": "Charitable", "firm_llp": "Firms & LLPs", "company_domestic": "Companies",
    "company_foreign": "Companies", "cooperative_society": "AOP / BOI / others",
}
ENTITY_ENGINE = {
    "individual_resident": "individual_resident", "individual_nri": "individual_nri", "huf": "huf",
    "aop_boi": "aop_boi", "trust_private_specific": None, "trust_private_discretionary": "trust_private_discretionary",
    "trust_charitable": "charitable", "firm_llp": "firm_llp", "company_domestic": "company",
    "company_foreign": "company_foreign", "cooperative_society": "cooperative",
}


def base_ruleset() -> dict:
    import copy
    from tax_rules_ext import (SOURCES_EXT, RESIDENCY, PERSONS, EXTRA_ASSETS, ASSET_CATEGORY, EXTRA_INCOME,
                               INCOME_CATEGORY, ELIGIBILITY, STRUCTURING, CATEGORIES, DTAA, HOME_COUNTRY)
    entities = copy.deepcopy(ENTITIES)
    for k, e in entities.items():
        e.setdefault("group", ENTITY_GROUP.get(k, "Other"))
        e.setdefault("engine", ENTITY_ENGINE.get(k))
    entities.update(copy.deepcopy(PERSONS))
    cg = copy.deepcopy(CAPITAL_GAINS)
    for a in cg["assets"]:
        a.setdefault("cat", ASSET_CATEGORY.get(a["id"], "Other"))
    cg["assets"].extend(copy.deepcopy(EXTRA_ASSETS))
    income = copy.deepcopy(INCOME)
    for i in income:
        i.setdefault("cat", INCOME_CATEGORY.get(i["id"], "Other"))
    income.extend(copy.deepcopy(EXTRA_INCOME))
    return {
        "version": BASE_VERSION,
        "meta": META,
        "sources": {**SOURCES, **SOURCES_EXT},
        "residency": RESIDENCY,
        "slabs": SLABS,
        "surcharge": SURCHARGE,
        "entities": entities,
        "mmr": MMR,
        "capital_gains": cg,
        "income": income,
        "eligibility": ELIGIBILITY,
        "dtaa": DTAA,
        "home_country": HOME_COUNTRY,
        "structuring": STRUCTURING,
        "categories": CATEGORIES,
        "stt": STT,
        "set_off": SET_OFF,
        "tds": TDS,
        "changes_2026": CHANGES_2026,
    }

