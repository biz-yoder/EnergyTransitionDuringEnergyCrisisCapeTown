# Energy Transition During Energy Crisis -- Cape Town's Experience

## Overview
This repository contains the code for the paper "Inequality in Resilience: Understanding Household Electricity Consumption During Load Shedding," a project within the Energy Transition During Energy Crisis project. In this study, we integrate data from a variety of sources to understand how households with and without solar home systems (SHS) consume electricity during load shedding.

The codebase is designed to be modular and reproducible, and is organized as follows:
<img width="1536" height="720" alt="code_map" src="https://github.com/user-attachments/assets/78c7c5f1-42da-47d4-8669-1b672a1b5743" />

## Repository Structure

```
.
├── data/
│   ├── raw/                # Unmodified data inputs
│   ├── processed/          # Cleaned and analysis-ready datasets
│   └── README.md 
│
├── src/
│   ├── 1a_Import_old_data.py              # Imports transaction data in old format
│   ├── 1b_Create_monthly_new_data.py      # Imports and transforms data in new format
│   ├── 1c_Create_monthly_old_data.py      # Transformats data in new format
│   ├── 2a_ImportLocation.py               # Import contract locations
│   ├── 2b_Contract_with_location.py        # Merge contracts with location
│   ├── 3_ContractLocation_with_building.py # Assign contracts to building
│   ├── 4_Building_with_SHS.py              # Assign SHS to building
│   ├── 5a_Contract_with_SHS.py             # Assign contracts to SHS
│   ├── 5b_SHS_assumptions.py               # Implement SHS assumptions
│   ├── 5c_SSEGRegistration.py              # Clean SHS data based on registrations
│   ├── 6_Add_blocks.py                     # Add load shedding blocks to contracts
│   └── 7_Add_loadshed.py                   # Add load sheddinding data to contracts
│
├── notebooks/
│   ├── 
│   └── 
│
├── results/
│   ├── 
│   └── 
│
├── environment.yml / requirements.txt
├── README.md
└── LICENSE
```
