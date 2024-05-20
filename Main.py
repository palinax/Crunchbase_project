from Load import load_data, rename_columns
from Transform import (process_dates, calculate_age, filter_and_select, merge_dataframes, add_new_columns)
from Utility import EU_COUNTRY_CODES

def main():
    # Load data
    organizations = load_data('input/organizations.csv')
    rounds = load_data('input/funding_rounds.csv')
    investments = load_data('input/investments.csv')

    # Rename columns
    org_rename = {'uuid':'org_uuid', 'name':'company_name'}
    round_rename = {'uuid':'funding_round_uuid'}
    investments_rename = {'name':'round_name'}
    organizations = rename_columns(organizations, org_rename)
    rounds = rename_columns(rounds, round_rename)
    investments = rename_columns(investments, investments_rename)

    # Process data
    organizations = process_dates(organizations, 'founded_on')
    organizations = calculate_age(organizations, 'founded_on')
    condition = f"country_code.isin(@EU_COUNTRY_CODES) & age <= 5 & primary_role == 'company'"
    selected_columns = ['org_uuid', 'company_name', 'status', 'country_code', 'city', 'short_description', 'category_groups_list', 'num_funding_rounds', 'total_funding_usd', 'founded_on', 'last_funding_on', 'employee_count', 'num_exits', 'age']
    startups = filter_and_select(organizations, condition, selected_columns)

    # Merge with rounds
    startups = merge_dataframes(startups, rounds[['funding_round_uuid', 'org_uuid', 'investment_type', 'announced_on', 'raised_amount_usd', 'post_money_valuation_usd', 'investor_count', 'lead_investor_uuids']], 'org_uuid')

    # Add columns for has_a_lead_investor and first_category
    startups = add_new_columns(startups, 'country_code', 'category_groups_list')

    # Merge investments DataFrame
    investments = merge_dataframes(investments, startups, 'funding_round_uuid')
    investments = add_new_columns(investments, 'country_code', 'category_groups_list')


    # Select specified columns for the investments table
    investments = investments[[
        "uuid", "round_name", "funding_round_uuid", "funding_round_name", 
        "investor_uuid", "investor_name", "investor_type", "is_lead_investor", 
        "org_uuid", "company_name", "status", "country_code", "city", 
        "short_description", "category_groups_list", "num_funding_rounds", 
        "total_funding_usd", "founded_on", "last_funding_on", "employee_count", 
        "num_exits", "age", "investment_type", "announced_on", "raised_amount_usd", 
        "post_money_valuation_usd", "investor_count", "lead_investor_uuids", 
        "has_a_lead_investor", "first_category", "country"
    ]]

    # Output the data
    startups.to_csv('output/startups.csv', index=False)
    investments.to_csv('output/investments.csv', index=False)

if __name__ == "__main__":
    main()