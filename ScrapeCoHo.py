# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

import requests
import json
import numpy as np
import pandas as pd
import datetime
import collections as col
import time
import dateutil
import csv
import os.path

ind_tracker = 0

def try_key(d, key):
    defaultvalue = None
    try:
        return d[key]
    except:
        return defaultvalue

def convert_date(input_date):
    if input_date == None:
        converted_date = None
    elif input_date == '':
        converted_date = None
    else:
        converted_date = dateutil.parser.parse(input_date)
    return converted_date


def get_CH_data_main(company_number_list):

    global rate_limiting_period
    global rate_limiting_requests
    global header_input
    global facts_list
    global actions_list
    global director_list
    global API_Calls
    global time_tracker
    global ind_tracker
    header_input={'Authorization': 'Basic NEM4clhnY09SeFZPRmxITTQwZ0VPVkF0Y3JUY2R6UjdWNmloYllMdTo='}
    rate_limiting_period = 60 * 5 + 1 # build in 1s leniency
    rate_limiting_requests = 599
    API_Calls = 0
    

    actions_list = []
    #dir_list = []
    companies_run = 0 
    #header_input ={'Authorization': 'Basic A49iKp9YukFPhHAKW2MzLSMD6NoWo9pH9Fb1sL8B'}
    #header_input ={'Authorization': 'Basic NEM4clhnY09SeFZPRmxITTQwZ0VPVkF0Y3JUY2R6UjdWNmloYllMdTo='}
    #header_input ={'Authorization': 'Basic A49iKp9YukFPhHAKW2MzLSMD6NoWo9pH9Fb1sL8B='}

    # print company_number_list
    for company_number in company_number_list:
        print(str(ind_tracker) + ':' + company_number)
        # print API_calls
        companies_run = companies_run + 1
        dict_facts = {}
        facts_list = []
        director_list = []
        ind_tracker=ind_tracker+1

        if len(str(company_number)) == 1:
            company_number_str = "0000000" + str(company_number)
        elif len(str(company_number)) == 2:
            company_number_str = "000000" + str(company_number)
        elif len(str(company_number)) == 3:
            company_number_str = "00000" + str(company_number)
        elif len(str(company_number)) == 4:
            company_number_str = "0000" + str(company_number)
        elif len(str(company_number)) == 5:
            company_number_str = "000" + str(company_number)
        elif len(str(company_number)) == 6:
            company_number_str = "00" + str(company_number)
        elif len(str(company_number)) == 7:
            company_number_str = "0" + str(company_number)
        elif len(str(company_number)) == 8:
            company_number_str = str(company_number)
        else:
            print("Company number is %s" % company_number)
            raise ValueError('company number must be 8 digits long')

        time_tracker = time.time()
        # print 'calling company'
        dict_facts = call_company(company_number_str, dict_facts)
        if dict_facts is not None:
            # print 'calling filing'
            actions_list = call_filing_outer(company_number_str, actions_list)
            # print 'calling charges'
            dict_facts, actions_list = call_charges(company_number_str, dict_facts, actions_list)
            # print 'calling insolvency'
            dict_facts, actions_list = call_insolvency(company_number_str, dict_facts, actions_list)
            # print 'calling directors'
            actions_list, director_list = call_directors(company_number_str, actions_list, director_list, header_input)
            facts_list.append(dict_facts)
        
            #print(facts_list)
            #dir_list.append(director_list)
            #append to csv as and when data is available
            #directors
            file_exists = os.path.isfile('/Users/datascience11/Desktop/Data_Acc/Director_data.csv')
            if director_list != []:
                keys = director_list[0].keys()
                with open('/Users/datascience11/Desktop/Data_Acc/Director_data.csv', 'a') as output_file:
                    dict_writer = csv.DictWriter(output_file, keys)
                    if not file_exists:
                        dict_writer.writeheader()
                    dict_writer.writerows(director_list)
                
            #factlist
            file_exists = os.path.isfile('/Users/datascience11/Desktop/Data_Acc/comp_facts_data.csv')
            keys = facts_list[0].keys()
            with open('/Users/datascience11/Desktop/Data_Acc/comp_facts_data.csv', 'a') as output_file:
                dict_writer = csv.DictWriter(output_file, keys)
                if not file_exists:
                    dict_writer.writeheader()
                dict_writer.writerows(facts_list)
            
            #action_list
            file_exists = os.path.isfile('/Users/datascience11/Desktop/Data_Acc/comp_actions_data.csv')
            keys = ['company_number', 'filing_date', 'Action','accounts_made_up_to','director appointed date','charge_number']
            #actions_list[0].keys()
            with open('/Users/datascience11/Desktop/Data_Acc/comp_actions_data.csv', 'a') as output_file:
                dict_writer = csv.DictWriter(output_file, keys)
                if not file_exists:
                    dict_writer.writeheader()
                dict_writer.writerows(actions_list)

    return (facts_list, actions_list)

def rate_limiting_pause():
    global API_calls
    global time_tracker
    API_calls = API_calls + 1
    if API_calls >= rate_limiting_requests:
        API_calls = 0
        time.sleep(max(0,rate_limiting_period-(time.time()-time_tracker)))
        time_tracker=time.time()
    return;

def call_company(company_number, dict_facts):
    #print("I am call_company")
    #header_input ={'Authorization': 'Basic A49iKp9YukFPhHAKW2MzLSMD6NoWo9pH9Fb1sL8B'}
    #header_input ={'Authorization': 'Basic NEM4clhnY09SeFZPRmxITTQwZ0VPVkF0Y3JUY2R6UjdWNmloYllMdTo='}
    company_address_template="https://api.companieshouse.gov.uk/company/{}"
    company_url = company_address_template.format(company_number)

    # how many API calls have I done?
    rate_limiting_pause()
    company_data  = json.loads(requests.get(company_url, headers = header_input).text)
    #print(company_data.keys())
    if "errors" in company_data.keys():
        print("company does not exist!")
        print(company_data)
        return
    #API_calls = API_calls + 1
    
    dict_facts["company_number"] = company_number
    dict_facts["company_name"] = try_key(company_data, "company_name")
    dict_facts["company_status"] = try_key(company_data, "company_status")
    dict_facts["jurisdiction"] = try_key(company_data, "jurisdiction")
    dict_facts["company_type"] = try_key(company_data, "type")  
    dict_facts["cessation_date"] = convert_date(try_key(company_data, "date_of_cessation"))    
    dict_facts["creation_date"] = convert_date(try_key(company_data, "date_of_creation"))
    dict_facts["has_been_liquidated"] = try_key(company_data, "has_been_liquidated")
    
    if "registered_office_address" in company_data:
        dict_facts["address_line_1"] = try_key(company_data["registered_office_address"], "address_line_1")
        dict_facts["address_line_2"] = try_key(company_data["registered_office_address"], "address_line_2")
        dict_facts["care_of"] = try_key(company_data["registered_office_address"], "care_of")
        dict_facts["locality"] = try_key(company_data["registered_office_address"], "locality")
        dict_facts["postal_code"] = try_key(company_data["registered_office_address"], "postal_code")
        dict_facts["care_of"] = try_key(company_data["registered_office_address"], "region")

    #what is the max number of sic codes we have?    
    if "sic_codes" in company_data:
        dict_facts["sic_code"] = company_data["sic_codes"][0]
        dict_facts["num_sic_codes"] = len(company_data["sic_codes"])

    if "accounts" in company_data:

        dict_facts["next_accounts_due"] = convert_date(try_key(company_data["accounts"], "next_due"))
        dict_facts["next_made_up_to"] = convert_date(try_key(company_data["accounts"], "next_made_up_to"))
        dict_facts["accounts_overdue"] = try_key(company_data["accounts"], "overdue")
        
        if "last_accounts" in company_data["accounts"]:
            
            dict_facts["last_accounts_made_up_to"] = convert_date(try_key(company_data["accounts"]["last_accounts"], "made_up_to"))
            dict_facts["last_accounts_type"] = try_key(company_data["accounts"]["last_accounts"], "type")

    if "annual_return" in company_data:

        dict_facts["last_return_made_up_to"] = convert_date(try_key(company_data["annual_return"], "last_made_up_to"))
        dict_facts["next_return_due"] = convert_date(try_key(company_data["annual_return"], "next_due"))
        dict_facts["next_return_made_up_to"] = convert_date(try_key(company_data["annual_return"], "next_made_up_to"))
        dict_facts["return_overdue"] = try_key(company_data["annual_return"], "overdue")

    return (dict_facts)
    
def call_filing_outer(company_number, actions_list):
    attempt_no = 0
    redo_ind = 1
    while redo_ind == 1:
        actions_list, redo_ind = call_filing(company_number, actions_list, attempt_no)
        attempt_no = attempt_no + 1

    return actions_list

def call_filing(company_number, actions_list, attempt_no):
        
    lookup_start = attempt_no*100
    redo_ind = 1
    #header_input ={'Authorization': 'Basic A49iKp9YukFPhHAKW2MzLSMD6NoWo9pH9Fb1sL8B'}
    #header_input ={'Authorization': 'Basic NEM4clhnY09SeFZPRmxITTQwZ0VPVkF0Y3JUY2R6UjdWNmloYllMdTo='}
    filing_address_template="https://api.companieshouse.gov.uk/company/{}/filing-history?items_per_page=100&start_index={}"
    filing_url = filing_address_template.format(company_number, lookup_start)
    
    # how many API calls have I done?
    rate_limiting_pause()
    filing_data = json.loads(requests.get(filing_url, headers = header_input).text)
    
    
    
        



    num_accounts_posted = 0

    #print(filing_data)

    if "items" in filing_data:
        for item in filing_data["items"]:
            if "category" in item:
                if item["category"]=="accounts":
                    num_accounts_posted = num_accounts_posted + 1
                    if "description_values" in item:
                        if "made_up_date" in item["description_values"]:
                            actions_list.append({"company_number": company_number, "filing_date": convert_date(item["date"]), "accounts_made_up_to": convert_date(item["description_values"]["made_up_date"]), "Action": "Accounts Posted"})
                        else:
                            actions_list.append({"company_number": company_number, "filing_date": convert_date(item["date"]), "Action": "Accounts Posted"})
                    else:
                        actions_list.append({"company_number": company_number, "filing_date": convert_date(item["date"]), "Action": "Accounts Posted"})
            # think that this is just the format for old resignations
            if "description_values" in item:
                
                if "description" in item["description_values"]:
                    if item["description_values"]["description"] == "Director resigned":
                        actions_list.append({"company_number": company_number, "filing_date": convert_date(item["date"]), "Action": "Director resigned"})
                    elif item["description_values"]["description"] == "Secretary resigned":
                        actions_list.append({"company_number": company_number, "filing_date": convert_date(item["date"]), "Action": "Secretary resigned"})

            if "description" in item:
                if item["description"][:20] =="termination-director" or item["description"][:27] =="termination-person-director": 
                    actions_list.append({"company_number": company_number, "filing_date": convert_date(item["date"]), "Action": "Director resigned"})
                elif item["description"][:21] =="termination-secretary" or item["description"][:28] =="termination-person-secretary ": 
                    actions_list.append({"company_number": company_number, "filing_date": convert_date(item["date"]), "Action": "Secretary resigned"})
    if len(filing_data["items"]) < 100:

        redo_ind = 0
        
    return (actions_list, redo_ind)

def get_insolv_info(company_number, header_input, actions_list, original_company_number, appointed_date):
    # This just says when a company director's other company has gone insolvent
#     header_input ={'Authorization': 'Basic NEM4clhnY09SeFZPRmxITTQwZ0VPVkF0Y3JUY2R6UjdWNmloYllMdTo='}
    insolv_address_template="https://api.companieshouse.gov.uk/company/{}/insolvency"
    insolv_url = insolv_address_template.format(company_number)
    
    # how many API calls have I done?
    rate_limiting_pause()
    insolvency_data = json.loads(requests.get(insolv_url, headers = header_input).text)
    #API_calls = API_calls + 1
        
    if insolvency_data != None and "errors" not in insolvency_data and "cases" in insolvency_data:  
        # print 'you have not errored'
       
        first_date = datetime.datetime(2099, 12, 31, 0, 0)
        
        for cases in insolvency_data["cases"]:
            insolvency_type = cases["type"]
            if len(cases["dates"]) > 0:
                this_first_date = convert_date(cases["dates"][0]["date"])
            else:
                this_first_date = datetime.datetime(2099, 12, 31, 0, 0)

            # this is for all the dates in the case
            if len(cases["dates"]) > 1:
                # print first_date
                for dates in cases["dates"][1:]:
                    date_cand = convert_date(dates["date"])
                    if date_cand < this_first_date:
                        this_first_date = date_cand
            
            if this_first_date < first_date:
                
                first_date = this_first_date
        
        actions_list.append({"company_number": original_company_number, "director appointed date": appointed_date, "filing_date": first_date, "Action": "Director previous company insolvent"})
    
    else:
        # print 'you have errored'
        print("Company number %s has no insolvency cases " % company_number)
        # raise ValueError('Error: The insolvent company has no insolvency case')

    return (actions_list)

def call_directors(company_number, actions_list, director_list, header_input):

    officer_address_template="https://api.companieshouse.gov.uk/company/{}/officers?items_per_page=100"
    officer_url = officer_address_template.format(company_number)

 
 
    # actually make the call
        # how many API calls have I done?
    rate_limiting_pause()
    officer_data = json.loads(requests.get(officer_url, headers = header_input).text)
    #API_calls = API_calls + 1
    
    if officer_data != {}:
        for i in range(0,officer_data["total_results"]):
            director={}
            #print(i)
            #print(try_key(officer_data["items"][i],"name"))
            director["CRN"]=company_number
            director["name"]=try_key(officer_data["items"][i],"name")
            director["resigned_on"] = try_key(officer_data["items"][i], "resigned_on")
            director["appointed_on"] = try_key(officer_data["items"][i], "appointed_on")
            director["officer_role"] = try_key(officer_data["items"][i], "officer_role")
            if "address" in officer_data["items"][i].keys():
                director["address_line_1"] = try_key(officer_data["items"][i]["address"], "address_line_1")
                director["address_line_2"] = try_key(officer_data["items"][i]["address"], "address_line_2")
                director["country"] = try_key(officer_data["items"][i]["address"], "country")
                director["locality"] = try_key(officer_data["items"][i]["address"], "locality")
                director["postal_code"] = try_key(officer_data["items"][i]["address"], "postal_code")
                director["premises"] = try_key(officer_data["items"][i]["address"], "premises")
                director["region"] = try_key(officer_data["items"][i]["address"], "region")
                director["country_of_residence"] = try_key(officer_data["items"][i], "country_of_residence")
            #there are other address fields but this should be enough    
            else:
                director["address_line_1"] = None
                director["address_line_2"] = None
                director["country"] = None
                director["locality"] = None
                director["postal_code"] = None
                director["premises"] = None
                director["region"] = None
                director["country_of_residence"] = None
            
            if "date_of_birth" in officer_data["items"][i].keys():
                director["month_of_birth"] = try_key(officer_data["items"][i]["date_of_birth"],"month")
                director["year_of_birth"] = try_key(officer_data["items"][i]["date_of_birth"],"year")
            else:
                director["month_of_birth"] = None
                director["year_of_birth"] = None
            
            director["nationality"] = try_key(officer_data["items"][i], "nationality")
            director["occupation"] = try_key(officer_data["items"][i], "occupation")
    
        #print(director)
            director_list.append(director)

        for officer in officer_data["items"][0:officer_data["active_count"]]:
    #     for all directors that are active (should be just the first that are active)
            if "resigned_on" in officer:
                print(company_number)
                print(officer["name"]) 
                raise ValueError('Error: The active company director is no longer active')

            if officer["officer_role"] == "director":
    #         get list of appointments
                appoint_link = officer["links"]["officer"]["appointments"]
                appoint_address_template = "https://api.companieshouse.gov.uk{}/?items_per_page=100"
                appoint_url = appoint_address_template.format(appoint_link)
            # print appoint_url
                # how many API calls have I done?
                rate_limiting_pause()
                appoint_data = json.loads(requests.get(appoint_url, headers = header_input).text)
            #API_calls = API_calls + 1
        
                appointed_date = try_key(officer,"appointed_on")
            # print appoint_data
            # for all the director's appointments
                if 'items' in appoint_data:
                    for appointment in appoint_data["items"]:
                #         only where they were directors
                        if appointment["officer_role"]== "director": 
                    #         not the same company
                                if appointment["appointed_to"]["company_number"] != company_number:
#         if that company went insolvent
                                    if "company_status" in appointment["appointed_to"]:
                                        if appointment["appointed_to"]["company_status"] == "liquidation":
                                    # print officer["name"]
                                            insol_company_number = appointment["appointed_to"]["company_number"]
                                    # print insol_company_number
                                            actions_list = get_insolv_info(insol_company_number, header_input, actions_list, company_number, appointed_date)

    return (actions_list, director_list)
        
def call_charges(company_number, dict_facts, actions_list):

    # Some compnies have > 1 page of charges, which won't all be captured here, but since we will be them, this shouldn't matter

    #header_input ={'Authorization': 'Basic A49iKp9YukFPhHAKW2MzLSMD6NoWo9pH9Fb1sL8B'}
    #header_input ={'Authorization': 'Basic NEM4clhnY09SeFZPRmxITTQwZ0VPVkF0Y3JUY2R6UjdWNmloYllMdTo='}
    charge_address_template="https://api.companieshouse.gov.uk/company/{}/charges"
    charge_url = charge_address_template.format(company_number)

 
    # how many API calls have I done?
    rate_limiting_pause()
    charge_data = json.loads(requests.get(charge_url, headers = header_input).text)
    #API_calls = API_calls + 1


    dict_facts["num_charges"] = try_key(charge_data, "total_count")
    dict_facts["num_part_satisfied_charges"] = try_key(charge_data, "part_satisfied_count")
    dict_facts["num_satisfied_charges"] = try_key(charge_data, "satisfied_count")
        
    if charge_data != {}:
        for charges in charge_data["items"]:
            if "created_on" in charges:
                actions_list.append({"company_number": company_number, "charge_number":charges["charge_number"], "filing_date": convert_date(charges["created_on"]), "Action": "Charge Raised"})
        
            if charges["status"] == "fully-satisfied":
                    if "satisfied_on" in charges:
                        actions_list.append({"company_number": company_number, "charge_number":charges["charge_number"], "filing_date": convert_date(charges["satisfied_on"]), "Action": "Charge fully satisfied"})
                    else:
                        # should do something about this - unknown satisfied date, so assume it's far in the past
                         actions_list.append({"company_number": company_number, "charge_number":charges["charge_number"], "filing_date": convert_date(charges["created_on"]), "Action": "Charge fully satisfied"})        
            if charges["status"] == "part-satisfied":
                    actions_list.append({"company_number": company_number, "charge_number":charges["charge_number"], "filing_date": convert_date(charges["transactions"][0]['delivered_on']), "Action": "Charge part satisfied"})

    return (dict_facts, actions_list)




def call_insolvency(company_number, dict_facts, actions_list):

    #header_input ={'Authorization': 'Basic A49iKp9YukFPhHAKW2MzLSMD6NoWo9pH9Fb1sL8B'}
    #header_input ={'Authorization': 'Basic NEM4clhnY09SeFZPRmxITTQwZ0VPVkF0Y3JUY2R6UjdWNmloYllMdTo='}
    insolv_address_template="https://api.companieshouse.gov.uk/company/{}/insolvency"
    insolv_url = insolv_address_template.format(company_number)

         # how many API calls have I done?
    rate_limiting_pause()
    insolvency_data = json.loads(requests.get(insolv_url, headers = header_input).text)
    #API_calls = API_calls + 1

    if insolvency_data != None and "errors" not in insolvency_data and "cases" in insolvency_data:  
        
        dict_facts["num_insolvency_cases"] = len(insolvency_data["cases"])

        for cases in insolvency_data["cases"]:
            insolvency_type = cases["type"]
            if len(cases["dates"]) > 0:
                first_date = convert_date(cases["dates"][0]["date"])
            else:
                first_date = datetime.datetime(2099, 12, 31, 0, 0)

            # this is for all the dates in the case
            if len(cases["dates"]) > 1:
                # print first_date
                for dates in cases["dates"][1:]:
                    date_cand = convert_date(dates["date"])
                    if date_cand < first_date:
                        first_date = date_cand

            if insolvency_type == "members-voluntary-liquidation": 
                actions_list.append({"company_number": company_number, "filing_date": first_date, "Action": "Members voluntary insolvency"})
            elif insolvency_type == "creditors-voluntary-liquidation":
                actions_list.append({"company_number": company_number, "filing_date": first_date, "Action": "Creditors voluntary insolvency"})
            elif insolvency_type == "compulsory-liquidation":
                actions_list.append({"company_number": company_number, "filing_date": first_date, "Action": "Compulsory liquidation"})
            elif insolvency_type == "corporate-voluntary-arrangement":
                actions_list.append({"company_number": company_number, "filing_date": first_date, "Action": "Corporate voluntary arrangement"})
            else:    
                actions_list.append({"company_number": company_number, "filing_date": first_date, "Action": "Other insolvency type"})

    else:
        dict_facts["num_insolvency_cases"] = 0

    return (dict_facts, actions_list)



#CRNs=["00445790","00004606"]

#get_CH_data_main(CRNs)
get_CH_data_main(CRN1)