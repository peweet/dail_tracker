import json
from flatten_json import flatten
import pandas as pd
import os
members_json_path = "members/members.json"

drop_cols = [
            'member_memberships_0_membership_represents_0_represent_representType',
            'member_uri', 
            'member_wikiTitle', 
            'member_memberships_0_membership_committees_0_uri',
            'member_memberships_0_membership_committees_0_houseNo',
            'member_memberships_0_membership_committees_0_houseCode',
            'member_memberships_0_membership_committees_2_uri',
            'member_memberships_0_membership_committees_1_houseNo',
            'member_memberships_0_membership_represents_0_represent_uri',
            'member_memberships_0_membership_uri',
            'member_memberships_0_membership_house_uri',
            'member_memberships_0_membership_committees_3_uri',
            'member_memberships_0_membership_committees_4_uri',
            'member_memberships_0_membership_parties_1_party_uri',
            'member_memberships_0_membership_parties_2_party_uri',
            'member_memberships_0_membership_parties_2_party_uri',
            'member_memberships_0_membership_committees_5_uri',
            'member_memberships_0_membership_committees_11_uri',
            'member_memberships_0_membership_committees_1_uri',
            'member_memberships_0_membership_committees_2_role',
            'member_memberships_0_membership_committees_4_role',
            'member_memberships_0_membership_committees_5_role',
            'member_memberships_0_membership_committees_6_uri',
            'member_memberships_0_membership_committees_7_uri',
            'member_memberships_0_membership_committees_8_uri',
            'member_memberships_0_membership_offices_5_office_officeName_uri',
            'member_memberships_0_membership_offices_4_office_officeName_uri',
            'member_memberships_0_membership_committees_9_uri',
            'member_memberships_0_membership_committees_10_uri',
            'member_image',
            'member_memberships_0_membership_committees_3_role',
            'member_memberships_0_membership_committees_1_houseCode',
            'member_memberships_0_membership_committees_1_role',
            'member_memberships_0_membership_committees_0_role',
            'member_showAs',
            'member_memberships_0_membership_dateRange_start',
            'member_memberships_0_membership_offices',
            'member_memberships_0_membership_parties_0_party_dateRange_start',
            'member_pId',
            'member_memberships_0_membership_committees',
            'member_gender',
            'member_memberships_0_membership_parties_0_party_uri',
            'member_memberships_0_membership_parties_0_party_partyCode',
            'member_memberships_0_membership_parties_0_party_dateRange_end'
            ]
rename = {
        'member_memberCode': 'unique_member_code',
        'member_memberships_0_membership_committees_0_mainStatus': 'committee_role',
        'member_memberships_0_membership_parties_0_party_showAs': 'party',
        # 'member_memberships_0_membership_committees_3_role': 'committee_role_"_title',
        'member_memberships_0_membership_committees_1_role_title': 'committee_role_1_title',
        'member_memberships_0_membership_represents_0_represent_representCode' : 'member_constituency',
        'member_memberships_0_membership_represents_0_represent_showAs': 'member_constiuency_no_dash',
        # 'member_memberships_0_membership_committees_0_committeeName_1_nameEn': 'committee_1_name_english_alt_CHECK',
        'member_memberships_0_membership_house_showAs' : 'dail_term',
        'member_memberships_0_membership_offices_0_office_officeName_showAs': 'ministerial_office',
        'member_memberships_0_membership_committees_0_committeeName_0_nameEn' : 'committee_1_name_english',
        'member_memberships_0_membership_committees_1_committeeName_0_nameEn':  'committee_2_name_english',
        'member_memberships_0_membership_committees_1_committeeName_0_dateRange_start': 'committee_2_start_date',
        # 'member_memberships_0_membership_committees_1_committeeDateRange_start': 'committee_2_start_date',
        'member_memberships_0_membership_committees_1_expiryType': 'committee_2_standing_order_type',
        'member_memberships_0_membership_committees_1_committeeName_0_dateRange_end': 'committee_2_end_date',
        'member_memberships_0_membership_committees_1_committeeID': 'committee_2_id',
        'member_memberships_0_membership_committees_0_committeeName_0_nameGa':  'committee_1_name_irish',
        'member_memberships_0_membership_committees_1_committeeName_0_nameGa':  'committee_2_name_irish',
        # 'member_memberships_0_membership_committees_0_committeeName_1_nameEn': 
        'member_memberships_0_membership_committees_3_committeeName_0_nameEn': 'committee_4_name_english',
        'member_memberships_0_membership_committees_3_committeeName_0_nameGa': 'committee_4_name_irish',
        'member_memberships_0_membership_committees_3_committeeID': 'committee_4_id',
        'member_memberships_0_membership_committees_3_committeeType_0': 'committee_4_type',
        'member_memberships_0_membership_committees_3_committeeDateRange_start': 'committee_4_start_date',
        'member_memberships_0_membership_committees_3_memberDateRange_start': 'committee_4_member_start_date',
        'member_memberships_0_membership_committees_3_memberDateRange_end': 'committee_4_member_end_date',
        'member_memberships_0_membership_committees_3_mainStatus': 'committee_4_main_status',
        'member_memberships_0_membership_committees_3_expiryType': 'committee_4_standing_order_type',
        'member_memberships_0_membership_committees_2_serviceUnit': 'committee_3_service_unit',
        'member_memberships_0_membership_committees_2_committeeType_0': 'committee_3_type',
        'member_memberships_0_membership_committees_2_committeeName_0_dateRange_end': 'committee_3_end_date',
        'member_memberships_0_membership_committees_3_committeeName_0_dateRange_start': 'committee_4_start_date',
        'member_memberships_0_membership_committees_3_committeeCode': 'committee_4_code',
        'member_memberships_0_membership_committees_3_status': 'committee_4_status',
        'member_memberships_0_membership_committees_3_committeeDateRange_end': 'committee_4_end_date',
        'member_memberships_0_membership_committees_3_committeeName_0_dateRange_end': 'committee_4_end_date',
        'member_memberships_0_membership_committees_4_memberDateRange_start': 'committee_5_start_date',
        'member_memberships_0_membership_committees_4_memberDateRange_end': 'committee_5_end_date',
        'member_memberships_0_membership_committees_4_committeeName_0_nameEn': 'committee_5_name_english',
        'member_memberships_0_membership_committees_4_mainStatus': 'committee_5_role',
        'member_memberships_0_membership_committees_4_serviceUnit': 'committee_5_service_unit',
        'member_memberships_0_membership_committees_4_committeeName_0_dateRange_start': 'committee_5_start_date',
        'member_memberships_0_membership_committees_4_committeeName_0_dateRange_end': 'committee_5_end_date',
        'member_memberships_0_membership_committees_4_committeeID': 'committee_5_id',
        'member_memberships_0_membership_committees_4_expiryType': 'committee_5_standing_order_type',
        'member_memberships_0_membership_committees_4_committeeType_0': 'committee_5_type',
        'member_memberships_0_membership_committees_4_committeeName_0_nameGa': 'committee_5_name_irish',
        'member_memberships_0_membership_committees_2_committeeName_0_nameEn': 'committee_3_name_english',
        'member_memberships_0_membership_committees_3_serviceUnit': 'committee_4_service_unit',
        'member_memberships_0_membership_committees_2_committeeName_0_dateRange_start': 'committee_3_start_date',
        'member_memberships_0_membership_committees_2_committeeName_0_nameGa': 'committee_3_name_irish',
        'member_memberships_0_membership_committees_4_status': 'committee_5_status',
        'member_memberships_0_membership_committees_2_expiryType': 'committee_3_standing_order_type',
        'member_memberships_0_membership_committees_2_committeeID': 'committee_3_id',
        'member_memberships_0_membership_committees_2_committeeDateRange_start': 'committee_3_start_date',
        'member_memberships_0_membership_committees_2_committeeDateRange_end': 'committee_3_end_date',
        'member_memberships_0_membership_committees_2_status': 'committee_3_status',
        'member_memberships_0_membership_house_houseCode': 'dail_code',
        'member_memberships_0_membership_committees_1_committeeType_0': 'committee_type_2',
        'member_memberships_0_membership_committees_0_committeeDateRange_start': 'committee_1_start_date',
        'member_memberships_0_membership_committees_0_committeeCode': 'committee_1_code',
        'member_memberships_0_membership_committees_0_committeeType_0': 'committee_1_type',
        'member_memberships_0_membership_committees_1_status': 'committee_2_status',
        'member_memberships_0_membership_committees_1_mainStatus': 'committee_2_main_status',
        'member_memberships_0_membership_committees_1_serviceUnit': 'committee_2_service_unit',
        'member_memberships_0_membership_committees_0_committeeDateRange_end' : 'committee_1_end_date',
        'member_memberships_0_membership_committees_0_expiryType' : 'committee_1_standing_order_type',
        'member_memberships_0_membership_house_houseNo' : 'dail_number',
        'member_memberships_0_membership_committees_5_committeeName_0_nameEn': 'committee_6_name_english',
        'member_memberships_0_membership_committees_5_committeeName_0_nameGa': 'committee_6_name_irish',
        'member_memberships_0_membership_committees_5_committeeID': 'committee_6_id',
        'member_memberships_0_membership_committees_5_expiryType': 'committee_6_standing_order_type',
        'member_memberships_0_membership_committees_5_committeeDateRange_start': 'committee_6_start_date',
        'member_memberships_0_membership_committees_5_committeeDateRange_end': 'committee_6_end_date',
        'member_memberships_0_membership_committees_5_status': 'committee_6_status',
        'member_memberships_0_membership_committees_5_committeeCode': 'committee_6_code',
        'member_memberships_0_membership_committees_5_committeeType_0': 'committee_6_type',
        'member_fullName': 'full_name',
        'member_firstName': 'first_name', 
        'member_lastName': 'last_name'
        }

json_data = json.load(open(members_json_path, "r"))
members = json_data

all_members = []
for member in members:
    all_members.extend(member["results"])

json.dump(all_members, open("members/filtered_members.json", "w"), indent=2)

list_of_names = [member["member"]["fullName"] for member in all_members]
print(f"Test for Total members (should be 175 - minus Ceann Comhairle): {len(list_of_names)}")

with open('C:\\Users\\pglyn\\PycharmProjects\\dail_extractor\\members\\filtered_members.json') as f:
    data = json.load(f)
    flattened_data = [flatten(member) for member in data]
    # Save to CSV and replace NaN with empty strings
    df = pd.DataFrame(flattened_data).fillna('Null')
    df = df.rename(
        rename, 
        axis=1
        )
    df = df.drop(columns=drop_cols)
    df.to_csv('members/flattened_members.csv', index=False
    )  # Drop the original fullName column after splitting
    print("CSV file created successfully.")

#delete no longer needed data
if os.path.exists('members/filtered_members.json' or os.path.exists('members/flattened_members.json') or os.path.exists('members/members.json')):
    os.remove('members/filtered_members.json')
    # os.remove('members/members.json')
    os.remove('members/flattened_members.json')
    print('Filtered and flattened JSON files deleted successfully.')
    