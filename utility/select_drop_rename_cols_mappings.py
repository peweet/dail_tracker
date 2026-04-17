# =============================================================================
# COLUMN MAPPINGS FILE
# Raw source: flattened_members.csv (flatten_json output from Oireachtas API)
# Convention: raw index _0 = human index 1 (committees_0 → committee_1, etc.)
# =============================================================================


# =============================================================================
# BILL MAPPINGS (DO NOT TOUCH)
# =============================================================================

bill_cols_to_drop = [
    "bill_amendmentLists",
    "bill_act",
    "bill_billTypeURI",
    "bill_debates_0_chamber_uri",
    "bill_debates_0_debateSectionId",
    "bill_debates_0_uri",
    "bill_stages_0_event_chamber_uri",
    "bill_debates_1_chamber_uri",
    "bill_debates_2_chamber_uri",
    "bill_amendmentLists_5_amendmentList_chamber_uri",
    "bill_events_3_event_chamber_uri",
    "bill_stages_0_event_house_uri",
    "bill_debates_11_chamber_uri",
    "bill_debates_12_chamber_uri",
    "bill_stages_1_event_chamber_uri",
    "bill_amendmentLists_4_amendmentList_chamber_uri",
    "bill_debates_10_chamber_uri",
    "bill_amendmentLists_0_amendmentList_chamber_uri",
    "bill_amendmentLists_1_amendmentList_chamber_uri",
    "bill_amendmentLists_2_amendmentList_chamber_uri",
    "bill_amendmentLists_3_amendmentList_chamber_uri",
    "bill_stages_2_event_chamber_uri",
    "bill_stages_4_event_chamber_uri",
    "bill_debates_9_chamber_uri",
    "bill_debates_8_chamber_uri",
    "bill_debates_7_chamber_uri",
    "bill_debates_6_chamber_uri",
    "bill_debates_5_chamber_uri",
    "bill_debates_4_chamber_uri",
    "bill_debates_3_chamber_uri",
    "bill_events_0_event_chamber_chamberCode",
    "bill_events_0_event_chamber_showAs",
    "bill_events_0_event_chamber_uri",
    "bill_stages_2_event_house_uri",
    "bill_events_1_event_chamber_chamberCode",
    "bill_mostRecentStage_event_stageURI",
    "bill_mostRecentStage_event_uri",
    "bill_originHouse_uri",
    "bill_stages_9_event_house_uri",
    "bill_stages_8_event_house_uri",
    "bill_stages_3_event_house_uri",
    "bill_stages_1_event_house_uri",
    "bill_originHouseURI",
    "bill_sourceURI",
    "bill_sponsors_0_sponsor_as_showAs",
    "bill_versions_0_version_lang",
    "bill_versions_0_version_uri",
    "bill_stages_7_event_house_uri",
    "bill_debates_10_chamber_uri",
    "bill_stages_9_event_chamber_uri",
    "bill_events_2_event_chamber_uri",
    "bill_events_3_event_chamber_uri",
    "bill_events_1_event_chamber_uri",
    "bill_mostRecentStage_event_chamber_uri",
    "bill_debates_11_chamber_uri",
    "bill_debates_12_chamber_uri",
    "bill_stages_3_event_chamber_uri",
    "bill_stages_4_event_house_uri",
    "bill_stages_5_event_chamber_uri",
    "bill_stages_5_event_house_uri",
    "bill_stages_6_event_chamber_uri",
    "bill_stages_6_event_house_uri",
    "bill_stages_7_event_chamber_uri",
    "bill_stages_8_event_chamber_uri",
    "bill_events_0_event_eventURI",
    "bill_events_0_event_uri",
    "bill_events_1_event_eventURI",
    "bill_events_1_event_uri",
    "bill_methodURI",
    "bill_mostRecentStage_event_house_uri",
    "bill_stages_0_event_house_chamberCode",
    "bill_stages_0_event_house_chamberType",
    "bill_stages_0_event_house_houseCode",
    "bill_mostRecentStage_event_house_uri",
    "bill_relatedDocs_0_relatedDoc_lang",
    "bill_relatedDocs_0_relatedDoc_uri",
    "bill_sponsors_0_sponsor_as_uri",
    "bill_stages_0_event_stageURI",
    "bill_stages_0_event_uri",
    "bill_statusURI",
    "bill_versions_0_version_formats_xml",
    "bill_sponsors_2_sponsor_as_showAs",
    "bill_sponsors_2_sponsor_as_uri",
    "bill_sponsors_3_sponsor_as_showAs",
    "bill_sponsors_3_sponsor_as_uri",
    "bill_relatedDocs_0_relatedDoc_formats_xml"
]

bill_rename = {
    "bill_debates_0_chamber_showAs" : "bill_chamber",
    "bill_debates_0_date" : "bill_debates_date",
    "bill_debates_0_showAs" : "bill_debate_stages",
    "bill_events_0_event_chamber_showAs" : "bill_event_chamber",
    "bill_mostRecentStage_event_stageCompleted" : "bill_enacted",
    "bill_mostRecentStage_event_stageOutcome" : "bill_final_status",
    "bill_shortTitleEn" : "bill_short_title_en",
    "bill_shortTitleGa" : "bill_short_title_irish",
    "bill_sponsors_0_sponsor_by_showAs" : "bill_sponsor",
    "bill_sponsors_0_sponsor_by_uri" : "unique_member_code_raw",
    "bill_sponsors_0_sponsor_isPrimary" : "bill_primary_sponsor",
    "billSort_billShortTitleEnSort" : "bill_short_dash_title_en",
    "billSort_billShortTitleGaSort" : "bill_short_dash_title_irish",
    "billSort_billYearSort" : "bill_year",
    "bill_relatedDocs_0_relatedDoc_formats_pdf_uri" : "bill_related_doc_pdf",
    "bill_relatedDocs_0_relatedDoc_showAs" : "bill_related_doc_type",
    "bill_act_statutebookURI" : "bill_statue_book_link",
    "bill_billNo" : "bill_number",
    "bill_billYear" : "bill_year",
    "bill_lastUpdated" : "bill_date_last_updated",
    "bill_longTitleEn" : "bill_long_title_english",
    "bill_longTitleGa" : "bill_long_title_irish"
}


# =============================================================================
# LOBBYING MAPPINGS (DO NOT TOUCH)
# =============================================================================

lobbying_rename = {
    "Id" :"primary_key",
    "Url" : "lobby_enterprise_uri",
    "Lobbyist Name" : "lobbyist_name",
    "Date Published" : "date_published_timestamp",
    "Period" : "lobbying_period",
    "DPOs Lobbied" : "dpo_lobbied",
    "Relevant Matter" : "relevant_matter",
    "Public Policy Area" : "public_policy_area",
    "Specific Details" : "specific_details",
    "Intended Results" : "intended_results",
    "Lobbying Activities" : "lobbying_activities",
    "Person primarily responsible for lobbying on this activity" : "person_primarily_responsible",
    "Any DPOs or Former DPOs who carried out lobbying activities" : "dpos_or_former_dpos_who_carried_out_lobbying",
    "Current or Former DPOs" : "current_or_former_dpos",
    "Was this a grassroots campaign?" : "was_this_a_grassroots_campaign",
    "Grassroots directive" : "grassroots_directive",
    "Was this lobbying done on behalf of a client?" : "was_this_lobbying_done_on_behalf_of_a_client",
    "Client(s)" : "clients"
}


# =============================================================================
# MEMBERS - HISTORIC COMMENTED OUT (DO NOT REMOVE)
# =============================================================================

# enrich_cols_to_select = ['join_key',
#                   'unique_member_code',
#                   'party',
#                   'first_name',
#                   'last_name',
#                   'member_constituency',
#                   'dail_term',
#                   'ministerial_office',
#                   ]

# members_drop_cols = [
#     'member_memberships_0_membership_represents_0_represent_representType',
#     'member_uri',
#     'member_wikiTitle',
#     'member_memberships_0_membership_committees_0_uri',
#     'member_memberships_0_membership_committees_0_houseNo',
#     'member_memberships_0_membership_committees_0_houseCode',
#     'member_memberships_0_membership_committees_2_uri',
#     'member_memberships_0_membership_committees_1_houseNo',
#     'member_memberships_0_membership_represents_0_represent_uri',
#     'member_memberships_0_membership_uri',
#     'member_memberships_0_membership_house_uri',
#     'member_memberships_0_membership_committees_3_uri',
#     'member_memberships_0_membership_committees_4_uri',
#     'member_memberships_0_membership_parties_1_party_uri',
#     'member_memberships_0_membership_parties_2_party_uri',
#     'member_memberships_0_membership_parties_2_party_uri',
#     'member_memberships_0_membership_committees_5_uri',
#     'member_memberships_0_membership_committees_11_uri',
#     'member_memberships_0_membership_committees_1_uri',
#     'member_memberships_0_membership_committees_2_role',
#     'member_memberships_0_membership_committees_4_role',
#     'member_memberships_0_membership_committees_5_role',
#     'member_memberships_0_membership_committees_6_uri',
#     'member_memberships_0_membership_committees_7_uri',
#     'member_memberships_0_membership_committees_8_uri',
#     'member_memberships_0_membership_offices_5_office_officeName_uri',
#     'member_memberships_0_membership_offices_4_office_officeName_uri',
#     'member_memberships_0_membership_committees_9_uri',
#     'member_memberships_0_membership_committees_10_uri',
#     'member_image',
#     'member_memberships_0_membership_committees_3_role',
#     'member_memberships_0_membership_committees_1_houseCode',
#     'member_memberships_0_membership_committees_1_role',
#     'member_memberships_0_membership_committees_0_role',
#     'member_showAs',
#     'member_memberships_0_membership_dateRange_start',
#     'member_memberships_0_membership_offices',
#     'member_memberships_0_membership_parties_0_party_dateRange_start',
#     'member_pId',
#     'member_memberships_0_membership_committees',
#     'member_gender',
#     'member_memberships_0_membership_parties_0_party_uri',
#     'member_memberships_0_membership_parties_0_party_partyCode',
#     'member_memberships_0_membership_parties_0_party_dateRange_end'
# ]


# =============================================================================
# MEMBERS RENAME
# Raw column → human-readable name
# Committees: raw index _0 = committee_1, _1 = committee_2, etc.
# Offices:    raw index _0 = office_1,    _1 = office_2,    etc.
# Parties:    raw index _0 = primary party (no suffix), _1/_2 = alternate parties
# =============================================================================

members_rename = {

    # -------------------------------------------------------------------------
    # CORE MEMBER INFO
    # -------------------------------------------------------------------------
    'member_fullName':   'full_name',
    'member_firstName':  'first_name',
    'member_lastName':   'last_name',
    'member_memberCode': 'unique_member_code',
    'member_gender':     'gender',
    'member_dateOfDeath':'date_of_death',

    # -------------------------------------------------------------------------
    # MEMBERSHIP DATE RANGE (the overall membership period for this record)
    # -------------------------------------------------------------------------
    'member_memberships_0_membership_dateRange_start': 'membership_start_date',
    'member_memberships_0_membership_dateRange_end':   'membership_end_date',

    # -------------------------------------------------------------------------
    # DAIL / HOUSE
    # -------------------------------------------------------------------------
    'member_memberships_0_membership_house_houseCode':   'dail_code',
    'member_memberships_0_membership_house_showAs':      'dail_term',
    'member_memberships_0_membership_house_houseNo':     'dail_number',
    'member_memberships_0_membership_house_chamberType': 'dail_chamber_type',

    # -------------------------------------------------------------------------
    # CONSTITUENCY / REPRESENTS
    # -------------------------------------------------------------------------
    'member_memberships_0_membership_represents_0_represent_showAs':        'constituency_name',
    'member_memberships_0_membership_represents_0_represent_representCode': 'constituency_code',

    # -------------------------------------------------------------------------
    # PRIMARY PARTY (raw: parties_0)
    # -------------------------------------------------------------------------
    'member_memberships_0_membership_parties_0_party_showAs':        'party',
    'member_memberships_0_membership_parties_0_party_partyCode':     'party_code',
    'member_memberships_0_membership_parties_0_party_dateRange_start':'party_start_date',
    'member_memberships_0_membership_parties_0_party_dateRange_end':  'party_end_date',

    # -------------------------------------------------------------------------
    # ALTERNATE PARTY 1 (raw: parties_1)
    # -------------------------------------------------------------------------
    'member_memberships_0_membership_parties_1_party_showAs':         'party_1_name',
    'member_memberships_0_membership_parties_1_party_partyCode':      'party_1_code',
    'member_memberships_0_membership_parties_1_party_dateRange_start':'party_1_start_date',
    'member_memberships_0_membership_parties_1_party_dateRange_end':  'party_1_end_date',

    # -------------------------------------------------------------------------
    # ALTERNATE PARTY 2 (raw: parties_2)
    # -------------------------------------------------------------------------
    'member_memberships_0_membership_parties_2_party_showAs':         'party_2_name',
    'member_memberships_0_membership_parties_2_party_partyCode':      'party_2_code',
    'member_memberships_0_membership_parties_2_party_dateRange_start':'party_2_start_date',
    'member_memberships_0_membership_parties_2_party_dateRange_end':  'party_2_end_date',

    # -------------------------------------------------------------------------
    # OFFICE 1 (raw: offices_0) - primary/ministerial office
    # -------------------------------------------------------------------------
    'member_memberships_0_membership_offices_0_office_officeName_showAs': 'office_1_name',
    'member_memberships_0_membership_offices_0_office_dateRange_start':   'office_1_start_date',
    'member_memberships_0_membership_offices_0_office_dateRange_end':     'office_1_end_date',

    # -------------------------------------------------------------------------
    # OFFICE 2 (raw: offices_1)
    # -------------------------------------------------------------------------
    'member_memberships_0_membership_offices_1_office_officeName_showAs': 'office_2_name',
    'member_memberships_0_membership_offices_1_office_dateRange_start':   'office_2_start_date',
    'member_memberships_0_membership_offices_1_office_dateRange_end':     'office_2_end_date',

    # -------------------------------------------------------------------------
    # OFFICE 3 (raw: offices_2)
    # -------------------------------------------------------------------------
    'member_memberships_0_membership_offices_2_office_officeName_showAs': 'office_3_name',
    'member_memberships_0_membership_offices_2_office_dateRange_start':   'office_3_start_date',
    'member_memberships_0_membership_offices_2_office_dateRange_end':     'office_3_end_date',

    # -------------------------------------------------------------------------
    # OFFICE 4 (raw: offices_3)
    # -------------------------------------------------------------------------
    'member_memberships_0_membership_offices_3_office_officeName_showAs': 'office_4_name',
    'member_memberships_0_membership_offices_3_office_dateRange_start':   'office_4_start_date',
    'member_memberships_0_membership_offices_3_office_dateRange_end':     'office_4_end_date',

    # -------------------------------------------------------------------------
    # OFFICE 5 (raw: offices_4)
    # -------------------------------------------------------------------------
    'member_memberships_0_membership_offices_4_office_officeName_showAs': 'office_5_name',
    'member_memberships_0_membership_offices_4_office_dateRange_start':   'office_5_start_date',
    'member_memberships_0_membership_offices_4_office_dateRange_end':     'office_5_end_date',

    # -------------------------------------------------------------------------
    # OFFICE 6 (raw: offices_5)
    # -------------------------------------------------------------------------
    'member_memberships_0_membership_offices_5_office_officeName_showAs': 'office_6_name',
    'member_memberships_0_membership_offices_5_office_dateRange_start':   'office_6_start_date',
    'member_memberships_0_membership_offices_5_office_dateRange_end':     'office_6_end_date',

    # -------------------------------------------------------------------------
    # COMMITTEE 1 (raw: committees_0)
    # Fields: code, service_unit, role, member dates, status, committee dates,
    #         type, expiry_type, name (en/ga), name dates, id,
    #         role dates/title, alt name (en/ga/dates)
    # -------------------------------------------------------------------------
    'member_memberships_0_membership_committees_0_committeeCode':              'committee_1_code',
    'member_memberships_0_membership_committees_0_serviceUnit':                'committee_1_service_unit',
    'member_memberships_0_membership_committees_0_role':                       'committee_1_role',
    'member_memberships_0_membership_committees_0_memberDateRange_start':      'committee_1_member_start_date',
    'member_memberships_0_membership_committees_0_memberDateRange_end':        'committee_1_member_end_date',
    'member_memberships_0_membership_committees_0_mainStatus':                 'committee_1_main_status',
    'member_memberships_0_membership_committees_0_status':                     'committee_1_status',
    'member_memberships_0_membership_committees_0_committeeDateRange_start':   'committee_1_start_date',
    'member_memberships_0_membership_committees_0_committeeDateRange_end':     'committee_1_end_date',
    'member_memberships_0_membership_committees_0_committeeType_0':            'committee_1_type',
    'member_memberships_0_membership_committees_0_expiryType':                 'committee_1_expiry_type',
    'member_memberships_0_membership_committees_0_committeeName_0_nameEn':     'committee_1_name_en',
    'member_memberships_0_membership_committees_0_committeeName_0_nameGa':     'committee_1_name_ga',
    'member_memberships_0_membership_committees_0_committeeName_0_dateRange_start': 'committee_1_name_start_date',
    'member_memberships_0_membership_committees_0_committeeName_0_dateRange_end':   'committee_1_name_end_date',
    'member_memberships_0_membership_committees_0_committeeID':                'committee_1_id',
    'member_memberships_0_membership_committees_0_role_dateRange_start':       'committee_1_role_start_date',
    'member_memberships_0_membership_committees_0_role_dateRange_end':         'committee_1_role_end_date',
    'member_memberships_0_membership_committees_0_role_title':                 'committee_1_role_title',
    'member_memberships_0_membership_committees_0_committeeName_1_nameEn':     'committee_1_alt_name_en',
    'member_memberships_0_membership_committees_0_committeeName_1_nameGa':     'committee_1_alt_name_ga',
    'member_memberships_0_membership_committees_0_committeeName_1_dateRange_start': 'committee_1_alt_name_start_date',
    'member_memberships_0_membership_committees_0_committeeName_1_dateRange_end':   'committee_1_alt_name_end_date',

    # -------------------------------------------------------------------------
    # COMMITTEE 2 (raw: committees_1)
    # -------------------------------------------------------------------------
    'member_memberships_0_membership_committees_1_committeeCode':              'committee_2_code',
    'member_memberships_0_membership_committees_1_serviceUnit':                'committee_2_service_unit',
    'member_memberships_0_membership_committees_1_role':                       'committee_2_role',
    'member_memberships_0_membership_committees_1_memberDateRange_start':      'committee_2_member_start_date',
    'member_memberships_0_membership_committees_1_memberDateRange_end':        'committee_2_member_end_date',
    'member_memberships_0_membership_committees_1_mainStatus':                 'committee_2_main_status',
    'member_memberships_0_membership_committees_1_status':                     'committee_2_status',
    'member_memberships_0_membership_committees_1_committeeDateRange_start':   'committee_2_start_date',
    'member_memberships_0_membership_committees_1_committeeDateRange_end':     'committee_2_end_date',
    'member_memberships_0_membership_committees_1_committeeType_0':            'committee_2_type',
    'member_memberships_0_membership_committees_1_expiryType':                 'committee_2_expiry_type',
    'member_memberships_0_membership_committees_1_committeeName_0_nameEn':     'committee_2_name_en',
    'member_memberships_0_membership_committees_1_committeeName_0_nameGa':     'committee_2_name_ga',
    'member_memberships_0_membership_committees_1_committeeName_0_dateRange_start': 'committee_2_name_start_date',
    'member_memberships_0_membership_committees_1_committeeName_0_dateRange_end':   'committee_2_name_end_date',
    'member_memberships_0_membership_committees_1_committeeID':                'committee_2_id',
    'member_memberships_0_membership_committees_1_role_dateRange_start':       'committee_2_role_start_date',
    'member_memberships_0_membership_committees_1_role_dateRange_end':         'committee_2_role_end_date',
    'member_memberships_0_membership_committees_1_role_title':                 'committee_2_role_title',
    'member_memberships_0_membership_committees_1_committeeName_1_nameEn':     'committee_2_alt_name_en',
    'member_memberships_0_membership_committees_1_committeeName_1_nameGa':     'committee_2_alt_name_ga',
    'member_memberships_0_membership_committees_1_committeeName_1_dateRange_start': 'committee_2_alt_name_start_date',
    'member_memberships_0_membership_committees_1_committeeName_1_dateRange_end':   'committee_2_alt_name_end_date',

    # -------------------------------------------------------------------------
    # COMMITTEE 3 (raw: committees_2)
    # -------------------------------------------------------------------------
    'member_memberships_0_membership_committees_2_committeeCode':              'committee_3_code',
    'member_memberships_0_membership_committees_2_serviceUnit':                'committee_3_service_unit',
    'member_memberships_0_membership_committees_2_role':                       'committee_3_role',
    'member_memberships_0_membership_committees_2_memberDateRange_start':      'committee_3_member_start_date',
    'member_memberships_0_membership_committees_2_memberDateRange_end':        'committee_3_member_end_date',
    'member_memberships_0_membership_committees_2_mainStatus':                 'committee_3_main_status',
    'member_memberships_0_membership_committees_2_status':                     'committee_3_status',
    'member_memberships_0_membership_committees_2_committeeDateRange_start':   'committee_3_start_date',
    'member_memberships_0_membership_committees_2_committeeDateRange_end':     'committee_3_end_date',
    'member_memberships_0_membership_committees_2_committeeType_0':            'committee_3_type',
    'member_memberships_0_membership_committees_2_expiryType':                 'committee_3_expiry_type',
    'member_memberships_0_membership_committees_2_committeeName_0_nameEn':     'committee_3_name_en',
    'member_memberships_0_membership_committees_2_committeeName_0_nameGa':     'committee_3_name_ga',
    'member_memberships_0_membership_committees_2_committeeName_0_dateRange_start': 'committee_3_name_start_date',
    'member_memberships_0_membership_committees_2_committeeName_0_dateRange_end':   'committee_3_name_end_date',
    'member_memberships_0_membership_committees_2_committeeID':                'committee_3_id',
    'member_memberships_0_membership_committees_2_role_dateRange_start':       'committee_3_role_start_date',
    'member_memberships_0_membership_committees_2_role_dateRange_end':         'committee_3_role_end_date',
    'member_memberships_0_membership_committees_2_role_title':                 'committee_3_role_title',
    'member_memberships_0_membership_committees_2_committeeName_1_nameEn':     'committee_3_alt_name_en',
    'member_memberships_0_membership_committees_2_committeeName_1_nameGa':     'committee_3_alt_name_ga',
    'member_memberships_0_membership_committees_2_committeeName_1_dateRange_start': 'committee_3_alt_name_start_date',
    'member_memberships_0_membership_committees_2_committeeName_1_dateRange_end':   'committee_3_alt_name_end_date',

    # -------------------------------------------------------------------------
    # COMMITTEE 4 (raw: committees_3)
    # -------------------------------------------------------------------------
    'member_memberships_0_membership_committees_3_committeeCode':              'committee_4_code',
    'member_memberships_0_membership_committees_3_serviceUnit':                'committee_4_service_unit',
    'member_memberships_0_membership_committees_3_role':                       'committee_4_role',
    'member_memberships_0_membership_committees_3_memberDateRange_start':      'committee_4_member_start_date',
    'member_memberships_0_membership_committees_3_memberDateRange_end':        'committee_4_member_end_date',
    'member_memberships_0_membership_committees_3_mainStatus':                 'committee_4_main_status',
    'member_memberships_0_membership_committees_3_status':                     'committee_4_status',
    'member_memberships_0_membership_committees_3_committeeDateRange_start':   'committee_4_start_date',
    'member_memberships_0_membership_committees_3_committeeDateRange_end':     'committee_4_end_date',
    'member_memberships_0_membership_committees_3_committeeType_0':            'committee_4_type',
    'member_memberships_0_membership_committees_3_expiryType':                 'committee_4_expiry_type',
    'member_memberships_0_membership_committees_3_committeeName_0_nameEn':     'committee_4_name_en',
    'member_memberships_0_membership_committees_3_committeeName_0_nameGa':     'committee_4_name_ga',
    'member_memberships_0_membership_committees_3_committeeName_0_dateRange_start': 'committee_4_name_start_date',
    'member_memberships_0_membership_committees_3_committeeName_0_dateRange_end':   'committee_4_name_end_date',
    'member_memberships_0_membership_committees_3_committeeID':                'committee_4_id',
    'member_memberships_0_membership_committees_3_role_dateRange_start':       'committee_4_role_start_date',
    'member_memberships_0_membership_committees_3_role_dateRange_end':         'committee_4_role_end_date',
    'member_memberships_0_membership_committees_3_role_title':                 'committee_4_role_title',
    'member_memberships_0_membership_committees_3_committeeName_1_nameEn':     'committee_4_alt_name_en',
    'member_memberships_0_membership_committees_3_committeeName_1_nameGa':     'committee_4_alt_name_ga',
    'member_memberships_0_membership_committees_3_committeeName_1_dateRange_start': 'committee_4_alt_name_start_date',
    'member_memberships_0_membership_committees_3_committeeName_1_dateRange_end':   'committee_4_alt_name_end_date',

    # -------------------------------------------------------------------------
    # COMMITTEE 5 (raw: committees_4)
    # -------------------------------------------------------------------------
    'member_memberships_0_membership_committees_4_committeeCode':              'committee_5_code',
    'member_memberships_0_membership_committees_4_serviceUnit':                'committee_5_service_unit',
    'member_memberships_0_membership_committees_4_role':                       'committee_5_role',
    'member_memberships_0_membership_committees_4_memberDateRange_start':      'committee_5_member_start_date',
    'member_memberships_0_membership_committees_4_memberDateRange_end':        'committee_5_member_end_date',
    'member_memberships_0_membership_committees_4_mainStatus':                 'committee_5_main_status',
    'member_memberships_0_membership_committees_4_status':                     'committee_5_status',
    'member_memberships_0_membership_committees_4_committeeDateRange_start':   'committee_5_start_date',
    'member_memberships_0_membership_committees_4_committeeDateRange_end':     'committee_5_end_date',
    'member_memberships_0_membership_committees_4_committeeType_0':            'committee_5_type',
    'member_memberships_0_membership_committees_4_expiryType':                 'committee_5_expiry_type',
    'member_memberships_0_membership_committees_4_committeeName_0_nameEn':     'committee_5_name_en',
    'member_memberships_0_membership_committees_4_committeeName_0_nameGa':     'committee_5_name_ga',
    'member_memberships_0_membership_committees_4_committeeName_0_dateRange_start': 'committee_5_name_start_date',
    'member_memberships_0_membership_committees_4_committeeName_0_dateRange_end':   'committee_5_name_end_date',
    'member_memberships_0_membership_committees_4_committeeID':                'committee_5_id',
    'member_memberships_0_membership_committees_4_role_dateRange_start':       'committee_5_role_start_date',
    'member_memberships_0_membership_committees_4_role_dateRange_end':         'committee_5_role_end_date',
    'member_memberships_0_membership_committees_4_role_title':                 'committee_5_role_title',

    # -------------------------------------------------------------------------
    # COMMITTEE 6 (raw: committees_5)
    # -------------------------------------------------------------------------
    'member_memberships_0_membership_committees_5_committeeCode':              'committee_6_code',
    'member_memberships_0_membership_committees_5_serviceUnit':                'committee_6_service_unit',
    'member_memberships_0_membership_committees_5_role':                       'committee_6_role',
    'member_memberships_0_membership_committees_5_memberDateRange_start':      'committee_6_member_start_date',
    'member_memberships_0_membership_committees_5_memberDateRange_end':        'committee_6_member_end_date',
    'member_memberships_0_membership_committees_5_mainStatus':                 'committee_6_main_status',
    'member_memberships_0_membership_committees_5_status':                     'committee_6_status',
    'member_memberships_0_membership_committees_5_committeeDateRange_start':   'committee_6_start_date',
    'member_memberships_0_membership_committees_5_committeeDateRange_end':     'committee_6_end_date',
    'member_memberships_0_membership_committees_5_committeeType_0':            'committee_6_type',
    'member_memberships_0_membership_committees_5_expiryType':                 'committee_6_expiry_type',
    'member_memberships_0_membership_committees_5_committeeName_0_nameEn':     'committee_6_name_en',
    'member_memberships_0_membership_committees_5_committeeName_0_nameGa':     'committee_6_name_ga',
    'member_memberships_0_membership_committees_5_committeeName_0_dateRange_start': 'committee_6_name_start_date',
    'member_memberships_0_membership_committees_5_committeeName_0_dateRange_end':   'committee_6_name_end_date',
    'member_memberships_0_membership_committees_5_committeeID':                'committee_6_id',
    'member_memberships_0_membership_committees_5_role_dateRange_start':       'committee_6_role_start_date',
    'member_memberships_0_membership_committees_5_role_dateRange_end':         'committee_6_role_end_date',
    'member_memberships_0_membership_committees_5_role_title':                 'committee_6_role_title',
    'member_memberships_0_membership_committees_5_committeeName_1_nameEn':     'committee_6_alt_name_en',
    'member_memberships_0_membership_committees_5_committeeName_1_nameGa':     'committee_6_alt_name_ga',
    'member_memberships_0_membership_committees_5_committeeName_1_dateRange_start': 'committee_6_alt_name_start_date',
    'member_memberships_0_membership_committees_5_committeeName_1_dateRange_end':   'committee_6_alt_name_end_date',

    # -------------------------------------------------------------------------
    # COMMITTEE 7 (raw: committees_6)
    # -------------------------------------------------------------------------
    'member_memberships_0_membership_committees_6_committeeCode':              'committee_7_code',
    'member_memberships_0_membership_committees_6_serviceUnit':                'committee_7_service_unit',
    'member_memberships_0_membership_committees_6_role':                       'committee_7_role',
    'member_memberships_0_membership_committees_6_memberDateRange_start':      'committee_7_member_start_date',
    'member_memberships_0_membership_committees_6_memberDateRange_end':        'committee_7_member_end_date',
    'member_memberships_0_membership_committees_6_mainStatus':                 'committee_7_main_status',
    'member_memberships_0_membership_committees_6_status':                     'committee_7_status',
    'member_memberships_0_membership_committees_6_committeeDateRange_start':   'committee_7_start_date',
    'member_memberships_0_membership_committees_6_committeeDateRange_end':     'committee_7_end_date',
    'member_memberships_0_membership_committees_6_committeeType_0':            'committee_7_type',
    'member_memberships_0_membership_committees_6_expiryType':                 'committee_7_expiry_type',
    'member_memberships_0_membership_committees_6_committeeName_0_nameEn':     'committee_7_name_en',
    'member_memberships_0_membership_committees_6_committeeName_0_nameGa':     'committee_7_name_ga',
    'member_memberships_0_membership_committees_6_committeeName_0_dateRange_start': 'committee_7_name_start_date',
    'member_memberships_0_membership_committees_6_committeeName_0_dateRange_end':   'committee_7_name_end_date',
    'member_memberships_0_membership_committees_6_committeeID':                'committee_7_id',
    'member_memberships_0_membership_committees_6_role_dateRange_start':       'committee_7_role_start_date',
    'member_memberships_0_membership_committees_6_role_dateRange_end':         'committee_7_role_end_date',
    'member_memberships_0_membership_committees_6_role_title':                 'committee_7_role_title',

    # -------------------------------------------------------------------------
    # COMMITTEE 8 (raw: committees_7)
    # -------------------------------------------------------------------------
    'member_memberships_0_membership_committees_7_committeeCode':              'committee_8_code',
    'member_memberships_0_membership_committees_7_serviceUnit':                'committee_8_service_unit',
    'member_memberships_0_membership_committees_7_role':                       'committee_8_role',
    'member_memberships_0_membership_committees_7_memberDateRange_start':      'committee_8_member_start_date',
    'member_memberships_0_membership_committees_7_memberDateRange_end':        'committee_8_member_end_date',
    'member_memberships_0_membership_committees_7_mainStatus':                 'committee_8_main_status',
    'member_memberships_0_membership_committees_7_status':                     'committee_8_status',
    'member_memberships_0_membership_committees_7_committeeDateRange_start':   'committee_8_start_date',
    'member_memberships_0_membership_committees_7_committeeDateRange_end':     'committee_8_end_date',
    'member_memberships_0_membership_committees_7_committeeType_0':            'committee_8_type',
    'member_memberships_0_membership_committees_7_expiryType':                 'committee_8_expiry_type',
    'member_memberships_0_membership_committees_7_committeeName_0_nameEn':     'committee_8_name_en',
    'member_memberships_0_membership_committees_7_committeeName_0_nameGa':     'committee_8_name_ga',
    'member_memberships_0_membership_committees_7_committeeName_0_dateRange_start': 'committee_8_name_start_date',
    'member_memberships_0_membership_committees_7_committeeName_0_dateRange_end':   'committee_8_name_end_date',
    'member_memberships_0_membership_committees_7_committeeID':                'committee_8_id',
    'member_memberships_0_membership_committees_7_role_dateRange_start':       'committee_8_role_start_date',
    'member_memberships_0_membership_committees_7_role_dateRange_end':         'committee_8_role_end_date',
    'member_memberships_0_membership_committees_7_role_title':                 'committee_8_role_title',
    'member_memberships_0_membership_committees_7_committeeName_1_nameEn':     'committee_8_alt_name_en',
    'member_memberships_0_membership_committees_7_committeeName_1_nameGa':     'committee_8_alt_name_ga',
    'member_memberships_0_membership_committees_7_committeeName_1_dateRange_start': 'committee_8_alt_name_start_date',
    'member_memberships_0_membership_committees_7_committeeName_1_dateRange_end':   'committee_8_alt_name_end_date',

    # -------------------------------------------------------------------------
    # COMMITTEE 9 (raw: committees_8)
    # -------------------------------------------------------------------------
    'member_memberships_0_membership_committees_8_committeeCode':              'committee_9_code',
    'member_memberships_0_membership_committees_8_serviceUnit':                'committee_9_service_unit',
    'member_memberships_0_membership_committees_8_role':                       'committee_9_role',
    'member_memberships_0_membership_committees_8_memberDateRange_start':      'committee_9_member_start_date',
    'member_memberships_0_membership_committees_8_memberDateRange_end':        'committee_9_member_end_date',
    'member_memberships_0_membership_committees_8_mainStatus':                 'committee_9_main_status',
    'member_memberships_0_membership_committees_8_status':                     'committee_9_status',
    'member_memberships_0_membership_committees_8_committeeDateRange_start':   'committee_9_start_date',
    'member_memberships_0_membership_committees_8_committeeDateRange_end':     'committee_9_end_date',
    'member_memberships_0_membership_committees_8_committeeType_0':            'committee_9_type',
    'member_memberships_0_membership_committees_8_expiryType':                 'committee_9_expiry_type',
    'member_memberships_0_membership_committees_8_committeeName_0_nameEn':     'committee_9_name_en',
    'member_memberships_0_membership_committees_8_committeeName_0_nameGa':     'committee_9_name_ga',
    'member_memberships_0_membership_committees_8_committeeName_0_dateRange_start': 'committee_9_name_start_date',
    'member_memberships_0_membership_committees_8_committeeName_0_dateRange_end':   'committee_9_name_end_date',
    'member_memberships_0_membership_committees_8_committeeID':                'committee_9_id',

    # -------------------------------------------------------------------------
    # COMMITTEE 10 (raw: committees_9)
    # -------------------------------------------------------------------------
    'member_memberships_0_membership_committees_9_committeeCode':              'committee_10_code',
    'member_memberships_0_membership_committees_9_serviceUnit':                'committee_10_service_unit',
    'member_memberships_0_membership_committees_9_role':                       'committee_10_role_title',
    'member_memberships_0_membership_committees_9_memberDateRange_start':      'committee_10_member_start_date',
    'member_memberships_0_membership_committees_9_memberDateRange_end':        'committee_10_member_end_date',
    'member_memberships_0_membership_committees_9_mainStatus':                 'committee_10_main_status',
    'member_memberships_0_membership_committees_9_status':                     'committee_10_status',
    'member_memberships_0_membership_committees_9_committeeDateRange_start':   'committee_10_start_date',
    'member_memberships_0_membership_committees_9_committeeDateRange_end':     'committee_10_end_date',
    'member_memberships_0_membership_committees_9_committeeType_0':            'committee_10_type',
    'member_memberships_0_membership_committees_9_expiryType':                 'committee_10_expiry_type',
    'member_memberships_0_membership_committees_9_committeeName_0_nameEn':     'committee_10_name_en',
    'member_memberships_0_membership_committees_9_committeeName_0_nameGa':     'committee_10_name_ga',
    'member_memberships_0_membership_committees_9_committeeName_0_dateRange_start': 'committee_10_name_start_date',
    'member_memberships_0_membership_committees_9_committeeName_0_dateRange_end':   'committee_10_name_end_date',
    'member_memberships_0_membership_committees_9_committeeID':                'committee_10_id',

    # -------------------------------------------------------------------------
    # COMMITTEE 11 (raw: committees_10)
    # -------------------------------------------------------------------------
    'member_memberships_0_membership_committees_10_committeeCode':              'committee_11_code',
    'member_memberships_0_membership_committees_10_serviceUnit':                'committee_11_service_unit',
    'member_memberships_0_membership_committees_10_role':                       'committee_11_role',
    'member_memberships_0_membership_committees_10_memberDateRange_start':      'committee_11_member_start_date',
    'member_memberships_0_membership_committees_10_memberDateRange_end':        'committee_11_member_end_date',
    'member_memberships_0_membership_committees_10_mainStatus':                 'committee_11_main_status',
    'member_memberships_0_membership_committees_10_status':                     'committee_11_status',
    'member_memberships_0_membership_committees_10_committeeDateRange_start':   'committee_11_start_date',
    'member_memberships_0_membership_committees_10_committeeDateRange_end':     'committee_11_end_date',
    'member_memberships_0_membership_committees_10_committeeType_0':            'committee_11_type',
    'member_memberships_0_membership_committees_10_expiryType':                 'committee_11_expiry_type',
    'member_memberships_0_membership_committees_10_committeeName_0_nameEn':     'committee_11_name_en',
    'member_memberships_0_membership_committees_10_committeeName_0_nameGa':     'committee_11_name_ga',
    'member_memberships_0_membership_committees_10_committeeName_0_dateRange_start': 'committee_11_name_start_date',
    'member_memberships_0_membership_committees_10_committeeName_0_dateRange_end':   'committee_11_name_end_date',
    'member_memberships_0_membership_committees_10_committeeID':                'committee_11_id',

    # -------------------------------------------------------------------------
    # COMMITTEE 12 (raw: committees_11)
    # -------------------------------------------------------------------------
    'member_memberships_0_membership_committees_11_committeeCode':              'committee_12_code',
    'member_memberships_0_membership_committees_11_serviceUnit':                'committee_12_service_unit',
    'member_memberships_0_membership_committees_11_role':                       'committee_12_role',
    'member_memberships_0_membership_committees_11_memberDateRange_start':      'committee_12_member_start_date',
    'member_memberships_0_membership_committees_11_memberDateRange_end':        'committee_12_member_end_date',
    'member_memberships_0_membership_committees_11_mainStatus':                 'committee_12_main_status',
    'member_memberships_0_membership_committees_11_status':                     'committee_12_status',
    'member_memberships_0_membership_committees_11_committeeDateRange_start':   'committee_12_start_date',
    'member_memberships_0_membership_committees_11_committeeDateRange_end':     'committee_12_end_date',
    'member_memberships_0_membership_committees_11_committeeType_0':            'committee_12_type',
    'member_memberships_0_membership_committees_11_expiryType':                 'committee_12_expiry_type',
    'member_memberships_0_membership_committees_11_committeeName_0_nameEn':     'committee_12_name_en',
    'member_memberships_0_membership_committees_11_committeeName_0_nameGa':     'committee_12_name_ga',
    'member_memberships_0_membership_committees_11_committeeName_0_dateRange_start': 'committee_12_name_start_date',
    'member_memberships_0_membership_committees_11_committeeName_0_dateRange_end':   'committee_12_name_end_date',
    'member_memberships_0_membership_committees_11_committeeID':                'committee_12_id',
    'member_memberships_0_membership_committees_11_role_dateRange_start':       'committee_12_role_start_date',
    'member_memberships_0_membership_committees_11_role_dateRange_end':         'committee_12_role_end_date',
    'member_memberships_0_membership_committees_11_role_title':                 'committee_12_role_title',
}


# =============================================================================
# MEMBERS DROP COLS
# NOTE: rename is applied FIRST in the pipeline, so columns that were NOT
# renamed appear here under their original raw name. Columns that were
# renamed appear here under their NEW name (if you choose to drop them
# after keeping in rename for traceability).
# All drops use RAW names since we never put drop-only cols in members_rename.
# =============================================================================

members_drop_cols = [
    'gender',
    # -------------------------------------------------------------------------
    # MEMBER DISPLAY / PLATFORM METADATA
    # -------------------------------------------------------------------------
    'member_showAs',     # display label, fully redundant with full_name
    'member_image',      # image URL, not useful for data analysis or joins
    'member_uri',        # API resource URI, internal to the Oireachtas API
    'member_wikiTitle',  # Wikipedia page title, not needed for analysis
    'member_pId',        # internal platform integer ID, memberCode is the stable key

    # -------------------------------------------------------------------------
    # MEMBERSHIP / HOUSE URI
    # -------------------------------------------------------------------------
    'member_memberships_0_membership_uri',        # API URI, no analytical value
    'member_memberships_0_membership_house_uri',  # API URI, no analytical value

    # -------------------------------------------------------------------------
    # CONSTITUENCY / REPRESENTS
    # -------------------------------------------------------------------------
    'member_memberships_0_membership_represents_0_represent_uri',            # API URI
    'member_memberships_0_membership_represents_0_represent_representType',  # always "constituency", zero variance

    # -------------------------------------------------------------------------
    # PARTY URIs (all three parties)
    # -------------------------------------------------------------------------
    'member_memberships_0_membership_parties_0_party_uri',  # API URI for primary party
    'member_memberships_0_membership_parties_1_party_uri',  # API URI for alternate party 1
    'member_memberships_0_membership_parties_2_party_uri',  # API URI for alternate party 2

    # -------------------------------------------------------------------------
    # OFFICE URIs (all six offices)
    # -------------------------------------------------------------------------
    'member_memberships_0_membership_offices_0_office_officeName_uri',  # API URI, name already captured in showAs
    'member_memberships_0_membership_offices_1_office_officeName_uri',  # API URI
    'member_memberships_0_membership_offices_2_office_officeName_uri',  # API URI
    'member_memberships_0_membership_offices_3_office_officeName_uri',  # API URI
    'member_memberships_0_membership_offices_4_office_officeName_uri',  # API URI
    'member_memberships_0_membership_offices_5_office_officeName_uri',  # API URI

    # -------------------------------------------------------------------------
    # RAW UNFLATTENED BLOBS
    # These are the original nested objects before flatten_json expanded them.
    # All their fields are captured in the individual committee/office columns.
    # -------------------------------------------------------------------------
    'member_memberships_0_membership_offices',     # raw blob, replaced by offices_0..5 columns
    'member_memberships_0_membership_committees',  # raw blob, replaced by committees_0..11 columns

    # -------------------------------------------------------------------------
    # COMMITTEE URIs (all 12 committees)
    # -------------------------------------------------------------------------
    'member_memberships_0_membership_committees_0_uri',   # API URI
    'member_memberships_0_membership_committees_1_uri',   # API URI
    'member_memberships_0_membership_committees_2_uri',   # API URI
    'member_memberships_0_membership_committees_3_uri',   # API URI
    'member_memberships_0_membership_committees_4_uri',   # API URI
    'member_memberships_0_membership_committees_5_uri',   # API URI
    'member_memberships_0_membership_committees_6_uri',   # API URI
    'member_memberships_0_membership_committees_7_uri',   # API URI
    'member_memberships_0_membership_committees_8_uri',   # API URI
    'member_memberships_0_membership_committees_9_uri',   # API URI
    'member_memberships_0_membership_committees_10_uri',  # API URI
    'member_memberships_0_membership_committees_11_uri',  # API URI

    # -------------------------------------------------------------------------
    # COMMITTEE HOUSE CODE / HOUSE NO (redundant per committee row)
    # These repeat the member-level dail_code / dail_number for every committee.
    # No additional information vs the top-level house fields.
    #  -------------------------------------------------------------------------
    'member_memberships_0_membership_committees_0_houseCode',
    'member_memberships_0_membership_committees_0_houseNo',
    'member_memberships_0_membership_committees_1_houseCode',
    'member_memberships_0_membership_committees_1_houseNo',
    'member_memberships_0_membership_committees_2_houseCode',
    'member_memberships_0_membership_committees_2_houseNo',
    'member_memberships_0_membership_committees_3_houseCode',
    'member_memberships_0_membership_committees_3_houseNo',
    'member_memberships_0_membership_committees_4_houseCode',
    'member_memberships_0_membership_committees_4_houseNo',
    # -------------------------------------------------------------------------
    # COMMITTEE SERVICE UNIT (redundant per committee row)
    # These repeat the committee-level service_unit /  for every committee.
    # No additional information vs the top-level house fields.
    # data sample: 'Committees' Secretariat', 'Journal Office' 
    # the above values are the two main values identified for the Dail
    # Committees’ Secretariat:
    # Organises committee meetings
    # Prepares briefing materials
    # Coordinates witnesses and reports

    # Core role: keeps committees functioning (where detailed scrutiny happens)
    # Journal Office
    # Prepares the daily agenda (Order Paper)
    # Records official decisions of the Dáil
    # Advises on parliamentary procedure
    # Core role: rules + official record of what happens in the chamber
    # -------------------------------------------------------------------------
    'committee_1_service_unit',
    'committee_2_service_unit',
    'committee_3_service_unit',
    'committee_4_service_unit',
    'committee_5_service_unit',
    'committee_6_service_unit',
    'committee_7_service_unit',
    'committee_8_service_unit',
    'committee_9_service_unit',
    'committee_10_service_unit',
    'committee_11_service_unit',
    'committee_12_service_unit'
    # -------------------------------------------------------------------------
    #Committee role is totally null in the dataset so dropping all 12 columns
    'committee_1_role',
    'committee_2_role',
    'committee_1_role',
    'committee_2_role',
    'committee_3_role',
    'committee_4_role',
    'committee_5_role',
    'committee_6_role',
    'committee_7_role',
    'committee_8_role',
    'committee_9_role',
    'committee_10_rol',
    'committee_11_rol',
    'committee_12_rol',
    #temporarily drop Irish named committes to simply the dataset, add back later
    #TODO create button to switch to Irish named committees in the dashboard
    'committee_1_name_ga',
    'committee_2_name_ga',
    'committee_3_name_ga',
    'committee_4_name_ga',
    'committee_5_name_ga',
    'committee_6_name_ga',
    'committee_7_name_ga',
    'committee_8_name_ga',
    'committee_9_name_ga',
    'committee_10_name_ga',
    'committee_11_name_ga',
    'committee_12_name_ga',
    
    'committee_1_alt_name_ga',
    'committee_2_alt_name_ga',
    'committee_3_alt_name_ga',
    'committee_4_alt_name_ga',
    'committee_6_alt_name_ga',
    'committee_8_alt_name_ga',
    'committee_6_alt_name_ga',
    #temporarily drop alternate committee ids, there are enough unique ones to simply the dataset, add back later
    'committee_1_id',
    'committee_2_id',
    'committee_3_id',
    'committee_4_id',
    'committee_5_id',
    'committee_6_id',
    'committee_7_id',
    'committee_8_id',
    'committee_9_id',
    'committee_10_id',
    'committee_11_id',
    'committee_12_id',
    # ditto for committee codes
    'committee_1_code',
    'committee_2_code',
    'committee_3_code',
    'committee_4_code',
    'committee_5_code',
    'committee_6_code',
    'committee_7_code',
    'committee_8_code',
    'committee_9_code',
    'committee_10_code',
    'committee_11_code',
    'committee_12_code'

]


# =============================================================================
# ENRICHMENT COLS TO SELECT
# Core member info for joining/enriching bills, lobbying etc.
# Uses RENAMED column names.
# =============================================================================

enrichment_cols_to_select = [
    # Core identity
    'unique_member_code',
    'full_name',
    'first_name',
    'last_name',
    'gender',
    'date_of_death',

    # Dail / membership
    'membership_start_date',
    'membership_end_date',
    'dail_code',
    'dail_term',
    'dail_number',
    'dail_chamber_type',

    # Constituency
    'constituency_name',
    'constituency_code',

    # Primary party
    'party',
    'party_code',
    'party_start_date',
    'party_end_date',

    # Alternate parties
    'party_1_name',
    'party_1_code',
    'party_1_start_date',
    'party_1_end_date',
    'party_2_name',
    'party_2_code',
    'party_2_start_date',
    'party_2_end_date',

    # Offices (1 = primary/ministerial, 2-6 = additional roles)
    'office_1_name',
    'office_1_start_date',
    'office_1_end_date',
    'office_2_name',
    'office_2_start_date',
    'office_2_end_date',
    'office_3_name',
    'office_3_start_date',
    'office_3_end_date',
    'office_4_name',
    'office_4_start_date',
    'office_4_end_date',
    'office_5_name',
    'office_5_start_date',
    'office_5_end_date',
    'office_6_name',
    'office_6_start_date',
    'office_6_end_date',
]


# =============================================================================
# COMMITTEES COLS TO SELECT
# Full committee membership detail per member.
# Uses RENAMED column names.
# =============================================================================

committees_cols_to_select = [
    'unique_member_code',
    'full_name',

    # Committee 1
    # 'committee_1_code',
    # 'committee_1_service_unit',
    # 'committee_1_role',
    'committee_1_member_start_date',
    'committee_1_member_end_date',
    'committee_1_main_status',
    'committee_1_status',
    'committee_1_start_date',
    'committee_1_end_date',
    'committee_1_type',
    'committee_1_expiry_type',
    'committee_1_name_en',
    # 'committee_1_name_ga',
    'committee_1_name_start_date',
    'committee_1_name_end_date',
    # 'committee_1_id',
    'committee_1_role_start_date',
    'committee_1_role_end_date',
    'committee_1_role_title',
    'committee_1_alt_name_en',
    # 'committee_1_alt_name_ga',
    'committee_1_alt_name_start_date',
    'committee_1_alt_name_end_date',

    # Committee 2
    # 'committee_2_code',
    # 'committee_2_service_unit',
    # 'committee_2_role',
    'committee_2_member_start_date',
    'committee_2_member_end_date',
    'committee_2_main_status',
    'committee_2_status',
    'committee_2_start_date',
    'committee_2_end_date',
    'committee_2_type',
    'committee_2_expiry_type',
    'committee_2_name_en',
    # 'committee_2_name_ga',
    'committee_2_name_start_date',
    'committee_2_name_end_date',
    # 'committee_2_id',
    'committee_2_role_start_date',
    'committee_2_role_end_date',
    'committee_2_role_title',
    'committee_2_alt_name_en',
    # 'committee_2_alt_name_ga',
    'committee_2_alt_name_start_date',
    'committee_2_alt_name_end_date',

    # Committee 3
    # 'committee_3_code',
    # 'committee_3_service_unit',
    # 'committee_3_role',
    'committee_3_member_start_date',
    'committee_3_member_end_date',
    'committee_3_main_status',
    'committee_3_status',
    'committee_3_start_date',
    'committee_3_end_date',
    'committee_3_type',
    'committee_3_expiry_type',
    'committee_3_name_en',
    # 'committee_3_name_ga',
    'committee_3_name_start_date',
    'committee_3_name_end_date',
    # 'committee_3_id',
    'committee_3_role_start_date',
    'committee_3_role_end_date',
    'committee_3_role_title',
    'committee_3_alt_name_en',
    # 'committee_3_alt_name_ga',
    'committee_3_alt_name_start_date',
    'committee_3_alt_name_end_date',

    # Committee 4
    # 'committee_4_code',
    # 'committee_4_service_unit',
    # 'committee_4_role',
    'committee_4_member_start_date',
    'committee_4_member_end_date',
    'committee_4_main_status',
    'committee_4_status',
    'committee_4_start_date',
    'committee_4_end_date',
    'committee_4_type',
    'committee_4_expiry_type',
    'committee_4_name_en',
    # 'committee_4_name_ga',
    'committee_4_name_start_date',
    'committee_4_name_end_date',
    # 'committee_4_id',
    'committee_4_role_start_date',
    'committee_4_role_end_date',
    'committee_4_role_title',
    'committee_4_alt_name_en',
    # 'committee_4_alt_name_ga',
    'committee_4_alt_name_start_date',
    'committee_4_alt_name_end_date',

    # Committee 5
    # 'committee_5_code',
    # 'committee_5_service_unit',
    # 'committee_5_role',
    'committee_5_member_start_date',
    'committee_5_member_end_date',
    'committee_5_main_status',
    'committee_5_status',
    'committee_5_start_date',
    'committee_5_end_date',
    'committee_5_type',
    'committee_5_expiry_type',
    'committee_5_name_en',
    # 'committee_5_name_ga',
    'committee_5_name_start_date',
    'committee_5_name_end_date',
    # 'committee_5_id',
    'committee_5_role_start_date',
    'committee_5_role_end_date',
    'committee_5_role_title',

    # Committee 6
    # 'committee_6_code',
    # 'committee_6_service_unit',
    # 'committee_6_role',
    'committee_6_member_start_date',
    'committee_6_member_end_date',
    'committee_6_main_status',
    'committee_6_status',
    'committee_6_start_date',
    'committee_6_end_date',
    'committee_6_type',
    'committee_6_expiry_type',
    'committee_6_name_en',
    # 'committee_6_name_ga',
    'committee_6_name_start_date',
    'committee_6_name_end_date',
    # 'committee_6_id',
    'committee_6_role_start_date',
    'committee_6_role_end_date',
    'committee_6_role_title',
    'committee_6_alt_name_en',
    # 'committee_6_alt_name_ga',
    'committee_6_alt_name_start_date',
    'committee_6_alt_name_end_date',

    # Committee 7
    # 'committee_7_code',
    # 'committee_7_service_unit',
    # 'committee_7_role',
    'committee_7_member_start_date',
    'committee_7_member_end_date',
    'committee_7_main_status',
    'committee_7_status',
    'committee_7_start_date',
    'committee_7_end_date',
    'committee_7_type',
    'committee_7_expiry_type',
    'committee_7_name_en',
    # 'committee_7_name_ga',
    'committee_7_name_start_date',
    'committee_7_name_end_date',
    # 'committee_7_id',
    'committee_7_role_start_date',
    'committee_7_role_end_date',
    'committee_7_role_title',

    # Committee 8
    # 'committee_8_code',
    # 'committee_8_service_unit',
    # 'committee_8_role',
    'committee_8_member_start_date',
    'committee_8_member_end_date',
    'committee_8_main_status',
    'committee_8_status',
    'committee_8_start_date',
    'committee_8_end_date',
    'committee_8_type',
    'committee_8_expiry_type',
    'committee_8_name_en',
    # 'committee_8_name_ga',
    'committee_8_name_start_date',
    'committee_8_name_end_date',
    # 'committee_8_id',
    'committee_8_role_start_date',
    'committee_8_role_end_date',
    'committee_8_role_title',
    'committee_8_alt_name_en',
    # 'committee_8_alt_name_ga',
    'committee_8_alt_name_start_date',
    'committee_8_alt_name_end_date',

    # Committee 9
    # 'committee_9_code',
    # 'committee_9_service_unit',
    # 'committee_9_role',
    'committee_9_member_start_date',
    'committee_9_member_end_date',
    'committee_9_main_status',
    'committee_9_status',
    'committee_9_start_date',
    'committee_9_end_date',
    'committee_9_type',
    'committee_9_expiry_type',
    'committee_9_name_en',
    # 'committee_9_name_ga',
    'committee_9_name_start_date',
    'committee_9_name_end_date',
    # 'committee_9_id',

    # Committee 10
    # 'committee_10_code',
    # 'committee_10_service_unit',
    # 'committee_10_role',
    'committee_10_member_start_date',
    'committee_10_member_end_date',
    'committee_10_main_status',
    'committee_10_status',
    'committee_10_start_date',
    'committee_10_end_date',
    'committee_10_type',
    'committee_10_expiry_type',
    'committee_10_name_en',
    # 'committee_10_name_ga',
    'committee_10_name_start_date',
    'committee_10_name_end_date',
    # 'committee_10_id',

    # Committee 11
    # 'committee_11_code',
    # 'committee_11_service_unit',
    # 'committee_11_role',
    'committee_11_member_start_date',
    'committee_11_member_end_date',
    'committee_11_main_status',
    'committee_11_status',
    'committee_11_start_date',
    'committee_11_end_date',
    'committee_11_type',
    'committee_11_expiry_type',
    'committee_11_name_en',
    # 'committee_11_name_ga',
    'committee_11_name_start_date',
    'committee_11_name_end_date',
    # 'committee_11_id',

    # Committee 12
    # 'committee_12_code',
    # 'committee_12_service_unit',
    # 'committee_12_role',
    'committee_12_member_start_date',
    'committee_12_member_end_date',
    'committee_12_main_status',
    'committee_12_status',
    'committee_12_start_date',
    'committee_12_end_date',
    'committee_12_type',
    'committee_12_expiry_type',
    'committee_12_name_en',
    # 'committee_12_name_ga',
    'committee_12_name_start_date',
    'committee_12_name_end_date',
    # 'committee_12_id',
    'committee_12_role_start_date',
    'committee_12_role_end_date',
    'committee_12_role_title',
]