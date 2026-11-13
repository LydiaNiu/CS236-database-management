CREATE TABLE NYC (
    unique_key BIGINT PRIMARY KEY,
    created_date TIMESTAMP,
    closed_date TIMESTAMP,
    closed_date_original TIMESTAMP,
    agency TEXT,
    agency_name TEXT,
    complaint_type TEXT,
    complaint_type_original TEXT,
    descriptor TEXT,
    descriptor_original TEXT,
    descriptor_2 TEXT,
    descriptor_2_original TEXT,
    location_type TEXT,
    location_type_original TEXT,
    incident_zip TEXT,
    incident_zip_original TEXT,
    incident_address TEXT,
    street_name TEXT,
    cross_street_1 TEXT,
    cross_street_2 TEXT,
    intersection_street_1 TEXT,
    intersection_street_2 TEXT,
    address_type TEXT,
    city TEXT,
    city_original TEXT,
    landmark TEXT,
    facility_type TEXT,
    status TEXT,
    status_original TEXT,
    due_date TIMESTAMP,
    resolution_description TEXT,
    resolution_action_updated_date TIMESTAMP,
    community_board TEXT,
    community_board_original TEXT,
    council_district TEXT,
    police_precinct TEXT,
    police_precinct_original TEXT,
    bbl TEXT,
    borough TEXT,
    borough_original TEXT,
    x_coordinate_state_plane DOUBLE PRECISION,
    y_coordinate_state_plane DOUBLE PRECISION,
    open_data_channel_type TEXT,
    open_data_channel_type_original TEXT,
    park_facility_name TEXT,
    park_borough TEXT,
    vehicle_type TEXT,
    taxi_company_borough TEXT,
    taxi_pick_up_location TEXT,
    bridge_highway_name TEXT,
    bridge_highway_direction TEXT,
    road_ramp TEXT,
    bridge_highway_segment TEXT,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    location TEXT,
    created_ts TIMESTAMP,
    cleaned_status TEXT,
    closed_without_date BOOLEAN,
    closed_before_created BOOLEAN,
    status_changed_to_closed BOOLEAN,
    is_suspected_duplicate BOOLEAN,
    is_valid_resolution BOOLEAN,
    resolution_hours NUMERIC(10, 4)
);

CREATE TABLE LA (
    srnumber TEXT PRIMARY KEY,
    createddate TIMESTAMP,
    updateddate TIMESTAMP,
    actiontaken TEXT,
    owner TEXT,
    requesttype TEXT,
    status TEXT,
    status_original TEXT,
    requestsource TEXT,
    createdbyuserorganization TEXT,
    mobileos TEXT,
    anonymous BOOLEAN,
    assignto TEXT,
    servicedate TIMESTAMP,
    closeddate TIMESTAMP,
    addressverified BOOLEAN,
    approximateaddress TEXT,
    address TEXT,
    housenumber TEXT,
    direction TEXT,
    streetname TEXT,
    suffix TEXT,
    suffix_original TEXT,
    zipcode TEXT,
    zipcode_original TEXT,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    location TEXT,
    tbmpage TEXT,
    tbmpage_original TEXT,
    tbmcolumn TEXT,
    tbmrow TEXT,
    apc TEXT,
    cd TEXT,
    cdmember TEXT,
    nc TEXT,
    ncname TEXT,
    policeprecinct TEXT,
    created_ts TIMESTAMP,
    cleaned_status TEXT,
    closed_without_date BOOLEAN,
    closed_before_created BOOLEAN,
    status_changed_to_closed BOOLEAN,
    is_valid_resolution BOOLEAN,
    resolution_hours NUMERIC(10, 4)
);

CREATE TABLE unified (
    uid TEXT PRIMARY KEY,
    source TEXT NOT NULL,
    id TEXT NOT NULL,
    created TIMESTAMP,
    closed TIMESTAMP,
    updated_date TIMESTAMP,
    status TEXT,
    status_detail TEXT,
    request_type TEXT,
    request_type_original TEXT,
    channel TEXT,
    channel_original TEXT,
    zip TEXT,
    agency TEXT,
    address TEXT,
    street_name TEXT,
    police_precinct TEXT,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    location TEXT,
    resolution_hours NUMERIC(10, 4),
    is_valid_resolution BOOLEAN,
    is_suspected_duplicate BOOLEAN
);

-- cleaned_la:
--   srnumber: object
--   createddate: object
--   updateddate: object
--   actiontaken: object
--   owner: object
--   requesttype: object
--   status: object
--   status_original: object
--   requestsource: object
--   createdbyuserorganization: object
--   mobileos: object
--   anonymous: object
--   assignto: object
--   servicedate: object
--   closeddate: object
--   addressverified: object
--   approximateaddress: object
--   address: object
--   housenumber: float64
--   direction: object
--   streetname: object
--   suffix: object
--   suffix_original: object
--   zipcode: float64
--   zipcode_original: object
--   latitude: float64
--   longitude: float64
--   location: object
--   tbmpage: float64
--   tbmpage_original: float64
--   tbmcolumn: object
--   tbmrow: float64
--   apc: object
--   cd: float64
--   cdmember: object
--   nc: float64
--   ncname: object
--   policeprecinct: object
--   created_ts: object
--   cleaned_status: object
--   closed_without_date: bool
--   closed_before_created: bool
--   status_changed_to_closed: bool
--   is_valid_resolution: bool
--   resolution_hours: float64

-- cleaned_nyc:
--   unique_key: int64
--   created_date: object
--   closed_date: object
--   closed_date_original: object
--   agency: object
--   agency_name: object
--   complaint_type: object
--   complaint_type_original: object
--   descriptor: object
--   descriptor_original: object
--   descriptor_2: object
--   descriptor_2_original: object
--   location_type: object
--   location_type_original: object
--   incident_zip: float64
--   incident_zip_original: object
--   incident_address: object
--   street_name: object
--   cross_street_1: object
--   cross_street_2: object
--   intersection_street_1: object
--   intersection_street_2: object
--   address_type: object
--   city: object
--   city_original: object
--   landmark: object
--   facility_type: object
--   status: object
--   status_original: object
--   due_date: object
--   resolution_description: object
--   resolution_action_updated_date: object
--   community_board: object
--   community_board_original: object
--   council_district: float64
--   police_precinct: object
--   police_precinct_original: object
--   bbl: float64
--   borough: object
--   borough_original: object
--   x_coordinate_state_plane: float64
--   y_coordinate_state_plane: float64
--   open_data_channel_type: object
--   open_data_channel_type_original: object
--   park_facility_name: object
--   park_borough: object
--   vehicle_type: object
--   taxi_company_borough: object
--   taxi_pick_up_location: object
--   bridge_highway_name: object
--   bridge_highway_direction: object
--   road_ramp: object
--   bridge_highway_segment: object
--   latitude: float64
--   longitude: float64
--   location: object
--   created_ts: object
--   cleaned_status: object
--   closed_without_date: bool
--   closed_before_created: bool
--   status_changed_to_closed: bool
--   is_suspected_duplicate: bool
--   is_valid_resolution: bool
--   resolution_hours: float64

-- unified:
--   uid: object
--   source: object
--   id: object
--   created: object
--   closed: object
--   updated_date: object
--   status: object
--   status_detail: object
--   request_type: object
--   request_type_original: object
--   channel: object
--   channel_original: object
--   zip: float64
--   agency: object
--   address: object
--   street_name: object
--   police_precinct: object
--   latitude: float64
--   longitude: float64
--   location: object
--   resolution_hours: float64
--   is_valid_resolution: bool
--   is_suspected_duplicate: bool