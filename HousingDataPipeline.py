import pandas as pd
import psycopg2

def main():

  #createSourceTable()
  #createDestinationTable()

  #readingInitialSourceCSVFile()
  #settingUpInitialDataInsert()

  #readingInitialSourceCSVFile()
  #readingNextIncrementalLoadCSVFile()
  #findingNewNetListing()

  readingInitialSourceCSVFile()
  readingNextIncrementalLoadCSVFile()
  findingTrackedVariableChanged()


def settingCSVFilePath():
  global CSVFile 
  CSVFile  = "/Users/danielvaughan/Desktop/Python/sample_data.csv"

#Reading from source data csv, 1000 records at a time & Creating DF SourceData
def readingInitialSourceCSVFile():

  global sourceData
  batchs = []
  for batch in pd.read_csv('/Users/danielvaughan/Desktop/Python/sample_data_Daniel.csv', chunksize=1000):
    batchs.append(batch)
  sourceData = pd.concat(batchs, ignore_index=True)
  #Replace NaN with None
  sourceData = sourceData.where(pd.notnull(sourceData), None)

#Reading Next Incremental Load from CSV of 1000 or less & creating DF UpdatedData
def readingNextIncrementalLoadCSVFile():
  global updatedData
  batchs = []
  for batch in pd.read_csv('/Users/danielvaughan/Desktop/Python/sample_data_updated.csv', chunksize=1000):
    batchs.append(batch)
  updatedData = pd.concat(batchs, ignore_index=True)
  #Replace NaN with None
  updatedData  = updatedData.where(pd.notnull(updatedData), None)
  #print("Updated", updatedData)


def findingNewNetListing():
 
  #Looping over DF of inital property Id's in source data
  sourceListingIds = []
  for row in sourceData.itertuples():
    propertyID = row.property_id
    sourceListingIds.append(propertyID)  
  #print("sourceListing")

  #Finding all possbile new property Id's in new csv file
  possibleNewNetListingIds = []
  for row in updatedData.itertuples():
    #Finding all property Id's in new csv file
    propertyID = row.property_id
    possibleNewNetListingIds.append(propertyID)  
  #print("possibleNewNetListing") 

  #Identifing new property Id's
  newNetListingIds = []
  for propertyNum in possibleNewNetListingIds:
    if propertyNum not in sourceListingIds:
      newNetListingIds.append(propertyNum)
  #print(newNetListingIds)

  #extracting entire row for inserting in Destination table
  #newEntryListing = []
  for propertyId in newNetListingIds:
    for row in updatedData.itertuples():
      if propertyId == row.property_id:
        newEntryListing = []
        newEntryListing.append([row.property_id,row.display_address, row.zip_code, row.building_name, row.property_type, row.property_sub_type, row.style, row.acres, row.square_feet, row.year_built, row.total_bedrooms, row.total_bathrooms, row.garage_spaces, row.lat, row.lng, row.area_name, row.city, row.county, row.market_name, row.state, row.original_listing_price, row.listing_price, row.sold_price, row.list_date, row.pending_date, row.sold_date, row.listing_agent_id, row.co_listing_agent_id, row.selling_agent_id, row.co_selling_agent_id, row.listing_office_id, row.co_listing_office_id, row.selling_office_id, row.co_selling_office_id, row.last_update_date])
        insertingNewNetListing(newEntryListing[0])
        #print(newEntryListing)

def insertingNewNetListing(newEntryToInsert):
  #print(newEntryToInsert[25])
  try:

    con = psycopg2.connect(database="source_db_name", user="source_db_user", password="SOURCE_DB_PASSWORD", host="127.0.0.1", port="5432")
    print("Database opened successfully")
    cur = con.cursor()
    #cur.execute("INSERT INTO source_table (property_id,display_address,zip_code,building_name,property_type,property_sub_type,style,acres,square_feet,year_built,total_bedrooms,total_bathrooms,garage_spaces,lat,lng,area_name,city,county,market_name,state,original_listing_price,listing_price,sold_price,list_date,pending_date,sold_date,listing_agent_id,co_listing_agent_id,selling_agent_id,co_selling_agent_id,listing_office_id,co_listing_office_id,selling_office_id,co_selling_office_id,last_update_date) VALUES (3420, 'John', 18, 'Computer Science', 'ICT')")
    postgres_insert_query = """ INSERT INTO destination_table (property_id,display_address,zip_code,building_name,property_type,property_sub_type,style,acres,square_feet,year_built,total_bedrooms,total_bathrooms,garage_spaces,lat,lng,area_name,city,county,market_name,state,original_listing_price,listing_price,sold_price,list_date,pending_date,sold_date,listing_agent_id,co_listing_agent_id,selling_agent_id,co_selling_agent_id,listing_office_id,co_listing_office_id,selling_office_id,co_selling_office_id,last_update_date) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
    record_to_insert = (newEntryToInsert[0], newEntryToInsert[1], newEntryToInsert[2], newEntryToInsert[3], newEntryToInsert[4], newEntryToInsert[5], newEntryToInsert[6], newEntryToInsert[7], newEntryToInsert[8], newEntryToInsert[9], newEntryToInsert[10], newEntryToInsert[11], newEntryToInsert[12], newEntryToInsert[13], newEntryToInsert[14], newEntryToInsert[15], newEntryToInsert[16], newEntryToInsert[17], newEntryToInsert[18], newEntryToInsert[19], newEntryToInsert[20], newEntryToInsert[21], newEntryToInsert[22], newEntryToInsert[23], newEntryToInsert[24], (newEntryToInsert[25]), newEntryToInsert[26], newEntryToInsert[27], newEntryToInsert[28], newEntryToInsert[29], newEntryToInsert[30], newEntryToInsert[31], newEntryToInsert[32], newEntryToInsert[33], newEntryToInsert[34])
    cur.execute(postgres_insert_query, record_to_insert)

    con.commit()
    print("Record inserted successfully")
  
  except (Exception, psycopg2.Error) as error :
    if(con):
        print("Failed to insert record into table:", error)

  finally:
    #closing database connection.
    if(con):
        #cur.close()
        con.close()
        print("PostgreSQL connection is closed")

def findingTrackedVariableChanged():
  #Looping over DF inital values in source data and looking at tracked values
  #sourceTrackedFields = []
  for row in sourceData.itertuples():
    sourceTrackedFields = []
    propertyId = row.property_id
    listingPrice = row.listing_price
    soldPrice = row.sold_price
    listDate = row.list_date
    pendingDate = row.pending_date
    soldDate = row.sold_date
    listingAgent = row.listing_agent_id
    coListingAgent = row.co_listing_agent_id
    sellingAgent = row.selling_agent_id
    coSellingAgent = row.co_selling_agent_id
    listingOffice = row.listing_office_id
    coListingOffice = row.co_listing_office_id
    sellingOffice = row.selling_office_id
    coSellingOffice = row.co_selling_office_id

    sourceTrackedFields.append([propertyId, listingPrice, soldPrice, listDate, pendingDate, soldDate, listingAgent, coListingAgent, sellingAgent,  coSellingAgent, listingOffice, coListingOffice, sellingOffice, coSellingOffice])  
    #print(sourceTrackedFields)


    for row in updatedData.itertuples():
      propertyId = row.property_id
      if sourceTrackedFields[0][0] == propertyId:

        listingPrice = row.listing_price
        soldPrice = row.sold_price
        listDate = row.list_date
        pendingDate = row.pending_date
        soldDate = row.sold_date
        listingAgent = row.listing_agent_id
        coListingAgent = row.co_listing_agent_id
        sellingAgent = row.selling_agent_id
        coSellingAgent = row.co_selling_agent_id
        listingOffice = row.listing_office_id
        coListingOffice = row.co_listing_office_id
        sellingOffice = row.selling_office_id
        coSellingOffice = row.co_selling_office_id

        if sourceTrackedFields[0][1] != listingPrice or sourceTrackedFields[0][2] != soldPrice or sourceTrackedFields[0][3] != listDate or sourceTrackedFields[0][4] != pendingDate or sourceTrackedFields[0][5] != soldDate or sourceTrackedFields[0][6] != listingAgent or sourceTrackedFields[0][7] != coListingAgent or sourceTrackedFields[0][8] != sellingAgent or sourceTrackedFields[0][9] != coSellingAgent or sourceTrackedFields[0][10] != listingOffice or sourceTrackedFields[0][11] != coListingOffice or sourceTrackedFields[0][12] != sellingOffice or sourceTrackedFields[0][13] != coSellingOffice:
          print("not Matching")

          newEntryListing = []
          newEntryListing.append([row.property_id,row.display_address, row.zip_code, row.building_name, row.property_type, row.property_sub_type, row.style, row.acres, row.square_feet, row.year_built, row.total_bedrooms, row.total_bathrooms, row.garage_spaces, row.lat, row.lng, row.area_name, row.city, row.county, row.market_name, row.state, row.original_listing_price, row.listing_price, row.sold_price, row.list_date, row.pending_date, row.sold_date, row.listing_agent_id, row.co_listing_agent_id, row.selling_agent_id, row.co_selling_agent_id, row.listing_office_id, row.co_listing_office_id, row.selling_office_id, row.co_selling_office_id, row.last_update_date])
          insertingNewNetListing(newEntryListing[0])

        #else:
          #print("Matching")
        
      #break
    #break

def insertingUpdatedDataIntoDestinationTable(newEntryToInsert):
  try:

    con = psycopg2.connect(database="source_db_name", user="source_db_user", password="SOURCE_DB_PASSWORD", host="127.0.0.1", port="5432")
    print("Database opened successfully")
    cur = con.cursor()
    #cur.execute("INSERT INTO source_table (property_id,display_address,zip_code,building_name,property_type,property_sub_type,style,acres,square_feet,year_built,total_bedrooms,total_bathrooms,garage_spaces,lat,lng,area_name,city,county,market_name,state,original_listing_price,listing_price,sold_price,list_date,pending_date,sold_date,listing_agent_id,co_listing_agent_id,selling_agent_id,co_selling_agent_id,listing_office_id,co_listing_office_id,selling_office_id,co_selling_office_id,last_update_date) VALUES (3420, 'John', 18, 'Computer Science', 'ICT')")
    postgres_insert_query = """ INSERT INTO destination_table (property_id,display_address,zip_code,building_name,property_type,property_sub_type,style,acres,square_feet,year_built,total_bedrooms,total_bathrooms,garage_spaces,lat,lng,area_name,city,county,market_name,state,original_listing_price,listing_price,sold_price,list_date,pending_date,sold_date,listing_agent_id,co_listing_agent_id,selling_agent_id,co_selling_agent_id,listing_office_id,co_listing_office_id,selling_office_id,co_selling_office_id,last_update_date) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
    record_to_insert = (newEntryToInsert[0], newEntryToInsert[1], newEntryToInsert[2], newEntryToInsert[3], newEntryToInsert[4], newEntryToInsert[5], newEntryToInsert[6], newEntryToInsert[7], newEntryToInsert[8], newEntryToInsert[9], newEntryToInsert[10], newEntryToInsert[11], newEntryToInsert[12], newEntryToInsert[13], newEntryToInsert[14], newEntryToInsert[15], newEntryToInsert[16], newEntryToInsert[17], newEntryToInsert[18], newEntryToInsert[19], newEntryToInsert[20], newEntryToInsert[21], newEntryToInsert[22], newEntryToInsert[23], newEntryToInsert[24], (newEntryToInsert[25]), newEntryToInsert[26], newEntryToInsert[27], newEntryToInsert[28], newEntryToInsert[29], newEntryToInsert[30], newEntryToInsert[31], newEntryToInsert[32], newEntryToInsert[33], newEntryToInsert[34])
    cur.execute(postgres_insert_query, record_to_insert)

    con.commit()
    print("Record inserted successfully")
  
  except (Exception, psycopg2.Error) as error :
    if(con):
        print("Failed to insert record into table:", error)

  finally:
    #closing database connection.
    if(con):
        #cur.close()
        con.close()
        print("PostgreSQL connection is closed")




def settingUpInitialDataInsert():
   for row in sourceData.itertuples():
     newEntryListing = []
     newEntryListing.append([row.property_id,row.display_address, row.zip_code, row.building_name, row.property_type, row.property_sub_type, row.style, row.acres, row.square_feet, row.year_built, row.total_bedrooms, row.total_bathrooms, row.garage_spaces, row.lat, row.lng, row.area_name, row.city, row.county, row.market_name, row.state, row.original_listing_price, row.listing_price, row.sold_price, row.list_date, row.pending_date, row.sold_date, row.listing_agent_id, row.co_listing_agent_id, row.selling_agent_id, row.co_selling_agent_id, row.listing_office_id, row.co_listing_office_id, row.selling_office_id, row.co_selling_office_id, row.last_update_date])
     insertingInitialDataIntoSourceTable(newEntryListing[0])

def insertingInitialDataIntoSourceTable(newEntryToInsert):
 
  try:
    
    con = psycopg2.connect(database="source_db_name", user="source_db_user", password="SOURCE_DB_PASSWORD", host="127.0.0.1", port="5432")
    #print("Database opened successfully")
    cur = con.cursor()
    #cur.execute("INSERT INTO source_table (property_id,display_address,zip_code,building_name,property_type,property_sub_type,style,acres,square_feet,year_built,total_bedrooms,total_bathrooms,garage_spaces,lat,lng,area_name,city,county,market_name,state,original_listing_price,listing_price,sold_price,list_date,pending_date,sold_date,listing_agent_id,co_listing_agent_id,selling_agent_id,co_selling_agent_id,listing_office_id,co_listing_office_id,selling_office_id,co_selling_office_id,last_update_date) VALUES (3420, 'John', 18, 'Computer Science', 'ICT')")
    postgres_insert_query = """ INSERT INTO source_table (property_id,display_address,zip_code,building_name,property_type,property_sub_type,style,acres,square_feet,year_built,total_bedrooms,total_bathrooms,garage_spaces,lat,lng,area_name,city,county,market_name,state,original_listing_price,listing_price,sold_price,list_date,pending_date,sold_date,listing_agent_id,co_listing_agent_id,selling_agent_id,co_selling_agent_id,listing_office_id,co_listing_office_id,selling_office_id,co_selling_office_id,last_update_date) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
    record_to_insert = (newEntryToInsert[0], newEntryToInsert[1], newEntryToInsert[2], newEntryToInsert[3], newEntryToInsert[4], newEntryToInsert[5], newEntryToInsert[6], newEntryToInsert[7], newEntryToInsert[8], newEntryToInsert[9], newEntryToInsert[10], newEntryToInsert[11], newEntryToInsert[12], newEntryToInsert[13], newEntryToInsert[14], newEntryToInsert[15], newEntryToInsert[16], newEntryToInsert[17], newEntryToInsert[18], newEntryToInsert[19], newEntryToInsert[20], newEntryToInsert[21], newEntryToInsert[22], newEntryToInsert[23], newEntryToInsert[24], (newEntryToInsert[25]), newEntryToInsert[26], newEntryToInsert[27], newEntryToInsert[28], newEntryToInsert[29], newEntryToInsert[30], newEntryToInsert[31], newEntryToInsert[32], newEntryToInsert[33], newEntryToInsert[34])
    cur.execute(postgres_insert_query, record_to_insert)

    con.commit()
    print("Record inserted successfully")
  
  except (Exception, psycopg2.Error) as error :
    if(con):
        print("Failed to insert record into table:", error)

  finally:
    #closing database connection.
    if(con):
        #cur.close()
        con.close()
        #print("PostgreSQL connection is closed")

#Creating Source Data Table
def createSourceTable():
  """ create source tables in the PostgreSQL database """
  try:
    con = psycopg2.connect(database="source_db_name", user="source_db_user", password="SOURCE_DB_PASSWORD", host="127.0.0.1", port="5432")
    print("Database opened successfully")

    cur = con.cursor()
    cur.execute('''CREATE TABLE source_table (
      property_id VARCHAR(255) PRIMARY KEY NOT NULL,
      display_address VARCHAR(255),
      zip_code VARCHAR(30),
      building_name VARCHAR(255),
      property_type VARCHAR(255),
      property_sub_type VARCHAR(255),
      style VARCHAR(255),
      acres NUMERIC(10,2),
      square_feet INT,
      year_built INT,
      total_bedrooms INT,
      total_bathrooms NUMERIC(5,2),
      garage_spaces INT,
      lat NUMERIC(9,6),
      lng NUMERIC(9,6),
      area_name VARCHAR(255),
      city VARCHAR(255),
      county VARCHAR(255),
      market_name VARCHAR(255),
      state CHARACTER(2),
      original_listing_price VARCHAR(255),
      listing_price VARCHAR(255),
      sold_price VARCHAR(255),
      list_date TIMESTAMP,
      pending_date TIMESTAMP,
      sold_date TIMESTAMP,
      listing_agent_id VARCHAR(255),
      co_listing_agent_id VARCHAR(255),
      selling_agent_id VARCHAR(255),
      co_selling_agent_id VARCHAR(255),
      listing_office_id VARCHAR(255),
      co_listing_office_id VARCHAR(255),
      selling_office_id VARCHAR(255),
      co_selling_office_id VARCHAR(255),
      last_update_date VARCHAR(255));''')

    print("Source Table created successfully")

    con.commit()
    #con.close()
  except (Exception, psycopg2.Error) as error :
    if(con):
      print("Did not create source datatabe", error)
  
  finally:
    #closing database connection.
    if(con):
        con.close()
        print("PostgreSQL connection is closed")

#Creating DestinationTable
def createDestinationTable():
  """ create tables in the PostgreSQL database """
  try:
    con = psycopg2.connect(database="source_db_name", user="source_db_user", password="SOURCE_DB_PASSWORD", host="127.0.0.1", port="5432")
    print("Database opened successfully")

    cur = con.cursor()
    cur.execute('''CREATE TABLE destination_table (
      property_id VARCHAR(255) PRIMARY KEY NOT NULL,
      display_address VARCHAR(255),
      zip_code VARCHAR(30),
      building_name VARCHAR(255),
      property_type VARCHAR(255),
      property_sub_type VARCHAR(255),
      style VARCHAR(255),
      acres NUMERIC(10,2),
      square_feet INT,
      year_built INT,
      total_bedrooms INT,
      total_bathrooms NUMERIC(5,2),
      garage_spaces INT,
      lat NUMERIC(9,6),
      lng NUMERIC(9,6),
      area_name VARCHAR(255),
      city VARCHAR(255),
      county VARCHAR(255),
      market_name VARCHAR(255),
      state CHARACTER(2),
      original_listing_price VARCHAR(255),
      listing_price VARCHAR(255),
      sold_price VARCHAR(255),
      list_date TIMESTAMP,
      pending_date TIMESTAMP,
      sold_date TIMESTAMP,
      listing_agent_id VARCHAR(255),
      co_listing_agent_id VARCHAR(255),
      selling_agent_id VARCHAR(255),
      co_selling_agent_id VARCHAR(255),
      listing_office_id VARCHAR(255),
      co_listing_office_id VARCHAR(255),
      selling_office_id VARCHAR(255),
      co_selling_office_id VARCHAR(255),
      last_update_date VARCHAR(255));''')

    print("Table created successfully")

    con.commit()
    #con.close()
  except (Exception, psycopg2.Error) as error :
    if(con):
      print("Did not create datatabe", error)
  
  finally:
    #closing database connection.
    if(con):
        con.close()
        print("PostgreSQL connection is closed")

if __name__ == "__main__": 
  main()
  
