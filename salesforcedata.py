from simple_salesforce import Salesforce, SalesforceLogin, SFType
import pandas as pd
import json


# SF = Salesforce module
# sf = Salesforce session

logininfo = json.load(open('login.json'))
username = logininfo["username"]
password = logininfo["password"]
security_token = logininfo["token"]
domain = 'login'

def import_data():

    #sf = Salesforce(username=username, password=password, security_token=security_token, domain=domain)
    session_id, instance = SalesforceLogin(username=username, password=password, security_token=security_token, domain=domain)
    sf = Salesforce(instance=instance, session_id=session_id)

    
    for element in dir(sf):
        if not element.startswith('_'):
            if isinstance(getattr(sf, element),str):
                print('Property Name: {0}; Value: {1}'.format(element,getattr(sf, element)))

    metadata_org = sf.describe()
    print(metadata_org.keys())
    #print(metadata_org['encoding'])
    #print(metadata_org['maxBatchSize'])
    #print(metadata_org['sobjects'])
    df_sobjects = pd.DataFrame(metadata_org['sobjects'])
    df_sobjects.to_csv('org metadata info.csv',index=False)


    #method 1
    opp = SFType('Opportunity', session_id, instance)
    opp_metadata = opp.describe()
    df_opp_metadata = pd.DataFrame(opp_metadata.get('fields'))
    df_opp_metadata.to_csv('opp meta data.csv', index = False)

    contact = SFType('Contact', session_id, instance)
    contact_metadata = contact.describe()
    df_contact_metadata = pd.DataFrame(contact_metadata.get('fields'))
    df_contact_metadata.to_csv('contact meta data.csv', index = False)

    ocr = SFType('OpportunityContactRole', session_id, instance)
    ocr_metadata = ocr.describe()
    df_ocr_metadata = pd.DataFrame(ocr_metadata.get('fields'))
    df_ocr_metadata.to_csv('ocr meta data.csv', index = False)

    querySOQL ="""Select Customer_Ref_Number__c, Name, Amount, Legacy_Primary_Contact__c From Opportunity  Where CreatedDate > 2021-01-01T00:00:00.000Z
"""

    #recordsAccount = sf.query(querySOQL)

    response = sf.query(querySOQL)
    print(response.keys())
    lstRecords = response.get('records')
    nextrecordURL = response.get('nextRecordsUrl')
    
    while not response.get('done'):
        response = sf.query_more(nextrecordURL, identifier_is_url=True)
        lstRecords.extend(response.get('records'))
        nextRecordsURL = response.get('nextRecordsURL')

    df_records = pd.DataFrame(lstRecords)
    df_records.to_csv('records.csv', index=False)



if __name__=="__main__":
    import_data()
    