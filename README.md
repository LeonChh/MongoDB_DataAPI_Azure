Test deployment

This repo details how to seamlessly set up an Azure function as a good alternative to the MongoDB Atlas Data APIs. You can deploy the Azure Function App using few clicks and use the Azure function URL and Function App key instead of the base url and api key of Atlas Data APIs. It details the Azure function based replacement solution and alleviates the burden off developers to build the function code and manually deploy it.

## Architecture
 ![](https://github.com/mongodb-partners/MongoDB_DataAPI_Azure/blob/main/images/DataAPIReplace.drawiov1.png)
 
The architecture diagram above depicts that when an API request is made to the Azure Function url, authorised by the API key, the corresponding MQL will be executed as specified by the “operation” in the url. An example of an Azure function URL being invoked would be : “https://<azure function name>.azurewebsites.net/api/mdb_dataapi/action/{operation}”. The url contains the “operation” which indicates which API is invoked (findOne, find, insertOne, insertMany, deleteOne, deleteMany, updateOne, updateMany, aggregate) and the corresponding python SDK code is executed using the parameters passed in the body of the request. The results of the MQL query (operation) are then passed back to the application. 

## Prerequisites

1. **Configure Atlas Environment**

Register for a new Atlas Account [here](https://www.mongodb.com/docs/atlas/tutorial/create-atlas-account/#register-a-new-service-account). Follow steps from 1 to 4 (Create an Atlas account, Deploy a Free cluster, Add your IP to the IP access list and Create a Database user) to set up the Atlas environment.

2. **Set Up Azure Function as Atlas Data API**
    To set up the Azure function which will host the code to act as Atlas Data APIs, we have **two** options - **1. Using GitHub Actions OR 2. Using Zip Deploy**

    Choose the GitHub actions method, if you are able to fork the current repo, can have GitHub actions enabled in that repo and that you would want to add more APIs and prefer a CI/CD or DevOps set up out of the box.
    However, if you are looking for a quick and easy way of deployment and just need the Azure function set up to substitute the Atlas Data APIs, go with the Zip deploy option.

    ### **Option 1: Set Up Azure function Using GitHub actions** ###
   
   a.Fork the [MongoDB repo](https://github.com/mongodb-partners/MongoDB_DataAPI_Azure). Note the new **forked repo url**. If GitHub actions is NOT enabled by default, enable them by going to the **Settings -> Actions -> General** in your forked repo and select one of Allow actions/ resusable workflows options.

   b.Click the below **Deploy to Azure** button to have the Azure function created in your tenant.

   [![Deploy to Azure](https://aka.ms/deploytoazurebutton)](https://portal.azure.com/#create/Microsoft.Template/uri/https%3A%2F%2Fraw.githubusercontent.com%2Fmongodb-partners%2FMongoDB_DataAPI_Azure%2Frefs%2Fheads%2Fmain%2FARM_template.json)

   c. Select or Create your Resource group which will contain the Azure function and its associated components (App Service Plan, Storage Account and App Insights). You can keep the function name and SKU as the defaults or change if you like to follow some specific standards.
   **We recommend that you add your Cluster name to the function app name so that its unique and easy to identify.**
   
   Give the MongoDB connection url for the Cluster against which this Azure function will run. This connection string will be saved as an Environmnet variable.
   Give your forked repo url as GitHub repo. Select **Create** and it will create the Azure function with the associated resources.
   **Note that at this stage the function app is created, env variables are populated but the actual function is not yet deployed to the function app.**
        
   d.  To have GitHub actions run from your repo and deploy the function, get the publishing profile from your created Azure function.

   It gets downloaded, open it in a Text editor and copy all its contents.
        
   ![](https://github.com/mongodb-partners/MongoDB_DataAPI_Azure/blob/main/images/GetPublishProfile.png)

   e.   Go to your GitHub repo -> Settings -> Secrets and variables -> Actions
             Click **New Respository secret** and copy the entire value in your publishing profile to a new secret named **"AZUREAPPSERVICE_PUBLISHPROFILE"**
   
   f.  Make a minor change in README and **Commit Changes** to invoke GitHub actions which would deploy the python code to the Azure function into your function app.
             **Now you should see the function available in the Functon App and the code in function_app.py deployed.**
   
   g. GitHub actions tab in GitHub repo will show the steps in the deployment (including the installation of dependencies) and the result of each step.

    ### **Option 2: Set Up Azure function Using ZipDeploy** ###
   
   a.  Click the below **Deploy to Azure** button to have the Azure function created in your tenant.

   [![Deploy to Azure](https://aka.ms/deploytoazurebutton)](https://portal.azure.com/#create/Microsoft.Template/uri/https%3A%2F%2Fraw.githubusercontent.com%2Fmongodb-partners%2FMongoDB_DataAPI_Azure%2Frefs%2Fheads%2Fmain%2FARM_template_zipdeploy.json)

   b. Select or Create your Resource group which will contain the Azure function and its associated components (App Service Plan, Storage Account and App Insights). You can keep the function name and SKU as the defaults or change if you like to follow some specific standards. We recommned that you add your Cluster name to the function app name so that its unique and easy to identify.

   Please **DO NOT** change the **Package Url** which points to the deployable zip file in this repo.

  Select **Create** and it will create the Azure function app, deploy the azure function along with the associated resources.

## How to get credentials

**Get the BaseUrl and API Key**

a. From the Function App, select your function and click **Get function URL** . Copy the function url from the beginning till "/action/" as shown in screenshot below. Replace {operation} with one of these depending on which API needs to be invoked. The valid operations are : findOne, find, insertOne, insertMany, deleteOne, deleteMany, updateOne, updateMany, aggregate 
This is the **Base Url** you will use to invoke any of the MongoDB CRUD/ Aggregate APIs.

![](https://github.com/mongodb-partners/MongoDB_DataAPI_Azure/blob/1d2b33a4e8bc7c2cbc325dac9840ded28ebdb1f7/images/Get%20Function%20url.png)

b. Go to your Function App -> Under Functions -> App keys , Grab either the *_master* or *default* API key for your Azure function
    This is the **API Key** you will use along with **Base Url** to create a MongoDB connection to invoke any of the MongoDB CRUD/ Aggregate APIs.

## Invoking the APIs using Azure function

Change your applications to use an url format like : "https://<azure_function_name>.azurewebsites.net/api/mdb_dataapi/action/{operation}" to invoke the Data APIs. Not the operation will have unique values for each of the Data API operations which are - findOne, find, insertOne, insertMany, deleteOne, deleteMany, updateOne, updateMany, aggregate. For example use "https://<azure_function_name>.azurewebsites.net/api/mdb_dataapi/action/findOne" to query the database and retrieve only one record.

Also note that in the authorisation add key name as "x-functions-key" and its value should be the API key of the Azure function. This should be part of header too. Rest of the inputs like "dataSource" for clustername, "database" for database name and "collection" for collection name should be passed in the request body along with other optional parameters.

See below curl as an example:

```
    curl --location 'https://<azure function name>.azurewebsites.net/api/mdb_dataapi/action/findOne' \
    --header 'Content-Type: application/json' \
    --header 'x-functions-key: < Azure function API key >' \
    --data '{
      "dataSource": "<cluster name>",
      "database": "< db name >",
      "collection": "< collection name >",
      "projection": {"<field name>":0}
    }'
```
Refer to the [swagger file](https://github.com/mongodb-partners/MongoDB_DataAPI_Azure/blob/main/MongoDB_clean_swagger.json) for the structure of each of the APIs.
## Known issues and limitations

Please follow this [link](https://learn.microsoft.com/en-us/azure/azure-functions/functions-scale) for the known limitations with the Azure functions like time outs and other service limits for each resource plans.

## Common errors and remedies

Typical API response codes apply here also. Any 4XX errors indicate issue with the request from the client. Make sure that the dataSource, database, collection are provided in a valid JSON format. Refer to this [Postman Collection](https://www.postman.com/grey-desert-5714/workspace/mongodb-dataapis-azurefcn/collection/5631262-a038ba24-f185-4671-acf2-530b3a3ddb55?action=share&creator=5631262) for examples. For 5XX errors, make sure the Azure function is up and running and check its trace to further investigate.
