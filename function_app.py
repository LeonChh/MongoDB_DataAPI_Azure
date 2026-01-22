import azure.functions as func
import logging
import json
import traceback
import os
from pymongo import MongoClient
from bson import ObjectId
from datetime import datetime
from aggregations import AGGREGATIONS

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

def connect_to_mongodb():
    conn_str = os.environ.get("MONGODBATLAS_CLUSTER_CONNECTIONSTRING")
    if not conn_str:
        raise Exception("MongoDB connection string not found in environment variables.")
    try:
        client = MongoClient(conn_str)
        return client
    except Exception as e:
        logging.error(f"Error connecting to MongoDB: {e}")
        raise


def success_response(body):
    return func.HttpResponse(
        json.dumps(body, cls=DateTimeEncoder),
        status_code=200,
        mimetype="application/json"
    )

def error_response(err):
    error_message = str(err)
    return func.HttpResponse(
        error_message,
        status_code=400,
        mimetype="application/json"
    )
# Used to convert datetime object(s) to string
class DateTimeEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, datetime):
            return o.isoformat()
        return super().default(o)
    
@app.route(route="mdb_dataapi/action/{operation}",methods=['POST'])
def mongodb_dataapi_replace(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    client = None

    try:
        payload = req.get_json()
        client = connect_to_mongodb()
        op = req.route_params.get('operation')
        # logging.info(op)
        db,coll =  payload.get('database'),payload.get('collection')
        # logging.info(db)
        # logging.info(coll)  
        if op == "findOne":
            filter_op = payload['filter'] if 'filter' in payload else {}
            projection = payload['projection'] if 'projection' in payload else {}
            result = {"document": client[db][coll].find_one(filter_op, projection)}
            # print("*************")
            # print(result)
            # print("*************")
            if result['document'] is not None:
                if '_id' in result['document'] and isinstance(result['document']['_id'], ObjectId):
                    result['document']['_id'] = str(result['document']['_id'])
        elif op == "find":
            agg_query = []

            if 'filter' in payload and payload['filter'] != {}:
                agg_query.append({"$match": payload['filter']})

            if "sort" in payload and payload['sort'] != {}:
                agg_query.append({"$sort": payload['sort']})

            if "skip" in payload:
                agg_query.append({"$skip": payload['skip']})

            if 'limit' in payload:
                agg_query.append({"$limit": payload['limit']})

            if "projection" in payload and payload['projection'] != {}:
                agg_query.append({"$project": payload['projection']})

            result = {"documents": list(client[db][coll].aggregate(agg_query))}
            for obj in result['documents']:
                if '_id' in obj and isinstance(obj['_id'], ObjectId):
                    obj['_id'] = str(obj['_id'])

        elif op == "insertOne":
            if "document" not in payload or payload['document'] == {}:
                return error_response("Send a document to insert")
            insert_op = client[db][coll].insert_one(payload['document'])
            result = {"insertedId": str(insert_op.inserted_id)}

        elif op == "insertMany":
            if "documents" not in payload or payload['documents'] == {}:
                return error_response("Send a document to insert")
            insert_op = client[db][coll].insert_many(payload['documents'])
            result = {"insertedIds": [str(_id) for _id in insert_op.inserted_ids]}

        elif op in ["updateOne", "updateMany"]:
            payload['upsert'] = payload['upsert'] if 'upsert' in payload else False
            if "_id" in payload['filter']:
                payload['filter']['_id'] = ObjectId(payload['filter']['_id'])
            if op == "/updateOne":
                update_op = client[db][coll].update_one(payload['filter'], payload['update'], upsert=payload['upsert'])
            else:
                update_op = client[db][coll].update_many(payload['filter'], payload['update'], upsert=payload['upsert'])
            result = {"matchedCount": update_op.matched_count, "modifiedCount": update_op.modified_count}

        elif op in ["deleteOne", "deleteMany"]:
            payload['filter'] = payload['filter'] if 'filter' in payload else {}
            if "_id" in payload['filter']:
                payload['filter']['_id'] = ObjectId(payload['filter']['_id'])
            if op == "/deleteOne":
                result = {"deletedCount": client[db][coll].delete_one(payload['filter']).deleted_count}
            else:
                result = {"deletedCount": client[db][coll].delete_many(payload['filter']).deleted_count}

        elif op == "aggregate":
            if "pipeline" not in payload or payload['pipeline'] == []:
                return error_response("Send a pipeline")
            docs = list(client[db][coll].aggregate(payload['pipeline']))
            for obj in docs:
                if '_id' in obj and isinstance(obj['_id'], ObjectId):
                    obj['_id'] = str(obj['_id'])
            result = {"documents": docs}

        else:
            return error_response("Not a valid operation")

        return success_response(result)

    except Exception as e:
        print(traceback.format_exc())
        return error_response(e)

    finally:
        if client:
            client.close()


@app.route(route="mdb_dataapi/custom/{aggregation_name}", methods=['POST'])
def mongodb_custom_aggregation(req: func.HttpRequest) -> func.HttpResponse:
    """Endpoint voor custom named aggregations."""
    logging.info('Custom aggregation request received.')

    client = None

    try:
        aggregation_name = req.route_params.get('aggregation_name')

        # Check of aggregation bestaat
        if aggregation_name not in AGGREGATIONS:
            return error_response(f"Aggregation '{aggregation_name}' not found. Available: {list(AGGREGATIONS.keys())}")

        # Parse parameters uit request body
        try:
            params = req.get_json() or {}
        except ValueError:
            params = {}

        # Instantieer en execute aggregation
        client = connect_to_mongodb()
        aggregation = AGGREGATIONS[aggregation_name]()
        documents = aggregation.execute(client, params)

        # Convert ObjectIds naar strings
        for doc in documents:
            if '_id' in doc and isinstance(doc['_id'], ObjectId):
                doc['_id'] = str(doc['_id'])

        return success_response({"documents": documents})

    except Exception as e:
        logging.error(f"Custom aggregation error: {traceback.format_exc()}")
        return error_response(e)

    finally:
        if client:
            client.close()
