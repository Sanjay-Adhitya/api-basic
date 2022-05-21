import logging,json
from uuid import uuid4
import azure.functions as func
from .utils import *

db_connection = ""

def main(req: func.HttpRequest) -> func.HttpResponse:

    method = req.route_params.get("method")
    id = req.route_params.get("id")
    route = req.route_params.get("route")

    if route == "patient":
        if method.lower() == "post":
            try:
                req_body = req.get_json()
                generated_id = uuid4()
                if req_body:
                    post_data(generated_id,db_connection)
                    return func.HttpResponse(req_body,status_code=200)
                else:
                    return func.HttpResponse("no request body to save (patient)",status_code=201)
            except ValueError:
                return func.HttpResponse("Error when trying to POST (patient)",status_code=201)
        elif method.lower() == "get":
            try:
                if id:
                    raw_body = get_data(id,db_connection)
                    logging.warning("raw_body : "+raw_body+" (patient)")
                    return func.HttpResponse(json.dumps(raw_body),status_code=200)
                else:
                    return func.HttpResponse("no id to get data (patient)",status_code=201)
            except Exception as e:
                return func.HttpResponse("Error when trying to GET (patient)",status_code=201)
        elif method.lower() == "delte":
            try:
                if id:
                    deleted_id = delete_data(id,db_connection)
                    logging.warning("deleted_id : "+deleted_id+" (patient)")
                    return func.HttpResponse(f"deleted = {deleted_id} (patient)",status_code=200)
                else:
                    return func.HttpResponse("no id to get data (patient)",status_code=201)
            except Exception as e:
                return func.HttpResponse("Error when trying to DELETE (patient)",status_code=201)
        else:
            return func.HttpResponse("GIVE METHOD (patient)",status_code=201)
    elif route == "vaccine":
        if method.lower() == "post":
            try:
                req_body = req.get_json()
                generated_id = uuid4()
                if req_body:
                    post_data(generated_id,db_connection)
                    return func.HttpResponse(req_body,status_code=200)
                else:
                    return func.HttpResponse("no request body to save (vaccine)",status_code=201)
            except ValueError:
                return func.HttpResponse("Error when trying to POST (vaccine)",status_code=201)
        elif method.lower() == "get":
            try:
                if id:
                    raw_body = get_data(id,db_connection)
                    logging.warning("raw_body : "+raw_body+" (vaccine)")
                    return func.HttpResponse(json.dumps(raw_body),status_code=200)
                else:
                    return func.HttpResponse("no id to get data (vaccine)",status_code=201)
            except Exception as e:
                return func.HttpResponse("Error when trying to GET (vaccine)",status_code=201)
        elif method.lower() == "delete":
            try:
                if id:
                    deleted_id = delete_data(id,db_connection)
                    logging.warning("deleted_id : "+deleted_id)
                    return func.HttpResponse(f"deleted = {deleted_id} (vaccine)",status_code=200)
                else:
                    return func.HttpResponse("no id to get data (vaccine)",status_code=201)
            except Exception as e:
                return func.HttpResponse("Error when trying to DELETE (vaccine)",status_code=201)
        else:
            return func.HttpResponse("GIVE METHOD (vaccine)",status_code=201)










