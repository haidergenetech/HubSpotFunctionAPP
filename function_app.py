import azure.functions as func
from datetime import datetime
import json
import logging
import os
import requests
import pycountry
import time

app = func.FunctionApp()

def wait_for_company_association(contact_id, headers, max_retries=5, delay=2):
    """
    Wait for company to be created and associated with contact
    Returns company_id if found, None otherwise
    """
    logger = logging.getLogger("hubspot_function")
    
    for attempt in range(max_retries):
        try:
            associations_url = f"https://api.hubapi.com/crm/v4/objects/contacts/{contact_id}/associations/companies"
            associations_response = requests.get(associations_url, headers=headers, timeout=30)
            
            if associations_response.status_code == 200:
                associations_data = associations_response.json()
                if associations_data.get("results"):
                    company_id = associations_data["results"][0]["toObjectId"]
                    logger.info(f"Company ID found on attempt {attempt + 1}: {company_id}")
                    return company_id
            
            if attempt < max_retries - 1:  # Don't sleep on last attempt
                logger.info(f"Company not found on attempt {attempt + 1}, waiting {delay} seconds...")
                time.sleep(delay)
                delay *= 1.5  # Exponential backoff
                
        except Exception as e:
            logger.error(f"Error checking for company on attempt {attempt + 1}: {str(e)}")
            if attempt < max_retries - 1:
                time.sleep(delay)
    
    logger.warning("Company association not found after all retry attempts")
    return None

def update_company_properties(company_id, user_country, headers):
    """
    Update company properties based on user country
    Returns True if successful, False otherwise
    """
    logger = logging.getLogger("hubspot_function")
    
    try:
        # Get country object
        country = pycountry.countries.get(alpha_2=user_country) if user_country else None
        
        filtered_country_codes = [
            # North America
            "AG", "AI", "AW", "BS", "BB", "BZ", "BM", "CA", "CR", "CU", "CW", "DM", "DO",
            "SV", "GD", "GP", "GT", "HT", "HN", "JM", "MQ", "MX", "MS", "NI", "PA", "PR",
            "KN", "LC", "MF", "PM", "VC", "SX", "TT", "US", "VG", "VI",
            # Oceania
            "AS", "AU", "CK", "FJ", "PF", "GU", "KI", "MH", "FM", "NR", "NC", "NZ", "NU",
            "NF", "MP", "PW", "PG", "PN", "WS", "SB", "TK", "TO", "TV", "VU", "WF",
            # South America
            "AR", "BO", "BR", "CL", "CO", "EC", "FK", "GF", "GY", "PY", "PE", "SR", "UY", "VE"
        ]

        a_rated_countries = [
            "US", "CA", "AU", "NZ", "GB", "DE", "FR", "NL",
            "SE", "CH", "NO", "FI", "DK", "BE", "AT", "JP",
            "KR", "SG"
        ]

        sales_office = "BPA US" if user_country in filtered_country_codes else "BPA CH"
        market_type = "A" if user_country in a_rated_countries else "B"
        sales_account_manager = (
            "Sebastien Rocco" if sales_office == "BPA US" else "Damien Emery"
        )

        company_update_data = {
            "properties": {
                "bpa_sales_office": sales_office,
                "market_type": market_type,
                "sales_account_manager": sales_account_manager,
                "country_region_code": user_country
            }
        }
        
        # Add country name if available
        if country:
            company_update_data["properties"]["country"] = country.name

        company_update_url = f"https://api.hubapi.com/crm/v3/objects/companies/{company_id}"
        logger.info(f"Updating company {company_id}...")

        company_update_response = requests.patch(
            company_update_url,
            headers=headers,
            json=company_update_data,
            timeout=30
        )

        logger.info(f"Company update status: {company_update_response.status_code}")
        logger.info(f"Company update response: {company_update_response.text}")

        if company_update_response.status_code == 200:
            logger.info("Company updated successfully")
            return True
        elif company_update_response.status_code == 400:
            logger.error("Company update failed with 400 error")
            logger.error(f"Error details: {company_update_response.text}")
        elif company_update_response.status_code == 404:
            logger.error(f"Company with ID {company_id} not found")
        else:
            logger.error(f"Unexpected error updating company: {company_update_response.status_code}")
            logger.error(f"Response: {company_update_response.text}")

        return False

    except Exception as e:
        logger.error(f"Error updating company: {str(e)}")
        return False

@app.route(route="HubspotAdd", auth_level=func.AuthLevel.ANONYMOUS)
def HubspotAdd(req: func.HttpRequest) -> func.HttpResponse:
    logger = logging.getLogger("hubspot_function")
    logger.setLevel(logging.INFO)

    try:
        logger.info("--- Incoming Request ---")
        logger.info(f"Timestamp: {datetime.utcnow().isoformat()}Z")
        logger.info(f"Method: {req.method}")
        logger.info(f"Headers: {dict(req.headers)}")

        # Handle CORS preflight
        if req.method == "OPTIONS":
            return func.HttpResponse(
                "",
                status_code=200,
                headers={
                    "Access-Control-Allow-Origin": "*",
                    "Access-Control-Allow-Methods": "POST, OPTIONS",
                    "Access-Control-Allow-Headers": "Content-Type, Authorization, User-Agent"
                }
            )

        # Only allow POST
        if req.method != "POST":
            logger.warning(f"Invalid method: {req.method}")
            return func.HttpResponse(
                json.dumps({"message": "Only POST method allowed"}),
                status_code=405,
                mimetype="application/json",
                headers={"Access-Control-Allow-Origin": "*"}
            )

        # Read request body
        try:
            body = req.get_body().decode("utf-8")
            logger.info(f"Body: {body}")
            data = json.loads(body)
        except json.JSONDecodeError as e:
            logger.error(f"JSON decode error: {str(e)}")
            return func.HttpResponse(
                json.dumps({"message": "Invalid JSON format"}),
                status_code=400,
                mimetype="application/json",
                headers={"Access-Control-Allow-Origin": "*"}
            )

        # Validate UserDetails exists
        if "UserDetails" not in data:
            logger.error("Missing UserDetails")
            return func.HttpResponse(
                json.dumps({"message": "UserDetails is required"}),
                status_code=400,
                mimetype="application/json",
                headers={"Access-Control-Allow-Origin": "*"}
            )

        user = data["UserDetails"]
        required_fields = ["Email", "FirstName", "LastName"]
        for field in required_fields:
            if not user.get(field):
                logger.error(f"Missing required field: {field}")
                return func.HttpResponse(
                    json.dumps({"message": f"Required field missing: {field}"}),
                    status_code=400,
                    mimetype="application/json",
                    headers={"Access-Control-Allow-Origin": "*"}
                )

        # Mapping for values without spaces
        solution_map = {
            "BPACRM365": "BPA CRM 365",
            "BPAMedical365": "BPA Medical 365",
            "BPAQuality365": "BPA Quality 365",
            "CRMandProjectManagementbyBPA": "CRM & Project Management by BPA",
            "QualityandRiskManagementbyBPA": "Quality & Risk Management by BPA",
            "Solutionbuilder": "Solution builder",
        }

        # Get raw value from user
        raw_solution = data.get("OfferTitle", "")
        solution_value = solution_map.get(raw_solution, raw_solution)

        country = None
        if user.get("Country", ""):
            country = pycountry.countries.get(alpha_2=user.get("Country", ""))

        filtered_country_codes = [
            # North America
            "AG", "AI", "AW", "BS", "BB", "BZ", "BM", "CA", "CR", "CU", "CW", "DM", "DO",
            "SV", "GD", "GP", "GT", "HT", "HN", "JM", "MQ", "MX", "MS", "NI", "PA", "PR",
            "KN", "LC", "MF", "PM", "VC", "SX", "TT", "US", "VG", "VI",
            # Oceania
            "AS", "AU", "CK", "FJ", "PF", "GU", "KI", "MH", "FM", "NR", "NC", "NZ", "NU",
            "NF", "MP", "PW", "PG", "PN", "WS", "SB", "TK", "TO", "TV", "VU", "WF",
            # South America
            "AR", "BO", "BR", "CL", "CO", "EC", "FK", "GF", "GY", "PY", "PE", "SR", "UY", "VE"
        ]

        a_rated_countries = [
            "US", "CA", "AU", "NZ", "GB", "DE", "FR", "NL",
            "SE", "CH", "NO", "FI", "DK", "BE", "AT", "JP",
            "KR", "SG"  
        ]
        
        sales_office = "BPA US" if user.get("Country", "") in filtered_country_codes else "BPA CH"
        market_type = "A" if user.get("Country", "") in a_rated_countries else "B"
        # Prepare HubSpot data
        hubspot_data = {
            "properties": {
                "email": user.get("Email", ""),
                "firstname": user.get("FirstName", ""),
                "lastname": user.get("LastName", ""),
                "phone": user.get("Phone", ""),
                "company": user.get("Company", ""),
                "jobtitle": user.get("Title", ""),
                "country_code": user.get("Country", ""),
                "which_solution_are_you_interested_in_": solution_value,
                "lifecyclestage": "marketingqualifiedlead",
                "hs_content_membership_notes": data.get("Description", ""),
                "lead_source": "AzureMarketplace",
                "contact_origin": "Marketplace",
                "account_type": "Inbound Lead",
                "bpa_sales_office": sales_office,
                "market_type": market_type,
                "country_region_code": user.get("Country", "")
                }
        }
        
        # Add country name if available
        if country:
            hubspot_data["properties"]["country"] = country.name
            hubspot_data["properties"]["country_name"] = country.name

        # HubSpot token from environment
        token = os.environ.get("HUBSPOT_TOKEN")
        if not token:
            logger.error("HubSpot token not set in environment variables")
            return func.HttpResponse(
                json.dumps({"message": "Server misconfiguration: missing token"}),
                status_code=500,
                mimetype="application/json",
                headers={"Access-Control-Allow-Origin": "*"}
            )

        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }

        # Try creating contact
        create_url = "https://api.hubapi.com/crm/v3/objects/contacts"
        logger.info("Sending POST to HubSpot...")
        r = requests.post(create_url, headers=headers, json=hubspot_data, timeout=30)
        logger.info(f"HubSpot POST status: {r.status_code}")
        logger.info(f"HubSpot POST response: {r.text}")

        contact_created = False
        
        if r.status_code == 400:
            logger.error("HubSpot error 400")
            return func.HttpResponse(
                json.dumps({"message": r.text}),
                status_code=400,
                mimetype="application/json",
                headers={"Access-Control-Allow-Origin": "*"}
            )
        # If conflict (contact exists), try update
        elif r.status_code == 409:
            email = user["Email"]
            update_url = f"https://api.hubapi.com/crm/v3/objects/contacts/{email}?idProperty=email"
            
            logger.info("Contact already exists (409). Preparing to update contact...")
            logger.info(f"PATCH URL: {update_url}")
            logger.info(f"Payload for update: {json.dumps(hubspot_data)}")

            try:
                r = requests.patch(update_url, headers=headers, json=hubspot_data, timeout=30)
                logger.info(f"HubSpot PATCH status: {r.status_code}")
                logger.info(f"HubSpot PATCH response: {r.text}")
            except Exception as patch_err:
                logger.error(f"PATCH request failed: {patch_err}")
                return func.HttpResponse(
                    json.dumps({"message": "PATCH update failed", "error": str(patch_err)}),
                    status_code=500,
                    mimetype="application/json",
                    headers={"Access-Control-Allow-Origin": "*"}
                )
            if r.status_code == 400:
                logger.error("HubSpot PATCH returned error 400")
                return func.HttpResponse(
                    json.dumps({"message": r.text}),
                    status_code=400,
                    mimetype="application/json",
                    headers={"Access-Control-Allow-Origin": "*"}
            )
        else:
            contact_created = True  # New contact was created

        contact_id = r.json()["id"]

        # Wait for company association (with retry logic)
        # Only wait if we created a new contact (company automation triggered)
        if contact_created:
            logger.info("New contact created, waiting for company association...")
            company_id = wait_for_company_association(contact_id, headers, max_retries=5, delay=2)
        else:
            logger.info("Contact updated, checking for existing company association...")
            company_id = wait_for_company_association(contact_id, headers, max_retries=1, delay=0)

        # Update company fields if company exists
        if company_id:
            success = update_company_properties(company_id, user.get("Country", ""), headers)
            if success:
                logger.info("Company update completed successfully")
            else:
                logger.warning("Company update failed, but contact processing continues")
        else:
            logger.info("No company associated with contact - skipping company update")

        # Return success
        return func.HttpResponse(
            json.dumps({
                "message": "Lead processed successfully",
                "timestamp": datetime.utcnow().isoformat(),
                "company_updated": company_id is not None
            }),
            status_code=200,
            mimetype="application/json",
            headers={"Access-Control-Allow-Origin": "*"}
        )

    except Exception as e:
        logger.exception("Unhandled error")
        return func.HttpResponse(
            json.dumps({"message": "Internal server error", "error": str(e)}),
            status_code=500,
            mimetype="application/json",
            headers={"Access-Control-Allow-Origin": "*"}
        )