import json
import os
import random
import time
from urllib.parse import urljoin
from xml.etree import ElementTree

import boto3
import requests
from botocore.exceptions import ClientError
from xml.sax.saxutils import escape

SSM = boto3.client("ssm")

# Cache SSM parameters to avoid throttling on concurrent Lambda invocations
_SSM_CACHE = {}
_SSM_CACHE_TTL = 300  # 5 minutes

RETRY_STATUS = {500, 502, 503, 504}
RATE_LIMIT_ERROR_CODES = {
    "REQUEST_LIMIT_EXCEEDED",
    "CONCURRENT_REQUEST_LIMIT_EXCEEDED",
    "LIMIT_EXCEEDED",
    "STORAGE_LIMIT_EXCEEDED",
    "LOGIN_RATE_EXCEEDED",
    "API_CURRENTLY_DISABLED",
}
RATE_LIMIT_MESSAGE_SNIPPETS = (
    "limit exceeded",
    "storage limit exceeded",
    "api currently disabled",
    "request limit exceeded",
    "concurrent request limit exceeded",
    "login rate exceeded",
    "too many requests",
)


class RateLimitError(Exception):
    pass


def _get_env_int(name, default):
    value = os.getenv(name)
    if not value:
        return default
    try:
        return int(value)
    except ValueError:
        return default


def _get_env_float(name, default):
    value = os.getenv(name)
    if not value:
        return default
    try:
        return float(value)
    except ValueError:
        return default


def _sleep_for_retry(attempt, response=None):
    base_delay = _get_env_float("HTTP_RETRY_BASE_SECONDS", 1.0)
    max_delay = _get_env_float("HTTP_RETRY_MAX_SECONDS", 30.0)
    retry_after = None
    if response is not None:
        header = response.headers.get("Retry-After")
        if header and header.isdigit():
            retry_after = int(header)

    delay = retry_after if retry_after is not None else base_delay * (2**attempt)
    delay = min(delay, max_delay)
    jitter = random.uniform(0, delay * 0.2)
    time.sleep(delay + jitter)


def _extract_error_details(response):
    try:
        payload = response.json()
    except ValueError:
        return "", response.text
    if isinstance(payload, list) and payload:
        first = payload[0]
        code = first.get("errorCode", "")
        message = first.get("message", "")
        return code, message
    if isinstance(payload, dict):
        return payload.get("errorCode", ""), payload.get("message", "")
    return "", response.text


def _is_rate_limit_error(response):
    code, message = _extract_error_details(response)
    if code and code in RATE_LIMIT_ERROR_CODES:
        return True, f"{code}: {message}".strip(": ")
    message_lower = (message or "").lower()
    for code in RATE_LIMIT_ERROR_CODES:
        if code.lower() in message_lower:
            return True, message
    for snippet in RATE_LIMIT_MESSAGE_SNIPPETS:
        if snippet in message_lower:
            return True, message
    return False, ""


def is_rate_limit_message(message):
    if not message:
        return False
    message_lower = message.lower()
    for code in RATE_LIMIT_ERROR_CODES:
        if code.lower() in message_lower:
            return True
    for snippet in RATE_LIMIT_MESSAGE_SNIPPETS:
        if snippet in message_lower:
            return True
    return False


def _request_with_retries(method, url, headers=None, params=None, data=None, stream=False, timeout=30):
    max_retries = _get_env_int("HTTP_MAX_RETRIES", 3)
    for attempt in range(max_retries + 1):
        try:
            response = requests.request(
                method,
                url,
                headers=headers,
                params=params,
                data=data,
                stream=stream,
                timeout=timeout,
            )
        except requests.exceptions.RequestException:
            if attempt >= max_retries:
                raise
            _sleep_for_retry(attempt)
            continue

        if response.status_code == 429:
            response.close()
            raise RateLimitError("HTTP 429: Too Many Requests")

        if response.status_code >= 400:
            is_limited, message = _is_rate_limit_error(response)
            if is_limited:
                response.close()
                raise RateLimitError(message or "API rate limit exceeded")

        if response.status_code in RETRY_STATUS:
            if attempt >= max_retries:
                response.raise_for_status()
            response.close()
            _sleep_for_retry(attempt, response)
            continue
        return response
    raise RuntimeError(f"Failed request after retries: {method} {url}")

class SalesforceSession:
    def __init__(self, instance_url, access_token, api_version):
        self.instance_url = instance_url.rstrip("/")
        self.access_token = access_token
        self.api_version = api_version

    def _headers(self):
        return {"Authorization": f"Bearer {self.access_token}"}

    def get_json(self, path):
        url = urljoin(self.instance_url + "/", f"services/data/v{self.api_version}/{path.lstrip('/')}")
        response = _request_with_retries("GET", url, headers=self._headers(), timeout=30)
        response.raise_for_status()
        return response.json()

    def post_json(self, path, payload):
        url = urljoin(self.instance_url + "/", f"services/data/v{self.api_version}/{path.lstrip('/')}")
        headers = {"Authorization": f"Bearer {self.access_token}", "Content-Type": "application/json"}
        response = _request_with_retries("POST", url, headers=headers, data=json.dumps(payload), timeout=30)
        response.raise_for_status()
        return response.json()

    def get_results(self, job_id, locator, max_records):
        url = urljoin(
            self.instance_url + "/",
            f"services/data/v{self.api_version}/jobs/query/{job_id}/results",
        )
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Accept": "text/csv",
            "Accept-Encoding": "gzip",
        }
        params = {"maxRecords": str(max_records)}
        if locator:
            params["locator"] = locator
        response = _request_with_retries(
            "GET",
            url,
            headers=headers,
            params=params,
            stream=True,
            timeout=60,
        )
        response.raise_for_status()
        return response


def _format_salesforce_error(response):
    try:
        payload = response.json()
    except ValueError:
        return response.text
    if isinstance(payload, list) and payload:
        first = payload[0]
        message = first.get("message", "")
        code = first.get("errorCode", "")
        if code or message:
            return f"{code}: {message}".strip(": ")
    if isinstance(payload, dict):
        message = payload.get("message")
        if message:
            return message
    return response.text


def _get_param(name, decrypt=False):
    # Check cache first
    cache_key = f"{name}:{decrypt}"
    cached = _SSM_CACHE.get(cache_key)
    if cached:
        value, expires_at = cached
        if time.time() < expires_at:
            return value

    # Retry with exponential backoff for throttling
    max_retries = 5
    for attempt in range(max_retries):
        try:
            resp = SSM.get_parameter(Name=name, WithDecryption=decrypt)
            value = resp["Parameter"]["Value"]
            if not value:
                raise ValueError(f"Empty SSM parameter: {name}")
            # Cache the value
            _SSM_CACHE[cache_key] = (value, time.time() + _SSM_CACHE_TTL)
            return value
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code")
            if code == "ParameterNotFound":
                raise ValueError(f"Missing SSM parameter: {name}") from exc
            if code == "ThrottlingException" and attempt < max_retries - 1:
                # Exponential backoff with jitter
                delay = (2 ** attempt) + random.uniform(0, 1)
                time.sleep(delay)
                continue
            raise


def _login_url():
    login_url = os.getenv("SF_LOGIN_URL")
    if login_url:
        return login_url.rstrip("/")
    domain = os.getenv("SF_DOMAIN", "login").strip()
    return f"https://{domain}.salesforce.com"


def _api_version():
    return os.getenv("SF_API_VERSION", "63.0")


def _get_required_env(name):
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Missing required environment variable: {name}")
    return value


def login():
    username_param = _get_required_env("SF_USERNAME_PARAM")
    password_param = _get_required_env("SF_PASSWORD_PARAM")
    token_param = _get_required_env("SF_TOKEN_PARAM")

    username = _get_param(username_param, decrypt=True)
    password = _get_param(password_param, decrypt=True)
    token = _get_param(token_param, decrypt=True)

    login_url = _login_url()
    api_version = _api_version()
    soap_url = f"{login_url}/services/Soap/u/{api_version}"
    payload = _soap_login_payload(username, password + token)
    headers = {"Content-Type": "text/xml", "SOAPAction": "login"}

    response = _request_with_retries("POST", soap_url, data=payload, headers=headers, timeout=30)
    response.raise_for_status()

    session_id, server_url = _parse_login_response(response.text)
    instance_url = server_url.split("/services/")[0]
    return SalesforceSession(instance_url, session_id, api_version)


def check_bulk_api_enabled(session):
    url = urljoin(session.instance_url + "/", f"services/data/v{session.api_version}/jobs/query")
    response = _request_with_retries("GET", url, headers=session._headers(), timeout=30)
    if response.status_code >= 400:
        message = _format_salesforce_error(response)
        response.close()
        raise ValueError(f"Bulk API access failed: {message}")
    response.close()
    return True


def _soap_login_payload(username, password):
    safe_user = escape(username)
    safe_pass = escape(password)
    return (
        "<?xml version=\"1.0\" encoding=\"utf-8\"?>"
        "<env:Envelope xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\" "
        "xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" "
        "xmlns:env=\"http://schemas.xmlsoap.org/soap/envelope/\">"
        "<env:Body>"
        "<n1:login xmlns:n1=\"urn:partner.soap.sforce.com\">"
        f"<n1:username>{safe_user}</n1:username>"
        f"<n1:password>{safe_pass}</n1:password>"
        "</n1:login>"
        "</env:Body>"
        "</env:Envelope>"
    )


def _parse_login_response(xml_text):
    namespaces = {
        "env": "http://schemas.xmlsoap.org/soap/envelope/",
        "sf": "urn:partner.soap.sforce.com",
    }
    root = ElementTree.fromstring(xml_text)
    session_node = root.find(".//sf:sessionId", namespaces)
    server_node = root.find(".//sf:serverUrl", namespaces)
    if session_node is None or server_node is None:
        raise ValueError("Failed to parse Salesforce SOAP login response")
    return session_node.text, server_node.text


def list_queryable_objects(session):
    payload = session.get_json("sobjects")
    objects = []
    for obj in payload.get("sobjects", []):
        if not obj.get("queryable"):
            continue
        if obj.get("deprecatedAndHidden"):
            continue
        name = obj.get("name")
        if name:
            objects.append(name)
    return sorted(objects)


def describe_object(session, object_name):
    payload = session.get_json(f"sobjects/{object_name}/describe")
    fields = []
    for field in payload.get("fields", []):
        fields.append(
            {
                "name": field.get("name"),
                "type": field.get("type"),
                "filterable": field.get("filterable"),
                "deprecatedAndHidden": field.get("deprecatedAndHidden"),
                "relationshipName": field.get("relationshipName"),
                "referenceTo": field.get("referenceTo"),
            }
        )
    return {
        "name": object_name,
        "queryable": payload.get("queryable"),
        "retrieveable": payload.get("retrieveable"),
        "deprecatedAndHidden": payload.get("deprecatedAndHidden"),
        "customSetting": payload.get("customSetting"),
        "fields": [f for f in fields if f.get("name")],
    }
