"""
A python package for Airtable; automates API calls and data flows.
NOTE: _process_records_for_at() currently does not handle AT data
    type 'attachement'.
NOTE: data type conversion from AT to python currently does not check
    data type content of AT arrays. May need to be adjusted in future,
    if arrays contain: Infinity, -Infinity, NaN.
"""

import json
from decimal import Decimal
from logging import Logger
from math import inf, isinf, isnan, nan
from typing import Any, Optional, Union

import requests


class AirTableCachingClient(object):
    def __init__(
        self,
        apikey: str,  # Change param name to personal_token / PAT, something, to account for auth changes with AT
        logger: Logger,
        process: bool = False,
        personal_token: Optional[
            str
        ] = None,  # TODO: eventually tweak client to use this as the sole authentication method in place of API key
        cache_token: Optional[str] = None,
        base: Optional[str] = None,
    ):
        """
        A hybrid client for AirTable that can use a caching layer when needed.

        Parameters
        ----------
        apikey : str
            AirTable API key. TODO: This is deprecated and will need to be replaced with PATs.
        logger : Logger
            Init the logger.
        process : bool
            Processes the records from AirTable to be a list[dict] where each dict corresponds to a row.
        personal_token : str, optional
            This is temporarily in here for the webhooks subclient until the API key fix is done.
        cache_token : str, optional
            Cache token to be used if the MW caching layer is being used.
        base : str, optional
            Airtable base. Doesn't have to be defined at the client level, which can be more dynamic.

        NOTE: When initialising ATC Client, 'process' can be used to set client's
            data process preferess:
            process = False (default) returns AT record format, and expects the
                same for write/update.
            process = True (default) uses more conveniently formatted data.
        """
        self._at_rec_id = "at_record_id"  # label given to ID for AT records
        self._cache_url = f"https://at-cache-hfrsqgk6ja-uc.a.run.app/airtable/v0"
        self._at_url = f"https://api.airtable.com/v0"
        if base is not None:  # check these first
            self._base = base
        if personal_token is not None:
            self._personal_token = personal_token
            # TODO: fix import
            # self.webhooks = AirtableWebhooksClient(AirTableCachingClient)
            # self._webhook_url = f"https://api.airtable.com/v0/bases/{self._base}/webhooks"  probably don't need this if we're keeping base on the function level
            self.__webhook_headers = {
                "Authorization": f"Bearer {self._personal_token}",
                "Content-Type": "application/json",
            }
        # self.webhooks = AirTableCachingClient.AirtableWebhooksClient(self, parent=AirTableCachingClient)  # TODO: make sure this works
        self.__headers = {
            "Authorization": f"Bearer {apikey}",
            "Content-Type": "application/json",
        }
        self.__cache_header = {"X-Mw-Bearer": cache_token}
        self._cache_token = cache_token
        self._session = requests.Session()
        self._logger = logger
        self._process = process
        # if personal_token is not None:
        #     self._personal_token = personal_token

    def _process_records_from_at(
        self, records: dict, keep_id: bool = True
    ) -> list[dict]:
        """
        Process AT records to make it easier to work with.
        Takes records as returned by AT: {'records':[{'id':1,'fields':{...}},...]}.
        Returns records as list[dict], with each dict representing one record row.

        Parameters
        ----------
        records : dict
            Raw records from AirTable.
        keep_id : bool
            This adds a pseudocolumn, self._at_rec_id, which corresponds to the record id in AirTable.

        Returns
        -------
        list[dict]
            A list of dictionaries where each dictionary corresponds to a row.
        """
        output = []
        for row in records:
            newrow = {self._at_rec_id: row["id"]} if keep_id else {}
            for k, v in row["fields"].items():
                # "some_numeric_field" : {"specialValue":"Infinity"}
                if type(v) == dict and v.get("specialValue"):
                    sv = v.get("specialValue")
                    if sv == "Infinity":
                        newrow[k] = inf
                    elif sv == "-Infinity":
                        newrow[k] = -inf
                    elif sv == "NaN":
                        newrow[k] = nan
                else:
                    newrow[k] = v
            output.append(newrow)
        return output

    def _process_records_for_at(
        self, records: list[dict], keep_none: bool = False, new: bool = False
    ) -> Union[dict, list[dict]]:
        """
        Formats records, where every dict is a record row, to be compatible
        with AT API format for write and update functions.
        NOTE: Does not currenlty handle AT data type 'attachments'.

        Parameters
        ----------
        records : list[dict]
            Records to be processed for AirTable write/update functions.
        keep_none : bool
            If True, None values are passed as ''
            If False (default), eliminates fields with None values.
        new : bool
            If True, it is assumed that the records are new and do not already have a record ID.
                Returns: [{'fields':{...}},...]
            If False (default), it is assumed that the record ID was included as the field self._at_rec_id in the dict.
                Returns: {'records':[{'id':1,'fields':{...}},...]}

        Returns
        -------
        dict or list[dict] depending on what is passed for new.
        """
        output = []
        for row in records:
            # 1. resolve None values
            newrow = (
                {k: v for k, v in row.items()}
                if keep_none
                else {k: v for k, v in row.items() if not isinstance(v, type(None))}
            )
            # 2. format specialValues for AT
            for k, v in newrow.items():
                if isinstance(v, float):
                    label = "specialValue"
                    if isinf(v) and v > 0:
                        newrow[k] = {label: "Infinity"}
                    elif isinf(v) and v < 0:
                        newrow[k] = {label: "-Infinity"}
                    elif isnan(v):
                        newrow[k] = {label: "NaN"}
            # 3. create AT data record format
            if new:
                output.append({"fields": newrow})
            else:
                data = {
                    key: val for key, val in newrow.items() if key != self._at_rec_id
                }  # moves ID outside data dict
                output.append({"id": newrow[self._at_rec_id], "fields": data})
        formatted_records = output if new else {"records": output}
        return formatted_records

    def _identify_errors(self, response: str) -> None:
        """
        Print error message if the records don't return correctly.

        Parameters
        ----------
        response : str
            Error response.

        Returns
        -------
        None
            Returns parsed error message to logging.
        """
        if "error" in response:
            try:
                self._logger.error(
                    'Oops! {} Error: "{}"'.format(
                        response["error"]["type"], response["error"]["message"]
                    )
                )
            except:
                self._logger.error("Oops! Error: {}".format(response["error"]))

    def _json_default_handler(self, obj: Any) -> Any:
        """
        To handle problems with different JSON objects.

        Parameters
        ----------
        obj : Any
            Object to be handled to be coerced into the correct format.

        Returns
        -------
        Any
            Depends on the object passed.
            Can be a datetime, float

        Raises
        ------
        ValueError
            If `obj` is neither an isoformat dt nor a Decimal.

        Sourced from https://stackoverflow.com/questions/455580/json-datetime-between-python-and-javascript
        """
        if hasattr(obj, "isoformat"):
            # handles date time
            return obj.isoformat()
        elif isinstance(obj, Decimal):
            # handles decimal
            return float(obj)
        else:
            raise TypeError(
                "Object of type {0} with value of {1} is not JSON serializable".format(
                    type(obj), repr(obj)
                )
            )

    def jsonify(self, obj: Union[dict, list[dict]]) -> str:
        return json.dumps(obj, default=self._json_default_handler)

    def get(
        self,
        table: str,
        view: Optional[str] = None,
        base: Optional[str] = None,
        process: Optional[bool] = None,
        direct: bool = True,
    ) -> list[dict]:
        """
        Get AirTable data from a specified base and table.

        Parameters
        ----------
        table : str
            AirTable base ID.
        view : str, optional
            The specific view on the table that you're trying to retrieve.
        base : str, optional
            The AirTable base that contains the table you're pulling.
            This is optional because it may be defined on the client level.
        process : bool, optional
            If True, process records into nicer (non-AT) formats.
            If False, keeps AT format.
        direct : bool
            If True, query AirTable directly.
            If False, use the caching layer. Requires cache token.

        Returns
        -------
        list[dict] or dict
            Returns the AirTable table data, format depends if you process or not.

        Raises
        ------
        RuntimeError
            If the status code returned is not a 200, RuntimeError is raised and status code and error text is returned.
        """
        if self._cache_token is None:
            direct = True
        base = base if base else self._base
        process = self._process if process is None else process
        url = self._at_url if direct else self._cache_url
        headers = self.__headers
        if direct is False:
            headers.update(self.__cache_header)
        url = f"{url}/{base}/{table}"
        params = {}
        # get a view if specified
        if view:
            params.update({"view": view})
        offset = True
        records = []
        while offset:
            if offset and offset is not True:
                params.update({"offset": offset})
            r = self._session.get(url, headers=headers, params=params)
            if r.status_code != 200:
                self._identify_errors(r)
                raise RuntimeError(
                    f"Airtable Cache did not return 200 ({r.status_code}): {r.text}"
                )
            json_object = r.json()
            records.extend(json_object["records"] if json_object.get("records") else [])
            offset = json_object.get("offset")
        if process:
            return self._process_records_from_at(records)
        else:
            return records

    def write(
        self,
        table: str,
        data: Union[list[dict], dict],
        base: Optional[str] = None,
        process: Optional[bool] = None,
    ) -> list[dict]:
        """
        Write new rows to AirTable. Data will be chunked into batches of 10 for AT.

        Parameters
        ----------
        table : str
            AirTable base ID.
        data : list[dict] or dict
            Data to be written to AirTable.
        base : str, optional
            The AirTable base that contains the table you're pulling.
            This is optional because it may be defined on the client level.
        process : bool, optional
            If True, process records into nicer (non-AT) formats. Data should be list[dict] with each row represented as a dict.
            If False, keeps AT format.  Data needs to be a dict of records in AT format with optional top-level 'records' field.

        NOTE: Does not currently process AT data type 'attachements' (not yet part of
        function _process_records_for_at)

        Returns
        -------
        list[dict]
            Records actually written to AT, given in AT format.

        Raises
        ------
        RuntimeError
            If the status code returned is not a 200, RuntimeError is raised and status code and error text is returned.
        """
        base = base if base else self._base
        process = self._process if process is None else process
        data = self._process_records_for_at(data, new=True) if process else data
        if type(data) == dict:
            data = data["records"]
        n = 10  # current limit per request
        chunks = [data[i : i + n] for i in range(0, len(data), n)]
        records = []
        for chunk in chunks:
            r = self._session.post(
                f"{self._at_url}/{base}/{table}",
                headers=self.__headers,
                data=self.jsonify({"records": chunk}),
            )
            if r.status_code != 200:
                airtable_response = r.json()
                if "error" in airtable_response:
                    self._identify_errors(r)
                raise RuntimeError(f"Airtable did not return 200: {r.text}")
            records.extend(r.json()["records"])
        return records

    def update(
        self,
        table: str,
        data: Union[dict, list[dict]],
        base: Optional[str] = None,
        process: Optional[bool] = None,
        keep_none: bool = False,
    ) -> list[dict]:
        """
        This uses the nondestructive PATCH method; only list fields to be changed.
        Data will be chunked into batches of 10 for AT.

        Parameters
        ----------
        table : str
            AirTable base ID.
        data : list[dict] or dict
            Data to be written to AirTable.
        base : str, optional
            The AirTable base that contains the table you're pulling.
            This is optional because it may be defined on the client level.
        process : bool, optional
            If True, process records into nicer (non-AT) formats. Data should be list[dict] with each row represented as a dict.
                Each dict needs the AT record id as a key/value pair of self._at_rec_id:<value>.
            If False, keeps AT format.  Data needs to be a dict of records in AT format with optional top-level 'records' field.
                {'records':[{'id':1,'fields':{...}},...]} format dictionary or a list of such records.
        keep_none : bool
            If True, sends None values as empty strings, allows overwrite of data currently in AirTable.
            If False, removes dictionary items that have None values.

        NOTE: Does not currently process AT data type 'attachements' (not yet part of
        function _process_records_for_at)

        Returns
        -------
        list[dict]
            Records updated in AT, given in AT format.

        Raises
        ------
        RuntimeError
            If the status code returned is not a 200, RuntimeError is raised and status code and error text is returned.
        """
        base = base if base else self._base
        process = self._process if process is None else process
        data = (
            self._process_records_for_at(data, keep_none=keep_none) if process else data
        )
        if type(data) == dict:
            data = data["records"]
        n = 10  # current limit per request
        chunks = [data[i : i + n] for i in range(0, len(data), n)]
        records = []
        for chunk in chunks:
            r = self._session.patch(
                f"{self._at_url}/{base}/{table}",
                headers=self.__headers,
                data=self.jsonify({"records": chunk}),
            )
            airtable_response = r.json()
            if r.status_code != 200:
                if "error" in airtable_response:
                    self._identify_errors(r)
                raise RuntimeError(f"Airtable did not return 200: {r.text}")
            records.extend(airtable_response["records"])
        return records

    def delete(
        self, table: str, list_of_ids: list[str], base: Optional[str] = None
    ) -> list:
        """
        This deletes records! forever! do not use lightly!
        Unlike other AT methods, this requires a list of AT record IDs passed as list_of_ids.
        Example: ['reca4gE7sJj682lui', 'recVO0FE7QQkkeD28', ...]
        Data will be chunked into batches of 10 for AT.

        Parameters
        ----------
        table : str
            AirTable base ID.
        list_of_ids : list[str]
            List of AirTable record IDs to be deleted. FOREVER.
        base : str, optional
            The AirTable base that contains the table you're pulling.
            This is optional because it may be defined on the client level.

        Returns
        -------
        None

        Raises
        ------
        RuntimeError
            If the status code returned is not a 200, RuntimeError is raised and status code and error text is returned.
        """
        base = base if base else self._base
        n = 10
        chunks = [list_of_ids[i : i + n] for i in range(0, len(list_of_ids), n)]
        records = []
        for chunk in chunks:
            r = self._session.delete(
                f"{self._at_url}/{base}/{table}",
                headers=self.__headers,
                params={"records[]": chunk},
            )
            if r.status_code != 200:
                self._identify_errors(r)
                raise RuntimeError(f"Airtable did not return 200: {r.text}")
            records.extend(r.json()["records"])
        return records

    def match_at_rec_id(
        self, data_wo_id: list[dict], data_w_id: list[dict], keys: list[str]
    ) -> tuple[list[dict], list[dict], list[dict]]:
        """
        Adds AT record ID to 'data_wo_id' based on AT record ID found in
        'data_w_id' by matching fields listed in 'keys'.
        Both sets of records need to be in formats matching those returned after processing.

        Parameters
        ----------
        data_wo_id : list[dict]
            AirTable record data without the AT ID attached.
        data_w_id : list[dict]
            AirTable record data with the AT ID attached.
        keys : list[str]
            List of keys to be used to match fields in the two sets of AirTable data.

        Returns
        -------
        Tuple[list[dict], list[dict], list[dict]]
            Tuple containing the following:
            updated: all records for which an AT record ID was added.
            missing_id: record UID field combinations for which no AT record ID was found.
            not_updated: remaining records for which no AT record ID could be added.

        Raises
        ------
        RuntimeError
            If the status code returned is not a 200, RuntimeError is raised and status code and error text is returned.
        """
        updated = data_wo_id.copy()
        missing_id = []  # collect UIDs without AT rec ID
        not_updated = []  # collect data w/o AT rec ID.
        for rec in updated:
            # get matching record that contains AT rec ID:
            rec_id_list = list(
                filter(lambda d: all(d[key] == rec[key] for key in keys), data_w_id)
            )
            if rec_id_list:
                if len(rec_id_list) > 1:
                    raise RuntimeError(
                        f"Using UID {keys} led to duplicate records found: {rec_id_list}"
                    )
                rec_id = rec_id_list[0]
                # add AT rec ID to data:
                rec[self._at_rec_id] = rec_id[self._at_rec_id]
            else:
                # keep record of failing UID matching
                uid = {k: rec[k] for k in keys}
                missing_id.append(uid)
                not_updated.append(rec)
                updated.remove(rec)
        return updated, missing_id, not_updated

    class AirtableWebhooksClient(object):
        """
        Subclass with methods to interact with the AirTable webhooks API endpoints.
        NOTE: needs a webhook token, different than API key, and different than cache token.
        Visit: https://airtable.com/create/tokens to create a personal token

        Webhooks are subject to 5 requests/second per base. Will receive a 429 status code if that's exceeded.
        Test Base for development: appLTqOSBDqXceNtp
        Test functionality using webhook.site
        """

        def __init__(self, parent: "AirTableCachingClient"):
            self._parent = parent
            self.atc = AirTableCachingClient(self)
            self.webhooks = AirTableCachingClient.AirtableWebhooksClient(
                self, parent=AirTableCachingClient
            )

        def create(
            self,
            specification: json,
            base: str,
            destinationUrl=Optional[str],
        ) -> dict:
            """
            Creates a new webhook.
            Requirements:
                Authentication: PAT
                Scope: Depend on the subscribed dataTypes, details here: https://airtable.com/developers/web/api/webhooks-overview#authorization
                User role: Creator permissions or greater.
            Header Params:
                baseId: AT Base
            Request Body:
                notificationUrl, specification

            Parameters
            ----------
            specification : json
                JSON object that describes the types of changes the webhook is interested in. Example below:
                    options: webhooks spec https://airtable.com/developers/web/api/model/webhooks-specification
                    example: {
                                "options": {
                                    "filters": {
                                    "dataTypes": [
                                        "tableData"
                                    ],
                                    "recordChangeScope": "tbl00000000000000"
                                    }
                                }
                                }
                            This will watch the data in a specified table.
            base : str
                The AirTable base you want to create the webhook for.
            destinationUrl : str, optional
                destination URL that can receive notification pings. This may be a Cloud Function URL.

            Returns
            -------
            dict
                Dictionary containing expirationTime, id, and macSecretBase64 for the created webhook.

            Raises
            ------
            RuntimeError
                If the status code returned is not a 200, RuntimeError is raised and status code and error text is returned.
            """
            # do stuff here
            url = f"https://api.airtable.com/v0/bases/{base}/webhooks"
            headers = self._parent.__webhook_headers
            if destinationUrl:
                notificationUrl = destinationUrl
            else:
                notificationUrl = ""  # TODO: explicitly define default URL for non-specified destinations. Can/should this work like a dead-letter?
            payload = {
                "notificationUrl": notificationUrl,
                "specification": specification,
            }
            response = []
            r = self._parent._session.post(url, headers=headers, data=payload)
            if r.status_code != 200:
                self._parent._identify_errors(r)
                raise RuntimeError(
                    f"Airtable did not return 200({r.status_code}): {r.text}"
                )
            json_object = r.json()
            response.extend(json_object)

            return response

        def delete(self, base: str, webhookId: str) -> None:
            """
            Deletes an existing webhook.
            Requirements:
                Authentication:
                Scope: Depend on the subscribed dataTypes, details here: https://airtable.com/developers/web/api/webhooks-overview#authorization
                User role: Creator permissions or greater.
            Header Params:
                baseId: str AT base
                webhookId: str

            Parameters
            ----------
            base : str
                AirTable base ID where the webhook you're deleting lives.
            webhookId : str
                The ID of the webhook to be deleted.

            Returns
            -------
            None
                Empty response on success.

            Raises
            ------
            RuntimeError
                If the status code returned is not a 200, RuntimeError is raised and status code and error text is returned.
            """
            url = f"https://api.airtable.com/v0/bases/{base}/webhooks/{webhookId}"
            headers = self._parent.__webhook_headers
            r = self._parent._session.delete(url, headers=headers)
            if r.status_code != 200:
                self._parent._identify_errors(r)
                raise RuntimeError(
                    f"Airtable did not return 200({r.status_code}): {r.text}"
                )
            else:
                return  # should I log this?

        def list_webhooks(self, base: str) -> json:
            """
            List all webhooks that are registered for a base, along with their statuses.
            Requirements:
                Authentication: PAT
                Scope: Depend on the subscribed dataTypes, details here: https://airtable.com/developers/web/api/webhooks-overview#authorization
                User role: Read level permissions or higher are required.
            Header Params:
                baseId: AT Base

            Parameters
            ----------
            base : str
                AirTable base ID.

            Returns
            -------
            JSON
                array of webhook objects containing:
                id: str - webhookId
                areNotificationsEnabled: bool - are they?
                cursorForNextPayload: num - cursor associated w next payload. increases everytime a new payload is generated for this webhook
                isHookEnabled: bool - is it?
                lastSuccessfulNotificationTime: str | null - identifier for created webhook
                notificationUrl: str | null - url registered w the webhook
                expirationTime: optional<str> - time when webhook expires. ISO format
                lastNotificationResult: Webhooks notification | null - yep
                specification: obj - the specification registered. options webhooks specifications

            Raises
            ------
            RuntimeError
                If the status code returned is not a 200, RuntimeError is raised and status code and error text is returned.
            """
            url = f"https://api.airtable.com/v0/bases/{base}/webhooks"
            headers = self._parent.__webhook_headers
            r = self._parent._session.get(url, headers=headers)
            if r.status_code != 200:
                self._parent._identify_errors(r)
                raise RuntimeError(
                    f"Airtable did not return 200({r.status_code}): {r.text}"
                )
            else:
                return r.content  # or r.text? or r.json??

        def return_payloads(
            self, base: str, webhookId: str, cursor: Optional[int], limit: Optional[int]
        ) -> json:
            """
            Enumerate the update messages for a client to consume. Clients should call this after they receive a ping.
            The webhook payload format can be found here and uses V2 cell value format.
            Calling this endpoint will also extend the life of the webhook if it is active with an expiration time. The new expiration time will be 7 days after the list payloads call
            Requirements:
                Authentication:
                Scope: Depend on the subscribed dataTypes, details here: https://airtable.com/developers/web/api/webhooks-overview#authorization
                User role: Read level permissions or higher are required.
            Header Params:
                baseId: AT Base
                webhookId: str
            Query Params: cursor, limit
                limit: If given, the limit specifies the max number of payloads to return. Hard cap is 50 payloads / request.

            Parameters
            ----------
            base : str
                AirTable base ID
            webhookId : str
                The ID of the webhook to return payloads of.
            cursor : int, optional
                First time this endpoint is hit, cursor may be omitted and will default to 1. After that, cursors should be saved between invocations.
                When a client receives a ping, it should use the last cursor that this action returned when polling for new payloads, no matter how
                old that cursor value is. NOTE: Cursor argument indicates the transaction number of the payload to start listing from.
            limit : int, optional
                If given, the limit specifies the max number of payloads to return. Hard cap is 50 payloads / request.

            Returns
            -------
            JSON
                JSON object of payloads. Example: https://airtable.com/developers/web/api/list-webhook-payloads
                contains cursor #, bool of mightHaveMore, and payloads - the array of webhooks payloads.

            Raises
            ------
            RuntimeError
                If the status code returned is not a 200, RuntimeError is raised and status code and error text is returned.
            """
            url = f"https://api.airtable.com/v0/bases/{base}/webhooks/{webhookId}/payloads"
            headers = self._parent.__webhook_headers
            payload = {"cursor": cursor, "limit": limit}
            r = self._parent._session.get(url, headers=headers, payload=payload)
            if r.status_code != 200:
                self._parent._identify_errors(r)
                raise RuntimeError(
                    f"Airtable did not return 200({r.status_code}): {r.text}"
                )
            else:
                return (
                    r.content
                )  # returns cursor #, bool of mightHaveMore, and payloads - the array of webhooks payloads
                # TODO: check if r.text or r.json works better than content

        def refresh(self, base: str, webhookId: str) -> dict:
            """
            Webhooks will expire and be disabled after 7 days from creation. Webhook life can be extended while it's still active by calling the
            refresh webhook or list webhook payload. After a webhook has expired and been disabled, the webhook's metadata and past payloads will
            be available for an additional 7 days.

            Extend the life of a webhook. The new expiration time will be 7 days after the refresh time.
            Note that this endpoint only applies to active webhooks with an expiration time.
            Requirements:
                Authentication:
                Scope: Depend on the subscribed dataTypes, details here: https://airtable.com/developers/web/api/webhooks-overview#authorization
                User role: base creator?

            Parameters
            ----------
            base : str
                AirTable base ID where the webhook to be refresh resides.
            webhookId : str
                The ID of the webhook to refresh

            Returns
            -------
            dict
                New expiration time for the given webhook.
                Example: {"expirationTime" : "2023-01-30T00:00:00.000Z"}

            Raises
            ------
            RuntimeError
                If the status code returned is not a 200, RuntimeError is raised and status code and error text is returned.
            """
            url = (
                f"https://api.airtable.com/v0/bases/{base}/webhooks/{webhookId}/refresh"
            )
            headers = self._parent.__webhook_headers
            # params
            r = self._parent._session.post(url, headers=headers)
            if r.status_code != 200:
                self._parent._identify_errors(r)
                raise RuntimeError(
                    f"Airtable did not return 200({r.status_code}): {r.text}"
                )
            else:
                return r.content

        def toggle_notifications(self, base: str, webhookId: str, notifications: bool):
            """
            Enables or disables notification pings for a webhook. See https://airtable.com/developers/web/api/webhooks-overview#webhook-notification-delivery
            Requirements:
                Authentication:
                Scope: Depend on the subscribed dataTypes, details here: https://airtable.com/developers/web/api/webhooks-overview#authorization
                User role: base creator?
            Request Body: enable

            Parameters
            ----------
            base : str
                AirTable base ID where the webhook resides.
            webhookId : str
                The ID of the webhook to toggle notifications of.
            notifications : bool
                If True, enable notifications.
                If False, disable notifications.

            Returns
            -------
            None
                Empty response on success

            Raises
            ------
            RuntimeError
                If the status code returned is not a 200, RuntimeError is raised and status code and error text is returned.
            """
            url = f"https://api.airtable.com/v0/bases/{base}/webhooks/{webhookId}/enableNotifications"
            headers = self._parent.__webhook_headers
            payload = {"enable": notifications}
            r = self._parent._session.post(url, headers=headers, payload=payload)
            if r.status_code != 200:  # remove this?
                self._parent._identify_errors(r)
                raise RuntimeError(
                    f"Airtable did not return 200({r.status_code}): {r.text}"
                )
            else:
                return  # empty response on success


if __name__ == "__example__":
    # Assume service account key, and AirTable PAT are stored

    # Declare project_id and Airtable base you will be testing the webhook on
    project_id = ""
    test_base = ""
    # _at_apikey = "[INSERT AIRTABLE API KEY HERE]"
    # pat = "[INSERT AIRTABLE PERSONAL ACCESS TOKEN HERE]"
    # destUrl = "[INSERT DESTINATION URL HERE (USE PERSONAL LINK FROM webhook.site)]"

    # Initialize clients make sure your secrets are stored correctly
    # smc = SecretManagerClient(logging.getLogger(), project_id)
    # atc = AirTableCachingClient(apikey=_at_apikey, logger=Logger, process=True, personal_token=pat, base=test_base))
    # atc.webhooks = AirTableCachingClient.AirtableWebhooksClient(parent=AirTableCachingClient)

    # Decalare specifications for what the webhook is watching for
    # This is pre-populated for the first table in the test base given above
    specs = {
        "options": {
            "filters": {
                "dataTypes": ["tableData"],
                "recordChangeScope": "tblP81fS3Bj6vgcIt",
            }
        }
    }

    # # Create a new webhook
    # atc.webhooks.create(specification=specs, base=test_base, destinationUrl=destUrl)

    # # Ensure the webhook was created
    # webhook_list = atc.AirtableWebhooksClient.list_webhooks(self=AirTableCachingClient, base=test_base)

    # test_hook_id = webhook_list[0]["id"]

    # # At this point you can alter data in the Airtable to test if it registers changes properly
    # # Now ensure the webhook is properly detecting and listing changes
    # payload = atc.webhooks.return_payloads(base=test_base, webhookId=test_hook_id, cursor= 1, limit=1)
    # # payload should contain a JSON object

    # # Test the refresh - should return a dictionary of the new expirationTime for the webhook
    # atc.webhooks.refresh(base=test_base, webhookId=test_hook_id)

    # # Finally, delete the webhook - this will return an empty response on success
    # atc.webhooks.delete(base=test_base, webhookId=test_hook_id)
