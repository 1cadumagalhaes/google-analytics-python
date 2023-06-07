from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient.discovery import build
import httplib2
from datetime import datetime, date, timedelta

import csv

import logging


class GoogleAnalyticsUtils:
    def __init__(
        self,
        service_account=None,
        logger=None,
    ):
        """
        Utility class for working with the Google Analytics API using Python.

        Args:
            service_account (str): Path to the service account JSON file.
            logger (Logger): Logger instance for logging messages (default: None).
        """
        self.service = self._get_service(service_account=service_account)

        if logger is None:
            self.logger = logging
        else:
            self.logger = logger

    def _get_service(self, service_account):
        """
        Creates and returns an authorized Google Analytics Reporting API V4 service object.

        Args:
            service_account (str): Path to the service account JSON file.

        Returns:
            An authorized Google Analytics Reporting API V4 service object.
        """
        scopes = ["https://www.googleapis.com/auth/analytics.readonly"]

        credentials = ServiceAccountCredentials.from_json_keyfile_name(
            service_account,
            scopes,
        )
        http = credentials.authorize(httplib2.Http())
        service = build(
            "analyticsreporting",
            "v4",
            http=http,
            discoveryServiceUrl=(
                "https://analyticsreporting.googleapis.com/$discovery/rest"
            ),
        )
        return service

    def get_report(
        self,
        view_id,
        report_date: date,
        dimensions: list = [{"name": "ga:country"}],
        metrics: list = [{"expression": "ga:sessions"}],
        sampling: str = "LARGE",
    ):
        """
        Queries the Google Analytics Reporting API V4 and retrieves a report.

        Args:
            view_id (str): View ID of the Google Analytics property.
            report_date (date): Date for which to retrieve the report.
            dimensions (list, optional): List of dimension objects (default: [{"name": "ga:country"}]).
            metrics (list, optional): List of metric objects (default: [{"expression": "ga:sessions"}]).
            sampling (str, optional): Sampling level for the report (default: "LARGE").

        Returns:
            dict: The retrieved report from the Google Analytics Reporting API V4.
        """
        report_date = report_date.strftime("%Y-%m-%d")
        dimensions = [{"name": dimension} for dimension in dimensions]
        metrics = [{"expression": metric} for metric in metrics]
        self.logger.info(f"[GAUtils.get_report] getting report for {report_date}")
        return (
            self.service.reports()
            .batchGet(
                body={
                    "reportRequests": [
                        {
                            "viewId": view_id,
                            "dateRanges": [
                                {"startDate": report_date, "endDate": report_date}
                            ],
                            "metrics": metrics,
                            "dimensions": dimensions,
                            "samplingLevel": sampling,
                            "pageSize": 100000,
                        }
                    ]
                }
            )
            .execute()
        )

    def parse_report(self, report, view_id):
        """
        Parses the retrieved report from the Google Analytics Reporting API V4.

        Args:
            report (dict): The retrieved report from the Google Analytics Reporting API V4.
            view_id (str): View ID of the Google Analytics property.

        Returns:
            list: Parsed report content.
        """
        report = report.get("reports", [])[0]
        self.logger.info(f"[GAUtils.get_report] parsing report")

        columnHeader = report.get("columnHeader", {})
        dimensionHeaders = columnHeader.get("dimensions", [])
        metricHeaders = columnHeader.get("metricHeader", {}).get(
            "metricHeaderEntries", []
        )

        dimensionHeaders = [
            dimension.replace("ga:", "") for dimension in dimensionHeaders
        ]
        metricHeaders = [
            metric.get("name").replace("ga:", "") for metric in metricHeaders
        ]
        headers = dimensionHeaders + metricHeaders + ["view_id"]

        content = [headers]
        rows = report.get("data", {}).get("rows", [])
        for row in rows:
            row = (
                row.get("dimensions")
                + row.get("metrics", [])[0].get("values")
                + [view_id]
            )
            content.append(row)

        return content

    def list_to_csv_file(
        self, filename: str, content: list, delimiter=",", extraction_date=None
    ):
        """
        Writes the content to a CSV file.

        Args:
            filename (str): Name of the output CSV file.
            content (list): Content to be written to the CSV file.
            delimiter (str, optional): Delimiter used in the CSV file (default: ",").
            extraction_date (str, optional): Extraction date to be added as a prefix in the CSV file (default: None).

        Returns:
            str: Name of the created CSV file.
        """
        try:
            self.logger.info(f"[GAUtils.download] saving report to {filename}")

            file = open(filename, "w", newline="")
            writer = csv.writer(file, delimiter=delimiter, quoting=csv.QUOTE_ALL)
            if extraction_date is None:
                writer.writerows(content)
            else:
                for line in content:
                    if line == content[0]:
                        newline = ["Extraction Date"] + line
                    else:
                        newline = [extraction_date] + line
                    writer.writerow(newline)
            file.close()
        except Exception as e:
            self.logger.error("[GAUtil.list_to_csv] Error creating temp csv file", e)
        return filename

    def get_batch_report(
        self,
        view_id,
        schema: list,
        report_name: str,
        start_date: date,
        end_date: date,
        dimensions: list = [{"name": "ga:country"}],
        metrics: list = [{"expression": "ga:sessions"}],
        sampling: str = "LARGE",
    ):
        """
        Retrieves a batch report for a specified date range.

        Args:
            view_id (str): View ID of the Google Analytics property.
            schema (list): List of schema objects.
            report_name (str): Name of the report.
            start_date (date): Start date of the report range.
            end_date (date): End date of the report range.
            dimensions (list, optional): List of dimension objects (default: [{"name": "ga:country"}]).
            metrics (list, optional): List of metric objects (default: [{"expression": "ga:sessions"}]).
            sampling (str, optional): Sampling level for the report (default: "LARGE").
        """
        current_date = start_date
        while end_date >= current_date:
            formated_date = current_date.strftime("%Y%m%d")
            self.logger.debug(
                f"Downloading report for view {view_id} at {formated_date}"
            )

            report = self.get_report(
                view_id=view_id,
                report_date=current_date,
                dimensions=dimensions,
                metrics=metrics,
                sampling=sampling,
            )
            report = self.parse_report(report, view_id)
            file = self.list_to_csv_file(
                filename=f"./files/{report_name}_{formated_date}.csv", content=report
            )
            current_date += timedelta(days=1)
