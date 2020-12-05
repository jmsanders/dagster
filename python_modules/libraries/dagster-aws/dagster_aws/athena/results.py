import dateutil


class AthenaResults:
    def __init__(self, client, execution_id):
        self.client = client
        self.execution_id = execution_id

    def fetchall(self):
        results = []

        paginator = self.client.get_paginator("get_query_results")
        for page in paginator.paginate(QueryExecutionId=self.execution_id):
            columns = page["ResultSet"]["ResultSetMetadata"]["ColumnInfo"]

            column_names = [column["Name"] for column in columns]
            column_types = [column["Type"] for column in columns]
            for row in page["ResultSet"]["Rows"]:
                values = [column.get("VarCharValue") for column in row["Data"]]

                if values == column_names:
                    continue

                results.append(self._parse_row(values, column_types))

        return results

    def _parse_page(self, page):
        results = []
        columns = page["ResultSet"]["ResultSetMetadata"]["ColumnInfo"]

        column_names = [column["Name"] for column in columns]
        column_types = [column["Type"] for column in columns]
        for row in page["ResultSet"]["Rows"]:
            values = [column.get("VarCharValue") for column in row["Data"]]

            if values == column_names:
                continue

            results.append(self._parse_row(values, column_types))
        return results

    def _parse_row(self, row, column_types):
        parsed = []
        for i, value in enumerate(row):
            column_type = column_types[i]
            if not value:
                parsed.append(None)
            elif column_type in ["boolean"]:
                parsed.append(value == "true")
            elif column_type in ["tinyint", "smallint", "int", "integer", "bigint"]:
                parsed.append(int(value))
            elif column_type in ["double", "real", "decimal"]:
                parsed.append(float(value))
            elif column_type in ["char", "varchar", "string"]:
                parsed.append(value)
            elif (column_type) in ["date"]:
                parsed.append(dateutil.parser.parse(value).date())
            elif (column_type) in ["timestamp"]:
                parsed.append(dateutil.parser.parse(value))
            else:
                parsed.append(value)
        return tuple(parsed)
