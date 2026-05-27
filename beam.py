# Apache Beam core library for building data pipelines
import apache_beam as beam
# PipelineOptions holds pipeline config like runner, project, region
from apache_beam.options.pipeline_options import PipelineOptions
# argparse reads command-line arguments like --input
import argparse
# re is used for regular expressions to remove special characters
import re


def clean_row(row):
    # Remove any trailing colon ':' from the end of the row
    row = row.rstrip(':')
    # Remove all non-ASCII characters (emojis, special symbols) using regex
    row = re.sub(r'[^\x00-\x7F]+', '', row)
    # Return the cleaned row
    return row


def to_json_delivered(row):
    # Split the CSV row string by comma into a list of fields
    fields = row.split(',')
    # Map each field by index to its column name and return as a dict
    return {
        'customer_id': fields[0],   # Column 0: customer ID
        'date': fields[1],          # Column 1: order date
        'timestamp': fields[2],     # Column 2: order time
        'order_id': fields[3],      # Column 3: unique order ID
        'items': fields[4],         # Column 4: items ordered
        'amount': fields[5],        # Column 5: transaction amount
        'mode': fields[6],          # Column 6: payment mode
        'restaurant': fields[7],    # Column 7: restaurant name
        'status': fields[8],        # Column 8: order status
        'ratings': fields[9],       # Column 9: customer rating
        'feedback': fields[10]      # Column 10: customer feedback
    }


def to_json_other(row):
    # Other status orders have the same schema, so reuse to_json_delivered
    return to_json_delivered(row)


def run():
    # Create argument parser to read command-line inputs
    parser = argparse.ArgumentParser()
    # Register --input as a required argument (GCS path to CSV file)
    parser.add_argument('--input', required=True)
    # known_args holds --input, pipeline_args holds Beam/Dataflow args like --runner, --project
    known_args, pipeline_args = parser.parse_known_args()

    # Wrap pipeline_args into PipelineOptions for Beam to use
    options = PipelineOptions(pipeline_args)

    # Create and start the Beam pipeline, 'p' is the pipeline object
    with beam.Pipeline(options=options) as p:

        # Step 1: Read CSV from GCS and clean each row
        cleaned = (
            p
            # Read the input file line by line from GCS, skip the header row
            | 'Read' >> beam.io.ReadFromText(known_args.input, skip_header_lines=1)
            # Apply clean_row function to every row in the PCollection
            | 'Clean' >> beam.Map(clean_row)
        )

        # Step 2: Filter rows where status column (index 8) equals 'delivered'
        delivered = cleaned | 'FilterDelivered' >> beam.Filter(
            lambda r: r.split(',')[8].strip().lower() == 'delivered'
        )

        # Step 3: Filter rows where status column (index 8) is NOT 'delivered'
        other = cleaned | 'FilterOther' >> beam.Filter(
            lambda r: r.split(',')[8].strip().lower() != 'delivered'
        )

        # Step 4: Count total number of rows in the cleaned PCollection
        total = cleaned | 'CountTotal' >> beam.combiners.Count.Globally()
        # Count number of delivered order rows
        del_count = delivered | 'CountDelivered' >> beam.combiners.Count.Globally()
        # Count number of other status order rows
        other_count = other | 'CountOther' >> beam.combiners.Count.Globally()

        # Step 5: Print the counts to console/logs
        total | 'PrintTotal' >> beam.Map(lambda c: print(f'Total: {c}'))
        del_count | 'PrintDelivered' >> beam.Map(lambda c: print(f'Delivered: {c}'))
        other_count | 'PrintOther' >> beam.Map(lambda c: print(f'Other: {c}'))

        # Step 6: Convert delivered rows to JSON dicts and write to BigQuery
        delivered | 'ToJsonDelivered' >> beam.Map(to_json_delivered) | 'WriteDelivered' >> beam.io.WriteToBigQuery(
            # Target BigQuery table: project:dataset.table
            'your-project:your_dataset.delivered_orders',
            # Append new rows to existing data instead of overwriting
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            # Create the table automatically if it does not exist
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
        )

        # Step 7: Convert other status rows to JSON dicts and write to BigQuery
        other | 'ToJsonOther' >> beam.Map(to_json_other) | 'WriteOther' >> beam.io.WriteToBigQuery(
            # Target BigQuery table for non-delivered orders
            'your-project:your_dataset.other_status_orders',
            # Append new rows to existing data instead of overwriting
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            # Create the table automatically if it does not exist
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
        )


# Entry point: only runs when script is executed directly, not when imported
if __name__ == '__main__':
    run()
