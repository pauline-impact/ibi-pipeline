import apache_beam as beam
import argparse
from apache_beam.options.pipeline_options import PipelineOptions
from sys import argv

PROJECT_ID = 'prod-itools'
SCHEMA = 'id:INTEGER,namekey:STRING'

def discard_incomplete(data):
    """Filters out records that don't have an information."""
    return len(data['id']) > 0 and len(data['namekey']) > 0 and len(data['namekey']) > 0 and len(data['status']) > 0


def convert_types(data):
    """Converts string values to their appropriate type."""
    data['id'] = int(data['id']) if 'id' in data else None
    data['namekey'] = str(data['namekey']) if 'namekey' in data else None
    """data['status'] = str(data['status']) if 'status' in data else None"""
    """data['r_categoryparent_id'] = int(data['r_categoryparent_id']) if 'r_categoryparent_id' in data else None"""
    """data['dlu'] = timestamp(data['dlu']) if 'dlu' in data else None"""
    """data['show_in_listing'] = bool(data['show_in_listing']) if 'show_in_listing' in data else None"""
    """data['display_name'] = str(data['display_name']) if 'display_name' in data else None"""
    """data['ulu'] = str(data['ulu']) if 'ulu' in data else None"""
    """data['doe'] = timestamp(data['doe']) if 'doe' in data else None"""
    """data['uoe'] = str(data['uoe']) if 'uoe' in data else None"""
    return data

def del_unwanted_cols(data):
    """Delete the unwanted columns"""
    """del data['doe']"""
    """del data['uoe']"""
    return data

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    known_args = parser.parse_known_args(argv)

    p = beam.Pipeline(options=PipelineOptions())

    (p | 'ReadData' >> beam.io.ReadFromText('gs://prod-itools-pipeline/batch/r_report.csv', skip_header_lines =1)
       | 'SplitData' >> beam.Map(lambda x: x.split(','))
       | 'FormatToDict' >> beam.Map(lambda x: {"id": x[0], "namekey": x[1]}) 
       | 'DeleteIncompleteData' >> beam.Filter(discard_incomplete)
       | 'ChangeDataType' >> beam.Map(convert_types)
       | 'DeleteUnwantedData' >> beam.Map(del_unwanted_cols)
       | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(
           '{0}:Internal_BI.r_report_test'.format(PROJECT_ID),
           schema=SCHEMA,
           write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND))
    result = p.run()
    result.wait_until_finish()
