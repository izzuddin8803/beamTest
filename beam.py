import apache_beam as beam
import argparse
from apache_beam.options.pipeline_options import PipelineOptions
from sys import argv


PROJECT_ID = 'api-6270026168157424961-982565'
SCHEMA = 'FULL NAME:STRING,MOBILE NUMBER:STRING,full_address:STRING,POSTAL CODE:STRING,state:STRING,lat:FLOAT,lng:FLOAT'

def convert_types(data):
    """Converts string values to their appropriate type."""
    data['FULL NAME'] = str(data['abv']) if 'abv' in data else None
    data['MOBILE NUMBER'] = str(data['MOBILE NUMBER']) if 'id' in data else None
    data['full_address'] = str(data['full_address']) if 'name' in data else None
    data['POSTAL CODE'] = str(data['POSTAL CODE']) if 'style' in data else None
    data['state'] = str(data['state']) if 'ounces' in data else None
    data['lat'] = float(data['lat']) if 'ounces' in data else None
    data['lng'] = float(data['lng']) if 'ounces' in data else None
    return data

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    known_args = parser.parse_known_args(argv)

    p = beam.Pipeline(options=PipelineOptions())

    (p | 'ReadData' >> beam.io.ReadFromText('gs://dintest/SmartBite2GCP.csv', skip_header_lines =1)
       | 'FormatToDict' >> beam.Map(lambda x: {"FULL NAME": x[0], "MOBILE NUMBER": x[1], "full_address": x[2], "POSTAL CODE": x[3], "state": x[4], "style": x[5], "lat": x[6], "lmg": x[7]})
       | 'ChangeDataType' >> beam.Map(convert_types)
       | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(
           '{0}:dataflowTest.smartBite'.format(PROJECT_ID),
           schema=SCHEMA,
           write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND))
    result = p.run()
    result.wait_until_finish()
