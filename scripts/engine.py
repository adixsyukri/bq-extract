import sys
import os
from log import sys_exit
import logging
logger = logging.getLogger(__name__)
from google.cloud import bigquery, storage
import pyarrow
from pyarrow.parquet import ParquetDataset
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from pathlib import PurePath, Path

bqclient = bigquery.Client()

class Engine:

    def __init__(self, config, settings):
        # Interface class instantiate
        self.config = config
        self.settings = settings

    def get_data(self, table, query):
        # Interface to get data
        raise NotImplementedError

    def write_output(self, table, data):
        # Interface to write output
        raise NotImplementedError

class LocalEngine(Engine):

    def get_data(self, table, query):
        try:
            dataframe = (
            bqclient.query(query)
            .result()
            .to_dataframe(
                # Optionally, explicitly request to use the BigQuery Storage API. As of
                # google-cloud-bigquery version 1.26.0 and above, the BigQuery Storage
                # API is used by default.
                create_bqstorage_client=True,
                )
            )
        except Exception:
            logger.error("bqclient exception: ", exc_info=True)
            sys_exit()

        return dataframe

    def write_output(self, table, data):
        output_file = '{source}_{table}_{rundate}'.format(
            source = self.config['source']['name'].upper(), 
            table = table['name'].upper(), 
            rundate = self.settings['rundate'])

        final_path = self.settings['folder'].joinpath(output_file)
        rows = len(data.index)
        self.to_output(data, final_path)
        
        summary_path = self.settings['folder'].joinpath(self.settings['summary'])
        with open(summary_path, 'a') as summmaryfile:
            summmaryfile.write('{filename}|{count}\n'.format(filename=output_file, count=rows))
        
        logger.info("Filename: {}; Count: {}".format(output_file, rows))

    def to_output(self, data, final_path):
        if self.config['output']['format'] == 'txt':
            output = data.to_csv(sep=config['separator'], header=True, quotechar='"', line_terminator='\n', index=False)
            with codecs.open('{0}.txt'.format(final_path), "w", "utf-8-sig") as writefile:
                writefile.writelines(output)
        
        elif self.config['output']['format'] == 'parquet':
            return data.to_parquet(path='{0}.parquet'.format(final_path), engine='auto', compression='snappy', index=None)
        
        else:
            logger.error("{} is not recognized".format(self.config['output']['format']))
            sys_exit()

class DataflowEngine(Engine):

    def get_data(self, table, query):

        if self.config['output']['format'] != 'parquet':
            logger.error("Only supporting parquet format")
            sys_exit()

        output_file = table['output_file']

        parquet_schema = self.get_parquet_schema(table)
        options = PipelineOptions([
            '--project', self.config['source']['project'],
            '--staging_location', 'gs://{0}/{1}'.format(self.config['output']['bucket'], 'staging'),
            '--temp_location', 'gs://{0}/{1}'.format(self.config['output']['bucket'], 'temp'),
            '--region', 'asia-southeast1',
            '--runner', 'DataflowRunner'
        ])
    
        # instantiate a pipeline with all the pipeline option
        p = beam.Pipeline(options=options)
        # processing and structure of pipeline
        p \
        | 'Input: QueryTable' >> beam.io.gcp.bigquery.ReadFromBigQuery(
            query=query,
            use_standard_sql=True) \
        | 'Output: Export to Parquet' >> beam.io.parquetio.WriteToParquet(
            file_path_prefix='gs://{0}/{1}/{2}/{3}/'.format(
                self.config['output']['bucket'],
                self.config['output']['destination'],
                self.settings['output_folder'],
                output_file),
            schema=parquet_schema,
            file_name_suffix='.parquet'
        )
    
        result = p.run()
        result.wait_until_finish()  # Makes job to display all the logs

    def write_output(self, table, data):
        storage_client = storage.Client()
        prefix = '{0}/{1}/{2}'.format(
            self.config['output']['destination'],
            self.settings['output_folder'],
            table['output_file'])

        blobs = storage_client.list_blobs(self.config['output']['bucket'], prefix=prefix)   
        fullpath = '{0}.parquet'.format(
            PurePath(
                self.config['output']['datadir'], 
                self.settings['output_folder'],
                table['output_file'])
        )
        for blob in blobs:
            blob.download_to_filename(fullpath)

        nrows = 0
        dataset = ParquetDataset(fullpath)
        for piece in dataset.pieces:
            nrows += piece.get_metadata().num_rows

        summary_path = self.settings['folder'].joinpath(self.settings['summary'])
        with open(summary_path, 'a') as summmaryfile:
            summmaryfile.write('{filename}|{count}\n'.format(filename=table['output_file'], count=nrows))
        
        logger.info("Filename: {}; Count: {}".format(table['output_file'], nrows))

    def get_parquet_schema(self, table):
        parquet_schema = pyarrow.schema([])
        header = table['header'] + ['load_date']
        for f in header:
            parquet_schema = parquet_schema.append(pyarrow.field(f, pyarrow.string()))
        return parquet_schema

def get_engine(config, settings):
    source = config['output']['engine']
    if source == 'dataflow':
        # instantiate dataflow engine
        engine = DataflowEngine(config, settings)
    elif source == 'local':
        # instantiate local engine
        engine = LocalEngine(config, settings)
    return engine
