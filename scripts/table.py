import logging
logger = logging.getLogger(__name__)

class TableModel:

    def __init__(self, config, tableinfo, engine, settings):
        self.config = config
        self.info = tableinfo
        self.engine = engine
        self.settings = settings
        self.info['output_file'] = '{source}_{table}_{rundate}'.format(
            source = config['source']['name'].upper(), 
            table = tableinfo['name'].upper(), 
            rundate = settings['rundate'])

    def compose_query(self):
        query = '''
            select
            {0},
            {3}
            from
            `{4}.{1}.{2}`
        '''.format(
            self.info['columns_query'], 
            self.config['source']['dataset'], 
            self.info['name'], 
            self.settings['load_date'],
            self.config['source']['project'])

        if self.config['source']['extract_type'] != 'full':
            if self.config['source']['date_type'] == 'equal':
                operator = '='
            elif self.config['source']['date_type'] == 'less_than':
                operator = '<'
            elif self.config['source']['date_type'] == 'bigger_equal':
                operator = '>='

            query = query + ' where {0} {2} "{1}" '.format(self.info['date_column'], self.settings['date'], operator)

        if self.config['environment'] == 'dev':
            query = query + ' limit 100 '
        
        return query

    def get_data(self, query):
        data = self.engine.get_data(self.info, query)
        return data

    def write_output(self, data):
        self.engine.write_output(self.info, data)