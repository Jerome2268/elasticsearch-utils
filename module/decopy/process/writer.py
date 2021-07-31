import csv
import logging

from module.decopy.utils.Constants import *
from module.decopy.utils.pseudonymize import pseudonymize

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class FileWriter:
    def __init__(self, parser):
        self.parser = parser
        self.pseudonymized_field = sa_pseudonymized_field

    def set_pseudonymized_field(self, pseudonymized_field):
        self.pseudonymized_field = pseudonymized_field

    @staticmethod
    def create_dir(path):
        is_existed = os.path.exists(path)
        if not is_existed:
            try:
                os.makedirs(path)
            except Exception:
                logger.error("Fail to create directory : %s, will exit the program" % path)
                exit(0)
        else:
            return True

    @staticmethod
    def align_to_sa_all(source):
        lack_schemas = sa_index_total_schema ^ source.keys()
        for schema in lack_schemas:
            source.__setitem__(schema, empty)
        return source

    def encrypt(self, source, pseudonymize):
        for field in self.pseudonymized_field:
            source.__setitem__(field, pseudonymize(source[field]))
        return source

    def write_to_csv(self, response, file, mode, sa_query=False):
        if len(response) < 1:
            return 0
        data = list(map(lambda message: message["_source"], response))
        if self.parser.get_pseudonymize():
            data = list(map(lambda source: self.encrypt(source, pseudonymize), data))
        if sa_query & usecase_all.__eq__(self.parser.get_alert_type()):
            data = list(map(lambda source: self.align_to_sa_all(source), data))
        csv_file = open(file, mode, newline='')
        writer = csv.writer(csv_file, delimiter=delimiter, quoting=csv.QUOTE_ALL)
        end = map(lambda message: list(message.values()), data)
        writer.writerow(data[0].keys())
        writer.writerows(end)
        csv_file.close()
        return 1
